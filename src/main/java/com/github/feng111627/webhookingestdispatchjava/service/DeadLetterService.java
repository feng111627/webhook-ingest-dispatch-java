package com.github.feng111627.webhookingestdispatchjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.webhookingestdispatchjava.dto.DeadEventView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.exception.RejectReason;
import com.github.feng111627.webhookingestdispatchjava.exception.WebhookRejectedException;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.model.FailureKind;
import com.github.feng111627.webhookingestdispatchjava.model.ReplayAttempt;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.model.TracePhase;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.store.TraceStore;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * 死信查询与重放（含人工修正负载）。
 *
 * <p>规则：
 * <ul>
 *   <li>只列出 state=DEAD 的事件，含失败性质、尝试次数、最后错误，判定可解释；</li>
 *   <li>重放请求体为空：沿用<b>原始负载</b>；请求体为 JSON 对象：作为本次<b>修正负载</b>，
 *       仅作用于本次分发，原始负载永不被覆盖；</li>
 *   <li>修正负载必须是 JSON 对象；其中 eventId/eventType 若给出必须与原事件一致
 *       （防止借重放串改事件身份或路由到别的处理器），缺省则自动补成原值；</li>
 *   <li>并发/重复点击：在记录锁内做 DEAD -&gt; PROCESSING 原子抢占，
 *       只有一个请求真正执行，其余一律 409，结论一致、不重复分发、不重复副作用；</li>
 *   <li>每次重放都留档（replayId、负载来源、修正负载、结论、失败性质、次数、最后错误）；</li>
 *   <li>重放复用同一分发链路（含退避重试）；成功移去死信，仍失败则保留 DEAD 并更新可解释原因。</li>
 * </ul>
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final EventStore eventStore;
    private final TraceStore traceStore;
    private final DispatchService dispatchService;
    private final ObjectMapper objectMapper;

    public DeadLetterService(EventStore eventStore, TraceStore traceStore,
                             DispatchService dispatchService, ObjectMapper objectMapper) {
        this.eventStore = eventStore;
        this.traceStore = traceStore;
        this.dispatchService = dispatchService;
        this.objectMapper = objectMapper;
    }

    public List<DeadEventView> listDead() {
        List<DeadEventView> views = eventStore.findDead().stream()
                .map(r -> new DeadEventView(
                        r.getEventId(),
                        r.getEventType(),
                        r.getFailureKind().name(),
                        r.getAttempts(),
                        r.getLastError(),
                        r.getReceivedAt(),
                        r.getUpdatedAt()))
                .toList();
        log.info("[deadletter] 查询死信，共 {} 条: {}", views.size(),
                views.stream().map(DeadEventView::eventId).toList());
        return views;
    }

    /** 无修正负载重放（向后兼容）。 */
    public ReplayResponse replay(String eventId) {
        return replay(eventId, null);
    }

    /**
     * 重放。
     *
     * @param correctedRawBody 为空表示沿用原始负载；非空时必须是 JSON 对象，作为本次修正负载
     */
    public ReplayResponse replay(String eventId, String correctedRawBody) {
        // 0. 先解析并校验修正负载（在抢占之前，非法负载直接 400，不改变事件状态）
        JsonNode corrected = parseAndValidateCorrection(eventId, correctedRawBody);
        String payloadSource = corrected != null ? "CORRECTED" : "ORIGINAL";

        EventRecord record = eventStore.find(eventId).orElse(null);
        if (record == null) {
            log.warn("[deadletter eventId={}] 重放被拒绝：事件不存在 -> 404", eventId);
            appendRejected(eventId, "事件不存在 -> 404", "HTTP_404");
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在: " + eventId);
        }
        if (record.getState() != EventState.DEAD) {
            log.warn("[deadletter eventId={}] 重放被拒绝：当前状态 {} 不是 DEAD -> 409",
                    eventId, record.getState());
            appendRejected(eventId, "当前状态 " + record.getState() + " 不是 DEAD -> 409", "HTTP_409");
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "事件当前状态为 " + record.getState() + "，只有 DEAD 状态允许重放");
        }

        // 1. 原子抢占 DEAD -> PROCESSING；同一事件的并发重放只有一个能赢
        String replayId = "rp-" + UUID.randomUUID();
        Instant submittedAt = Instant.now();
        boolean[] claimed = {false};
        int[] replaySeq = {0};
        eventStore.update(eventId, r -> {
            if (r.getState() != EventState.DEAD) {
                return r;
            }
            r.setState(EventState.PROCESSING);
            r.setFailureKind(FailureKind.NONE);
            r.setLastError("replay " + replayId + " started");
            r.setUpdatedAt(Instant.now());
            replaySeq[0] = r.getReplayHistory().size() + 1;
            r.getReplayHistory().add(new ReplayAttempt(
                    replayId, replaySeq[0], payloadSource, corrected,
                    submittedAt, null, EventState.PROCESSING.name(),
                    FailureKind.NONE.name(), 0, "replay started"));
            claimed[0] = true;
            return r;
        });
        if (!claimed[0]) {
            // 权威结论以锁内判定为准：抢占失败说明已有重放 PROCESSING（或已被并发请求改成终态）
            EventState nowState = eventStore.find(eventId).map(EventRecord::getState)
                    .orElse(EventState.PROCESSING);
            String msg = "该事件已有重放在执行（当前状态 " + nowState + "），同一时刻只允许一个重放";
            log.warn("[deadletter eventId={}] 重放被拒绝：抢占失败({}) -> 409", eventId, nowState);
            appendRejected(eventId, "并发抢占失败：" + msg + " -> 409", "HTTP_409");
            throw new ResponseStatusException(HttpStatus.CONFLICT, msg);
        }

        log.info("[deadletter eventId={}] 重放开始 replayId={} seq={} payloadSource={} corrected={}",
                eventId, replayId, replaySeq[0], payloadSource,
                corrected != null ? "yes" : "no(原始负载)");
        traceStore.append(TraceEntry.builder(eventId, TracePhase.REPLAY_REQUESTED)
                .payloadSource(payloadSource).replayId(replayId)
                .decision("重放请求抢占成功 DEAD->PROCESSING，第" + replaySeq[0]
                        + "次重放，本次使用" + ("CORRECTED".equals(payloadSource) ? "人工修正负载" : "原始负载"))
                .apiConclusion("REPLAY_RUNNING"));

        // 2. 本次实际使用的负载：修正负载只作用于本次分发，绝不回写覆盖 record.originalPayload
        EventRecord fresh = eventStore.find(eventId).orElseThrow();
        JsonNode effectivePayload = corrected != null ? corrected : fresh.getPayload();
        EventRecord finished = dispatchService.redispatch(fresh, effectivePayload, payloadSource, replayId);

        // 3. 把本次重放结论写回历史（按 replayId 定位替换），无论成功失败都可查
        Instant finishedAt = Instant.now();
        eventStore.update(eventId, r -> {
            replaceReplayAttempt(r, replayId, ra -> new ReplayAttempt(
                    ra.replayId(), ra.seq(), ra.payloadSource(), ra.correctedPayload(),
                    ra.submittedAt(), finishedAt, finished.getState().name(),
                    finished.getFailureKind().name(), finished.getAttempts(), finished.getLastError()));
            return r;
        });

        if (finished.getState() == EventState.SUCCEEDED) {
            log.info("[deadletter eventId={}] 重放成功 replayId={} payloadSource={} attempts={}，已移去死信",
                    eventId, replayId, payloadSource, finished.getAttempts());
            return new ReplayResponse(eventId, "SUCCEEDED",
                    "重放成功(replayId=" + replayId + ")，本次使用"
                            + ("CORRECTED".equals(payloadSource) ? "人工修正负载" : "原始负载")
                            + "；原始负载保留未改；副作用受幂等台账保护，无重复副作用",
                    finished.getAttempts(), 0, replayId, payloadSource);
        }
        log.warn("[deadletter eventId={}] 重放后仍失败 replayId={} failureKind={} attempts={} lastError={}",
                eventId, replayId, finished.getFailureKind(), finished.getAttempts(), finished.getLastError());
        return new ReplayResponse(eventId, "DEAD",
                "重放后仍失败，继续保留在死信(replayId=" + replayId + "，failureKind="
                        + finished.getFailureKind() + "，attempts=" + finished.getAttempts()
                        + ")：" + finished.getLastError(),
                finished.getAttempts(), 0, replayId, payloadSource);
    }

    /**
     * 解析并校验修正负载。
     *
     * @return 修正后的负载节点；rawBody 为空时返回 null（表示沿用原始负载）
     */
    private JsonNode parseAndValidateCorrection(String eventId, String rawBody) {
        if (rawBody == null || rawBody.isBlank()) {
            return null;
        }
        final JsonNode node;
        try {
            node = objectMapper.readTree(rawBody);
        } catch (Exception ex) {
            throw badCorrection(eventId, "修正负载不是合法 JSON: " + ex.getMessage());
        }
        if (node == null || !node.isObject()) {
            throw badCorrection(eventId, "修正负载必须是 JSON 对象");
        }
        // 以原事件为基准补齐/校验身份字段，防止借重放串改 eventId 或把事件路由到别的处理器
        EventRecord original = eventStore.find(eventId).orElse(null);
        com.fasterxml.jackson.databind.node.ObjectNode merged;
        try {
            merged = ((com.fasterxml.jackson.databind.node.ObjectNode) node).deepCopy();
        } catch (ClassCastException ex) {
            throw badCorrection(eventId, "修正负载必须是 JSON 对象");
        }
        if (node.hasNonNull("eventId") && !node.get("eventId").asText().equals(eventId)) {
            throw badCorrection(eventId,
                    "修正负载的 eventId(" + node.get("eventId").asText() + ")必须与路径 eventId(" + eventId + ")一致");
        }
        if (original != null && node.hasNonNull("eventType")
                && !node.get("eventType").asText().equalsIgnoreCase(original.getEventType())) {
            throw badCorrection(eventId,
                    "修正负载的 eventType(" + node.get("eventType").asText()
                            + ")必须与原事件类型(" + original.getEventType() + ")一致");
        }
        if (original != null && !node.hasNonNull("eventType")) {
            merged.put("eventType", original.getEventType());
        }
        if (!node.hasNonNull("eventId")) {
            merged.put("eventId", eventId);
        }
        return merged;
    }

    private WebhookRejectedException badCorrection(String eventId, String message) {
        log.warn("[deadletter eventId={}] 重放被拒绝：修正负载不合法 -> 400: {}", eventId, message);
        appendRejected(eventId, "修正负载不合法：" + message + " -> 400", "BAD_REQUEST");
        return new WebhookRejectedException(RejectReason.BAD_REQUEST, message, HttpStatus.BAD_REQUEST.value());
    }

    private void appendRejected(String eventId, String decision, String apiConclusion) {
        traceStore.append(TraceEntry.builder(eventId, TracePhase.REPLAY_REJECTED)
                .decision(decision)
                .apiConclusion(apiConclusion));
    }

    private static void replaceReplayAttempt(EventRecord r, String replayId,
                                             java.util.function.UnaryOperator<ReplayAttempt> mapper) {
        List<ReplayAttempt> history = r.getReplayHistory();
        for (int i = 0; i < history.size(); i++) {
            ReplayAttempt ra = history.get(i);
            if (ra.replayId().equals(replayId)) {
                history.set(i, mapper.apply(ra));
                return;
            }
        }
    }
}
