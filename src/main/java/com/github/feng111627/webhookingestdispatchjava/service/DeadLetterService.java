package com.github.feng111627.webhookingestdispatchjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.webhookingestdispatchjava.dto.DeadEventView;
import com.github.feng111627.webhookingestdispatchjava.dto.EventDetailView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.model.ReplayRecord;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.model.TraceStep;
import com.github.feng111627.webhookingestdispatchjava.model.FailureKind;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.trace.TraceStore;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * 死信查询与重放。
 *
 * <p>规则：
 * <ul>
 *   <li>只列出 state=DEAD 的事件，含失败性质、尝试次数、最后错误，判定可解释；</li>
 *   <li>只有 DEAD 状态允许重放；不存在 404，非 DEAD 409（拒绝措辞使用锁内捕获的真实状态，
 *       与事件当时状态严格一致）；</li>
 *   <li>重放可携带人工修正后的负载：本次分发处理修正内容，原始负载永不被覆盖，
 *       每次重放实际使用的负载来源与修正原文都写入重放历史，可随时查询核对；</li>
 *   <li>并发/重复重放：每事件重放门闩以"一批并发请求"为单位互斥——同批第一个请求
 *       在记录锁内原子完成 DEAD -&gt; PROCESSING 抢占并真正执行，同批其余请求
 *       （无论是否携带修正负载）一律拿到同一个 409 + REPLAY_IN_PROGRESS 结论；
 *       整批结束后门闩复位，之后的重放按当时状态重新判定（顺序重放合法）；</li>
 *   <li>重放复用同一分发链路（含退避重试），成功后从死信列表消失；
 *       仍失败则留在死信，failureKind/attempts/lastError 更新可查；
 *       重放执行本身出现未预期异常时事件保守回到 DEAD，绝不会卡在 PROCESSING；</li>
 *   <li>处理器内副作用经台账幂等保护，重放不会产生重复副作用。</li>
 * </ul>
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    /** 并发/重复重放的统一拒绝措辞：同批所有被拒请求拿到完全一致的结论 */
    static final String REPLAY_IN_PROGRESS_MESSAGE =
            "该事件已有重放请求在执行，同一批并发重放只会有一个真正执行；本次请求被拒绝，请待其结束后重试";

    /**
     * 每事件重放门闩：以"一批并发请求"为单位互斥。
     * 同批第一个到达的请求抢占执行权后，本批其余请求（无论是否携带修正负载）
     * 一律统一拒绝；整批请求全部结束后门闩复位，之后的重放请求按当时状态重新判定。
     */
    private final ConcurrentHashMap<String, ReplayGate> replayGates = new ConcurrentHashMap<>();

    private static final class ReplayGate {
        /** 本批正在处理中的重放请求数（含将被拒绝的）；归 0 时门闩复位并移除 */
        int inFlight;
        /** 本批是否已有一个请求抢占执行权 */
        boolean executing;
    }

    private final EventStore eventStore;
    private final DispatchService dispatchService;
    private final TraceStore traceStore;

    public DeadLetterService(EventStore eventStore, DispatchService dispatchService,
                             TraceStore traceStore) {
        this.eventStore = eventStore;
        this.dispatchService = dispatchService;
        this.traceStore = traceStore;
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

    /**
     * 重放一条死信。
     *
     * @param correctedPayload 人工修正后的负载；null 表示沿用原始负载
     */
    public ReplayResponse replay(String eventId, JsonNode correctedPayload) {
        trace(eventId, TraceStep.REPLAY_REQUESTED,
                correctedPayload != null ? "CORRECTED" : "ORIGINAL",
                correctedPayload != null ? "收到携带修正负载的重放请求" : "收到重放请求（沿用原始负载）");

        // 修正负载必须是 JSON 对象；若其中带 eventId，必须与路径一致，避免张冠李戴
        if (correctedPayload != null) {
            if (!correctedPayload.isObject()) {
                trace(eventId, TraceStep.REJECTED, "REPLAY_BAD_PAYLOAD",
                        "修正负载必须是 JSON 对象");
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "修正负载必须是 JSON 对象");
            }
            JsonNode idNode = correctedPayload.get("eventId");
            if (idNode != null && idNode.isTextual() && !eventId.equals(idNode.asText())) {
                trace(eventId, TraceStep.REJECTED, "REPLAY_EVENT_ID_MISMATCH",
                        "修正负载中的 eventId(" + idNode.asText() + ")与路径(" + eventId + ")不一致");
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "修正负载中的 eventId 与要重放的事件不一致");
            }
        }

        String payloadSource = correctedPayload != null ? "CORRECTED" : "ORIGINAL";

        // 登记进本批（inFlight 增减与门闩复位都在 compute 内原子完成，
        // 保证门闩只会在整批请求全部结束后才复位/移除，不会与下一批混用）
        ReplayGate[] holder = new ReplayGate[1];
        replayGates.compute(eventId, (key, g) -> {
            ReplayGate gate = g == null ? new ReplayGate() : g;
            gate.inFlight++;
            holder[0] = gate;
            return gate;
        });
        ReplayGate gate = holder[0];
        try {
            synchronized (gate) {
                // 同批已有一个请求抢占执行权：无论它还在执行还是已快速执行完，
                // 本批其余请求一律拿到同一个明确、稳定的拒绝结论
                if (gate.executing) {
                    EventState current = eventStore.find(eventId)
                            .map(EventRecord::getState).orElse(null);
                    log.warn("[deadletter eventId={}] 重放被拒绝：同一批并发重放已有执行者"
                            + "（当前状态 {}）-> 409 REPLAY_IN_PROGRESS", eventId, current);
                    trace(eventId, TraceStep.REJECTED, "REPLAY_IN_PROGRESS",
                            "重放被拒绝：同一批并发重放已有一个请求真正执行（事件当前状态 "
                                    + current + "），本请求不再执行");
                    throw new ResponseStatusException(HttpStatus.CONFLICT, REPLAY_IN_PROGRESS_MESSAGE);
                }

                // 在记录锁内原子完成：存在性判定 + 状态判定 + DEAD -> PROCESSING 抢占
                // + 登记本次实际使用的负载 + 追加重放历史。
                // 拒绝结论一律使用锁内捕获的真实状态，措辞与事件当时状态严格一致。
                boolean[] exists = {false};
                boolean[] claimed = {false};
                EventState[] seen = new EventState[1];
                eventStore.update(eventId, r -> {
                    exists[0] = true;
                    seen[0] = r.getState();
                    if (r.getState() == EventState.DEAD) {
                        r.setState(EventState.PROCESSING);
                        r.setReplayPayload(correctedPayload);
                        ReplayRecord replay = new ReplayRecord();
                        replay.setStartedAt(Instant.now());
                        replay.setPayloadSource(payloadSource);
                        replay.setCorrectedPayload(correctedPayload);
                        replay.setResult("IN_PROGRESS");
                        replay.setAttemptsAfter(r.getAttempts());
                        r.getReplays().add(replay);
                        claimed[0] = true;
                    }
                    return r;
                });
                if (!exists[0]) {
                    log.warn("[deadletter eventId={}] 重放被拒绝：事件不存在 -> 404", eventId);
                    trace(eventId, TraceStep.REJECTED, "EVENT_NOT_FOUND", "重放被拒绝：事件不存在");
                    throw new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在: " + eventId);
                }
                if (!claimed[0]) {
                    log.warn("[deadletter eventId={}] 重放被拒绝：当前状态 {} 不是 DEAD -> 409",
                            eventId, seen[0]);
                    trace(eventId, TraceStep.REJECTED, "REPLAY_NOT_DEAD",
                            "重放被拒绝：当前状态 " + seen[0] + "，只有 DEAD 状态允许重放");
                    throw new ResponseStatusException(HttpStatus.CONFLICT,
                            "事件当前状态为 " + seen[0] + "，只有 DEAD 状态允许重放");
                }
                gate.executing = true;
            }

            log.info("[deadletter eventId={}] 重放开始(payloadSource={})", eventId, payloadSource);
            trace(eventId, TraceStep.REPLAY_STARTED, payloadSource,
                    "重放抢占成功，本次实际使用" + ("CORRECTED".equals(payloadSource) ? "人工修正负载" : "原始负载"));

            EventRecord finished;
            try {
                EventRecord fresh = eventStore.find(eventId).orElseThrow();
                finished = dispatchService.redispatch(fresh);
            } catch (RuntimeException ex) {
                // 重放执行本身出现未预期异常：保守把事件带回 DEAD 并记录原因，
                // 绝不能把事件卡在 PROCESSING（否则既不在死信列表也无法再次重放）
                log.error("[deadletter eventId={}] 重放执行异常，事件回到死信: {}", eventId, ex.toString());
                eventStore.update(eventId, r -> {
                    r.setState(EventState.DEAD);
                    r.setFailureKind(FailureKind.RETRYABLE);
                    r.setLastError("重放执行异常: " + ex.getMessage());
                    return r;
                });
                finalizeLastReplay(eventId, "DEAD", "重放执行异常: " + ex.getMessage());
                trace(eventId, TraceStep.DEAD_LETTERED, "REPLAY_ERROR",
                        "重放执行异常，事件回到死信: " + ex.getMessage());
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "重放执行异常，事件已回到死信: " + ex.getMessage());
            }

            // 回写本次重放历史结论（只追加不改写，原始负载始终保留）
            EventRecord done = finished;
            finalizeLastReplay(eventId, done.getState().name(), done.getLastError());

            if (finished.getState() == EventState.SUCCEEDED) {
                log.info("[deadletter eventId={}] 重放成功，已移去死信(attempts={}, payloadSource={})",
                        eventId, finished.getAttempts(), payloadSource);
                return new ReplayResponse(eventId, "SUCCEEDED",
                        "重放成功；副作用受幂等台账保护，无重复副作用", finished.getAttempts(), 0, payloadSource);
            }
            log.warn("[deadletter eventId={}] 重放后仍失败，留在死信(failureKind={}, attempts={}, lastError={})",
                    eventId, finished.getFailureKind(), finished.getAttempts(), finished.getLastError());
            return new ReplayResponse(eventId, "DEAD",
                    "重放后仍失败，继续保留在死信: " + finished.getLastError(),
                    finished.getAttempts(), 0, payloadSource);
        } finally {
            // 本批请求全部结束时复位门闩；之后的重放请求按当时状态重新判定（顺序重放合法）
            replayGates.compute(eventId, (key, g) -> {
                if (g == null) {
                    return null;
                }
                if (--g.inFlight == 0) {
                    g.executing = false;
                    return null;
                }
                return g;
            });
        }
    }

    /** 回写最近一次重放历史的结论（结束时间、结果、尝试次数、备注） */
    private void finalizeLastReplay(String eventId, String result, String note) {
        eventStore.update(eventId, r -> {
            if (!r.getReplays().isEmpty()) {
                ReplayRecord last = r.getReplays().get(r.getReplays().size() - 1);
                last.setFinishedAt(Instant.now());
                last.setResult(result);
                last.setAttemptsAfter(r.getAttempts());
                last.setNote(note);
            }
            return r;
        });
    }

    /** 单条事件完整视图：原始负载 + 全部重放历史（含每次实际使用的负载） */
    public EventDetailView detail(String eventId) {
        EventRecord r = eventStore.find(eventId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "事件不存在: " + eventId));
        return new EventDetailView(
                r.getEventId(),
                r.getEventType(),
                r.getState().name(),
                r.getFailureKind().name(),
                r.getAttempts(),
                r.getLastError(),
                r.getPayload(),
                List.copyOf(r.getReplays()),
                r.getReceivedAt(),
                r.getUpdatedAt());
    }

    /** 按事件标识查询完整判定轨迹 */
    public List<TraceEntry> traceOf(String eventId) {
        List<TraceEntry> entries = traceStore.findByEventId(eventId);
        if (entries.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "没有该事件的轨迹记录: " + eventId);
        }
        return entries;
    }

    private void trace(String eventId, TraceStep step, String decision, String detail) {
        traceStore.append(new TraceEntry(eventId, step, decision, detail, Instant.now()));
    }
}
