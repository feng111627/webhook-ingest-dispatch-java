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
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.trace.TraceStore;
import java.time.Instant;
import java.util.List;
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
 *   <li>只有 DEAD 状态允许重放；不存在 404，非 DEAD 409；</li>
 *   <li>重放可携带人工修正后的负载：本次分发处理修正内容，原始负载永不被覆盖，
 *       每次重放实际使用的负载来源与修正原文都写入重放历史，可随时查询核对；</li>
 *   <li>并发/重复重放：DEAD -> PROCESSING 的抢占在记录锁内原子完成，
 *       同一事件同一时刻只有一个重放真正执行，其余得到一致的 409 结论；</li>
 *   <li>重放复用同一分发链路（含退避重试），成功后从死信列表消失；
 *       仍失败则留在死信，failureKind/attempts/lastError 更新可查；</li>
 *   <li>处理器内副作用经台账幂等保护，重放不会产生重复副作用。</li>
 * </ul>
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

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

        EventRecord record = eventStore.find(eventId)
                .orElseThrow(() -> {
                    log.warn("[deadletter eventId={}] 重放被拒绝：事件不存在 -> 404", eventId);
                    trace(eventId, TraceStep.REJECTED, "EVENT_NOT_FOUND", "重放被拒绝：事件不存在");
                    return new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在: " + eventId);
                });

        if (record.getState() != EventState.DEAD) {
            log.warn("[deadletter eventId={}] 重放被拒绝：当前状态 {} 不是 DEAD -> 409",
                    eventId, record.getState());
            trace(eventId, TraceStep.REJECTED, "REPLAY_NOT_DEAD",
                    "重放被拒绝：当前状态 " + record.getState() + "，只有 DEAD 状态允许重放");
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "事件当前状态为 " + record.getState() + "，只有 DEAD 状态允许重放");
        }

        // 在记录锁内原子完成：DEAD -> PROCESSING 抢占 + 登记本次实际使用的负载 + 追加重放历史。
        // 并发/重复重放（无论是否携带修正负载）只会有一个在此抢占成功，其余拿到一致的 409。
        boolean[] claimed = {false};
        eventStore.update(eventId, r -> {
            if (r.getState() == EventState.DEAD) {
                r.setState(EventState.PROCESSING);
                r.setReplayPayload(correctedPayload);
                ReplayRecord replay = new ReplayRecord();
                replay.setStartedAt(Instant.now());
                replay.setPayloadSource(correctedPayload != null ? "CORRECTED" : "ORIGINAL");
                replay.setCorrectedPayload(correctedPayload);
                replay.setResult("IN_PROGRESS");
                replay.setAttemptsAfter(r.getAttempts());
                r.getReplays().add(replay);
                claimed[0] = true;
            }
            return r;
        });
        if (!claimed[0]) {
            log.warn("[deadletter eventId={}] 重放被拒绝：已有重放在执行 -> 409", eventId);
            trace(eventId, TraceStep.REJECTED, "REPLAY_IN_PROGRESS",
                    "重放被拒绝：该事件已有重放在执行（并发重放只会有一个真正执行）");
            throw new ResponseStatusException(HttpStatus.CONFLICT, "该事件已有重放在执行");
        }

        String payloadSource = correctedPayload != null ? "CORRECTED" : "ORIGINAL";
        log.info("[deadletter eventId={}] 重放开始(failureKind={}, 上次attempts={}, payloadSource={})",
                eventId, record.getFailureKind(), record.getAttempts(), payloadSource);
        trace(eventId, TraceStep.REPLAY_STARTED, payloadSource,
                "重放抢占成功，本次实际使用" + ("CORRECTED".equals(payloadSource) ? "人工修正负载" : "原始负载"));

        EventRecord fresh = eventStore.find(eventId).orElseThrow();
        EventRecord finished = dispatchService.redispatch(fresh);

        // 回写本次重放历史结论（只追加不改写，原始负载始终保留）
        eventStore.update(eventId, r -> {
            if (!r.getReplays().isEmpty()) {
                ReplayRecord last = r.getReplays().get(r.getReplays().size() - 1);
                last.setFinishedAt(Instant.now());
                last.setResult(finished.getState().name());
                last.setAttemptsAfter(finished.getAttempts());
                last.setNote(finished.getLastError());
            }
            return r;
        });

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
