package com.github.feng111627.webhookingestdispatchjava.service;

import com.github.feng111627.webhookingestdispatchjava.dedup.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.dedup.ProcessedEventRepository;
import com.github.feng111627.webhookingestdispatchjava.dlq.DeadLetterRecord;
import com.github.feng111627.webhookingestdispatchjava.dlq.DeadLetterRepository;
import com.github.feng111627.webhookingestdispatchjava.dispatch.EventDispatcher;
import com.github.feng111627.webhookingestdispatchjava.model.DecisionCode;
import com.github.feng111627.webhookingestdispatchjava.model.DeadLetterView;
import com.github.feng111627.webhookingestdispatchjava.model.EventStatus;
import com.github.feng111627.webhookingestdispatchjava.model.ReplayResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 死信查询与重放入口。
 *
 * 重放幂等：
 * - CAS 抢占 replayInFlight，重放进行中的重复请求返回 REPLAY_SKIPPED_DUPLICATE；
 * - 已成功重放（事件不再在死信中）同样返回 REPLAY_SKIPPED_DUPLICATE，不再执行；
 * - 处理逻辑自身幂等，即使被重复投递也只产生一次副作用。
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final DeadLetterRepository deadLetterRepository;
    private final ProcessedEventRepository eventRepository;
    private final EventDispatcher dispatcher;
    private final ObjectMapper objectMapper;

    public DeadLetterService(DeadLetterRepository deadLetterRepository,
                             ProcessedEventRepository eventRepository,
                             EventDispatcher dispatcher,
                             ObjectMapper objectMapper) {
        this.deadLetterRepository = deadLetterRepository;
        this.eventRepository = eventRepository;
        this.dispatcher = dispatcher;
        this.objectMapper = objectMapper;
    }

    public List<DeadLetterView> list() {
        return deadLetterRepository.findAll().stream()
                .map(r -> new DeadLetterView(
                        r.getEvent().getEventId(),
                        r.getEvent().getEventType(),
                        r.getReason(),
                        r.getAttempts(),
                        r.getDeadAt(),
                        r.getDetail()))
                .toList();
    }

    public ReplayResult replay(String eventId) {
        return replay(eventId, null);
    }

    /**
     * 重放死信。
     *
     * @param updatedRawBody 可选的更新后负载（合法 JSON 对象）。用于上游修复问题后携带新增字段重放；
     *                       eventType 不允许借重放改变，避免把事件路由到别的处理逻辑。
     *                       为 null/空时沿用死信中保存的原始负载。
     */
    public ReplayResult replay(String eventId, String updatedRawBody) {
        DeadLetterRecord dead = deadLetterRepository.find(eventId).orElse(null);
        if (dead == null) {
            // 不存在死信：可能从未失败，也可能已经重放成功移出。
            EventRecord record = eventRepository.find(eventId).orElse(null);
            if (record != null && (record.getStatus() == EventStatus.REPLAYED
                    || record.getStatus() == EventStatus.SUCCEEDED)) {
                log.info("[eventId={}] 判定=REPLAY_SKIPPED_DUPLICATE 依据=事件已成功处理，不重复重放", eventId);
                return new ReplayResult(eventId, DecisionCode.REPLAY_SKIPPED_DUPLICATE,
                        "event already succeeded; replay skipped to avoid duplicate side effects");
            }
            log.info("[eventId={}] 判定=NOT_FOUND 依据=死信中不存在该事件", eventId);
            return new ReplayResult(eventId, DecisionCode.NOT_FOUND, "dead letter not found");
        }

        EventRecord record = eventRepository.find(eventId).orElseThrow();
        if (record.isReplayInFlight()) {
            log.info("[eventId={}] 判定=REPLAY_SKIPPED_DUPLICATE 依据=已有重放进行中", eventId);
            return new ReplayResult(eventId, DecisionCode.REPLAY_SKIPPED_DUPLICATE,
                    "a replay is already in progress");
        }
        // CAS 抢占：只有一个重放请求进入真正分发。
        if (!record.compareAndSetReplayInFlight(false, true)) {
            log.info("[eventId={}] 判定=REPLAY_SKIPPED_DUPLICATE 依据=并发抢占重放失败", eventId);
            return new ReplayResult(eventId, DecisionCode.REPLAY_SKIPPED_DUPLICATE,
                    "a concurrent replay already claimed this event");
        }

        if (updatedRawBody != null && !updatedRawBody.isBlank()) {
            try {
                applyUpdatedPayload(record, dead, updatedRawBody);
            } catch (IllegalArgumentException e) {
                record.clearReplayInFlight();
                log.info("[eventId={}] 判定=REPLAY_FAILED 依据=重放负载不合法: {}", eventId, e.getMessage());
                return new ReplayResult(eventId, DecisionCode.REPLAY_FAILED, "invalid replay payload: " + e.getMessage());
            }
        }

        DecisionCode code = dispatcher.replay(record);
        return switch (code) {
            case REPLAY_SUCCEEDED -> new ReplayResult(eventId, code, record.getReason());
            case RETRYING -> new ReplayResult(eventId, DecisionCode.RETRYING,
                    "replay transiently failed, backoff retry scheduled: " + record.getReason());
            // 暂时失败重试耗尽 / 永久失败 / 不可路由
            default -> new ReplayResult(eventId, DecisionCode.REPLAY_FAILED,
                    code + ": " + record.getReason());
        };
    }

    /**
     * 用重放请求携带的新负载覆盖死信事件负载。保持 eventId/eventType 不变，
     * 只更新数据负载，新增字段天然被 JsonNode 接纳（前向兼容）。
     */
    private void applyUpdatedPayload(EventRecord record, DeadLetterRecord dead, String rawBody) {
        try {
            com.fasterxml.jackson.databind.JsonNode root = objectMapper.readTree(rawBody);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("replay payload must be a JSON object");
            }
            com.fasterxml.jackson.databind.JsonNode typeNode = root.get("eventType");
            if (typeNode != null && !typeNode.asText().equals(record.getEvent().getEventType())) {
                throw new IllegalArgumentException(
                        "eventType cannot be changed during replay: keeping "
                                + record.getEvent().getEventType());
            }
            record.getEvent().setPayload(root);
            log.info("[eventId={}] 重放已应用更新负载，新增字段被JsonNode原样接纳",
                    record.getEvent().getEventId());
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("replay payload is not valid JSON");
        }
    }
}
