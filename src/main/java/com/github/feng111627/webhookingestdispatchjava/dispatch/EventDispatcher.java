package com.github.feng111627.webhookingestdispatchjava.dispatch;

import com.github.feng111627.webhookingestdispatchjava.dedup.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.dlq.DeadLetterRecord;
import com.github.feng111627.webhookingestdispatchjava.dlq.DeadLetterRepository;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandler;
import com.github.feng111627.webhookingestdispatchjava.model.DecisionCode;
import com.github.feng111627.webhookingestdispatchjava.model.EventStatus;
import com.github.feng111627.webhookingestdispatchjava.model.FailureType;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * 分发核心：路由 -> 调用处理逻辑 -> 按结论（成功/暂时失败/永久失败/不可路由）
 * 决定重试、进入死信或成功；死信重放复用同一条判定路径，保证各处结论一致、可解释。
 *
 * 重试计数：attemptNumber 从 1 开始，包含首次处理；达到 maxAttempts 仍暂时失败
 * 则以 RETRIES_EXHAUSTED 进入死信。永久失败不重试，直接以 PERMANENT_FAILURE 进入死信。
 */
@Component
public class EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(EventDispatcher.class);

    private final EventRouter router;
    private final RetryScheduler retryScheduler;
    private final DeadLetterRepository deadLetterRepository;

    public EventDispatcher(EventRouter router,
                           RetryScheduler retryScheduler,
                           DeadLetterRepository deadLetterRepository) {
        this.router = router;
        this.retryScheduler = retryScheduler;
        this.deadLetterRepository = deadLetterRepository;
    }

    /**
     * 执行一次分发尝试（首次处理与重试共用）。
     *
     * @param record        去重台账记录
     * @param attemptNumber 尝试序号（1 表示首次）
     * @return 本次尝试后的统一判定结论
     */
    public DecisionCode dispatch(EventRecord record, int attemptNumber) {
        return dispatch(record, attemptNumber, false);
    }

    /**
     * @param replay true 表示该尝试链来自死信重放，成功结论记为 REPLAY_SUCCEEDED。
     */
    private DecisionCode dispatch(EventRecord record, int attemptNumber, boolean replay) {
        WebhookEvent event = record.getEvent();
        record.setStatus(EventStatus.PROCESSING);
        record.setAttempts(attemptNumber);

        EventRouter.Route route = router.route(event.getEventId(), event.getEventType());
        if (!route.routable()) {
            // 不可路由视为永久失败：不重试，进入死信，绝不静默丢弃。
            return moveToDead(record, DecisionCode.UNROUTABLE_EVENT, attemptNumber,
                    "no handler registered for type " + event.getEventType());
        }

        EventHandler handler = route.handler();
        HandlerResult result;
        try {
            result = handler.handle(event);
        } catch (RuntimeException e) {
            // 处理逻辑抛异常按暂时失败处理，交给退避重试。
            log.warn("[eventId={}] type={} 第{}次尝试 判定=TRANSIENT 依据=处理逻辑抛出异常: {}",
                    event.getEventId(), event.getEventType(), attemptNumber, e.toString());
            result = HandlerResult.transientFailure("handler threw: " + e);
        }

        if (result.success()) {
            return markSucceeded(record, attemptNumber, result.reason(), replay);
        }

        if (result.failureType() == FailureType.PERMANENT) {
            return moveToDead(record, DecisionCode.PERMANENT_FAILURE, attemptNumber, result.reason());
        }

        // 暂时失败：还有机会则退避重试，否则进入死信。
        if (retryScheduler.hasMoreAttempts(attemptNumber)) {
            record.setDecision(DecisionCode.RETRYING);
            record.setReason(result.reason());
            retryScheduler.scheduleWithBackoff(event.getEventId(), attemptNumber,
                    () -> dispatch(record, attemptNumber + 1, replay));
            return DecisionCode.RETRYING;
        }
        return moveToDead(record, DecisionCode.RETRIES_EXHAUSTED, attemptNumber,
                result.reason() + " (tried " + attemptNumber + " times)");
    }

    /**
     * 死信重放：复用 dispatch 路径。成功后移出死信；
     * 处理逻辑自身幂等（AbstractIdempotentEventHandler），重放不产生重复副作用。
     * 调用方需先通过 record 的 CAS 抢占 replayInFlight。
     */
    public DecisionCode replay(EventRecord record) {
        WebhookEvent event = record.getEvent();
        log.info("[eventId={}] type={} 判定=REPLAY_START 依据=从死信重放，历史尝试{}次",
                event.getEventId(), event.getEventType(), record.getAttempts());
        record.setAttempts(0);
        return dispatch(record, 1, true);
    }

    private DecisionCode markSucceeded(EventRecord record, int attempt, String reason, boolean replay) {
        WebhookEvent event = record.getEvent();
        record.setStatus(replay ? EventStatus.REPLAYED : EventStatus.SUCCEEDED);
        DecisionCode code = replay ? DecisionCode.REPLAY_SUCCEEDED : DecisionCode.ACCEPTED;
        record.setDecision(code);
        record.setReason(reason);
        record.clearReplayInFlight();
        deadLetterRepository.remove(event.getEventId());
        log.info("[eventId={}] type={} 第{}次尝试 判定={} 依据=处理逻辑返回成功: {}",
                event.getEventId(), event.getEventType(), attempt, code, reason);
        completeFirstHandle(record, code);
        return code;
    }

    private DecisionCode moveToDead(EventRecord record, DecisionCode code, int attempt, String detail) {
        WebhookEvent event = record.getEvent();
        record.setStatus(EventStatus.DEAD);
        record.setDecision(code);
        record.setReason(detail);
        record.clearReplayInFlight();
        deadLetterRepository.save(new DeadLetterRecord(event, code, attempt, Instant.now(), detail));
        log.warn("[eventId={}] type={} 第{}次尝试 判定=DEAD_LETTERED/{} 依据={}",
                event.getEventId(), event.getEventType(), attempt, code, detail);
        completeFirstHandle(record, code);
        return code;
    }

    /**
     * 首次处理（含其异步重试链）落定后，唤醒等待同一 eventId 的并发重复请求。
     * RETRYING 不是终态，不完成 future；成功或进入死信才是终态。
     */
    private void completeFirstHandle(EventRecord record, DecisionCode terminal) {
        record.firstHandle().complete(terminal);
    }
}
