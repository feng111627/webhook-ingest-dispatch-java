package com.github.feng111627.webhookingestdispatchjava.service;

import com.github.feng111627.webhookingestdispatchjava.config.WebhookProperties;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandler;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandlerRegistry;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.model.FailureKind;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.model.TraceStep;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.trace.TraceStore;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 同步分发器（本地演示实现，结论确定性强，便于测试与解释）。
 *
 * <p>规则（与文档保持一致）：
 * <ul>
 *   <li>按事件类型从 {@link EventHandlerRegistry} 取处理器；无处理器的事件在入口已被拒绝；</li>
 *   <li>SUCCESS：置 SUCCEEDED；</li>
 *   <li>PERMANENT：立即置 DEAD（failureKind=PERMANENT），不再重试；</li>
 *   <li>RETRYABLE：按指数退避 base * 2^(n-1) 重试，直到达到 webhook.max-attempts（含首次）后置 DEAD；</li>
 *   <li>处理器抛出未预期异常时，保守视为 RETRYABLE（基础设施类故障默认可重试）。</li>
 * </ul>
 */
@Service
public class DispatchService {

    private static final Logger log = LoggerFactory.getLogger(DispatchService.class);

    private final EventHandlerRegistry registry;
    private final EventStore eventStore;
    private final WebhookProperties properties;
    private final TraceStore traceStore;

    public DispatchService(EventHandlerRegistry registry, EventStore eventStore,
                           WebhookProperties properties, TraceStore traceStore) {
        this.registry = registry;
        this.eventStore = eventStore;
        this.properties = properties;
        this.traceStore = traceStore;
    }

    /**
     * 分发一条已接收事件。
     *
     * @param record 已占位的事件记录
     * @return 分发结束后的最终记录（从存储中重新读取）
     */
    public EventRecord dispatch(EventRecord record) {
        String eventId = record.getEventId();
        EventHandler handler = registry.lookup(record.getEventType())
                .orElseThrow(() -> new IllegalStateException(
                        "分发时找不到处理器(不应发生，入口已校验): " + record.getEventType()));

        markProcessing(eventId);
        int maxAttempts = Math.max(1, properties.getMaxAttempts());

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            EventRecord current = eventStore.find(eventId).orElseThrow();
            HandlerResult result;
            try {
                log.info("[dispatch eventId={}] type={} 第 {}/{} 次尝试开始",
                        eventId, handler.eventType(), attempt, maxAttempts);
                // 重放携带修正负载时，本次分发处理的是修正后的内容；原始负载不被改写
                result = handler.handle(eventId, current.effectivePayload());
            } catch (RuntimeException ex) {
                // 未预期异常：保守归类为可重试
                result = new HandlerResult(HandlerResult.Outcome.RETRYABLE,
                        "unexpected exception: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
                log.warn("[dispatch eventId={}] 第{}次尝试抛出异常，保守判定 RETRYABLE: {}",
                        eventId, attempt, ex.toString());
            }

            trace(eventId, TraceStep.ATTEMPT, result.outcome().name(),
                    "第 " + attempt + "/" + maxAttempts + " 次尝试 -> " + result.outcome()
                            + "（依据: " + result.reason() + "）");

            int finalAttempt = attempt;
            HandlerResult finalResult = result;
            eventStore.update(eventId, r -> {
                r.setAttempts(finalAttempt);
                r.setLastError(finalResult.reason());
                r.setState(switch (finalResult.outcome()) {
                    case SUCCESS -> EventState.SUCCEEDED;
                    case PERMANENT -> EventState.DEAD;
                    case RETRYABLE -> EventState.PROCESSING;
                });
                r.setFailureKind(switch (finalResult.outcome()) {
                    case SUCCESS -> FailureKind.NONE;
                    case PERMANENT -> FailureKind.PERMANENT;
                    case RETRYABLE -> FailureKind.RETRYABLE;
                });
                return r;
            });

            switch (result.outcome()) {
                case SUCCESS -> {
                    log.info("[dispatch eventId={}] 第{}次尝试 SUCCESS -> 事件完成", eventId, attempt);
                    trace(eventId, TraceStep.SUCCEEDED, "SUCCEEDED",
                            "第 " + attempt + " 次尝试成功，事件完成");
                    return eventStore.find(eventId).orElseThrow();
                }
                case PERMANENT -> {
                    log.warn("[dispatch eventId={}] 第{}次尝试 PERMANENT(reason={}) -> 立即进入死信，不再重试",
                            eventId, attempt, result.reason());
                    trace(eventId, TraceStep.DEAD_LETTERED, "PERMANENT",
                            "永久失败（不再重试）: " + result.reason());
                    return eventStore.find(eventId).orElseThrow();
                }
                case RETRYABLE -> {
                    if (attempt >= maxAttempts) {
                        eventStore.update(eventId, r -> {
                            r.setState(EventState.DEAD);
                            r.setFailureKind(FailureKind.RETRYABLE);
                            r.setLastError("重试上限耗尽(" + maxAttempts + "次): " + r.getLastError());
                            return r;
                        });
                        log.warn("[dispatch eventId={}] 已达最大尝试次数{} -> 进入死信(RETRYABLE)",
                                eventId, maxAttempts);
                        trace(eventId, TraceStep.DEAD_LETTERED, "RETRYABLE_EXHAUSTED",
                                "重试上限耗尽(" + maxAttempts + " 次)，进入死信");
                        return eventStore.find(eventId).orElseThrow();
                    }
                    long backoff = backoffMillis(attempt);
                    log.info("[dispatch eventId={}] 第{}次尝试 RETRYABLE -> {}ms 后退避重试",
                            eventId, attempt, backoff);
                    trace(eventId, TraceStep.BACKOFF, "RETRYABLE",
                            "第 " + attempt + " 次暂时失败，" + backoff + "ms 后退避重试");
                    sleep(backoff);
                }
            }
        }
        return eventStore.find(eventId).orElseThrow();
    }

    /** 重放时从第一条记录开始重新分发（处理器侧副作用有幂等保护） */
    public EventRecord redispatch(EventRecord record) {
        log.info("[dispatch eventId={}] 死信重放开始：重置状态为 PROCESSING，负载来源={}",
                record.getEventId(), record.getReplayPayload() != null ? "CORRECTED" : "ORIGINAL");
        eventStore.update(record.getEventId(), r -> {
            r.setState(EventState.PROCESSING);
            r.setFailureKind(FailureKind.NONE);
            r.setLastError("replay started");
            return r;
        });
        return dispatch(record);
    }

    private void trace(String eventId, TraceStep step, String decision, String detail) {
        traceStore.append(new TraceEntry(eventId, step, decision, detail, Instant.now()));
    }

    private void markProcessing(String eventId) {
        eventStore.update(eventId, r -> {
            r.setState(EventState.PROCESSING);
            return r;
        });
    }

    private long backoffMillis(int failedAttempt) {
        // 第 failedAttempt 次失败后等待 base * 2^(failedAttempt-1)
        long base = Math.max(0L, properties.getBackoffBaseMillis());
        long factor = 1L << (failedAttempt - 1);
        try {
            return Math.multiplyExact(base, factor);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
