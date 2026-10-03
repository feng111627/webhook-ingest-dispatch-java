package com.github.feng111627.webhookingestdispatchjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.webhookingestdispatchjava.config.WebhookProperties;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandler;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandlerRegistry;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.model.FailureKind;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.model.TracePhase;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.store.TraceStore;
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
 *   <li>处理器抛出未预期异常时，保守视为 RETRYABLE（基础设施类故障默认可重试）；</li>
 *   <li>重放可携带"本次实际使用的负载"（修正负载），只作用于本次分发，绝不回写覆盖原始负载。</li>
 * </ul>
 */
@Service
public class DispatchService {

    private static final Logger log = LoggerFactory.getLogger(DispatchService.class);

    private final EventHandlerRegistry registry;
    private final EventStore eventStore;
    private final TraceStore traceStore;
    private final WebhookProperties properties;

    public DispatchService(EventHandlerRegistry registry, EventStore eventStore,
                           TraceStore traceStore, WebhookProperties properties) {
        this.registry = registry;
        this.eventStore = eventStore;
        this.traceStore = traceStore;
        this.properties = properties;
    }

    /** 首次接收分发：使用事件记录中的原始负载。 */
    public EventRecord dispatch(EventRecord record) {
        return runDispatch(record, record.getPayload(), "ORIGINAL", null);
    }

    /**
     * 死信重放分发。
     *
     * <p>{@code effectivePayload} 是本次重放实际处理的负载：修正重放时为人工修正负载，
     * 普通重放时为原始负载。<b>该负载只用于本次尝试，绝不回写覆盖事件的原始负载。</b>
     * 调用方（{@link DeadLetterService}）负责把状态从 DEAD 原子抢占为 PROCESSING。
     *
     * @param record           已抢占为 PROCESSING 的死信记录
     * @param effectivePayload 本次重放实际使用的负载
     * @param payloadSource    ORIGINAL / CORRECTED
     * @param replayId         本次重放标识
     */
    public EventRecord redispatch(EventRecord record, JsonNode effectivePayload,
                                  String payloadSource, String replayId) {
        log.info("[dispatch eventId={}] 死信重放开始(replayId={}, payloadSource={})",
                record.getEventId(), replayId, payloadSource);
        return runDispatch(record, effectivePayload, payloadSource, replayId);
    }

    private EventRecord runDispatch(EventRecord record, JsonNode effectivePayload,
                                    String payloadSource, String replayId) {
        String eventId = record.getEventId();
        boolean isReplay = replayId != null;
        EventHandler handler = registry.lookup(record.getEventType())
                .orElseThrow(() -> new IllegalStateException(
                        "分发时找不到处理器(不应发生，入口已校验): " + record.getEventType()));

        markProcessing(eventId);
        int maxAttempts = Math.max(1, properties.getMaxAttempts());

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            HandlerResult result;
            try {
                log.info("[dispatch eventId={}] type={} 第 {}/{} 次尝试开始(payloadSource={}, replayId={})",
                        eventId, handler.eventType(), attempt, maxAttempts, payloadSource, replayId);
                result = handler.handle(eventId, effectivePayload);
                traceStore.append(TraceEntry.builder(eventId, TracePhase.ATTEMPT)
                        .attemptNo(attempt).maxAttempts(maxAttempts)
                        .payloadSource(payloadSource).replayId(replayId)
                        .failureKind(outcomeKind(result.outcome()))
                        .decision("第" + attempt + "/" + maxAttempts + "次尝试处理器结论=" + result.outcome()
                                + (result.reason() == null ? "" : "(" + result.reason() + ")"))
                        .apiConclusion(isReplay ? "REPLAY_RUNNING" : null));
            } catch (RuntimeException ex) {
                // 未预期异常：保守归类为可重试
                result = new HandlerResult(HandlerResult.Outcome.RETRYABLE,
                        "unexpected exception: " + ex.getClass().getSimpleName() + ": " + ex.getMessage());
                log.warn("[dispatch eventId={}] 第{}次尝试抛出异常，保守判定 RETRYABLE: {}",
                        eventId, attempt, ex.toString());
                traceStore.append(TraceEntry.builder(eventId, TracePhase.ATTEMPT)
                        .attemptNo(attempt).maxAttempts(maxAttempts)
                        .payloadSource(payloadSource).replayId(replayId)
                        .failureKind(FailureKind.RETRYABLE.name())
                        .decision("第" + attempt + "次尝试抛出未预期异常，保守判定 RETRYABLE: " + ex)
                        .apiConclusion(isReplay ? "REPLAY_RUNNING" : null));
            }

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
                r.setUpdatedAt(Instant.now());
                return r;
            });

            switch (result.outcome()) {
                case SUCCESS -> {
                    log.info("[dispatch eventId={}] 第{}次尝试 SUCCESS -> 事件完成", eventId, attempt);
                    traceStore.append(TraceEntry.builder(eventId,
                                    isReplay ? TracePhase.REPLAY_SUCCEEDED : TracePhase.SUCCEEDED)
                            .attemptNo(attempt).payloadSource(payloadSource).replayId(replayId)
                            .failureKind(FailureKind.NONE.name())
                            .decision("第" + attempt + "次尝试成功，副作用受幂等台账保护，无重复副作用")
                            .apiConclusion(isReplay ? "REPLAY_SUCCEEDED" : "SUCCEEDED"));
                    return eventStore.find(eventId).orElseThrow();
                }
                case PERMANENT -> {
                    log.warn("[dispatch eventId={}] 第{}次尝试 PERMANENT(reason={}) -> 立即进入死信，不再重试",
                            eventId, attempt, result.reason());
                    appendDeadTrace(eventId, attempt, maxAttempts, payloadSource, replayId,
                            FailureKind.PERMANENT, result.reason(), "永久失败，不重试，直接进入死信");
                    return eventStore.find(eventId).orElseThrow();
                }
                case RETRYABLE -> {
                    if (attempt >= maxAttempts) {
                        EventRecord[] holder = new EventRecord[1];
                        eventStore.update(eventId, r -> {
                            r.setState(EventState.DEAD);
                            r.setFailureKind(FailureKind.RETRYABLE);
                            r.setLastError("重试上限耗尽(" + maxAttempts + "次): " + r.getLastError());
                            r.setUpdatedAt(Instant.now());
                            holder[0] = r;
                            return r;
                        });
                        log.warn("[dispatch eventId={}] 已达最大尝试次数{} -> 进入死信(RETRYABLE)",
                                eventId, maxAttempts);
                        appendDeadTrace(eventId, attempt, maxAttempts, payloadSource, replayId,
                                FailureKind.RETRYABLE, holder[0].getLastError(),
                                "重试" + maxAttempts + "次仍失败，进入死信");
                        return eventStore.find(eventId).orElseThrow();
                    }
                    long backoff = backoffMillis(attempt);
                    log.info("[dispatch eventId={}] 第{}次尝试 RETRYABLE -> {}ms 后退避重试",
                            eventId, attempt, backoff);
                    traceStore.append(TraceEntry.builder(eventId, TracePhase.BACKOFF)
                            .attemptNo(attempt).maxAttempts(maxAttempts)
                            .payloadSource(payloadSource).replayId(replayId)
                            .failureKind(FailureKind.RETRYABLE.name())
                            .decision("第" + attempt + "次暂时失败，" + backoff + "ms 后重试(指数退避)")
                            .apiConclusion(isReplay ? "REPLAY_RUNNING" : null));
                    sleep(backoff);
                }
            }
        }
        return eventStore.find(eventId).orElseThrow();
    }

    private void appendDeadTrace(String eventId, int attempt, int maxAttempts,
                                 String payloadSource, String replayId,
                                 FailureKind kind, String lastError, String decision) {
        traceStore.append(TraceEntry.builder(eventId,
                        replayId == null ? TracePhase.DEAD : TracePhase.REPLAY_FAILED)
                .attemptNo(attempt).maxAttempts(maxAttempts)
                .payloadSource(payloadSource).replayId(replayId)
                .failureKind(kind.name())
                .decision(decision + "：failureKind=" + kind + "，attempts=" + attempt
                        + "，lastError=" + lastError)
                .apiConclusion(replayId == null ? "DEAD" : "REPLAY_FAILED"));
    }

    private static String outcomeKind(HandlerResult.Outcome outcome) {
        return switch (outcome) {
            case SUCCESS -> FailureKind.NONE.name();
            case PERMANENT -> FailureKind.PERMANENT.name();
            case RETRYABLE -> FailureKind.RETRYABLE.name();
        };
    }

    private void markProcessing(String eventId) {
        eventStore.update(eventId, r -> {
            r.setState(EventState.PROCESSING);
            r.setUpdatedAt(Instant.now());
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
