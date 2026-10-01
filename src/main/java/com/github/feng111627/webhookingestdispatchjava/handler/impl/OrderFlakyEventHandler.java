package com.github.feng111627.webhookingestdispatchjava.handler.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandler;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * order.flaky 处理器：用于本地演示"暂时失败重试 / 永久失败直入死信 / 重放成功"。
 *
 * <p>由负载字段 {@code fail} 控制（缺省视为成功）：
 * <ul>
 *   <li>{@code "retryable"}：始终暂时失败 —— 退避重试到上限后进入死信，重放仍可成功；</li>
 *   <li>{@code "permanent"}：永久失败 —— 首次即判 PERMANENT，不再重试，直接死信；</li>
 *   <li>{@code "retry-then-ok"}：前 {@code failUntil}（默认 2）次尝试暂时失败，之后成功；</li>
 *   <li>其它/缺省：直接成功。</li>
 * </ul>
 */
@Component
public class OrderFlakyEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderFlakyEventHandler.class);

    private final EffectLedger effectLedger;

    /** 每个事件在本处理器内已经历的尝试次数（跨重试、跨重放累积，成功后清除） */
    private final ConcurrentHashMap<String, Integer> flakyAttempts = new ConcurrentHashMap<>();

    public OrderFlakyEventHandler(EffectLedger effectLedger) {
        this.effectLedger = effectLedger;
    }

    @Override
    public String eventType() {
        return "order.flaky";
    }

    @Override
    public String effectKeyPrefix() {
        return "order-flaky";
    }

    @Override
    public HandlerResult handle(String eventId, JsonNode payload) {
        String mode = payload != null && payload.hasNonNull("fail")
                ? payload.get("fail").asText() : "ok";

        // 永久失败：不做副作用，由分发器立即置死信
        if ("permanent".equalsIgnoreCase(mode)) {
            log.warn("[handler order.flaky eventId={}] 判定 PERMANENT：业务规则拒绝(payload={})，不再重试",
                    eventId, shortPayload(payload));
            return new HandlerResult(HandlerResult.Outcome.PERMANENT, "permanent business rejection");
        }

        // 幂等短路：副作用此前已完成（例如重放命中），直接成功且绝不重复执行
        String effectKey = effectKeyPrefix() + ":" + eventId;
        if (effectLedger.exists(effectKey)) {
            log.info("[handler order.flaky eventId={}] 判定 SUCCESS：副作用台账已存在，幂等跳过", eventId);
            return new HandlerResult(HandlerResult.Outcome.SUCCESS, "effect already applied; idempotent skip");
        }

        if ("retry-then-ok".equalsIgnoreCase(mode)) {
            int threshold = payload.hasNonNull("failUntil") ? payload.get("failUntil").asInt(2) : 2;
            int seen = flakyAttempts.merge(eventId, 1, Integer::sum);
            if (seen <= threshold) {
                log.warn("[handler order.flaky eventId={}] 判定 RETRYABLE：第{}次暂时失败(阈值{})",
                        eventId, seen, threshold);
                return new HandlerResult(HandlerResult.Outcome.RETRYABLE,
                        "temporary failure " + seen + "/" + threshold);
            }
            flakyAttempts.remove(eventId);
            log.info("[handler order.flaky eventId={}] 暂时失败已恢复，第{}次尝试将提交副作用", seen);
            // 落到下面的成功分支
        } else if ("retryable".equalsIgnoreCase(mode)) {
            int seen = flakyAttempts.merge(eventId, 1, Integer::sum);
            log.warn("[handler order.flaky eventId={}] 判定 RETRYABLE：模拟下游不可用(第{}次)", eventId, seen);
            return new HandlerResult(HandlerResult.Outcome.RETRYABLE,
                    "simulated downstream unavailable (attempt " + seen + ")");
        }

        effectLedger.recordIfAbsent(effectKey);
        log.info("[handler order.flaky eventId={}] 执行副作用并判定 SUCCESS", eventId);
        return new HandlerResult(HandlerResult.Outcome.SUCCESS, "flaky handler succeeded");
    }

    private static String shortPayload(JsonNode payload) {
        String s = payload == null ? "" : payload.toString();
        return s.length() <= 80 ? s : s.substring(0, 80) + "...";
    }
}
