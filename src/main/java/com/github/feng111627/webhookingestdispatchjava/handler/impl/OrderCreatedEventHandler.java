package com.github.feng111627.webhookingestdispatchjava.handler.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandler;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * order.created 处理器。
 *
 * <p>设计要点：
 * <ul>
 *   <li>只读取已知字段（orderId/amountCents），负载里后续新增的字段一律忽略 —— 新增字段不破坏现有逻辑；</li>
 *   <li>真实副作用（此处以台账登记模拟"发货单创建"）以稳定 key 幂等，重放不会重复执行。</li>
 * </ul>
 */
@Component
public class OrderCreatedEventHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedEventHandler.class);

    private final EffectLedger effectLedger;

    public OrderCreatedEventHandler(EffectLedger effectLedger) {
        this.effectLedger = effectLedger;
    }

    @Override
    public String eventType() {
        return "order.created";
    }

    @Override
    public String effectKeyPrefix() {
        return "order-created";
    }

    @Override
    public HandlerResult handle(String eventId, JsonNode payload) {
        // 只读已知字段；未知字段天然被忽略（前向兼容）
        String orderId = payload != null && payload.hasNonNull("orderId")
                ? payload.get("orderId").asText() : "(unknown-order)";
        long amountCents = payload != null && payload.hasNonNull("amountCents")
                ? payload.get("amountCents").asLong() : 0L;

        String effectKey = effectKeyPrefix() + ":" + eventId;
        if (!effectLedger.recordIfAbsent(effectKey)) {
            log.info("[handler order.created eventId={}] 判定 SUCCESS：副作用此前已完成，幂等跳过(orderId={})",
                    eventId, orderId);
            return new HandlerResult(HandlerResult.Outcome.SUCCESS,
                    "effect already applied; idempotent skip");
        }

        // —— 真实副作用发生点（本地以台账登记模拟，例如：创建发货单） ——
        log.info("[handler order.created eventId={}] 执行副作用：创建发货单(orderId={}, amountCents={})",
                eventId, orderId, amountCents);
        return new HandlerResult(HandlerResult.Outcome.SUCCESS, "shipment created");
    }
}
