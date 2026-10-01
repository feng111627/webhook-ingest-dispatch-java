package com.github.feng111627.webhookingestdispatchjava.handler;

import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;

/**
 * 某一种事件类型的处理逻辑。
 *
 * <p>实现要求：只根据本逻辑关心的字段做判断；负载里后续新增的字段不得破坏现有实现。
 * 副作用通过 effectKey 做幂等保护，重放不会产生重复副作用。
 */
public interface EventHandler {

    /** 支持的事件类型 */
    String eventType();

    /** 幂等副作用标识前缀 */
    String effectKeyPrefix();

    /**
     * 处理事件。
     *
     * @param eventId 事件标识，用于日志与幂等
     * @param payload 事件负载（读取已知字段，忽略未知字段）
     */
    HandlerResult handle(String eventId, com.fasterxml.jackson.databind.JsonNode payload);
}
