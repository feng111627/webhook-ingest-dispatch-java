package com.github.feng111627.webhookingestdispatchjava.handler;

import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;

/**
 * 按事件类型实现的处理逻辑。
 * 实现类应保证同一 eventId 重复执行不会产生重复副作用
 * （可继承 {@link AbstractIdempotentEventHandler}）。
 */
public interface EventHandler {

    /** 该处理逻辑负责的事件类型。 */
    String supportedType();

    /** 处理事件，返回成功 / 暂时失败 / 永久失败结论。 */
    HandlerResult handle(WebhookEvent event);
}
