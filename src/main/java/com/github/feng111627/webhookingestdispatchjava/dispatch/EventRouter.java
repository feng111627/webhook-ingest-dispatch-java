package com.github.feng111627.webhookingestdispatchjava.dispatch;

import com.github.feng111627.webhookingestdispatchjava.handler.EventHandler;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 事件路由：按类型找处理逻辑；找不到时明确给出 UNROUTABLE_EVENT 判定，
 * 不允许静默丢弃（由上层决定拒绝或进入死信）。
 */
@Component
public class EventRouter {

    private static final Logger log = LoggerFactory.getLogger(EventRouter.class);

    private final EventHandlerRegistry registry;

    public EventRouter(EventHandlerRegistry registry) {
        this.registry = registry;
    }

    public record Route(EventHandler handler, boolean routable) {
    }

    public Route route(String eventId, String eventType) {
        return registry.find(eventType)
                .map(handler -> {
                    log.info("[eventId={}] type={} 判定=ROUTED 依据=命中处理逻辑 {}",
                            eventId, eventType, handler.getClass().getSimpleName());
                    return new Route(handler, true);
                })
                .orElseGet(() -> {
                    log.warn("[eventId={}] type={} 判定=UNROUTABLE_EVENT 依据=没有注册该类型的处理逻辑",
                            eventId, eventType);
                    return new Route(null, false);
                });
    }
}
