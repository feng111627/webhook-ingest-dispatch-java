package com.github.feng111627.webhookingestdispatchjava.handler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 事件类型 -> 处理器路由表。
 *
 * <p>启动时把所有 {@link EventHandler} Bean 按 {@link EventHandler#eventType()} 建索引；
 * 运行时查不到处理器的事件，由入口明确拒绝（UNSUPPORTED_EVENT_TYPE），禁止静默丢弃。
 */
@Component
public class EventHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventHandlerRegistry.class);

    private final Map<String, EventHandler> byType;

    public EventHandlerRegistry(List<EventHandler> handlers) {
        this.byType = handlers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        h -> h.eventType().toLowerCase(),
                        Function.identity(),
                        (a, b) -> {
                            throw new IllegalStateException("事件类型存在重复处理器: " + a.eventType());
                        }));
        log.info("已注册事件处理器: {}", byType.keySet());
    }

    public Optional<EventHandler> lookup(String eventType) {
        if (eventType == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(byType.get(eventType.toLowerCase()));
    }
}
