package com.github.feng111627.webhookingestdispatchjava.handler;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 按事件类型索引处理逻辑。启动时收集容器内全部 {@link EventHandler}。
 */
@Component
public class EventHandlerRegistry {

    private final Map<String, EventHandler> handlers;

    public EventHandlerRegistry(List<EventHandler> handlerBeans) {
        this.handlers = handlerBeans.stream()
                .collect(Collectors.toUnmodifiableMap(
                        EventHandler::supportedType,
                        Function.identity(),
                        (a, b) -> {
                            throw new IllegalStateException("重复的事件类型处理逻辑: " + a.supportedType());
                        }));
    }

    public Optional<EventHandler> find(String eventType) {
        return Optional.ofNullable(handlers.get(eventType));
    }
}
