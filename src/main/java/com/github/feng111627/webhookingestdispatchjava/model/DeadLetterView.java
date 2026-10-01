package com.github.feng111627.webhookingestdispatchjava.model;

import java.time.Instant;

/**
 * 死信查询视图。
 */
public record DeadLetterView(String eventId,
                             String eventType,
                             DecisionCode reason,
                             int attempts,
                             Instant deadAt,
                             String detail) {
}
