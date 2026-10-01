package com.github.feng111627.webhookingestdispatchjava.dto;

import java.time.Instant;

/** 死信列表中的一条记录 */
public record DeadEventView(
        String eventId,
        String eventType,
        String failureKind,
        int attempts,
        String lastError,
        Instant receivedAt,
        Instant updatedAt) {
}
