package com.github.feng111627.webhookingestdispatchjava.dto;

/** 死信重放的结论 */
public record ReplayResponse(String eventId, String status, String reason, int attempts, int effects) {
}
