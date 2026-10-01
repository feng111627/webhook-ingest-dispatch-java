package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 死信重放结果。
 */
public record ReplayResult(String eventId, DecisionCode decision, String reason) {
}
