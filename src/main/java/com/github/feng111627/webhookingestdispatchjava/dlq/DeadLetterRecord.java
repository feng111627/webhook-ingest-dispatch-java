package com.github.feng111627.webhookingestdispatchjava.dlq;

import com.github.feng111627.webhookingestdispatchjava.model.DecisionCode;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;

import java.time.Instant;

/**
 * 死信记录。
 */
public class DeadLetterRecord {

    private final WebhookEvent event;
    private final DecisionCode reason;
    private final int attempts;
    private final Instant deadAt;
    private final String detail;

    public DeadLetterRecord(WebhookEvent event, DecisionCode reason, int attempts, Instant deadAt, String detail) {
        this.event = event;
        this.reason = reason;
        this.attempts = attempts;
        this.deadAt = deadAt;
        this.detail = detail;
    }

    public WebhookEvent getEvent() {
        return event;
    }

    public DecisionCode getReason() {
        return reason;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getDeadAt() {
        return deadAt;
    }

    public String getDetail() {
        return detail;
    }
}
