package com.github.feng111627.webhookingestdispatchjava.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * 规范化后的事件。payload 使用 JsonNode 而不是强类型 DTO，
 * 上游新增字段时不会破坏反序列化，保证前向兼容。
 */
public class WebhookEvent {

    private String eventId;
    private String eventType;
    private Instant occurredAt;
    private JsonNode payload;

    public WebhookEvent() {
    }

    public WebhookEvent(String eventId, String eventType, Instant occurredAt, JsonNode payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.occurredAt = occurredAt;
        this.payload = payload;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
}
