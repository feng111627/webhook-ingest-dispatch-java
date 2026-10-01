package com.github.feng111627.webhookingestdispatchjava.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * 一个被接收的事件及其处理结论（内存存储，本地演示用）。
 */
public class EventRecord {

    private String eventId;
    private String eventType;
    private JsonNode payload;
    private String rawBody;
    private EventState state = EventState.RECEIVED;
    private FailureKind failureKind = FailureKind.NONE;
    private int attempts = 0;
    private String lastError;
    private Instant receivedAt;
    private Instant updatedAt;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public JsonNode getPayload() { return payload; }
    public void setPayload(JsonNode payload) { this.payload = payload; }

    public String getRawBody() { return rawBody; }
    public void setRawBody(String rawBody) { this.rawBody = rawBody; }

    public EventState getState() { return state; }
    public void setState(EventState state) { this.state = state; }

    public FailureKind getFailureKind() { return failureKind; }
    public void setFailureKind(FailureKind failureKind) { this.failureKind = failureKind; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public Instant getReceivedAt() { return receivedAt; }
    public void setReceivedAt(Instant receivedAt) { this.receivedAt = receivedAt; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
