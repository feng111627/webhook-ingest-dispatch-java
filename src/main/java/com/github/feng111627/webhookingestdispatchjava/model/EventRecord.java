package com.github.feng111627.webhookingestdispatchjava.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
    /**
     * 本次/最近一次重放实际使用的负载；null 表示使用原始负载。
     * 原始负载 payload 永不被改写，重放结束后此字段保留以便查询"这次重放用的是哪份负载"。
     */
    private JsonNode replayPayload;
    /** 历次重放历史（只追加），含每次实际使用的负载来源与修正负载原文 */
    private List<ReplayRecord> replays = new ArrayList<>();

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

    public JsonNode getReplayPayload() { return replayPayload; }
    public void setReplayPayload(JsonNode replayPayload) { this.replayPayload = replayPayload; }

    public List<ReplayRecord> getReplays() { return replays; }
    public void setReplays(List<ReplayRecord> replays) { this.replays = replays; }

    /** 本次分发实际应使用的负载：重放携带修正负载时优先，否则原始负载 */
    public JsonNode effectivePayload() {
        return replayPayload != null ? replayPayload : payload;
    }
}
