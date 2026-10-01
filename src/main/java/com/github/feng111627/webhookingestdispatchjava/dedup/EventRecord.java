package com.github.feng111627.webhookingestdispatchjava.dedup;

import com.github.feng111627.webhookingestdispatchjava.model.DecisionCode;
import com.github.feng111627.webhookingestdispatchjava.model.EventStatus;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 去重记录：一个 eventId 全局只创建一条。
 * firstHandle 用于并发去重：只有 putIfAbsent 胜出的线程负责分发，
 * 其他并发请求等待同一条记录的结论，按重复成功返回。
 */
public class EventRecord {

    private final WebhookEvent event;
    private volatile EventStatus status = EventStatus.RECEIVED;
    private volatile DecisionCode decision;
    private volatile String reason;
    private volatile int attempts;
    private volatile Instant updatedAt = Instant.now();
    private final AtomicBoolean replayInFlight = new AtomicBoolean(false);
    private final CompletableFuture<DecisionCode> firstHandle = new CompletableFuture<>();

    public EventRecord(WebhookEvent event) {
        this.event = event;
    }

    public WebhookEvent getEvent() {
        return event;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public DecisionCode getDecision() {
        return decision;
    }

    public void setDecision(DecisionCode decision) {
        this.decision = decision;
        this.updatedAt = Instant.now();
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
        this.updatedAt = Instant.now();
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public boolean isReplayInFlight() {
        return replayInFlight.get();
    }

    /** CAS 抢占重放，返回 true 表示当前调用成功获得重放权。 */
    public boolean compareAndSetReplayInFlight(boolean expect, boolean update) {
        return replayInFlight.compareAndSet(expect, update);
    }

    /** 终态落定后清除重放标记。 */
    public void clearReplayInFlight() {
        replayInFlight.set(false);
    }

    public CompletableFuture<DecisionCode> firstHandle() {
        return firstHandle;
    }
}
