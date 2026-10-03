package com.github.feng111627.webhookingestdispatchjava.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.webhookingestdispatchjava.model.ReplayRecord;
import java.time.Instant;
import java.util.List;

/**
 * 单条事件的完整视图：原始负载 + 当前生效信息 + 全部重放历史。
 * 原始负载与每次重放实际使用的负载都可查，便于事后核对。
 */
public record EventDetailView(
        String eventId,
        String eventType,
        String state,
        String failureKind,
        int attempts,
        String lastError,
        JsonNode originalPayload,
        List<ReplayRecord> replays,
        Instant receivedAt,
        Instant updatedAt) {
}
