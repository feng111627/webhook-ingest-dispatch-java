package com.github.feng111627.webhookingestdispatchjava.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;

/**
 * 单个事件的详情视图：原始接收负载（永不被重放覆盖）+ 当前判定状态 + 历次重放留档。
 *
 * <p>核对"这次重放基于什么内容成功/失败"：对照 {@code originalPayload}
 * 与 {@code replays} 中某次重放的 {@code correctedPayload} 即可看出改了什么。
 */
public record EventDetailView(
        String eventId,
        String eventType,
        String state,
        String failureKind,
        int attempts,
        String lastError,
        JsonNode originalPayload,
        Instant receivedAt,
        Instant updatedAt,
        List<ReplayAttemptView> replays) {
}
