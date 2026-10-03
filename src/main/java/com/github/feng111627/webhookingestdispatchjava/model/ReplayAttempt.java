package com.github.feng111627.webhookingestdispatchjava.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * 一次死信重放的留档（原始接收负载永不被覆盖，这里单独记录本次重放实际使用的负载）。
 *
 * <p>{@code payloadSource}=ORIGINAL 表示沿用接收时的原始负载；
 * {@code payloadSource}=CORRECTED 表示使用了人工修正负载，此时 {@code correctedPayload} 非空。
 * 重放结束后可凭 {@code replayId} 查到本次到底用了哪份负载、结论与失败原因。
 */
public record ReplayAttempt(
        String replayId,
        int seq,
        String payloadSource,
        JsonNode correctedPayload,
        Instant submittedAt,
        Instant finishedAt,
        String status,
        String failureKind,
        int attempts,
        String lastError) {
}
