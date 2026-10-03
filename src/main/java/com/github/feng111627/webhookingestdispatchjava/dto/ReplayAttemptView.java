package com.github.feng111627.webhookingestdispatchjava.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

/**
 * 一次重放的留档视图：重放结束后可据此查到本次实际使用的是哪份负载。
 *
 * <p>{@code payloadSource}=CORRECTED 时 {@code correctedPayload} 为本次人工修正负载；
 * 无论是否修正，事件的原始负载都不会被覆盖（另见 EventDetailView.originalPayload）。
 */
public record ReplayAttemptView(
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
