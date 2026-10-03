package com.github.feng111627.webhookingestdispatchjava.dto;

/**
 * 死信重放的结论。
 *
 * @param payloadSource 本次重放实际使用的负载来源：ORIGINAL / CORRECTED
 */
public record ReplayResponse(String eventId, String status, String reason,
                             int attempts, int effects, String payloadSource) {
}
