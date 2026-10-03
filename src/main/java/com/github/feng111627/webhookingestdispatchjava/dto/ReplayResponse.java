package com.github.feng111627.webhookingestdispatchjava.dto;

/**
 * 死信重放的结论。
 *
 * @param replayId     本次重放的唯一标识，可据此查询本次实际使用的负载与轨迹
 * @param payloadSource 本次实际处理负载来源：ORIGINAL（原始负载）/ CORRECTED（人工修正负载）
 * @param effects      保留字段：副作用台账幂等由处理器侧保证（始终 0 个"新增"副作用可由台账核对）
 */
public record ReplayResponse(
        String eventId,
        String status,
        String reason,
        int attempts,
        int effects,
        String replayId,
        String payloadSource) {
}
