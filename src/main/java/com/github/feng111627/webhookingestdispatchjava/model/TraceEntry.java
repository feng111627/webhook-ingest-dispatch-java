package com.github.feng111627.webhookingestdispatchjava.model;

import java.time.Instant;

/**
 * 事件轨迹中的一条判定记录。
 *
 * @param eventId  事件标识（轨迹的主键）
 * @param step     步骤类型
 * @param decision 判定结论（机读，如 DUPLICATE / UNSUPPORTED_EVENT_TYPE / RETRYABLE / SUCCEEDED）
 * @param detail   判定依据（人读，与接口返回的说明对齐）
 * @param at       发生时间
 */
public record TraceEntry(String eventId, TraceStep step, String decision, String detail, Instant at) {
}
