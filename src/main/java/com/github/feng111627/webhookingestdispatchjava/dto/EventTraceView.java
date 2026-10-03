package com.github.feng111627.webhookingestdispatchjava.dto;

import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import java.util.List;

/**
 * 单事件判定轨迹视图：每一步都带事件标识与判定依据，按 seq 严格有序。
 */
public record EventTraceView(String eventId, List<TraceEntry> entries, int count) {
}
