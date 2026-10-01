package com.github.feng111627.webhookingestdispatchjava.dedup;

import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 已接收事件的内存台账：承担幂等去重与并发合并。
 */
@Repository
public class ProcessedEventRepository {

    private final ConcurrentMap<String, EventRecord> records = new ConcurrentHashMap<>();

    /**
     * 原子占位：key 不存在时放入新记录并返回 null（调用方胜出，负责分发）；
     * key 已存在时原样返回已有记录（调用方是重复请求）。
     */
    public EventRecord putIfAbsent(WebhookEvent event) {
        return records.putIfAbsent(event.getEventId(), new EventRecord(event));
    }

    public Optional<EventRecord> find(String eventId) {
        return Optional.ofNullable(records.get(eventId));
    }

    /** 仅供测试重置。 */
    public void clear() {
        records.clear();
    }
}
