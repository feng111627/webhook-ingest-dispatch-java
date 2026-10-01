package com.github.feng111627.webhookingestdispatchjava.dlq;

import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 死信内存台账。
 */
@Repository
public class DeadLetterRepository {

    private final ConcurrentMap<String, DeadLetterRecord> dead = new ConcurrentHashMap<>();

    public void save(DeadLetterRecord record) {
        // 骨架
        dead.put(record.getEvent().getEventId(), record);
    }

    public Optional<DeadLetterRecord> find(String eventId) {
        return Optional.ofNullable(dead.get(eventId));
    }

    public List<DeadLetterRecord> findAll() {
        return List.copyOf(dead.values());
    }

    public boolean remove(String eventId) {
        return dead.remove(eventId) != null;
    }

    public void clear() {
        dead.clear();
    }
}
