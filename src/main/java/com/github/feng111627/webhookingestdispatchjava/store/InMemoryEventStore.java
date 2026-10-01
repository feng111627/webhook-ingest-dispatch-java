package com.github.feng111627.webhookingestdispatchjava.store;

import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内存事件存储（本地演示/测试用，重启即清空）。
 *
 * <p>"同一事件只处理一次"由 {@link #claimIfAbsent} 的原子占位保证：
 * 无论请求是串行重复还是并发到达，只有第一个调用拿到 null（占位成功），
 * 其余调用都会拿到已存在的记录（含 PROCESSING 状态的在途事件）。
 */
@Component
public class InMemoryEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryEventStore.class);

    private final ConcurrentHashMap<String, EventRecord> records = new ConcurrentHashMap<>();

    /**
     * 幂等占位。
     *
     * @param eventId         事件标识
     * @param initialSupplier 首次到达时如何构造初始记录
     * @return 已存在的记录；占位成功（首次）返回 null
     */
    @Override
    public EventRecord claimIfAbsent(String eventId, UnaryOperator<EventRecord> initialSupplier) {
        // ConcurrentHashMap.compute 对同一 key 串行执行，天然实现并发去重
        EventRecord[] existing = new EventRecord[1];
        records.compute(eventId, (key, current) -> {
            if (current != null) {
                existing[0] = current;
                return current;
            }
            EventRecord fresh = initialSupplier.apply(null);
            fresh.setEventId(key);
            existing[0] = null;
            return fresh;
        });
        if (existing[0] != null) {
            log.info("[store eventId={}] claim 被拒绝：事件已存在(state={}, attempts={}) -> 按重复处理",
                    eventId, existing[0].getState(), existing[0].getAttempts());
        } else {
            log.info("[store eventId={}] claim 成功：首次接收，占位 RECEIVED", eventId);
        }
        return existing[0];
    }

    @Override
    public Optional<EventRecord> find(String eventId) {
        return Optional.ofNullable(records.get(eventId));
    }

    /** 在记录对象锁内更新，避免重试与重放并发时状态互相覆盖 */
    @Override
    public void update(String eventId, UnaryOperator<EventRecord> mutator) {
        records.computeIfPresent(eventId, (key, current) -> {
            synchronized (current) {
                EventRecord updated = mutator.apply(current);
                updated.setEventId(key);
                return updated;
            }
        });
    }

    @Override
    public List<EventRecord> findDead() {
        return records.values().stream()
                .filter(r -> r.getState() == EventState.DEAD)
                .sorted(java.util.Comparator.comparing(EventRecord::getReceivedAt))
                .toList();
    }
}
