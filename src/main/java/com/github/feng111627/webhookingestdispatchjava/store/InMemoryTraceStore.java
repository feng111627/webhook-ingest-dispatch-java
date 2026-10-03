package com.github.feng111627.webhookingestdispatchjava.store;

import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内存轨迹存储（本地演示/测试用，重启清空）。
 *
 * <p>每个事件一个有序列表；append 对同一事件加事件级锁，保证 seq 严格递增且并发安全。
 */
@Component
public class InMemoryTraceStore implements TraceStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryTraceStore.class);

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<TraceEntry>> traces = new ConcurrentHashMap<>();

    /** 每个事件独立的序号生成器，避免全局锁竞争 */
    private final ConcurrentHashMap<String, AtomicLong> seqGenerators = new ConcurrentHashMap<>();

    @Override
    public TraceEntry append(TraceEntry.Builder builder) {
        String eventId = builder.eventId();
        AtomicLong generator = seqGenerators.computeIfAbsent(eventId, k -> new AtomicLong());
        CopyOnWriteArrayList<TraceEntry> list =
                traces.computeIfAbsent(eventId, k -> new CopyOnWriteArrayList<>());
        TraceEntry entry;
        synchronized (generator) {
            long seq = generator.incrementAndGet();
            entry = builder.seq(seq).at(Instant.now()).build();
            list.add(entry);
        }
        log.info("[trace eventId={}] #{}/{} {} : {}",
                eventId, entry.seq(), entry.phase(),
                entry.decision() == null ? "" : entry.decision(),
                entry.apiConclusion() == null ? "" : entry.apiConclusion());
        return entry;
    }

    @Override
    public List<TraceEntry> findByEventId(String eventId) {
        CopyOnWriteArrayList<TraceEntry> list = traces.get(eventId);
        if (list == null) {
            return List.of();
        }
        return List.copyOf(list);
    }
}
