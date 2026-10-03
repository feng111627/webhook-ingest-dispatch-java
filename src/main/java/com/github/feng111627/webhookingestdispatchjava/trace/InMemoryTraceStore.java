package com.github.feng111627.webhookingestdispatchjava.trace;

import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 内存轨迹存储（本地演示/测试用，重启即清空）。
 *
 * <p>只追加不改写，保证轨迹是可审计的判定历史；
 * 同一 eventId 的并发追加由 {@link CopyOnWriteArrayList} 保证不丢条目。
 */
@Component
public class InMemoryTraceStore implements TraceStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryTraceStore.class);

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<TraceEntry>> traces =
            new ConcurrentHashMap<>();

    @Override
    public void append(TraceEntry entry) {
        if (entry == null || entry.eventId() == null || entry.eventId().isBlank()) {
            // 无法归键的判定（如缺少 eventId 的坏请求）不进轨迹，避免污染
            log.debug("[trace] 忽略无 eventId 的轨迹条目: step={}", entry == null ? null : entry.step());
            return;
        }
        traces.computeIfAbsent(entry.eventId(), k -> new CopyOnWriteArrayList<>()).add(entry);
        log.info("[trace eventId={}] {} -> {} | {}", entry.eventId(), entry.step(),
                entry.decision(), entry.detail());
    }

    @Override
    public List<TraceEntry> findByEventId(String eventId) {
        if (eventId == null) {
            return List.of();
        }
        return List.copyOf(traces.getOrDefault(eventId, new CopyOnWriteArrayList<>()));
    }
}
