package com.github.feng111627.webhookingestdispatchjava.service;

import com.github.feng111627.webhookingestdispatchjava.dto.EventTraceView;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.store.TraceStore;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * 判定轨迹查询：按事件标识把接收、命中重复、无类型被拒、每次尝试与退避、进死信、
 * 到每次重放的每一步串成一条有序轨迹，每条都带 eventId 与判定依据。
 *
 * <p>事件记录或轨迹只要存在其一即可查询（例如"无处理器被拒"的事件没有入库，但有轨迹）；
 * 两者都没有则 404。
 */
@Service
public class EventQueryService {

    private static final Logger log = LoggerFactory.getLogger(EventQueryService.class);

    private final TraceStore traceStore;
    private final EventStore eventStore;

    public EventQueryService(TraceStore traceStore, EventStore eventStore) {
        this.traceStore = traceStore;
        this.eventStore = eventStore;
    }

    public EventTraceView trace(String eventId) {
        List<TraceEntry> entries = traceStore.findByEventId(eventId);
        if (entries.isEmpty() && eventStore.find(eventId).isEmpty()) {
            log.warn("[trace eventId={}] 查询轨迹：事件与轨迹均不存在 -> 404", eventId);
            throw new ResponseStatusException(HttpStatus.NOT_FOUND,
                    "没有事件 " + eventId + " 的判定轨迹");
        }
        log.info("[trace eventId={}] 查询轨迹，共 {} 步: {}", eventId, entries.size(),
                entries.stream().map(e -> e.seq() + ":" + e.phase()).toList());
        return new EventTraceView(eventId, entries, entries.size());
    }
}
