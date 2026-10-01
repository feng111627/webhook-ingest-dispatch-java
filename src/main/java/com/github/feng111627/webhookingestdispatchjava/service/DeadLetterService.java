package com.github.feng111627.webhookingestdispatchjava.service;

import com.github.feng111627.webhookingestdispatchjava.dto.DeadEventView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * 死信查询与重放。
 *
 * <p>规则：
 * <ul>
 *   <li>只列出 state=DEAD 的事件，含失败性质、尝试次数、最后错误，判定可解释；</li>
 *   <li>只有 DEAD 状态允许重放；不存在 404，非 DEAD 409；</li>
 *   <li>重放复用同一分发链路（含退避重试），成功后从死信列表消失；</li>
 *   <li>处理器内副作用经台账幂等保护，重放不会产生重复副作用。</li>
 * </ul>
 */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final EventStore eventStore;
    private final DispatchService dispatchService;

    public DeadLetterService(EventStore eventStore, DispatchService dispatchService) {
        this.eventStore = eventStore;
        this.dispatchService = dispatchService;
    }

    public List<DeadEventView> listDead() {
        List<DeadEventView> views = eventStore.findDead().stream()
                .map(r -> new DeadEventView(
                        r.getEventId(),
                        r.getEventType(),
                        r.getFailureKind().name(),
                        r.getAttempts(),
                        r.getLastError(),
                        r.getReceivedAt(),
                        r.getUpdatedAt()))
                .toList();
        log.info("[deadletter] 查询死信，共 {} 条: {}", views.size(),
                views.stream().map(DeadEventView::eventId).toList());
        return views;
    }

    public ReplayResponse replay(String eventId) {
        EventRecord record = eventStore.find(eventId)
                .orElseThrow(() -> {
                    log.warn("[deadletter eventId={}] 重放被拒绝：事件不存在 -> 404", eventId);
                    return new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在: " + eventId);
                });

        if (record.getState() != EventState.DEAD) {
            log.warn("[deadletter eventId={}] 重放被拒绝：当前状态 {} 不是 DEAD -> 409",
                    eventId, record.getState());
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "事件当前状态为 " + record.getState() + "，只有 DEAD 状态允许重放");
        }

        // 在记录锁内把 DEAD -> PROCESSING 抢占，避免并发重放重复执行
        boolean[] claimed = {false};
        eventStore.update(eventId, r -> {
            if (r.getState() == EventState.DEAD) {
                r.setState(EventState.PROCESSING);
                claimed[0] = true;
            }
            return r;
        });
        if (!claimed[0]) {
            log.warn("[deadletter eventId={}] 重放被拒绝：已有重放在执行 -> 409", eventId);
            throw new ResponseStatusException(HttpStatus.CONFLICT, "该事件已有重放在执行");
        }

        log.info("[deadletter eventId={}] 重放开始(failureKind={}, 上次attempts={})",
                eventId, record.getFailureKind(), record.getAttempts());
        EventRecord fresh = eventStore.find(eventId).orElseThrow();
        EventRecord finished = dispatchService.redispatch(fresh);

        if (finished.getState() == EventState.SUCCEEDED) {
            log.info("[deadletter eventId={}] 重放成功，已移去死信(attempts={})",
                    eventId, finished.getAttempts());
            return new ReplayResponse(eventId, "SUCCEEDED",
                    "重放成功；副作用受幂等台账保护，无重复副作用", finished.getAttempts(), 0);
        }
        log.warn("[deadletter eventId={}] 重放后仍失败，留在死信(failureKind={}, attempts={}, lastError={})",
                eventId, finished.getFailureKind(), finished.getAttempts(), finished.getLastError());
        return new ReplayResponse(eventId, "DEAD",
                "重放后仍失败，继续保留在死信: " + finished.getLastError(),
                finished.getAttempts(), 0);
    }
}
