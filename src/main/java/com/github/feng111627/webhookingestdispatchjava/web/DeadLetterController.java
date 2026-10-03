package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.dto.DeadEventView;
import com.github.feng111627.webhookingestdispatchjava.dto.EventDetailView;
import com.github.feng111627.webhookingestdispatchjava.dto.EventTraceView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayAttemptView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.model.ReplayAttempt;
import com.github.feng111627.webhookingestdispatchjava.service.DeadLetterService;
import com.github.feng111627.webhookingestdispatchjava.service.EventQueryService;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** 死信查询、事件详情与重放入口 */
@RestController
@RequestMapping("/admin/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;
    private final EventStore eventStore;
    private final EventQueryService eventQueryService;

    public DeadLetterController(DeadLetterService deadLetterService,
                                EventStore eventStore,
                                EventQueryService eventQueryService) {
        this.deadLetterService = deadLetterService;
        this.eventStore = eventStore;
        this.eventQueryService = eventQueryService;
    }

    @GetMapping
    public List<DeadEventView> list() {
        return deadLetterService.listDead();
    }

    /**
     * 重放。
     *
     * <p>请求体为空：沿用原始负载；请求体为 JSON 对象：作为本次人工修正负载
     * （仅作用于本次重放，原始负载保留不覆盖）。
     */
    @PostMapping("/{eventId}/replay")
    public ReplayResponse replay(@PathVariable String eventId,
                                 @RequestBody(required = false) String correctedBody) {
        return deadLetterService.replay(eventId, correctedBody);
    }

    /** 事件详情：原始负载 + 当前状态 + 历次重放留档（可核对每次重放实际用了哪份负载）。 */
    @GetMapping("/{eventId}")
    public EventDetailView detail(@PathVariable String eventId) {
        EventRecord record = eventStore.find(eventId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在: " + eventId));
        List<ReplayAttemptView> replays = record.getReplayHistory().stream()
                .map(this::toView)
                .toList();
        return new EventDetailView(
                record.getEventId(),
                record.getEventType(),
                record.getState().name(),
                record.getFailureKind().name(),
                record.getAttempts(),
                record.getLastError(),
                record.getPayload(),
                record.getReceivedAt(),
                record.getUpdatedAt(),
                replays);
    }

    /** 单事件判定轨迹查询；事件/轨迹均不存在时返回 404。 */
    @GetMapping("/{eventId}/trace")
    public EventTraceView trace(@PathVariable String eventId) {
        return eventQueryService.trace(eventId);
    }

    private ReplayAttemptView toView(ReplayAttempt ra) {
        return new ReplayAttemptView(
                ra.replayId(), ra.seq(), ra.payloadSource(), ra.correctedPayload(),
                ra.submittedAt(), ra.finishedAt(), ra.status(),
                ra.failureKind(), ra.attempts(), ra.lastError());
    }
}
