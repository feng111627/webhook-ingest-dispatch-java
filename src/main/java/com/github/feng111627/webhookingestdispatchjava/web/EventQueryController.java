package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.dto.EventDetailView;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.service.DeadLetterService;
import java.util.List;
import org.springframework.web.bind.annotation.*;

/**
 * 事件维度查询：完整详情（原始负载 + 重放历史）与判定轨迹。
 */
@RestController
@RequestMapping("/admin/events")
public class EventQueryController {

    private final DeadLetterService deadLetterService;

    public EventQueryController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    /** 事件详情：原始负载、当前状态、每次重放实际使用的负载来源与修正原文 */
    @GetMapping("/{eventId}")
    public EventDetailView detail(@PathVariable String eventId) {
        return deadLetterService.detail(eventId);
    }

    /** 事件轨迹：从接收到最终结论的每一步判定，含判定依据 */
    @GetMapping("/{eventId}/trace")
    public List<TraceEntry> trace(@PathVariable String eventId) {
        return deadLetterService.traceOf(eventId);
    }
}
