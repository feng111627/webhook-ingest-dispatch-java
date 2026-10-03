package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.dto.DeadEventView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayRequest;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.service.DeadLetterService;
import java.util.List;
import org.springframework.web.bind.annotation.*;

/**
 * 死信查询与重放入口。
 *
 * <p>重放请求体可选：携带 {@code {"payload": {...}}} 时，本次重放以人工修正后的负载
 * 走原分发与重试链路；不带请求体则沿用原始负载。原始负载永不被覆盖。
 */
@RestController
@RequestMapping("/admin/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public List<DeadEventView> list() {
        return deadLetterService.listDead();
    }

    @PostMapping("/{eventId}/replay")
    public ReplayResponse replay(@PathVariable String eventId,
                                 @RequestBody(required = false) ReplayRequest request) {
        return deadLetterService.replay(eventId, request == null ? null : request.payload());
    }
}
