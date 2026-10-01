package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.dto.DeadEventView;
import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.service.DeadLetterService;
import java.util.List;
import org.springframework.web.bind.annotation.*;

/** 死信查询与重放入口 */
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
    public ReplayResponse replay(@PathVariable String eventId) {
        return deadLetterService.replay(eventId);
    }
}
