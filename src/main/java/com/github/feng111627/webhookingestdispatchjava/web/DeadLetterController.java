package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.model.DeadLetterView;
import com.github.feng111627.webhookingestdispatchjava.model.ReplayResult;
import com.github.feng111627.webhookingestdispatchjava.service.DeadLetterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 死信查询与重放端点。
 */
@RestController
@RequestMapping("/webhooks/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public List<DeadLetterView> list() {
        return deadLetterService.list();
    }

    @PostMapping("/{eventId}/replay")
    public ResponseEntity<ReplayResult> replay(@PathVariable String eventId,
                                               @RequestBody(required = false) String updatedPayload) {
        ReplayResult result = deadLetterService.replay(eventId, updatedPayload);
        return ResponseEntity.status(result.decision().httpStatus()).body(result);
    }
}
