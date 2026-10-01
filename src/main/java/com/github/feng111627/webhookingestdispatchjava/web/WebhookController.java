package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.dto.IngestResponse;
import com.github.feng111627.webhookingestdispatchjava.service.WebhookIngestService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Webhook 事件接收入口。
 *
 * <p>重复事件同样返回 2xx（DUPLICATE），调用方无需重试；
 * 校验失败见 {@link GlobalExceptionHandler} 的结构化错误。
 */
@RestController
@RequestMapping("/webhooks/events")
public class WebhookController {

    private final WebhookIngestService ingestService;

    public WebhookController(WebhookIngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping
    public ResponseEntity<IngestResponse> receive(
            @RequestHeader(value = "X-Webhook-Timestamp", required = false) String timestamp,
            @RequestHeader(value = "X-Webhook-Signature", required = false) String signature,
            @RequestBody(required = false) String body) {
        IngestResponse response = ingestService.receive(timestamp, signature, body);
        return ResponseEntity.ok(response);
    }
}
