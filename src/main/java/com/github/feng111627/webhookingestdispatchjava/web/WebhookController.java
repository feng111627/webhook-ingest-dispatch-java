package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.model.IngestResponse;
import com.github.feng111627.webhookingestdispatchjava.service.WebhookIngestService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Webhook 接收端点。
 */
@RestController
@RequestMapping("/webhooks/events")
public class WebhookController {

    public static final String SIGNATURE_HEADER = "X-Webhook-Signature";
    public static final String TIMESTAMP_HEADER = "X-Webhook-Timestamp";

    private final WebhookIngestService ingestService;

    public WebhookController(WebhookIngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping
    public ResponseEntity<IngestResponse> receive(@RequestBody String rawBody,
                                                  @RequestHeader(value = SIGNATURE_HEADER, required = false)
                                                  String signature,
                                                  @RequestHeader(value = TIMESTAMP_HEADER, required = false)
                                                  String timestamp) {
        IngestResponse response = ingestService.ingest(rawBody, signature, timestamp);
        return ResponseEntity.status(response.decision().httpStatus()).body(response);
    }
}
