package com.github.feng111627.webhookingestdispatchjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.webhookingestdispatchjava.dedup.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.dedup.ProcessedEventRepository;
import com.github.feng111627.webhookingestdispatchjava.dispatch.EventDispatcher;
import com.github.feng111627.webhookingestdispatchjava.model.DecisionCode;
import com.github.feng111627.webhookingestdispatchjava.model.IngestResponse;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import com.github.feng111627.webhookingestdispatchjava.signature.SignatureVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 接收入口编排：校验 -> 解析 -> 去重/并发合并 -> 分发。
 *
 * 去重语义：
 * - 同一 eventId 只有第一个请求会真正分发；
 * - 串行重复请求按 200 成功返回 DUPLICATE_EVENT，不再次分发；
 * - 并发重复请求在第一个请求的首次尝试落定后返回同一结论，同样标记 DUPLICATE_EVENT；
 * - 若首个事件已进入死信，重复投递不再分发（避免覆盖重放状态），由死信重放接口处理。
 */
@Service
public class WebhookIngestService {

    private static final Logger log = LoggerFactory.getLogger(WebhookIngestService.class);
    /** 并发重复请求等待首个请求首次尝试落定的上限（首次处理是同步执行的，正常极快）。 */
    private static final long FIRST_ATTEMPT_WAIT_SECONDS = 10;

    private final SignatureVerifier signatureVerifier;
    private final ProcessedEventRepository eventRepository;
    private final EventDispatcher dispatcher;
    private final ObjectMapper objectMapper;

    public WebhookIngestService(SignatureVerifier signatureVerifier,
                                ProcessedEventRepository eventRepository,
                                EventDispatcher dispatcher,
                                ObjectMapper objectMapper) {
        this.signatureVerifier = signatureVerifier;
        this.eventRepository = eventRepository;
        this.dispatcher = dispatcher;
        this.objectMapper = objectMapper;
    }

    public IngestResponse ingest(String rawBody, String signatureHeader, String timestampHeader) {
        // 1. 签名 + 时间戳（两类失败明确区分）
        SignatureVerifier.Verification verification =
                signatureVerifier.verify(signatureHeader, timestampHeader, rawBody);
        if (!verification.valid()) {
            return new IngestResponse(null, verification.code(), verification.reason());
        }

        // 2. 解析（payload 为 JsonNode，新增字段不会破坏已有逻辑）
        WebhookEvent event;
        try {
            event = parse(rawBody, verification.timestamp());
        } catch (IllegalArgumentException e) {
            log.info("接收 判定=INVALID_PAYLOAD 依据={}", e.getMessage());
            return new IngestResponse(null, DecisionCode.INVALID_PAYLOAD, e.getMessage());
        }

        // 3. 去重 / 并发合并（原子占位）：putIfAbsent 保证并发下只有一个胜出者负责分发
        EventRecord existing = eventRepository.putIfAbsent(event);
        if (existing != null) {
            return handleDuplicate(existing);
        }
        EventRecord record = eventRepository.find(event.getEventId()).orElseThrow();

        // 4. 首个请求负责首次分发（同步得到首次尝试结论；暂时失败会异步继续重试）
        DecisionCode code = dispatcher.dispatch(record, 1);
        log.info("[eventId={}] 接收完成 判定={} 原因={}", event.getEventId(), code, record.getReason());
        return new IngestResponse(event.getEventId(), code, record.getReason());
    }

    private IngestResponse handleDuplicate(EventRecord record) {
        String eventId = record.getEvent().getEventId();
        DecisionCode terminal = record.getDecision();
        if (terminal == null || terminal == DecisionCode.RETRYING) {
            // 首个请求仍在首次处理中：等待其首次尝试落定，避免并发重复分发。
            terminal = awaitFirstAttempt(record);
        }
        String reason = "duplicate event: original decision=" + terminal
                + ", status=" + record.getStatus() + ", not dispatched again";
        log.info("[eventId={}] 判定=DUPLICATE_EVENT 依据=事件已存在({})，按成功返回且不重复分发",
                eventId, record.getStatus());
        return new IngestResponse(eventId, DecisionCode.DUPLICATE_EVENT, reason);
    }

    private DecisionCode awaitFirstAttempt(EventRecord record) {
        try {
            return record.firstHandle().get(FIRST_ATTEMPT_WAIT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return record.getDecision() == null ? DecisionCode.RETRYING : record.getDecision();
        } catch (ExecutionException | TimeoutException e) {
            return record.getDecision() == null ? DecisionCode.RETRYING : record.getDecision();
        }
    }

    private WebhookEvent parse(String rawBody, Long requestTimestamp) {
        JsonNode root;
        try {
            root = objectMapper.readTree(rawBody);
        } catch (Exception e) {
            throw new IllegalArgumentException("request body is not valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("request body must be a JSON object");
        }
        JsonNode idNode = root.get("eventId");
        JsonNode typeNode = root.get("eventType");
        if (idNode == null || idNode.asText().isBlank()) {
            throw new IllegalArgumentException("missing required field: eventId");
        }
        if (typeNode == null || typeNode.asText().isBlank()) {
            throw new IllegalArgumentException("missing required field: eventType");
        }
        // occurredAt 缺省取请求头时间戳；负载额外字段原样保留在 payload 中。
        Instant occurredAt = Instant.ofEpochSecond(
                requestTimestamp == null ? Instant.now().getEpochSecond() : requestTimestamp);
        JsonNode occurredNode = root.get("occurredAt");
        if (occurredNode != null && !occurredNode.asText().isBlank()) {
            try {
                occurredAt = Instant.parse(occurredNode.asText());
            } catch (Exception ignored) {
                // 非法 occurredAt 回退为请求时间，不影响主流程
            }
        }
        return new WebhookEvent(idNode.asText(), typeNode.asText(), occurredAt, root);
    }
}
