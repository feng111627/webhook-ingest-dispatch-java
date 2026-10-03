package com.github.feng111627.webhookingestdispatchjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.webhookingestdispatchjava.dto.IngestResponse;
import com.github.feng111627.webhookingestdispatchjava.exception.RejectReason;
import com.github.feng111627.webhookingestdispatchjava.exception.WebhookRejectedException;
import com.github.feng111627.webhookingestdispatchjava.handler.EventHandlerRegistry;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.model.FailureKind;
import com.github.feng111627.webhookingestdispatchjava.model.TraceEntry;
import com.github.feng111627.webhookingestdispatchjava.model.TraceStep;
import com.github.feng111627.webhookingestdispatchjava.security.SignatureVerifier;
import com.github.feng111627.webhookingestdispatchjava.store.EventStore;
import com.github.feng111627.webhookingestdispatchjava.trace.TraceStore;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * 接收入口编排，判定顺序全局唯一（保证各处结论一致、可解释）：
 * <pre>
 *   空体/非 JSON对象      -> 400 BAD_REQUEST
 *   时间戳缺失/非法        -> 401 TIMESTAMP_MISSING
 *   时间戳过期/超前        -> 401 TIMESTAMP_EXPIRED
 *   签名缺失              -> 401 SIGNATURE_MISSING
 *   签名不匹配            -> 401 SIGNATURE_INVALID
 *   缺 eventId/eventType  -> 400 BAD_REQUEST
 *   无匹配处理器           -> 422 UNSUPPORTED_EVENT_TYPE（不入库，绝不静默丢弃）
 *   重复（含在途并发）      -> 200 DUPLICATE（不再分发）
 *   首次                 -> 分发：200 SUCCEEDED 或 200 DEAD（死信可查可重放）
 * </pre>
 */
@Service
public class WebhookIngestService {

    private static final Logger log = LoggerFactory.getLogger(WebhookIngestService.class);

    private final SignatureVerifier signatureVerifier;
    private final EventStore eventStore;
    private final EventHandlerRegistry registry;
    private final DispatchService dispatchService;
    private final ObjectMapper objectMapper;
    private final TraceStore traceStore;

    public WebhookIngestService(SignatureVerifier signatureVerifier,
                                EventStore eventStore,
                                EventHandlerRegistry registry,
                                DispatchService dispatchService,
                                ObjectMapper objectMapper,
                                TraceStore traceStore) {
        this.signatureVerifier = signatureVerifier;
        this.eventStore = eventStore;
        this.registry = registry;
        this.dispatchService = dispatchService;
        this.objectMapper = objectMapper;
        this.traceStore = traceStore;
    }

    public IngestResponse receive(String timestampHeader, String signatureHeader, String rawBody) {
        // 0. 请求体必须是非空的 JSON 对象
        if (rawBody == null || rawBody.isBlank()) {
            throw rejected(RejectReason.BAD_REQUEST, "请求体为空", null);
        }
        final JsonNode payload;
        try {
            JsonNode node = objectMapper.readTree(rawBody);
            if (node == null || !node.isObject()) {
                throw rejected(RejectReason.BAD_REQUEST, "请求体必须是 JSON 对象", null);
            }
            payload = node;
        } catch (WebhookRejectedException ex) {
            throw ex;
        } catch (Exception ex) {
            throw rejected(RejectReason.BAD_REQUEST, "请求体不是合法 JSON: " + ex.getMessage(), null);
        }

        String eventId = textField(payload, "eventId");
        String eventType = textField(payload, "eventType");

        // 1~2. 时间戳（先缺失后过期）
        signatureVerifier.verify(eventId, timestampHeader, signatureHeader, rawBody)
                .ifPresent(reason -> {
                    trace(eventId, TraceStep.REJECTED, reason.name(),
                            "入口校验拒绝（时间戳/签名）: " + reason);
                    String message = switch (reason) {
                        case TIMESTAMP_MISSING -> "时间戳缺失或不是 Unix 秒，请携带 X-Webhook-Timestamp";
                        case TIMESTAMP_EXPIRED -> "时间戳超出容忍窗口，请检查时钟或重放窗口";
                        case SIGNATURE_MISSING -> "签名缺失，请携带 X-Webhook-Signature";
                        case SIGNATURE_INVALID -> "签名不匹配，请检查密钥与签名内容(timestamp + '.' + body)";
                        default -> "请求被拒绝: " + reason;
                    };
                    throw rejected(reason, message, eventId);
                });

        // 3. 业务字段
        if (eventId == null || eventId.isBlank()) {
            throw rejected(RejectReason.BAD_REQUEST, "负载缺少 eventId", null);
        }
        if (eventType == null || eventType.isBlank()) {
            throw rejected(RejectReason.BAD_REQUEST, "负载缺少 eventType", eventId);
        }

        // 4. 无匹配处理器：明确拒绝，绝不静默丢弃（放在占位之前，坏类型不污染存储）
        if (registry.lookup(eventType).isEmpty()) {
            log.info("[ingest eventId={}] REJECT: 无匹配处理器(type={}) -> UNSUPPORTED_EVENT_TYPE",
                    eventId, eventType);
            trace(eventId, TraceStep.REJECTED, "UNSUPPORTED_EVENT_TYPE",
                    "没有匹配事件类型 '" + eventType + "' 的处理器，事件不会被静默丢弃");
            throw new WebhookRejectedException(RejectReason.UNSUPPORTED_EVENT_TYPE,
                    "没有匹配事件类型 '" + eventType + "' 的处理器，事件不会被静默丢弃",
                    HttpStatus.UNPROCESSABLE_ENTITY.value());
        }

        // 5. 原子占位：串行重复与并发重复都只会有一个调用进入分发
        Instant now = Instant.now();
        EventRecord existing = eventStore.claimIfAbsent(eventId, ignored -> {
            EventRecord r = new EventRecord();
            r.setEventType(eventType);
            r.setPayload(payload);
            r.setRawBody(rawBody);
            r.setState(EventState.RECEIVED);
            r.setFailureKind(FailureKind.NONE);
            r.setAttempts(0);
            r.setReceivedAt(now);
            r.setUpdatedAt(now);
            return r;
        });
        if (existing != null) {
            // 重复请求按成功返回，但绝不再次分发
            log.info("[ingest eventId={}] DUPLICATE: 当前状态 state={} attempts={} -> 返回成功且不再分发",
                    eventId, existing.getState(), existing.getAttempts());
            trace(eventId, TraceStep.DUPLICATE, "DUPLICATE",
                    "事件已存在(state=" + existing.getState() + ", attempts=" + existing.getAttempts()
                            + ")，按成功返回且不再分发");
            return new IngestResponse(eventId, "DUPLICATE",
                    "事件已接收/处理过，按成功返回且不重复分发", existing.getAttempts());
        }

        // 6. 首次事件：同步分发（本地演示），成功或死信都在本次请求给出最终结论
        trace(eventId, TraceStep.RECEIVED, "RECEIVED",
                "首次接收，占位成功，进入分发(type=" + eventType + ")");
        EventRecord record = eventStore.find(eventId).orElseThrow();
        EventRecord finished = dispatchService.dispatch(record);
        log.info("[ingest eventId={}] 分发结束 state={} failureKind={} attempts={}",
                eventId, finished.getState(), finished.getFailureKind(), finished.getAttempts());
        return new IngestResponse(
                eventId,
                finished.getState() == EventState.SUCCEEDED ? "SUCCEEDED" : "DEAD",
                finished.getState() == EventState.SUCCEEDED
                        ? "处理成功"
                        : "处理失败已进入死信，可通过 /admin/dead-letters 查询并重放: " + finished.getLastError(),
                finished.getAttempts());
    }

    private void trace(String eventId, TraceStep step, String decision, String detail) {
        traceStore.append(new TraceEntry(eventId, step, decision, detail, Instant.now()));
    }

    private static String textField(JsonNode payload, String field) {
        if (payload.hasNonNull(field) && payload.get(field).isTextual()) {
            String v = payload.get(field).asText();
            return v.isBlank() ? null : v;
        }
        return null;
    }

    private static WebhookRejectedException rejected(RejectReason reason, String message, String eventId) {
        log.info("[ingest eventId={}] REJECT: {} -> {}", eventId, reason, message);
        int status = reason == RejectReason.UNSUPPORTED_EVENT_TYPE
                ? HttpStatus.UNPROCESSABLE_ENTITY.value()
                : reason == RejectReason.BAD_REQUEST
                        ? HttpStatus.BAD_REQUEST.value()
                        : HttpStatus.UNAUTHORIZED.value();
        return new WebhookRejectedException(reason, message, status);
    }
}
