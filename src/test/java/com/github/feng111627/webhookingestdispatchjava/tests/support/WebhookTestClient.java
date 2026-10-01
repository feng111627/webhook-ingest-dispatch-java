package com.github.feng111627.webhookingestdispatchjava.tests.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.webhookingestdispatchjava.signature.SignatureVerifier;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;

/**
 * 测试辅助：用与生产完全相同的 {@link SignatureVerifier#sign} 算法构造请求，
 * 保证“怎么签的就怎么验”。
 * 使用 JDK HttpClient：对 4xx/5xx 也原样返回状态码与响应体，不会抛异常。
 */
public class WebhookTestClient {

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String secret;
    private final String baseUrl;

    public WebhookTestClient(String baseUrl, String secret) {
        this.baseUrl = baseUrl;
        this.secret = secret;
    }

    public static String body(String eventId, String eventType) {
        return "{\"eventId\":\"" + eventId + "\",\"eventType\":\"" + eventType + "\"}";
    }

    public static String bodyWith(String eventId, String eventType, String extraJsonField) {
        return "{\"eventId\":\"" + eventId + "\",\"eventType\":\"" + eventType + "\","
                + extraJsonField + "}";
    }

    public String sign(long timestamp, String rawBody) {
        return SignatureVerifier.sign(secret, timestamp, rawBody);
    }

    /** 用当前时间与正确签名发送。 */
    public HttpResponse<String> postValid(String rawBody) {
        return post(rawBody, System.currentTimeMillis() / 1000);
    }

    /** 用正确算法、指定时间戳签名发送。 */
    public HttpResponse<String> post(String rawBody, long timestamp) {
        return post(rawBody, sign(timestamp, rawBody), Long.toString(timestamp));
    }

    /** 自定义签名/时间戳头，用于构造非法场景；传 null 表示不带该头。 */
    public HttpResponse<String> post(String rawBody, String signature, String timestamp) {
        TreeMap<String, String> headers = new TreeMap<>();
        headers.put("Content-Type", "application/json");
        if (signature != null) {
            headers.put("X-Webhook-Signature", signature);
        }
        if (timestamp != null) {
            headers.put("X-Webhook-Timestamp", timestamp);
        }
        return invoke("/webhooks/events", "POST", rawBody, headers);
    }

    public HttpResponse<String> listDeadLetters() {
        return invoke("/webhooks/dead-letters", "GET", null, Map.of());
    }

    public HttpResponse<String> replay(String eventId) {
        return invoke("/webhooks/dead-letters/" + eventId + "/replay", "POST", "", Map.of());
    }

    /** 携带更新后负载重放（上游修复后带新增字段）。 */
    public HttpResponse<String> replayWithPayload(String eventId, String rawJson) {
        return invoke("/webhooks/dead-letters/" + eventId + "/replay", "POST", rawJson,
                java.util.Map.of("Content-Type", "application/json"));
    }

    private HttpResponse<String> invoke(String path, String method, String body, Map<String, String> headers) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path));
        headers.forEach(builder::header);
        HttpRequest.BodyPublisher publisher = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
        builder.method(method, publisher);
        try {
            return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode toJson(String raw) {
        try {
            return objectMapper.readTree(raw);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
