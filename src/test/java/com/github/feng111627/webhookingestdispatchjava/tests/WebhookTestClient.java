package com.github.feng111627.webhookingestdispatchjava.tests;

import java.nio.charset.StandardCharsets;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

/**
 * 测试侧构造合法/非法 Webhook 请求的工具。
 * 签名规则与服务端保持一致：hex(HMAC_SHA256(secret, timestamp + "." + body))。
 */
public final class WebhookTestClient {

    public static final String SECRET = "test-secret";

    private WebhookTestClient() {
    }

    public static String sign(String timestamp, String body) {
        return sign(SECRET, timestamp, body);
    }

    public static String sign(String secret, String timestamp, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] raw = mac.doFinal((timestamp + "." + body).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(raw.length * 2);
            for (byte b : raw) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 用当前时间构造一个完全合法的请求并发送 */
    public static ResponseEntity<String> postValid(TestRestTemplate rest, String baseUrl, String body) {
        return post(rest, baseUrl, body, String.valueOf(System.currentTimeMillis() / 1000), null, SECRET);
    }

    public static ResponseEntity<String> post(TestRestTemplate rest, String baseUrl, String body,
                                              String timestamp, String signatureOverride,
                                              String secret) {
        HttpHeaders headers = new HttpHeaders();
        if (timestamp != null) {
            headers.add("X-Webhook-Timestamp", timestamp);
        }
        if (signatureOverride != null) {
            headers.add("X-Webhook-Signature", signatureOverride);
        } else if (timestamp != null) {
            headers.add("X-Webhook-Signature", sign(secret, timestamp, body));
        }
        return rest.postForEntity(baseUrl + "/webhooks/events", new HttpEntity<>(body, headers), String.class);
    }

    public static String body(String eventId, String eventType, String extraJson) {
        String base = "\"eventId\":\"" + eventId + "\",\"eventType\":\"" + eventType + "\"";
        return "{" + base + (extraJson == null || extraJson.isBlank() ? "" : "," + extraJson) + "}";
    }
}
