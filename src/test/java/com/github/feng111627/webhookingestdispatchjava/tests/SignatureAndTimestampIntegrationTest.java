package com.github.feng111627.webhookingestdispatchjava.tests;

import com.github.feng111627.webhookingestdispatchjava.tests.support.AbstractWebhookIntegrationTest;
import com.github.feng111627.webhookingestdispatchjava.tests.support.WebhookTestClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 签名与时间戳边界：两类失败必须能区分（INVALID_SIGNATURE 401 / TIMESTAMP_EXPIRED 401，
 * 但 decision 不同），缺头与坏 body 返回 400，合法请求 200。
 */
class SignatureAndTimestampIntegrationTest extends AbstractWebhookIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SignatureAndTimestampIntegrationTest.class);

    @Test
    void validSignatureAndFreshTimestamp_isAccepted() {
        String body = WebhookTestClient.body("evt-valid", "order.created");
        long now = System.currentTimeMillis() / 1000;
        HttpResponse<String> resp = client.post(body, now);
        log.info("[eventId=evt-valid] 合法签名+新鲜时间戳 -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("ACCEPTED"));
    }

    @Test
    void missingHeaders_isRejectedAsMissingHeaders() {
        String body = WebhookTestClient.body("evt-no-header", "order.created");
        HttpResponse<String> resp = client.post(body, null, null);
        log.info("[eventId=evt-no-header] 缺少签名头 -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("MISSING_HEADERS"));
    }

    @Test
    void wrongSignature_isRejectedAsInvalidSignature() {
        String body = WebhookTestClient.body("evt-bad-sig", "order.created");
        long now = System.currentTimeMillis() / 1000;
        HttpResponse<String> resp = client.post(body, "deadbeef", Long.toString(now));
        log.info("[eventId=evt-bad-sig] 签名不合法 -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(401, resp.statusCode());
        assertTrue(resp.body().contains("INVALID_SIGNATURE"));
    }

    @Test
    void signatureFromOtherSecret_isInvalidSignature() {
        String body = WebhookTestClient.body("evt-other-secret", "order.created");
        long now = System.currentTimeMillis() / 1000;
        String forged = com.github.feng111627.webhookingestdispatchjava.signature.SignatureVerifier
                .sign("wrong-secret", now, body);
        HttpResponse<String> resp = client.post(body, forged, Long.toString(now));
        log.info("[eventId=evt-other-secret] 用错误密钥签名 -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(401, resp.statusCode());
        assertTrue(resp.body().contains("INVALID_SIGNATURE"));
    }

    @Test
    void expiredTimestamp_isRejectedAsTimestampExpired() {
        String body = WebhookTestClient.body("evt-old", "order.created");
        long oldTs = System.currentTimeMillis() / 1000 - (TOLERANCE_SECONDS + 60);
        // 用正确密钥对（旧时间戳 + body）签名，确保失败原因是“过期”而不是“签名错”
        HttpResponse<String> resp = client.post(body, oldTs);
        log.info("[eventId=evt-old] 过期时间戳（签名本身正确）-> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(401, resp.statusCode());
        assertTrue(resp.body().contains("TIMESTAMP_EXPIRED"),
                "过期时间戳必须明确判定为 TIMESTAMP_EXPIRED 而不是 INVALID_SIGNATURE，实际: " + resp.body());
    }

    @Test
    void futureTimestampBeyondTolerance_isRejectedAsTimestampExpired() {
        String body = WebhookTestClient.body("evt-future", "order.created");
        long futureTs = System.currentTimeMillis() / 1000 + (TOLERANCE_SECONDS + 60);
        HttpResponse<String> resp = client.post(body, futureTs);
        log.info("[eventId=evt-future] 未来时间戳超容忍 -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(401, resp.statusCode());
        assertTrue(resp.body().contains("TIMESTAMP_EXPIRED"));
    }

    @Test
    void timestampExactlyAtToleranceBoundary_isAccepted() {
        String body = WebhookTestClient.body("evt-boundary", "order.created");
        // 边界内：容忍 60s，取偏差 59s（避免取整导致的抖动）
        long ts = System.currentTimeMillis() / 1000 - 59;
        HttpResponse<String> resp = client.post(body, ts);
        log.info("[eventId=evt-boundary] 偏差59s<容忍60s -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("ACCEPTED"));
    }

    @Test
    void nonNumericTimestamp_isMissingHeaders() {
        String body = WebhookTestClient.body("evt-bad-ts", "order.created");
        HttpResponse<String> resp = client.post(body, client.sign(1, body), "not-a-number");
        log.info("[eventId=evt-bad-ts] 非数字时间戳 -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("MISSING_HEADERS"));
    }

    @Test
    void invalidJson_isRejectedAsInvalidPayload() {
        String body = "this-is-not-json";
        long now = System.currentTimeMillis() / 1000;
        HttpResponse<String> resp = client.post(body, now);
        log.info("[eventId=<none>] 非法JSON -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("INVALID_PAYLOAD"));
    }

    @Test
    void missingEventType_isInvalidPayload() {
        String body = "{\"eventId\":\"evt-no-type\"}";
        long now = System.currentTimeMillis() / 1000;
        HttpResponse<String> resp = client.post(body, now);
        log.info("[eventId=evt-no-type] 缺eventType -> status={} body={}", resp.statusCode(), resp.body());

        assertEquals(400, resp.statusCode());
        assertTrue(resp.body().contains("INVALID_PAYLOAD"));
    }
}
