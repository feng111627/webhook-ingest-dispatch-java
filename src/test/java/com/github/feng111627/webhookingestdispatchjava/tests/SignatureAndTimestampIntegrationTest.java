package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.post;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.sign;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

/**
 * 签名与时间戳边界场景：
 * 合法请求成功；时间戳缺失/过期/超前、签名缺失/错误密钥/篡改内容分别被拒绝且错误码可区分。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SignatureAndTimestampIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SignatureAndTimestampIntegrationTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String base() {
        return "http://localhost:" + port;
    }

    @Test
    void validRequest_succeeds() {
        String eventId = "sig-ok-1";
        String payload = body(eventId, "order.created", "\"orderId\":\"O-1\",\"amountCents\":1000");
        ResponseEntity<String> resp = postValid(rest, base(), payload);

        log.info("[case eventId={}] 合法请求 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("SUCCEEDED").contains(eventId);
    }

    @Test
    void missingTimestamp_rejectedWithDistinctCode() {
        String eventId = "sig-no-ts";
        String payload = body(eventId, "order.created", null);
        // 只有签名、没有时间戳
        ResponseEntity<String> resp = post(rest, base(), payload, null, "any-signature", WebhookTestClient.SECRET);

        log.info("[case eventId={}] 缺时间戳 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("TIMESTAMP_MISSING");
    }

    @Test
    void nonNumericTimestamp_rejectedAsTimestampMissing() {
        String eventId = "sig-bad-ts";
        String payload = body(eventId, "order.created", null);
        ResponseEntity<String> resp = post(rest, base(), payload, "not-a-number", null, WebhookTestClient.SECRET);

        log.info("[case eventId={}] 非数字时间戳 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("TIMESTAMP_MISSING");
    }

    @Test
    void expiredTimestamp_rejectedWithDistinctCode() {
        String eventId = "sig-old-ts";
        String payload = body(eventId, "order.created", null);
        // 默认容忍窗口 300s；用 600s 前的时间戳，且签名本身正确 -> 必须报时间戳过期而非签名错误
        String oldTs = String.valueOf(System.currentTimeMillis() / 1000 - 600);
        ResponseEntity<String> resp = post(rest, base(), payload, oldTs, null, WebhookTestClient.SECRET);

        log.info("[case eventId={}] 过期时间戳 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("TIMESTAMP_EXPIRED");
        assertThat(resp.getBody()).doesNotContain("SIGNATURE_INVALID");
    }

    @Test
    void futureTimestampBeyondTolerance_rejected() {
        String eventId = "sig-future-ts";
        String payload = body(eventId, "order.created", null);
        String futureTs = String.valueOf(System.currentTimeMillis() / 1000 + 600);
        ResponseEntity<String> resp = post(rest, base(), payload, futureTs, null, WebhookTestClient.SECRET);

        log.info("[case eventId={}] 超前时间戳 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("TIMESTAMP_EXPIRED");
    }

    @Test
    void missingSignature_rejectedWithDistinctCode() {
        String eventId = "sig-no-sig";
        String payload = body(eventId, "order.created", null);
        String ts = String.valueOf(System.currentTimeMillis() / 1000);
        // timestamp 传 null 表示不加签名头；但需要自定义：直接构造
        var headers = new org.springframework.http.HttpHeaders();
        headers.add("X-Webhook-Timestamp", ts);
        ResponseEntity<String> resp = rest.postForEntity(
                base() + "/webhooks/events",
                new org.springframework.http.HttpEntity<>(payload, headers), String.class);

        log.info("[case eventId={}] 缺签名 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("SIGNATURE_MISSING");
    }

    @Test
    void wrongSecretSignature_rejectedAsSignatureInvalid() {
        String eventId = "sig-wrong-secret";
        String payload = body(eventId, "order.created", null);
        String ts = String.valueOf(System.currentTimeMillis() / 1000);
        String badSig = sign("wrong-secret", ts, payload);
        ResponseEntity<String> resp = post(rest, base(), payload, ts, badSig, WebhookTestClient.SECRET);

        log.info("[case eventId={}] 错误密钥签名 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("SIGNATURE_INVALID");
    }

    @Test
    void tamperedBody_rejectedAsSignatureInvalid() {
        String eventId = "sig-tampered";
        String signedPayload = body(eventId, "order.created", "\"orderId\":\"O-ORIG\"");
        String sentPayload = body(eventId, "order.created", "\"orderId\":\"O-TAMPERED\"");
        String ts = String.valueOf(System.currentTimeMillis() / 1000);
        String sig = sign(ts, signedPayload);
        ResponseEntity<String> resp = post(rest, base(), sentPayload, ts, sig, WebhookTestClient.SECRET);

        log.info("[case eventId={}] 负载被篡改 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().value()).isEqualTo(401);
        assertThat(resp.getBody()).contains("SIGNATURE_INVALID");
    }

    @Test
    void timestampWithinBoundary_accepted() {
        String eventId = "sig-boundary";
        String payload = body(eventId, "order.created", null);
        // 边界内侧：恰好 290s 前（窗口 300s），签名正确，应当通过
        String ts = String.valueOf(System.currentTimeMillis() / 1000 - 290);
        ResponseEntity<String> resp = post(rest, base(), payload, ts, null, WebhookTestClient.SECRET);

        log.info("[case eventId={}] 窗口内时间戳 -> http={} body={}", eventId, resp.getStatusCode(), resp.getBody());
        assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(resp.getBody()).contains("SUCCEEDED");
    }

    @Test
    void malformedJsonAndMissingFields_rejected() {
        String ts = String.valueOf(System.currentTimeMillis() / 1000);

        ResponseEntity<String> badJson = post(rest, base(), "{not-json", ts, null, WebhookTestClient.SECRET);
        log.info("[case 坏JSON] -> http={} body={}", badJson.getStatusCode(), badJson.getBody());
        assertThat(badJson.getStatusCode().value()).isEqualTo(400);
        assertThat(badJson.getBody()).contains("BAD_REQUEST");

        String missingType = "{\"eventId\":\"no-type\"}";
        ResponseEntity<String> noType = post(rest, base(), missingType, ts, null, WebhookTestClient.SECRET);
        log.info("[case 缺eventType] -> http={} body={}", noType.getStatusCode(), noType.getBody());
        assertThat(noType.getStatusCode().value()).isEqualTo(400);
        assertThat(noType.getBody()).contains("BAD_REQUEST");

        ResponseEntity<String> empty = post(rest, base(), "", ts, null, WebhookTestClient.SECRET);
        assertThat(empty.getStatusCode().value()).isEqualTo(400);
    }
}
