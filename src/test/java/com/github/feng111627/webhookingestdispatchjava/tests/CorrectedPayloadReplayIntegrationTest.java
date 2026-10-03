package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.adminGet;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.replayWithBody;
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
 * 人工修正负载重放：
 * 1) 修正负载只作用于本次重放：原始负载保留不覆盖，本次按修正内容处理成功；
 * 2) 重放结束后能查到本次实际使用的负载（payloadSource=CORRECTED + correctedPayload）；
 * 3) 修正非法（非 JSON / eventType 串改）-> 400，不改变死信；
 * 4) 修正负载携带未知嵌套/数组字段不破坏处理。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class CorrectedPayloadReplayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(CorrectedPayloadReplayIntegrationTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String base() {
        return "http://localhost:" + port;
    }

    @Test
    void correctedPayload_replaysUsingCorrection_butKeepsOriginalPayload() {
        String eventId = "cp-ok-1";
        // 首轮：订单金额错误导致始终暂时失败 -> 死信（演示"上游字段写错"）
        String original = body(eventId, "order.flaky",
                "\"fail\":\"retryable\",\"orderId\":\"O-WRONG\",\"amountCents\":1");
        ResponseEntity<String> ingest = postValid(rest, base(), original);
        log.info("[case eventId={}] 首轮(错误负载) -> {}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("DEAD");

        // 用人工修正负载重放：把 fail 改为 ok，同时修正业务字段；再带未知嵌套/数组字段
        String corrected = body(eventId, "order.flaky",
                "\"fail\":\"ok\",\"orderId\":\"O-FIXED\",\"amountCents\":19900,"
                        + "\"fixMeta\":{\"by\":\"ops\",\"refs\":[\"t-1\",\"t-2\"]}");
        ResponseEntity<String> replay = replayWithBody(rest, base(), eventId, corrected);
        log.info("[case eventId={}] 修正负载重放 -> http={} body={}",
                eventId, replay.getStatusCode(), replay.getBody());
        assertThat(replay.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(replay.getBody()).contains("SUCCEEDED").contains("CORRECTED").contains("rp-");

        // 事件详情：原始负载必须保留且仍是错误内容；重放留档记录本次用的是修正负载
        ResponseEntity<String> detail = adminGet(rest, base(), "/admin/dead-letters/" + eventId);
        log.info("[case eventId={}] 事件详情={}", eventId, detail.getBody());
        assertThat(detail.getBody())
                .contains("\"state\":\"SUCCEEDED\"")
                // 原始负载未被覆盖：仍是 retryable / O-WRONG
                .contains("O-WRONG").contains("\"fail\":\"retryable\"")
                // 本次重放留档：来源为修正负载，且能看到修正内容
                .contains("\"payloadSource\":\"CORRECTED\"")
                .contains("O-FIXED").contains("fixMeta");

        // 已从死信列表消失
        ResponseEntity<String> dead = adminGet(rest, base(), "/admin/dead-letters");
        assertThat(dead.getBody()).doesNotContain(eventId);
    }

    @Test
    void invalidCorrection_badJsonOrTypeMismatch_rejected400_withoutChangingDeadLetter() {
        String eventId = "cp-bad-1";
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 非 JSON 修正负载 -> 400
        ResponseEntity<String> notJson = replayWithBody(rest, base(), eventId, "not-a-json");
        log.info("[case eventId={}] 非JSON修正 -> http={} {}", eventId, notJson.getStatusCode(), notJson.getBody());
        assertThat(notJson.getStatusCode().value()).isEqualTo(400);
        assertThat(notJson.getBody()).contains("BAD_REQUEST");

        // eventType 串改成别的类型 -> 400（防止借重放改路由）
        String wrongType = body(eventId, "order.created", "\"fail\":\"ok\"");
        ResponseEntity<String> mismatch = replayWithBody(rest, base(), eventId, wrongType);
        log.info("[case eventId={}] 串改eventType -> http={} {}", eventId, mismatch.getStatusCode(), mismatch.getBody());
        assertThat(mismatch.getStatusCode().value()).isEqualTo(400);
        assertThat(mismatch.getBody()).contains("BAD_REQUEST");

        // eventId 与路径不一致 -> 400
        String wrongId = body("cp-someone-else", "order.flaky", "\"fail\":\"ok\"");
        ResponseEntity<String> wrongIdResp = replayWithBody(rest, base(), eventId, wrongId);
        assertThat(wrongIdResp.getStatusCode().value()).isEqualTo(400);

        // 非法重放后事件仍是 DEAD，可再次正常重放
        ResponseEntity<String> stillDead = adminGet(rest, base(), "/admin/dead-letters/" + eventId);
        assertThat(stillDead.getBody()).contains("\"state\":\"DEAD\"");

        ResponseEntity<String> fixed = replayWithBody(rest, base(), eventId,
                body(eventId, "order.flaky", "\"fail\":\"ok\""));
        assertThat(fixed.getBody()).contains("SUCCEEDED").contains("CORRECTED");
    }
}
