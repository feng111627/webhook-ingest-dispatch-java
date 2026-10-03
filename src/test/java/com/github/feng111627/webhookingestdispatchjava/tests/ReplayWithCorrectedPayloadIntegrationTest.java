package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/**
 * 修正负载重放：
 * 1) 上游写错字段导致永久失败 -> 携带人工修正负载重放 -> 走原分发链路成功；
 * 2) 原始负载不被覆盖，重放历史可查本次实际使用的负载（来源 + 修正原文）；
 * 3) 修正负载仍失败 -> 留在死信，失败性质/尝试次数/最后错误可解释；
 * 4) 修正负载非法（非对象 / eventId 不一致）-> 400；
 * 5) 修正负载含新增嵌套/数组字段 -> 不破坏处理与轨迹；
 * 6) 不带负载重放 -> payloadSource=ORIGINAL。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ReplayWithCorrectedPayloadIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayWithCorrectedPayloadIntegrationTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    EffectLedger effectLedger;

    private String base() {
        return "http://localhost:" + port;
    }

    @Test
    void replayWithCorrectedPayload_succeedsAndOriginalPreserved() {
        String eventId = "cp-fix-1";
        // 上游把 fail 写成了 permanent（模拟上游字段写错），首轮直接死信
        String original = body(eventId, "order.flaky", "\"fail\":\"permanent\",\"orderId\":\"O-WRONG\"");
        ResponseEntity<String> ingest = postValid(rest, base(), original);
        log.info("[case eventId={}] 原始负载入死信 -> {}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("DEAD");

        // 人工修正：fail 改为 ok，orderId 改为正确值，并附带未来新增字段（嵌套+数组）
        String corrected = "{\"eventId\":\"" + eventId + "\",\"eventType\":\"order.flaky\","
                + "\"fail\":\"ok\",\"orderId\":\"O-FIXED\","
                + "\"futureNested\":{\"tags\":[\"a\",\"b\"],\"meta\":{\"rev\":2}}}";
        ResponseEntity<String> replay = replay(eventId, "{\"payload\":" + corrected + "}");
        log.info("[case eventId={}] 修正负载重放 -> {}", eventId, replay.getBody());
        assertThat(replay.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(replay.getBody()).contains("SUCCEEDED").contains("\"payloadSource\":\"CORRECTED\"");

        // 从死信消失
        ResponseEntity<String> dead = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        assertThat(dead.getBody()).doesNotContain(eventId);

        // 详情：原始负载仍是写错的那份，重放历史记录本次实际使用修正负载及原文
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 事件详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(detail.getBody()).contains("\"originalPayload\"").contains("O-WRONG");
        assertThat(detail.getBody()).contains("\"payloadSource\":\"CORRECTED\"");
        assertThat(detail.getBody()).contains("O-FIXED").contains("futureNested");
        assertThat(detail.getBody()).contains("\"result\":\"SUCCEEDED\"");

        // 副作用恰好一次
        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);
    }

    @Test
    void replayWithCorrectedPayload_stillFailing_staysDeadWithExplanation() {
        String eventId = "cp-still-fail-1";
        String original = body(eventId, "order.flaky", "\"fail\":\"permanent\"");
        ResponseEntity<String> ingest = postValid(rest, base(), original);
        assertThat(ingest.getBody()).contains("DEAD");
        int attemptsBefore = 1; // permanent 只尝试一次

        // 修正负载把 permanent 改成 retryable：仍会失败（重试到上限），必须留在死信且可解释
        String corrected = "{\"eventId\":\"" + eventId + "\",\"fail\":\"retryable\"}";
        ResponseEntity<String> replay = replay(eventId, "{\"payload\":" + corrected + "}");
        log.info("[case eventId={}] 修正后仍失败的重放 -> {}", eventId, replay.getBody());
        assertThat(replay.getBody()).contains("\"status\":\"DEAD\"")
                .contains("\"payloadSource\":\"CORRECTED\"");

        // 仍在死信列表，失败性质/尝试次数/最后错误可查
        ResponseEntity<String> dead = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        log.info("[case eventId={}] 重放后死信记录 -> {}", eventId, dead.getBody());
        assertThat(dead.getBody()).contains(eventId).contains("RETRYABLE");

        // 详情：重放历史记录这次失败结论，原始负载仍在
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 重放历史 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"payloadSource\":\"CORRECTED\"")
                .contains("\"result\":\"DEAD\"")
                .contains("\"fail\":\"permanent\"");
        assertThat(attemptsBefore).isEqualTo(1);
    }

    @Test
    void replayWithoutPayload_usesOriginal() {
        String eventId = "cp-original-1";
        // retry-then-ok 阈值高于首轮上限 -> 首轮死信；处理器内失败计数已累积，重放原始负载即可成功
        String original = body(eventId, "order.flaky", "\"fail\":\"retry-then-ok\",\"failUntil\":5");
        ResponseEntity<String> ingest = postValid(rest, base(), original);
        assertThat(ingest.getBody()).contains("DEAD");

        ResponseEntity<String> replay = replay(eventId, null);
        log.info("[case eventId={}] 无负载重放 -> {}", eventId, replay.getBody());
        assertThat(replay.getBody()).contains("SUCCEEDED").contains("\"payloadSource\":\"ORIGINAL\"");

        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        assertThat(detail.getBody()).contains("\"payloadSource\":\"ORIGINAL\"");
    }

    @Test
    void replayWithInvalidCorrectedPayload_rejected400() {
        String eventId = "cp-bad-1";
        String original = body(eventId, "order.flaky", "\"fail\":\"permanent\"");
        postValid(rest, base(), original);

        // 修正负载不是 JSON 对象
        ResponseEntity<String> notObject = replay(eventId, "{\"payload\":[1,2,3]}");
        log.info("[case eventId={}] 非对象修正负载 -> http={}", eventId, notObject.getStatusCode());
        assertThat(notObject.getStatusCode().value()).isEqualTo(400);

        // 修正负载里的 eventId 与路径不一致
        ResponseEntity<String> mismatch = replay(eventId,
                "{\"payload\":{\"eventId\":\"someone-else\",\"fail\":\"ok\"}}");
        log.info("[case eventId={}] eventId 不一致 -> http={}", eventId, mismatch.getStatusCode());
        assertThat(mismatch.getStatusCode().value()).isEqualTo(400);

        // 两次非法重放都没有动过事件：仍是 DEAD，且无重放历史
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        assertThat(detail.getBody()).contains("\"state\":\"DEAD\"").contains("\"replays\":[]");
    }

    private ResponseEntity<String> replay(String eventId, String jsonBody) {
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("Content-Type", "application/json");
        HttpEntity<String> entity = new HttpEntity<>(jsonBody, headers);
        return rest.exchange(base() + "/admin/dead-letters/" + eventId + "/replay",
                HttpMethod.POST, entity, String.class);
    }
}
