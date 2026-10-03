package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/**
 * 事件轨迹：从接收、命中重复、类型被拒、每次尝试与退避、进入死信到最终重放，
 * 每一步都能按 eventId 串成一条轨迹，且与接口当时返回的结论对得上。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EventTraceIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(EventTraceIntegrationTest.class);

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String base() {
        return "http://localhost:" + port;
    }

    @Test
    void fullLifecycleTrace_matchesApiConclusions() {
        String eventId = "trace-full-1";
        // 1) 接收 -> 暂时失败重试到上限 -> 死信
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        log.info("[case eventId={}] 首次接收结论={}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("\"status\":\"DEAD\"");

        // 2) 重复投递 -> DUPLICATE
        ResponseEntity<String> dup = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(dup.getBody()).contains("DUPLICATE");

        // 3) 修正负载重放 -> 成功
        ResponseEntity<String> replay = replay(eventId,
                "{\"payload\":{\"eventId\":\"" + eventId + "\",\"fail\":\"ok\"}}");
        log.info("[case eventId={}] 修正负载重放结论={}", eventId, replay.getBody());
        assertThat(replay.getBody()).contains("SUCCEEDED").contains("CORRECTED");

        // 4) 轨迹：每一步都在，且顺序与结论对得上
        ResponseEntity<String> trace = rest.getForEntity(
                base() + "/admin/events/" + eventId + "/trace", String.class);
        log.info("[case eventId={}] 完整轨迹={}", eventId, trace.getBody());
        String t = trace.getBody();
        assertThat(trace.getStatusCode().is2xxSuccessful()).isTrue();
        // 接收 -> 3次尝试+2次退避 -> 死信 -> 重复 -> 重放请求/开始 -> 成功
        assertThat(t).contains("RECEIVED");
        assertThat(t).contains("ATTEMPT").contains("RETRYABLE");
        assertThat(t).contains("BACKOFF");
        assertThat(t).contains("DEAD_LETTERED").contains("RETRYABLE_EXHAUSTED");
        assertThat(t).contains("DUPLICATE");
        assertThat(t).contains("REPLAY_REQUESTED").contains("REPLAY_STARTED").contains("CORRECTED");
        assertThat(t).contains("SUCCEEDED");
        // 每条轨迹都带事件标识与判定依据
        assertThat(t).contains(eventId).contains("detail");
        // 顺序校验：RECEIVED 在 DEAD_LETTERED 前，DEAD_LETTERED 在 REPLAY_STARTED 前，最后才是 SUCCEEDED
        assertThat(t.indexOf("RECEIVED")).isLessThan(t.indexOf("DEAD_LETTERED"));
        assertThat(t.indexOf("DEAD_LETTERED")).isLessThan(t.indexOf("REPLAY_STARTED"));
        assertThat(t.indexOf("REPLAY_STARTED")).isLessThan(t.lastIndexOf("SUCCEEDED"));
    }

    @Test
    void rejectedUnsupportedType_hasTraceEvenWithoutStoredEvent() {
        String eventId = "trace-reject-1";
        // 无处理器类型：422 拒绝、不入库，但轨迹必须可查（判定不能查不到）
        ResponseEntity<String> rejected = postValid(rest, base(),
                body(eventId, "billing.unknown", null));
        log.info("[case eventId={}] 无处理器类型 -> http={} body={}", eventId,
                rejected.getStatusCode(), rejected.getBody());
        assertThat(rejected.getStatusCode().value()).isEqualTo(422);
        assertThat(rejected.getBody()).contains("UNSUPPORTED_EVENT_TYPE");

        // 事件未入库：详情 404
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        assertThat(detail.getStatusCode().value()).isEqualTo(404);

        // 但轨迹可查，且结论与接口返回一致
        ResponseEntity<String> trace = rest.getForEntity(
                base() + "/admin/events/" + eventId + "/trace", String.class);
        log.info("[case eventId={}] 被拒事件轨迹={}", eventId, trace.getBody());
        assertThat(trace.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(trace.getBody()).contains("REJECTED").contains("UNSUPPORTED_EVENT_TYPE");
    }

    @Test
    void traceOfUnknownEvent_is404() {
        ResponseEntity<String> trace = rest.getForEntity(
                base() + "/admin/events/never-seen-evt/trace", String.class);
        log.info("[case] 未知事件轨迹 -> http={}", trace.getStatusCode());
        assertThat(trace.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void newFieldsInPayload_doNotBreakTraceOrHandling() {
        String eventId = "trace-compat-1";
        // 负载带新增嵌套对象与数组字段：处理与轨迹都必须正常
        String payload = body(eventId, "order.flaky",
                "\"fail\":\"retry-then-ok\",\"failUntil\":1,"
                        + "\"brandNew\":{\"nested\":{\"deep\":[1,2,{\"x\":true}]},\"arr\":[{\"k\":\"v\"}]}");
        ResponseEntity<String> ingest = postValid(rest, base(), payload);
        log.info("[case eventId={}] 新增字段负载 -> {}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("SUCCEEDED");

        ResponseEntity<String> trace = rest.getForEntity(
                base() + "/admin/events/" + eventId + "/trace", String.class);
        log.info("[case eventId={}] 新增字段事件轨迹={}", eventId, trace.getBody());
        assertThat(trace.getBody()).contains("RECEIVED").contains("ATTEMPT")
                .contains("SUCCEEDED").contains(eventId);

        // 详情里原始负载完整保留新增字段
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        assertThat(detail.getBody()).contains("brandNew").contains("nested");
    }

    @Test
    void replayFailureTrace_explainsWhyStillDead() {
        String eventId = "trace-still-dead-1";
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 重放仍失败：接口结论 DEAD
        ResponseEntity<String> replay = replay(eventId, null);
        log.info("[case eventId={}] 重放仍失败结论={}", eventId, replay.getBody());
        assertThat(replay.getBody()).contains("\"status\":\"DEAD\"");

        // 轨迹能解释：重放被受理(REPLAY_STARTED/ORIGINAL) -> 又一轮尝试 -> 再次死信
        ResponseEntity<String> trace = rest.getForEntity(
                base() + "/admin/events/" + eventId + "/trace", String.class);
        log.info("[case eventId={}] 重放失败轨迹={}", eventId, trace.getBody());
        String t = trace.getBody();
        assertThat(t).contains("REPLAY_STARTED").contains("ORIGINAL");
        // 两轮死信（首轮 + 重放轮）
        int firstDead = t.indexOf("DEAD_LETTERED");
        int secondDead = t.indexOf("DEAD_LETTERED", firstDead + 1);
        assertThat(secondDead).isGreaterThan(firstDead);
        // 死信列表里失败性质/尝试次数/最后错误可解释
        ResponseEntity<String> dead = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        assertThat(dead.getBody()).contains(eventId).contains("RETRYABLE").contains("lastError");
    }

    private ResponseEntity<String> replay(String eventId, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        return rest.exchange(base() + "/admin/dead-letters/" + eventId + "/replay",
                HttpMethod.POST, new HttpEntity<>(jsonBody, headers), String.class);
    }
}
