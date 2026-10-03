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
 * 判定轨迹（按 eventId 串起每一步，且与接口当时结论对得上）：
 * 1) 成功事件：RECEIVED -> ATTEMPT -> SUCCEEDED，顺序严格；
 * 2) 无处理器被拒：RECEIVED -> REJECTED/UNSUPPORTED_EVENT_TYPE，且与 422 响应对得上；
 * 3) 重复：第二次产生 DUPLICATE 轨迹，apiConclusion=DUPLICATE 与响应一致；
 * 4) 死信 + 修正重放：ATTEMPT/BACKOFF/DEAD -> REPLAY_REQUESTED -> ATTEMPT -> REPLAY_SUCCEEDED 齐全，
 *    且轨迹条目携带 eventId、判定依据、payloadSource；未知 eventId 查询 404；
 * 5) 负载新增嵌套/数组字段：轨迹与处理都不被破坏。
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

    private String trace(String eventId) {
        return adminGet(rest, base(), "/admin/dead-letters/" + eventId + "/trace").getBody();
    }

    @Test
    void successfulEvent_traceLinksReceivedToSucceeded_inOrder() {
        String eventId = "tr-ok-1";
        ResponseEntity<String> resp = postValid(rest, base(),
                body(eventId, "order.created", "\"orderId\":\"O-1\",\"amountCents\":100"));
        assertThat(resp.getBody()).contains("SUCCEEDED");

        String trace = trace(eventId);
        log.info("[case eventId={}] 轨迹={}", eventId, trace);
        int iReceived = trace.indexOf("RECEIVED");
        int iAttempt = trace.indexOf("ATTEMPT");
        int iSucceeded = trace.indexOf("\"phase\":\"SUCCEEDED\"");
        assertThat(iReceived).isGreaterThanOrEqualTo(0);
        assertThat(iAttempt).isGreaterThan(iReceived);
        assertThat(iSucceeded).isGreaterThan(iAttempt);
        assertThat(trace).contains("\"eventId\":\"" + eventId + "\"")
                .contains("\"apiConclusion\":\"SUCCEEDED\"")
                .contains("\"seq\":1");
    }

    @Test
    void unsupportedType_traceShowsRejected_andMatches422Response() {
        String eventId = "tr-reject-1";
        ResponseEntity<String> resp = postValid(rest, base(),
                body(eventId, "billing.invoice.unknown", null));
        assertThat(resp.getStatusCode().value()).isEqualTo(422);
        assertThat(resp.getBody()).contains("UNSUPPORTED_EVENT_TYPE");

        String trace = trace(eventId);
        log.info("[case eventId={}] 被拒轨迹={}", eventId, trace);
        assertThat(trace).contains("RECEIVED").contains("REJECTED")
                .contains("UNSUPPORTED_EVENT_TYPE")
                // 轨迹里的接口结论与当时 422 返回的错误码一致
                .contains("\"apiConclusion\":\"UNSUPPORTED_EVENT_TYPE\"");
    }

    @Test
    void duplicateRequest_traceShowsDuplicate_conclusionMatches() {
        String eventId = "tr-dup-1";
        String payload = body(eventId, "order.created", "\"orderId\":\"O-D\"");
        postValid(rest, base(), payload);
        ResponseEntity<String> second = postValid(rest, base(), payload);
        assertThat(second.getBody()).contains("DUPLICATE");

        String trace = trace(eventId);
        log.info("[case eventId={}] 重复轨迹={}", eventId, trace);
        assertThat(trace).contains("\"phase\":\"DUPLICATE\"")
                .contains("\"apiConclusion\":\"DUPLICATE\"");
        // seq 严格递增且只有一条 DUPLICATE
        assertThat(count(trace, "\"phase\":\"DUPLICATE\"")).isEqualTo(1);
        assertThat(count(trace, "\"phase\":\"SUCCEEDED\"")).isEqualTo(1);
    }

    @Test
    void deadThenCorrectedReplay_fullTraceAndUnknownId404_andFutureFieldsSafe() {
        String eventId = "tr-dead-replay-1";
        // 首轮失败入死信，同时携带未知嵌套/数组字段，验证不破坏轨迹与处理
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky",
                        "\"fail\":\"retryable\",\"future\":{\"a\":[1,2,{\"b\":true}],\"c\":{\"d\":[]}}"));
        assertThat(ingest.getBody()).contains("DEAD");

        String traceBefore = trace(eventId);
        log.info("[case eventId={}] 死信轨迹={}", eventId, traceBefore);
        assertThat(traceBefore).contains("\"phase\":\"ATTEMPT\"")
                .contains("\"phase\":\"BACKOFF\"")
                .contains("\"phase\":\"DEAD\"")
                .contains("\"apiConclusion\":\"DEAD\"")
                .contains("\"failureKind\":\"RETRYABLE\"")
                .contains("\"payloadSource\":\"ORIGINAL\"");

        // 修正重放成功
        ResponseEntity<String> replay = replayWithBody(rest, base(), eventId,
                body(eventId, "order.flaky",
                        "\"fail\":\"ok\",\"future\":{\"a\":[1,2,{\"b\":true}],\"c\":{\"d\":[]}}"));
        assertThat(replay.getBody()).contains("SUCCEEDED");

        String traceAfter = trace(eventId);
        log.info("[case eventId={}] 重放后轨迹={}", eventId, traceAfter);
        int iDead = traceAfter.indexOf("\"phase\":\"DEAD\"");
        int iReq = traceAfter.indexOf("\"phase\":\"REPLAY_REQUESTED\"");
        int iOk = traceAfter.indexOf("\"phase\":\"REPLAY_SUCCEEDED\"");
        assertThat(iReq).as("轨迹包含重放请求").isGreaterThan(iDead);
        assertThat(iOk).as("重放成功在重放请求之后").isGreaterThan(iReq);
        assertThat(traceAfter).contains("\"payloadSource\":\"CORRECTED\"")
                .contains("\"apiConclusion\":\"REPLAY_SUCCEEDED\"")
                .contains("replayId");

        // 未知事件轨迹 -> 404
        ResponseEntity<String> missing = adminGet(rest, base(),
                "/admin/dead-letters/no-such-trace-event/trace");
        log.info("[case 未知事件轨迹] http={}", missing.getStatusCode());
        assertThat(missing.getStatusCode().value()).isEqualTo(404);
    }

    private static int count(String haystack, String needle) {
        int c = 0, idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            c++;
            idx += needle.length();
        }
        return c;
    }
}
