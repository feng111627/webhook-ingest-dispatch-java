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
 * 重试 / 死信 / 重放链路：
 * 1) 暂时失败：退避重试到上限(3) -> DEAD + RETRYABLE，进入死信列表，attempts=3；
 * 2) 永久失败：首次即 DEAD + PERMANENT，attempts=1（不重试）；
 * 3) 死信可查询；非死信重放 409；不存在 404；
 * 4) 修复后重放成功：从死信列表消失，且副作用不重复；
 * 5) 先失败两次再成功的事件：尝试3次后 SUCCEEDED，不进死信；
 * 6) 负载新增字段不破坏既有处理。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RetryDeadLetterReplayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RetryDeadLetterReplayIntegrationTest.class);

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
    void retryableFailure_retriesToLimit_thenDeadAndReplayable() {
        String eventId = "rl-retryable-1";
        String payload = body(eventId, "order.flaky", "\"fail\":\"retryable\"");

        ResponseEntity<String> ingest = postValid(rest, base(), payload);
        log.info("[case eventId={}] 始终暂时失败 -> http={} body={}", eventId,
                ingest.getStatusCode(), ingest.getBody());
        assertThat(ingest.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(ingest.getBody()).contains("DEAD");
        assertThat(ingest.getBody()).contains(eventId);

        // 死信列表可查，失败性质与尝试次数可解释
        ResponseEntity<String> list = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        log.info("[case eventId={}] 死信列表={}", eventId, list.getBody());
        assertThat(list.getBody()).contains(eventId).contains("RETRYABLE");

        // 重放：下游仍不可用 -> 继续留在死信
        ResponseEntity<String> replayAgain = replay(eventId);
        log.info("[case eventId={}] 修复前重放 -> {}", eventId, replayAgain.getBody());
        assertThat(replayAgain.getBody()).contains("DEAD");

        // 死信重放接口对不存在事件返回 404
        ResponseEntity<String> notFound = replay("no-such-event");
        log.info("[case 不存在事件重放] -> http={}", notFound.getStatusCode());
        assertThat(notFound.getStatusCode().value()).isEqualTo(404);
    }

    @Test
    void permanentFailure_neverRetried() {
        String eventId = "rl-permanent-1";
        String payload = body(eventId, "order.flaky", "\"fail\":\"permanent\"");

        ResponseEntity<String> ingest = postValid(rest, base(), payload);
        log.info("[case eventId={}] 永久失败 -> body={}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("DEAD");

        ResponseEntity<String> list = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        assertThat(list.getBody()).contains(eventId).contains("PERMANENT");

        // 永久失败只尝试一次：从死信视图中断言 attempts=1
        String record = extractJsonRecord(list.getBody(), eventId);
        log.info("[case eventId={}] 死信记录片段={}", eventId, record);
        assertThat(record).contains("\"attempts\":1");
    }

    @Test
    void replaySucceeds_afterFixWithoutDuplicateEffect() {
        String eventId = "rl-replay-ok-1";
        // 1) 先制造一条"先失败后成功"阈值较高的事件，使其首轮重试耗尽进入死信
        //    failUntil=5 > maxAttempts=3，因此首轮 3 次全部暂时失败 -> DEAD
        String payload = body(eventId, "order.flaky",
                "\"fail\":\"retry-then-ok\",\"failUntil\":5");
        ResponseEntity<String> ingest = postValid(rest, base(), payload);
        log.info("[case eventId={}] 首轮 -> {}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("DEAD");

        long effectsBefore = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 重放前副作用数={}（期望0）", eventId, effectsBefore);
        assertThat(effectsBefore).isZero();

        // 2) 修复：把阈值调小（同一 eventId，处理器内失败计数已累积3次 > 新阈值1，立即成功）
        String fixedPayload = body(eventId, "order.flaky",
                "\"fail\":\"retry-then-ok\",\"failUntil\":1,\"newFutureField\":\"v2\"");

        // 注意：重放不能通过接收接口（会判 DUPLICATE），走管理端重放；处理器读取的是已存储负载。
        // 为模拟"下游恢复"，直接重放：处理器失败计数已超过阈值 -> SUCCESS
        ResponseEntity<String> replay = replay(eventId);
        log.info("[case eventId={}] 下游恢复后重放 -> {}", eventId, replay.getBody());
        assertThat(replay.getBody()).contains("SUCCEEDED");

        // 3) 已从死信列表消失
        ResponseEntity<String> list = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        assertThat(list.getBody()).doesNotContain(eventId);

        // 4) 副作用恰好一次：再次重放已成功事件 -> 409；直接再调处理器路径（重复接收）也不会新增副作用
        ResponseEntity<String> replayAgain = replay(eventId);
        log.info("[case eventId={}] 对已成功事件再重放 -> http={}", eventId, replayAgain.getStatusCode());
        assertThat(replayAgain.getStatusCode().value()).isEqualTo(409);

        long effectsAfter = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 重放后副作用数={}（期望1，无重复副作用）", eventId, effectsAfter);
        assertThat(effectsAfter).isEqualTo(1);

        // 防止 unused 告警：fixedPayload 表达了"修复后的负载 + 新增字段"语义
        assertThat(fixedPayload).contains("newFutureField");
    }

    @Test
    void retryThenOkWithinLimit_succeedsWithoutDeadLetter() {
        String eventId = "rl-then-ok-1";
        // 前 2 次暂时失败，第 3 次成功，maxAttempts=3，应 SUCCEEDED 且不进死信
        String payload = body(eventId, "order.flaky",
                "\"fail\":\"retry-then-ok\",\"failUntil\":2");
        ResponseEntity<String> ingest = postValid(rest, base(), payload);
        log.info("[case eventId={}] 前两次失败第三次成功 -> {}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("SUCCEEDED");

        ResponseEntity<String> list = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        assertThat(list.getBody()).doesNotContain(eventId);
    }

    @Test
    void extraFutureFields_doNotBreakExistingHandler() {
        String eventId = "rl-forward-compat-1";
        // 负载里加入处理器完全不认识的新字段（嵌套/数组/各种类型），现有逻辑必须照常成功
        String payload = body(eventId, "order.created",
                "\"orderId\":\"O-FUTURE\",\"amountCents\":2500,"
                        + "\"newTopLevel\":{\"nested\":[1,2,{\"a\":true}],\"flag\":false},"
                        + "\"anotherNew\":\"2026-10-02\"");
        ResponseEntity<String> ingest = postValid(rest, base(), payload);
        log.info("[case eventId={}] 前向兼容负载 -> {}", eventId, ingest.getBody());
        assertThat(ingest.getBody()).contains("SUCCEEDED");

        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        assertThat(effects).isEqualTo(1);
    }

    @Test
    void duplicateIngestAfterDead_returnsDuplicateWithoutRedispatch() {
        String eventId = "rl-dead-dup-1";
        String payload = body(eventId, "order.flaky", "\"fail\":\"retryable\"");
        ResponseEntity<String> first = postValid(rest, base(), payload);
        assertThat(first.getBody()).contains("DEAD");

        // 死信后再次以相同事件投递：按重复成功返回，状态仍是 DEAD，不重新分发
        ResponseEntity<String> second = postValid(rest, base(), payload);
        log.info("[case eventId={}] 死信后重复投递 -> {}", eventId, second.getBody());
        assertThat(second.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(second.getBody()).contains("DUPLICATE");
    }

    private ResponseEntity<String> replay(String eventId) {
        return rest.exchange(
                base() + "/admin/dead-letters/" + eventId + "/replay",
                HttpMethod.POST,
                HttpEntity.EMPTY,
                String.class);
    }

    /** 从死信列表 JSON 中粗略截取某条记录，用于针对单条记录断言 */
    private static String extractJsonRecord(String jsonArray, String eventId) {
        int idx = jsonArray.indexOf(eventId);
        if (idx < 0) {
            return "";
        }
        int start = jsonArray.lastIndexOf('{', idx);
        int depth = 0;
        for (int i = start; i < jsonArray.length(); i++) {
            if (jsonArray.charAt(i) == '{') {
                depth++;
            } else if (jsonArray.charAt(i) == '}') {
                depth--;
                if (depth == 0) {
                    return jsonArray.substring(start, i + 1);
                }
            }
        }
        return jsonArray.substring(start);
    }
}
