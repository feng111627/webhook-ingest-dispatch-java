package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 * 并发与重复重放的幂等语义（确定性设计：用 sleepMillis 撑开"执行中"窗口，
 * 并轮询确认在途后再打并发，可反复连跑不偶发失败）：
 * 1) 同一条死信被 N 个并发请求同时重放：恰好一个真正执行，其余全部拿到
 *    完全一致的 409（REPLAY_IN_PROGRESS）结论，副作用恰好一次，重放历史恰好一条；
 * 2) 带修正负载与不带修正负载混合的并发重放：赢家在途期间，其余请求无论是否
 *    携带修正负载，一律得到同一个 409 结论；赢家成功后再重放得到 409（非 DEAD），
 *    拒绝消息中的状态与事件真实状态一致（不自相矛盾）；
 * 3) 重放仍失败：事件留在死信，failureKind/attempts/lastError 更新可查，
 *    并发请求结束后单独再发起重放不受前面状态影响，可正常执行并成功。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrentReplayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentReplayIntegrationTest.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
    void concurrentReplays_onlyOneExecutes_noDuplicateEffects() throws Exception {
        String eventId = "conc-replay-1";
        // 首轮永久失败进死信（快速，不拖慢用例）
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"permanent\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 全部线程都携带会沉睡的修正负载：赢家的执行窗口被撑开，
        // 其余并发请求必然落在"处理中"窗口内 -> 确定性的 1 成功 + N-1 个 409
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 8, i ->
                "{\"payload\":{\"eventId\":\"" + eventId
                        + "\",\"fail\":\"sleep\",\"sleepMillis\":1200}}");

        long ok = all.stream().filter(r -> r.getStatusCode().is2xxSuccessful()).count();
        long conflict = all.stream().filter(r -> r.getStatusCode().value() == 409).count();
        log.info("[case eventId={}] 并发重放 {} 个请求 -> 成功执行 {} 个，409 拒绝 {} 个",
                eventId, all.size(), ok, conflict);
        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));

        // 恰好一个真正执行，其余全部 409
        assertThat(ok).isEqualTo(1);
        assertThat(conflict).isEqualTo(all.size() - 1);

        // 所有被拒请求拿到完全一致的拒绝结论（稳定、可解释，且与在途状态一致）
        List<String> rejectBodies = all.stream()
                .filter(r -> r.getStatusCode().value() == 409)
                .map(ResponseEntity::getBody)
                .toList();
        assertThat(rejectBodies).isNotEmpty();
        rejectBodies.forEach(b -> assertThat(b).isEqualTo(rejectBodies.get(0)));
        assertThat(rejectBodies.get(0)).contains("message").contains("已有重放在执行");

        // 副作用恰好一次
        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);

        // 重放历史恰好一条（只有一个真正执行），事件最终成功且已离开死信
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"SUCCEEDED\"");
        assertThat(detail.getBody()).contains("\"result\":\"SUCCEEDED\"")
                .contains("\"payloadSource\":\"CORRECTED\"");
        assertThat(replayCount(detail)).isEqualTo(1);
        assertThat(rest.getForEntity(base() + "/admin/dead-letters", String.class).getBody())
                .doesNotContain(eventId);

        // 已成功的事件再重放：409，消息中的状态与真实状态一致（SUCCEEDED，不自相矛盾）
        ResponseEntity<String> after = replay(eventId, null);
        log.info("[case eventId={}] 成功后再重放 -> http={} body={}", eventId, after.getStatusCode(), after.getBody());
        assertThat(after.getStatusCode().value()).isEqualTo(409);
        assertThat(after.getBody()).contains("SUCCEEDED").contains("只有 DEAD 状态允许重放");
    }

    @Test
    void mixedConcurrentReplays_consistentConclusions_exactlyOneSuccess() throws Exception {
        String eventId = "conc-replay-mixed-1";
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"permanent\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 先单独发起一个携带修正负载（沉睡后成功）的重放作为"赢家"，
        // 并轮询确认它已在途 —— 之后打出的并发请求必然落在执行窗口内，用例确定
        ExecutorService winnerPool = Executors.newSingleThreadExecutor();
        Future<ResponseEntity<String>> winner = winnerPool.submit(() -> replay(eventId,
                "{\"payload\":{\"eventId\":\"" + eventId + "\",\"fail\":\"ok\",\"sleepMillis\":1500}}"));
        awaitProcessing(eventId);

        // 混合并发：一半带修正负载（若执行会成功）、一半不带（若执行会永久失败）。
        // 无论谁先谁后、带不带负载，都必须得到同一个 409 结论，且一个都不会真正执行
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 8, i -> i % 2 == 0
                ? "{\"payload\":{\"eventId\":\"" + eventId + "\",\"fail\":\"ok\"}}"
                : null);

        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));
        List<String> rejectBodies = all.stream().map(ResponseEntity::getBody).toList();
        for (ResponseEntity<String> r : all) {
            assertThat(r.getStatusCode().value()).isEqualTo(409);
        }
        // 所有拒绝结论逐字节一致，且明确指向"已有重放在执行"
        rejectBodies.forEach(b -> assertThat(b).isEqualTo(rejectBodies.get(0)));
        assertThat(rejectBodies.get(0)).contains("message").contains("已有重放在执行");

        // 赢家完成：恰好一个 SUCCEEDED，副作用恰好一次，重放历史恰好一条
        ResponseEntity<String> won = winner.get(10, TimeUnit.SECONDS);
        winnerPool.shutdown();
        log.info("[case eventId={}] 赢家重放 -> http={} body={}", eventId, won.getStatusCode(), won.getBody());
        assertThat(won.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(won.getBody()).contains("SUCCEEDED").contains("\"payloadSource\":\"CORRECTED\"");

        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);

        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 混合并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"SUCCEEDED\"");
        assertThat(replayCount(detail)).isEqualTo(1);
    }

    @Test
    void concurrentReplayStillFails_staysDead_thenSoloReplayWorks() throws Exception {
        String eventId = "conc-replay-still-fail-1";
        // 始终暂时失败：首轮 3 次尝试耗尽进死信
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 并发重放（修正负载：沉睡撑开窗口后仍然暂时失败）：
        // 恰好一个执行且仍失败，其余全部拿到一致的 409
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 6, i ->
                "{\"payload\":{\"eventId\":\"" + eventId
                        + "\",\"fail\":\"retryable\",\"sleepMillis\":800}}");
        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));

        long ok = all.stream().filter(r -> r.getStatusCode().is2xxSuccessful()).count();
        long conflict = all.stream().filter(r -> r.getStatusCode().value() == 409).count();
        log.info("[case eventId={}] 并发重放(仍失败) -> 执行 {} 个，409 拒绝 {} 个", eventId, ok, conflict);
        assertThat(ok).isEqualTo(1);
        assertThat(conflict).isEqualTo(all.size() - 1);
        List<String> rejectBodies = all.stream()
                .filter(r -> r.getStatusCode().value() == 409)
                .map(ResponseEntity::getBody)
                .toList();
        rejectBodies.forEach(b -> assertThat(b).isEqualTo(rejectBodies.get(0)));
        assertThat(rejectBodies.get(0)).contains("已有重放在执行");

        // 执行的那个返回"仍失败"结论
        ResponseEntity<String> executed = all.stream()
                .filter(r -> r.getStatusCode().is2xxSuccessful()).findFirst().orElseThrow();
        assertThat(executed.getBody()).contains("\"status\":\"DEAD\"");

        // 失败边界：事件留在死信，失败性质/尝试次数/最后错误可查
        ResponseEntity<String> dead = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        log.info("[case eventId={}] 并发重放后死信记录 -> {}", eventId, dead.getBody());
        assertThat(dead.getBody()).contains(eventId).contains("RETRYABLE")
                .contains("simulated downstream unavailable");
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"DEAD\"")
                .contains("\"failureKind\":\"RETRYABLE\"")
                .contains("\"attempts\":3")
                .contains("\"result\":\"DEAD\"");
        assertThat(replayCount(detail)).isEqualTo(1);

        // 并发请求全部结束后，单独再发起一次重放：不被前面并发留下的状态卡住，正常执行并成功
        ResponseEntity<String> solo = replay(eventId,
                "{\"payload\":{\"eventId\":\"" + eventId + "\",\"fail\":\"ok\"}}");
        log.info("[case eventId={}] 并发结束后单独重放 -> http={} body={}",
                eventId, solo.getStatusCode(), solo.getBody());
        assertThat(solo.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(solo.getBody()).contains("SUCCEEDED").contains("\"payloadSource\":\"CORRECTED\"");

        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);
        assertThat(rest.getForEntity(base() + "/admin/dead-letters", String.class).getBody())
                .doesNotContain(eventId);
    }

    @Test
    void replayAfterReplayFailed_staysDeadWithExplanation() {
        String eventId = "conc-replay-fail-1";
        // 始终暂时失败：首轮 3 次尝试耗尽进死信
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 重放仍失败：留在死信，结论可解释
        ResponseEntity<String> replay = replay(eventId, null);
        log.info("[case eventId={}] 重放仍失败 -> {}", eventId, replay.getBody());
        assertThat(replay.getBody()).contains("\"status\":\"DEAD\"");

        ResponseEntity<String> dead = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        log.info("[case eventId={}] 重放后死信记录 -> {}", eventId, dead.getBody());
        assertThat(dead.getBody()).contains(eventId).contains("RETRYABLE");

        // 详情里能看到重放历史：两次（首轮+重放）都留下结论与尝试次数
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 重放历史 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"DEAD\"")
                .contains("\"result\":\"DEAD\"")
                .contains("\"lastError\"");
    }

    /** 轮询直到事件进入 PROCESSING（重放已在途），超时则失败 —— 保证后续并发请求落在执行窗口内 */
    private void awaitProcessing(String eventId) throws Exception {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            String detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class).getBody();
            if (detail != null && detail.contains("\"state\":\"PROCESSING\"")) {
                log.info("[case eventId={}] 赢家重放已在途(state=PROCESSING)，开始打并发", eventId);
                return;
            }
            Thread.sleep(25);
        }
        throw new IllegalStateException("等待重放在途超时: " + eventId);
    }

    /** 详情中的重放历史条数（用于断言"恰好一个真正执行"） */
    private int replayCount(ResponseEntity<String> detail) throws Exception {
        JsonNode replays = MAPPER.readTree(detail.getBody()).get("replays");
        return replays == null ? 0 : replays.size();
    }

    /** 同时发出 count 个重放请求（bodyFn 按序号给出请求体，null 表示不带体），收集全部响应 */
    private List<ResponseEntity<String>> fireConcurrentReplays(
            String eventId, int count, java.util.function.IntFunction<String> bodyFn) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        CountDownLatch ready = new CountDownLatch(count);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<ResponseEntity<String>> responses = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < count; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    ready.countDown();
                    go.await(5, TimeUnit.SECONDS);
                    responses.add(replay(eventId, bodyFn.apply(idx)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
        return List.copyOf(responses);
    }

    private ResponseEntity<String> replay(String eventId, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        return rest.exchange(base() + "/admin/dead-letters/" + eventId + "/replay",
                HttpMethod.POST, new HttpEntity<>(jsonBody, headers), String.class);
    }
}
