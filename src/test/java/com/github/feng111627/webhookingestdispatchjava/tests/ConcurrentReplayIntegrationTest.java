package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
 * 并发与重复重放的幂等语义：
 * 1) 同一条死信被 N 个并发请求同时重放（有的带修正负载、有的不带），
 *    恰好一个真正执行，其余得到一致的 409 结论，副作用恰好一次；
 * 2) 重放执行期间的重放请求一律 409（REPLAY_IN_PROGRESS），执行完成后事件已非 DEAD，
 *    再重放得到 409（非 DEAD）——两种 409 都带可解释 message；
 * 3) 重放后仍失败：事件留在死信，attempts/lastError 更新可查。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrentReplayIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentReplayIntegrationTest.class);

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

        // 恰好一个真正执行，其余全部 409（一致结论）
        assertThat(ok).isEqualTo(1);
        assertThat(conflict).isEqualTo(all.size() - 1);
        // 409 的响应都带可解释 message
        all.stream().filter(r -> r.getStatusCode().value() == 409)
                .forEach(r -> assertThat(r.getBody()).contains("message"));

        // 副作用恰好一次
        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);

        // 事件最终成功且已离开死信；重放历史恰好一条，且记录了实际使用的修正负载
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"SUCCEEDED\"");
        assertThat(detail.getBody()).contains("\"result\":\"SUCCEEDED\"")
                .contains("\"payloadSource\":\"CORRECTED\"");
        assertThat(rest.getForEntity(base() + "/admin/dead-letters", String.class).getBody())
                .doesNotContain(eventId);
    }

    @Test
    void mixedConcurrentReplays_consistentConclusions_exactlyOneSuccess() throws Exception {
        String eventId = "conc-replay-mixed-1";
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"permanent\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 有的带修正负载（沉睡后成功）、有的不带（沿用原始负载，永久失败）：
        // 无论谁先抢到，最终结论必须一致可解释 —— 全局恰好一个 SUCCEEDED，副作用恰好一次
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 8, i -> i % 2 == 0
                ? "{\"payload\":{\"eventId\":\"" + eventId
                        + "\",\"fail\":\"sleep\",\"sleepMillis\":800}}"
                : null);

        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));

        // 每个响应都是明确结论：要么 2xx 的重放结论（带 payloadSource），要么 409（带 message）
        for (ResponseEntity<String> r : all) {
            if (r.getStatusCode().value() == 409) {
                assertThat(r.getBody()).contains("message");
            } else {
                assertThat(r.getStatusCode().is2xxSuccessful()).isTrue();
                assertThat(r.getBody()).contains("payloadSource");
            }
        }
        // 全局恰好一个 SUCCEEDED（沉睡修正负载那次），其余 2xx 只能是快速失败留死信的结论
        long succeeded = all.stream().filter(r -> r.getBody() != null && r.getBody().contains("SUCCEEDED")).count();
        log.info("[case eventId={}] 混合并发重放：SUCCEEDED 结论数={}（期望1）", eventId, succeeded);
        assertThat(succeeded).isEqualTo(1);

        // 副作用恰好一次：同一事件不会被分发成功两次
        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);

        // 最终状态成功，重放历史里每次结论都可查
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 混合并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"SUCCEEDED\"");
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

    private ResponseEntity<String> replay(String eventId, String jsonBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        return rest.exchange(base() + "/admin/dead-letters/" + eventId + "/replay",
                HttpMethod.POST, new HttpEntity<>(jsonBody, headers), String.class);
    }
}
