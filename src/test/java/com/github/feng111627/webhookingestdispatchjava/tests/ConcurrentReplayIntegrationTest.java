package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.adminGet;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.replayEmpty;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.replayWithBody;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
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
import org.springframework.http.ResponseEntity;

/**
 * 重放并发与幂等：
 * 1) 多人/多脚本同时对同一条死信重放（全部带相同修正负载）：
 *    恰好一个真正执行（2xx SUCCEEDED），其余全部 409；副作用恰好一次；重放留档恰好一条；
 * 2) 带/不带修正负载混合并发：仍恰好一个真正执行，其余 409 结论一致，留档唯一；
 *    （赢家可能是修正负载或原始负载，按赢家分支断言）；
 * 3) 重放仍失败：事件继续留在死信，failureKind/attempts/lastError 可解释，且之后仍可再次重放成功；
 * 4) 成功后再点重放：409（非 DEAD 不允许）。
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
    void concurrentReplaysAllCorrected_onlyOneExecutes_singleEffectSingleHistory() throws Exception {
        String eventId = "cr-all-corrected-1";
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        int threads = 16;
        String correction = body(eventId, "order.flaky", "\"fail\":\"ok\"");
        Result result = fireConcurrent(threads, i -> replayWithBody(rest, base(), eventId, correction));

        long ok = result.countStatus(2);
        long conflict = result.countStatus(409);
        log.info("[case eventId={}] 全修正负载并发重放 threads={} 2xx={}(期望1) 409={}(期望{})",
                eventId, threads, ok, conflict, threads - 1);
        assertThat(ok).isEqualTo(1);
        assertThat(conflict).isEqualTo(threads - 1);
        // 只匹配重放响应 JSON 的 status 字段；409 错误体没有该字段，避免把错误信息里的状态词误计
        assertThat(result.bodies.stream().filter(b -> b.contains("\"status\":\"SUCCEEDED\"")).count())
                .isEqualTo(1);

        ResponseEntity<String> detail = adminGet(rest, base(), "/admin/dead-letters/" + eventId);
        assertThat(detail.getBody()).contains("\"state\":\"SUCCEEDED\"");
        assertThat(countOccurrences(detail.getBody(), "\"replayId\":\"rp-")).isEqualTo(1);
        assertThat(detail.getBody()).contains("\"payloadSource\":\"CORRECTED\"");

        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        assertThat(effects).as("副作用恰好一次").isEqualTo(1);
    }

    @Test
    void concurrentReplaysMixed_onlyOneExecutes_othersConsistent409() throws Exception {
        String eventId = "cr-mixed-1";
        // failUntil 很高：若赢家是空体（原始负载），本次重放仍失败留 DEAD；若是修正负载(fail=ok)，则成功
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retry-then-ok\",\"failUntil\":99"));
        assertThat(ingest.getBody()).contains("DEAD");

        int corrected = 8;
        int original = 8;
        int threads = corrected + original;
        String correction = body(eventId, "order.flaky", "\"fail\":\"ok\"");
        Result result = fireConcurrent(threads, i -> i < corrected
                ? replayWithBody(rest, base(), eventId, correction)
                : replayEmpty(rest, base(), eventId));

        long ok = result.countStatus(2);
        long conflict = result.countStatus(409);
        log.info("[case eventId={}] 混合并发重放 threads={} 2xx={}(期望1) 409={}(期望{}) 响应={}",
                eventId, threads, ok, conflict, threads - 1, result.bodies);
        assertThat(ok).as("无论带不带修正负载，只有一个请求真正执行").isEqualTo(1);
        assertThat(conflict).as("其余并发请求一律 409，结论一致").isEqualTo(threads - 1);

        ResponseEntity<String> detail = adminGet(rest, base(), "/admin/dead-letters/" + eventId);
        String d = detail.getBody();
        assertThat(countOccurrences(d, "\"replayId\":\"rp-"))
                .as("抢占失败者不得产生重放留档").isEqualTo(1);

        if (d.contains("\"state\":\"SUCCEEDED\"")) {
            // 赢家是修正负载：成功，payloadSource=CORRECTED，副作用一次
            assertThat(d).contains("\"payloadSource\":\"CORRECTED\"");
            long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
            assertThat(effects).isEqualTo(1);
        } else {
            // 赢家是空体原始负载：本次仍失败，继续留死信，payloadSource=ORIGINAL，无副作用
            assertThat(d).contains("\"state\":\"DEAD\"")
                    .contains("\"payloadSource\":\"ORIGINAL\"")
                    .contains("RETRYABLE");
            long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
            assertThat(effects).isZero();
            // 之后修正重放仍可成功
            ResponseEntity<String> fixed = replayWithBody(rest, base(), eventId, correction);
            assertThat(fixed.getBody()).contains("SUCCEEDED");
        }
    }

    @Test
    void replayThatStillFails_remainsDeadExplainable_andCanReplayAgain() {
        String eventId = "cr-stilldead-1";
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 空体重放：下游仍故障 -> DEAD，但带 replayId/payloadSource=ORIGINAL，原因可解释
        ResponseEntity<String> r1 = replayEmpty(rest, base(), eventId);
        log.info("[case eventId={}] 空体重放仍失败 -> {}", eventId, r1.getBody());
        assertThat(r1.getBody())
                .contains("\"status\":\"DEAD\"")
                .contains("ORIGINAL")
                .contains("failureKind=RETRYABLE")
                .contains("attempts=3")
                .contains("rp-");

        ResponseEntity<String> dead = adminGet(rest, base(), "/admin/dead-letters");
        assertThat(dead.getBody()).contains(eventId).contains("RETRYABLE");

        ResponseEntity<String> detail = adminGet(rest, base(), "/admin/dead-letters/" + eventId);
        assertThat(detail.getBody())
                .contains("\"state\":\"DEAD\"")
                .contains("\"payloadSource\":\"ORIGINAL\"")
                .contains("lastError");

        // 之后可用修正负载再重放成功
        ResponseEntity<String> r2 = replayWithBody(rest, base(), eventId,
                body(eventId, "order.flaky", "\"fail\":\"ok\""));
        log.info("[case eventId={}] 修正后再重放 -> {}", eventId, r2.getBody());
        assertThat(r2.getBody()).contains("SUCCEEDED").contains("CORRECTED");

        // 成功后再点重放 -> 409（非 DEAD）
        ResponseEntity<String> r3 = replayEmpty(rest, base(), eventId);
        log.info("[case eventId={}] 成功后再重放 -> http={}", eventId, r3.getStatusCode());
        assertThat(r3.getStatusCode().value()).isEqualTo(409);

        // 留档正好两次：一次失败(ORIGINAL)、一次成功(CORRECTED)；副作用只一次
        ResponseEntity<String> detail2 = adminGet(rest, base(), "/admin/dead-letters/" + eventId);
        assertThat(countOccurrences(detail2.getBody(), "\"replayId\":\"rp-")).isEqualTo(2);
        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        assertThat(effects).isEqualTo(1);
    }

    // ---------- 并发驱动小工具 ----------

    @FunctionalInterface
    private interface ReplayCall {
        ResponseEntity<String> fire(int index);
    }

    private record Result(ConcurrentLinkedQueue<Integer> statuses,
                          ConcurrentLinkedQueue<String> bodies) {
        long countStatus(int familyOrExact) {
            return statuses.stream().filter(s -> familyOrExact == 2
                    ? (s >= 200 && s < 300)
                    : s == familyOrExact).count();
        }
    }

    private Result fireConcurrent(int threads, ReplayCall call) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> statuses = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> bodies = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < threads; i++) {
            final int index = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    ResponseEntity<String> resp = call.fire(index);
                    statuses.add(resp.getStatusCode().value());
                    bodies.add(resp.getStatusCode().value() + " " + resp.getBody());
                } catch (Exception e) {
                    statuses.add(-1);
                    bodies.add("ERROR:" + e);
                }
            });
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(40, TimeUnit.SECONDS)).isTrue();
        return new Result(statuses, bodies);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0, idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
