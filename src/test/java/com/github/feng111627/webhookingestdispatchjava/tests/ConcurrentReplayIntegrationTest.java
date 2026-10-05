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
 * 并发与重复重放的幂等语义（所有用例确定性断言，可反复连跑）：
 * 1) 同一条死信被 N 个并发请求同时重放：恰好一个真正执行（2xx），
 *    其余全部 409 且拒绝结论逐字节一致（同一个明确、稳定的 REPLAY_IN_PROGRESS 结论）；
 * 2) 混合场景（一半带修正负载、一半不带）：无论谁抢到，全局恰好一个被受理、
 *    恰好一个 SUCCEEDED、副作用恰好一次 —— 断言不依赖谁先到达；
 * 3) 重放仍失败：事件留在死信，failureKind/attempts/lastError 可查；
 *    该批并发请求全部结束后，单独再发起一次重放仍能正常执行（不被批次状态卡住）；
 * 4) 拒绝措辞与事件当时真实状态严格一致（不报自相矛盾的状态）。
 *
 * <p>退避基数调大到 200ms，让"重放执行窗口"足够宽，同批请求必然落在窗口内，
 * 避免时序敏感导致的偶发失败。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"webhook.backoff-base-millis=200"})
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
    void concurrentReplays_onlyOneExecutes_othersGetIdenticalRejection() throws Exception {
        String eventId = "conc-replay-1";
        // 首轮永久失败进死信（快速，不拖慢用例）
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"permanent\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 全部线程都携带会沉睡的修正负载：赢家的执行窗口被撑开，
        // 其余并发请求必然落在"同批已在执行"窗口内 -> 确定性的 1 个 2xx + N-1 个一致 409
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 8, i ->
                "{\"payload\":{\"eventId\":\"" + eventId
                        + "\",\"fail\":\"sleep\",\"sleepMillis\":1000}}");

        long ok = all.stream().filter(r -> r.getStatusCode().is2xxSuccessful()).count();
        long conflict = all.stream().filter(r -> r.getStatusCode().value() == 409).count();
        log.info("[case eventId={}] 并发重放 {} 个请求 -> 受理 {} 个，409 拒绝 {} 个",
                eventId, all.size(), ok, conflict);
        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));

        // 恰好一个真正执行，其余全部 409
        assertThat(ok).isEqualTo(1);
        assertThat(conflict).isEqualTo(all.size() - 1);

        // 所有被拒请求拿到逐字节一致的拒绝结论（同一个明确、稳定的 message）
        List<String> rejectionBodies = all.stream()
                .filter(r -> r.getStatusCode().value() == 409)
                .map(ResponseEntity::getBody)
                .toList();
        assertThat(rejectionBodies).allMatch(b -> b != null && b.contains("message"));
        assertThat(rejectionBodies).allMatch(b -> b.contains("已有重放请求在执行"));
        assertThat(rejectionBodies.stream().distinct().count())
                .as("同批所有 409 结论必须完全一致")
                .isEqualTo(1);

        // 副作用恰好一次：同一事件绝不会被分发成功两次
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
    void mixedConcurrentReplays_exactlyOneAccepted_exactlyOneSuccess() throws Exception {
        String eventId = "conc-replay-mixed-1";
        // 原始负载：前 5 次尝试暂时失败、第 6 次成功。
        // 首轮 ingest 耗尽 3 次尝试进死信；若 ORIGINAL 重放抢到，第 4/5 次失败、第 6 次成功，
        // 执行窗口含两次退避（200+400ms），同批其余请求必然落在窗口内。
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retry-then-ok\",\"failUntil\":5"));
        assertThat(ingest.getBody()).contains("DEAD");

        // 一半带修正负载（沉睡 800ms 后成功）、一半不带（沿用原始负载，慢成功）：
        // 无论谁抢到，结论都必须确定：恰好一个被受理且成功，其余全部 409
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 8, i -> i % 2 == 0
                ? "{\"payload\":{\"eventId\":\"" + eventId
                        + "\",\"fail\":\"sleep\",\"sleepMillis\":800}}"
                : null);

        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));

        // 恰好一个请求被受理（2xx），其余全部 409（带可解释 message）——与谁先到达无关
        List<ResponseEntity<String>> accepted = all.stream()
                .filter(r -> r.getStatusCode().is2xxSuccessful()).toList();
        List<ResponseEntity<String>> rejected = all.stream()
                .filter(r -> r.getStatusCode().value() == 409).toList();
        assertThat(accepted).hasSize(1);
        assertThat(rejected).hasSize(all.size() - 1);
        rejected.forEach(r -> assertThat(r.getBody()).contains("message"));

        // 被受理的那个必然 SUCCEEDED（两种负载最终都成功），全局恰好一个 SUCCEEDED
        String winner = accepted.get(0).getBody();
        log.info("[case eventId={}] 混合并发重放唯一受理结论 -> {}", eventId, winner);
        assertThat(winner).contains("\"status\":\"SUCCEEDED\"").contains("payloadSource");
        long succeeded = all.stream().filter(r -> r.getBody() != null && r.getBody().contains("SUCCEEDED")).count();
        assertThat(succeeded).isEqualTo(1);

        // 副作用恰好一次：同一事件不会被分发成功两次
        long effects = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用数={}（期望1）", eventId, effects);
        assertThat(effects).isEqualTo(1);

        // 最终状态成功、离开死信、重放历史恰好一条
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 混合并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"SUCCEEDED\"");
        assertThat(rest.getForEntity(base() + "/admin/dead-letters", String.class).getBody())
                .doesNotContain(eventId);
    }

    @Test
    void concurrentReplayStillFails_staysDead_thenFreshReplayWorks() throws Exception {
        String eventId = "conc-replay-fail-1";
        // 始终暂时失败：首轮 3 次尝试耗尽进死信
        ResponseEntity<String> ingest = postValid(rest, base(),
                body(eventId, "order.flaky", "\"fail\":\"retryable\""));
        assertThat(ingest.getBody()).contains("DEAD");

        // 一批并发重放（都不带修正负载，沿用原始负载 => 仍然失败）：
        // 恰好一个被受理并真正执行，其余全部 409
        List<ResponseEntity<String>> all = fireConcurrentReplays(eventId, 6, i -> null);
        all.forEach(r -> log.info("[case eventId={}]   http={} body={}", eventId, r.getStatusCode(), r.getBody()));

        List<ResponseEntity<String>> accepted = all.stream()
                .filter(r -> r.getStatusCode().is2xxSuccessful()).toList();
        List<ResponseEntity<String>> rejected = all.stream()
                .filter(r -> r.getStatusCode().value() == 409).toList();
        assertThat(accepted).hasSize(1);
        assertThat(rejected).hasSize(all.size() - 1);

        // 被受理的那次重放仍然失败：结论 DEAD，事件继续留在死信
        String winner = accepted.get(0).getBody();
        log.info("[case eventId={}] 唯一受理的重放仍失败 -> {}", eventId, winner);
        assertThat(winner).contains("\"status\":\"DEAD\"");

        // 失败边界可查：死信列表里能看到失败性质、已尝试次数、最后一次失败原因
        ResponseEntity<String> dead = rest.getForEntity(base() + "/admin/dead-letters", String.class);
        log.info("[case eventId={}] 并发重放后死信记录 -> {}", eventId, dead.getBody());
        assertThat(dead.getBody()).contains(eventId).contains("RETRYABLE").contains("重试上限耗尽");

        // 详情里能看到本次重放历史：结论 DEAD、尝试次数与最后错误都在
        ResponseEntity<String> detail = rest.getForEntity(base() + "/admin/events/" + eventId, String.class);
        log.info("[case eventId={}] 并发重放后详情 -> {}", eventId, detail.getBody());
        assertThat(detail.getBody()).contains("\"state\":\"DEAD\"")
                .contains("\"result\":\"DEAD\"")
                .contains("\"lastError\"");

        // 该批并发请求全部结束后，单独再发起一次重放（带修正负载）仍能正常执行：
        // 不被前面并发请求留下的状态卡住或干扰判定
        ResponseEntity<String> retry = replay(eventId,
                "{\"payload\":{\"eventId\":\"" + eventId + "\",\"note\":\"manual-fixed\"}}");
        log.info("[case eventId={}] 批次结束后单独重放 -> {}", eventId, retry.getBody());
        assertThat(retry.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(retry.getBody()).contains("\"status\":\"SUCCEEDED\"")
                .contains("\"payloadSource\":\"CORRECTED\"");
        assertThat(rest.getForEntity(base() + "/admin/dead-letters", String.class).getBody())
                .doesNotContain(eventId);
    }

    @Test
    void replayRejection_messageMatchesActualState() {
        // 已成功的事件：拒绝措辞必须报出真实状态 SUCCEEDED，不能自相矛盾
        String okEvent = "conc-reject-ok-1";
        ResponseEntity<String> ingest = postValid(rest, base(), body(okEvent, "order.created",
                "\"orderId\":\"O-9001\",\"amountCents\":100"));
        assertThat(ingest.getBody()).contains("SUCCEEDED");

        ResponseEntity<String> conflict = replay(okEvent, null);
        log.info("[case eventId={}] 非 DEAD 事件重放被拒 -> http={} body={}",
                okEvent, conflict.getStatusCode(), conflict.getBody());
        assertThat(conflict.getStatusCode().value()).isEqualTo(409);
        assertThat(conflict.getBody()).contains("SUCCEEDED").contains("只有 DEAD 状态允许重放");
        // 绝不能出现"当前状态为 DEAD，只有 DEAD 状态允许重放"式的自相矛盾
        assertThat(conflict.getBody()).doesNotContain("当前状态为 DEAD");

        // 不存在的事件：404 且带可解释 message
        ResponseEntity<String> notFound = replay("conc-no-such-event", null);
        log.info("[case] 不存在事件重放 -> http={} body={}", notFound.getStatusCode(), notFound.getBody());
        assertThat(notFound.getStatusCode().value()).isEqualTo(404);
        assertThat(notFound.getBody()).contains("message");
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
