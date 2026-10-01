package com.github.feng111627.webhookingestdispatchjava.tests;

import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.body;
import static com.github.feng111627.webhookingestdispatchjava.tests.WebhookTestClient.postValid;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.feng111627.webhookingestdispatchjava.store.EffectLedger;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;

/**
 * 幂等去重场景：
 * 1) 串行重复：首次 SUCCEEDED，后续全部 DUPLICATE，且副作用只发生一次；
 * 2) 并发重复：N 个相同请求同时到达，恰好一个 SUCCEEDED，其余 DUPLICATE，副作用只有一次；
 * 3) 无匹配处理器的事件被明确拒绝（不静默丢弃、不入死信）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DeduplicationIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DeduplicationIntegrationTest.class);

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
    void serialDuplicate_processedOnce_returnsSuccessEachTime() {
        String eventId = "dup-serial-1";
        String payload = body(eventId, "order.created", "\"orderId\":\"O-DUP\"");

        ResponseEntity<String> first = postValid(rest, base(), payload);
        ResponseEntity<String> second = postValid(rest, base(), payload);
        ResponseEntity<String> third = postValid(rest, base(), payload);

        log.info("[case eventId={}] 串行重复三次 -> {} | {} | {}", eventId,
                first.getBody(), second.getBody(), third.getBody());

        assertThat(first.getBody()).contains("SUCCEEDED");
        assertThat(second.getBody()).contains("DUPLICATE");
        assertThat(third.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(third.getBody()).contains("DUPLICATE");

        // 副作用台账中该事件只应有一条记录
        long effectCount = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用台账记录数={}（期望1）", eventId, effectCount);
        assertThat(effectCount).isEqualTo(1);
    }

    @Test
    void concurrentDuplicates_onlyOneDispatch() throws Exception {
        String eventId = "dup-concurrent-1";
        String payload = body(eventId, "order.created", "\"orderId\":\"O-CONC\"");
        int threads = 16;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> responses = new ConcurrentLinkedQueue<>();
        AtomicInteger http2xx = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    ResponseEntity<String> resp = postValid(rest, base(), payload);
                    if (resp.getStatusCode().is2xxSuccessful()) {
                        http2xx.incrementAndGet();
                    }
                    responses.add(resp.getBody() == null ? "null" : resp.getBody());
                } catch (Exception e) {
                    responses.add("ERROR:" + e);
                }
            });
        }
        assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(20, TimeUnit.SECONDS)).isTrue();

        long succeeded = responses.stream().filter(r -> r.contains("SUCCEEDED")).count();
        long duplicated = responses.stream().filter(r -> r.contains("DUPLICATE")).count();

        log.info("[case eventId={}] 并发 {} 请求：SUCCEEDED={}（期望1）, DUPLICATE={}（期望{}）, 2xx={}",
                eventId, threads, succeeded, duplicated, threads - 1, http2xx.get());

        assertThat(succeeded).as("只有一个请求真正分发").isEqualTo(1);
        assertThat(duplicated).as("其余全部按重复成功返回").isEqualTo(threads - 1);
        assertThat(http2xx.get()).isEqualTo(threads);

        long effectCount = effectLedger.keys().stream().filter(k -> k.endsWith(":" + eventId)).count();
        log.info("[case eventId={}] 副作用台账记录数={}（期望1）", eventId, effectCount);
        assertThat(effectCount).isEqualTo(1);
    }

    @Test
    void unsupportedEventType_isRejected_notSilentlyDropped() {
        String eventId = "unknown-type-1";
        String payload = body(eventId, "billing.invoice.unknown", null);
        ResponseEntity<String> resp = postValid(rest, base(), payload);

        log.info("[case eventId={}] 无处理器类型 -> http={} body={}", eventId,
                resp.getStatusCode(), resp.getBody());

        assertThat(resp.getStatusCode().value()).isEqualTo(422);
        assertThat(resp.getBody()).contains("UNSUPPORTED_EVENT_TYPE");
    }

    @Test
    void differentEventIds_bothProcessed() {
        Set<String> ids = new HashSet<>(List.of("dup-id-a", "dup-id-b"));
        for (String id : ids) {
            ResponseEntity<String> resp = postValid(rest, base(), body(id, "order.created", null));
            log.info("[case eventId={}] 独立事件 -> {}", id, resp.getBody());
            assertThat(resp.getBody()).contains("SUCCEEDED");
        }
        for (String id : ids) {
            long effectCount = effectLedger.keys().stream().filter(k -> k.endsWith(":" + id)).count();
            assertThat(effectCount).isEqualTo(1);
        }
    }
}
