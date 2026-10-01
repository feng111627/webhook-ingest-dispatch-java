package com.github.feng111627.webhookingestdispatchjava.tests;

import com.github.feng111627.webhookingestdispatchjava.tests.support.AbstractWebhookIntegrationTest;
import com.github.feng111627.webhookingestdispatchjava.tests.support.WebhookTestClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重复与并发：
 * - 同一 eventId 串行重复：第二次按成功返回 DUPLICATE_EVENT，且不重复分发；
 * - 同一 eventId 并发发送：恰好一个首次处理（ACCEPTED），其余全部 DUPLICATE_EVENT。
 */
class IdempotencyConcurrencyIntegrationTest extends AbstractWebhookIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyConcurrencyIntegrationTest.class);

    @Test
    void sequentialDuplicate_returnsSuccessButDispatchesOnce() {
        String body = WebhookTestClient.body("evt-dup-seq", "order.created");

        HttpResponse<String> first = client.postValid(body);
        HttpResponse<String> second = client.postValid(body);
        HttpResponse<String> third = client.postValid(body);

        log.info("[eventId=evt-dup-seq] 首次 status={} body={}", first.statusCode(), first.body());
        log.info("[eventId=evt-dup-seq] 重复2 status={} body={}", second.statusCode(), second.body());
        log.info("[eventId=evt-dup-seq] 重复3 status={} body={}", third.statusCode(), third.body());

        assertEquals(200, first.statusCode());
        assertTrue(first.body().contains("ACCEPTED"), "首次必须真正处理: " + first.body());
        assertEquals(200, second.statusCode());
        assertEquals(200, third.statusCode());
        assertTrue(second.body().contains("DUPLICATE_EVENT"));
        assertTrue(third.body().contains("DUPLICATE_EVENT"));

        // 台账只有一条记录且状态为成功，证明只处理一次
        var record = eventRepository.find("evt-dup-seq").orElseThrow();
        assertEquals(1, record.getAttempts(), "重复请求不得触发再次处理");
        log.info("[eventId=evt-dup-seq] 判定依据=台账attempts={}（期望1），状态={}",
                record.getAttempts(), record.getStatus());
    }

    @Test
    void concurrentDuplicates_dispatchExactlyOnce() throws Exception {
        int threads = 12;
        String body = WebhookTestClient.body("evt-dup-concurrent", "order.created");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch fire = new CountDownLatch(1);
        ConcurrentLinkedQueue<Integer> statuses = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> decisions = new ConcurrentLinkedQueue<>();
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    fire.await();
                    HttpResponse<String> resp = client.postValid(body);
                    statuses.add(resp.statusCode());
                    decisions.add(resp.body());
                    if (resp.body().contains("\"decision\":\"ACCEPTED\"")) {
                        accepted.incrementAndGet();
                    }
                    if (resp.body().contains("DUPLICATE_EVENT")) {
                        duplicate.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("并发请求异常", e);
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        fire.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));

        log.info("[eventId=evt-dup-concurrent] 并发{}个请求 ACCEPTED={} DUPLICATE_EVENT={}",
                threads, accepted.get(), duplicate.get());

        assertEquals(threads, statuses.size(), "所有请求都应得到响应");
        Set<Integer> uniqueStatus = new HashSet<>(statuses);
        assertEquals(Set.of(200), uniqueStatus, "并发重复请求全部应按成功(200)返回，实际: " + uniqueStatus);
        assertEquals(1, accepted.get(), "并发下只能有一个首次真正处理");
        assertEquals(threads - 1, duplicate.get(), "其余并发请求必须判定为重复且不分发");

        var record = eventRepository.find("evt-dup-concurrent").orElseThrow();
        assertEquals(1, record.getAttempts(), "并发重复不得导致多次分发");
        log.info("[eventId=evt-dup-concurrent] 判定依据=台账attempts={}（期望1）", record.getAttempts());
    }
}
