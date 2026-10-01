package com.github.feng111627.webhookingestdispatchjava.tests;

import com.github.feng111627.webhookingestdispatchjava.tests.support.AbstractWebhookIntegrationTest;
import com.github.feng111627.webhookingestdispatchjava.tests.support.Await;
import com.github.feng111627.webhookingestdispatchjava.tests.support.WebhookTestClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 前向兼容：事件负载后续新增字段时，已有处理逻辑与接收链路不被破坏。
 * - order.created 增加任意未知嵌套字段，仍正常处理；
 * - invoice.rejected 增加的可选字段 replayShouldSucceed，能让死信重放成功；
 *   字段缺失时维持原有永久失败行为（同一逻辑的新旧负载行为都正确）。
 */
class PayloadForwardCompatibilityTest extends AbstractWebhookIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(PayloadForwardCompatibilityTest.class);

    @Test
    void extraUnknownFields_doNotBreakExistingHandler() {
        // V2 负载：在 V1 基础上新增未来字段（含嵌套对象、数组）
        String v2 = """
                {
                  "eventId": "evt-extra-fields",
                  "eventType": "order.created",
                  "amount": 99.9,
                  "newFeature": {"flag": true, "tags": ["a","b"]},
                  "anotherFutureField": 123
                }
                """;
        HttpResponse<String> resp = client.postValid(v2);
        log.info("[eventId=evt-extra-fields] 含新增字段负载 status={} body={}", resp.statusCode(), resp.body());

        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("ACCEPTED"),
                "新增未知字段不得破坏已有处理逻辑: " + resp.body());

        var record = eventRepository.find("evt-extra-fields").orElseThrow();
        assertEquals(com.github.feng111627.webhookingestdispatchjava.model.EventStatus.SUCCEEDED,
                record.getStatus());
        // 原始负载（含新字段）被完整保留，可供后续逻辑使用
        assertTrue(record.getEvent().getPayload().has("newFeature"));
    }

    @Test
    void newOptionalField_allowsDeadLetterReplayToRecover() {
        // 1) 旧负载（无新字段）：永久失败进死信
        String oldBody = WebhookTestClient.body("evt-fc-replay", "invoice.rejected");
        HttpResponse<String> first = client.postValid(oldBody);
        log.info("[eventId=evt-fc-replay] 旧负载（无开关字段）status={} body={}", first.statusCode(), first.body());
        assertEquals(422, first.statusCode());
        assertTrue(first.body().contains("PERMANENT_FAILURE"));
        Await.until(() -> deadLetterRepository.find("evt-fc-replay").isPresent(), Duration.ofSeconds(5));

        // 2) 不带新字段重放：仍永久失败（旧逻辑行为不变，REPLAY_FAILED，事件仍在死信）
        HttpResponse<String> replayOld = client.replay("evt-fc-replay");
        log.info("[eventId=evt-fc-replay] 不带新字段重放 status={} body={}", replayOld.statusCode(), replayOld.body());
        assertEquals(422, replayOld.statusCode());
        assertTrue(replayOld.body().contains("REPLAY_FAILED"));
        assertTrue(deadLetterRepository.find("evt-fc-replay").isPresent(), "重放失败事件应仍在死信");

        // 3) 携带“后续新增的可选字段”重放：成功，且只产生一次副作用
        String newBody = WebhookTestClient.bodyWith("evt-fc-replay", "invoice.rejected",
                "\"replayShouldSucceed\":true");
        HttpResponse<String> replayNew = client.replayWithPayload("evt-fc-replay", newBody);
        log.info("[eventId=evt-fc-replay] 携带新增可选字段重放 status={} body={}",
                replayNew.statusCode(), replayNew.body());
        assertEquals(200, replayNew.statusCode());
        assertTrue(replayNew.body().contains("REPLAY_SUCCEEDED"), replayNew.body());
        assertFalse(deadLetterRepository.find("evt-fc-replay").isPresent(), "重放成功应移出死信");

        // 4) 再次重放被判重复，不产生重复副作用
        HttpResponse<String> replayAgain = client.replay("evt-fc-replay");
        assertEquals(409, replayAgain.statusCode());
        assertTrue(replayAgain.body().contains("REPLAY_SKIPPED_DUPLICATE"));
        log.info("[eventId=evt-fc-replay] 判定依据=新增字段仅影响当次重放结论，成功后重放幂等");
    }

}
