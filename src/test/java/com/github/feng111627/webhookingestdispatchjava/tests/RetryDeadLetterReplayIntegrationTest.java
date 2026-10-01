package com.github.feng111627.webhookingestdispatchjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.feng111627.webhookingestdispatchjava.tests.support.AbstractWebhookIntegrationTest;
import com.github.feng111627.webhookingestdispatchjava.tests.support.Await;
import com.github.feng111627.webhookingestdispatchjava.tests.support.ControllableHandler;
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
 * 失败重试 -> 进入死信 -> 死信查询 -> 重放 的完整链路，正常与异常路径都断言。
 */
class RetryDeadLetterReplayIntegrationTest extends AbstractWebhookIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RetryDeadLetterReplayIntegrationTest.class);
    private static final Duration WAIT = Duration.ofSeconds(10);

    private void waitDeadLetter(String eventId) {
        Await.until(() -> deadLetterRepository.find(eventId).isPresent(), WAIT);
    }

    private void waitStatus(String eventId, com.github.feng111627.webhookingestdispatchjava.model.EventStatus status) {
        Await.until(() -> eventRepository.find(eventId)
                .map(r -> r.getStatus() == status).orElse(false), WAIT);
    }

    @Test
    void transientFailure_retriesUntilSuccess_withoutDeadLetter() {
        String id = "evt-retry-recover";
        ControllableHandler.succeedAfterAttempt.put(id, 2); // 第1次暂时失败，第2次成功
        String body = WebhookTestClient.body(id, ControllableHandler.TYPE);

        HttpResponse<String> resp = client.postValid(body);
        log.info("[eventId={}] 首次同步结论 status={} body={}", id, resp.statusCode(), resp.body());
        assertEquals(202, resp.statusCode());
        assertTrue(resp.body().contains("RETRYING"));

        waitStatus(id, com.github.feng111627.webhookingestdispatchjava.model.EventStatus.SUCCEEDED);
        waitDeadLetterAbsent(id);

        assertEquals(2, ControllableHandler.invocations(id), "暂时失败必须真正重试，共执行2次");
        assertEquals(1, ControllableHandler.sideEffects(id), "副作用只在成功时提交1次");
        log.info("[eventId={}] 判定=ACCEPTED 依据=第2次重试成功，死信中不存在，invocations={} sideEffects={}",
                id, ControllableHandler.invocations(id), ControllableHandler.sideEffects(id));
    }

    private void waitDeadLetterAbsent(String id) {
        Await.until(() -> deadLetterRepository.find(id).isEmpty(), WAIT);
    }

    @Test
    void thrownException_isTreatedAsTransientAndRetried() {
        String id = "evt-throw";
        ControllableHandler.succeedAfterAttempt.put(id, 2);
        ControllableHandler.throwOnce.add(id); // 第1次抛异常
        String body = WebhookTestClient.body(id, ControllableHandler.TYPE);

        HttpResponse<String> resp = client.postValid(body);
        log.info("[eventId={}] 抛异常首次结论 status={} body={}", id, resp.statusCode(), resp.body());
        assertEquals(202, resp.statusCode(), "异常应按暂时失败处理并安排重试");

        waitStatus(id, com.github.feng111627.webhookingestdispatchjava.model.EventStatus.SUCCEEDED);
        assertEquals(1, ControllableHandler.sideEffects(id));
        log.info("[eventId={}] 判定依据=异常被当作TRANSIENT，重试后成功 sideEffects={}", id,
                ControllableHandler.sideEffects(id));
    }

    @Test
    void transientFailure_exhaustsRetries_thenDeadLettered_andReplaySucceeds() {
        String id = "evt-exhaust";
        // 始终暂时失败（不设置恢复阈值）-> 3次尝试后进入死信
        String body = WebhookTestClient.body(id, ControllableHandler.TYPE);

        HttpResponse<String> resp = client.postValid(body);
        log.info("[eventId={}] 首次结论 status={} body={}", id, resp.statusCode(), resp.body());
        assertEquals(202, resp.statusCode());

        waitDeadLetter(id);
        var dl = deadLetterRepository.find(id).orElseThrow();
        log.info("[eventId={}] 判定=DEAD_LETTERED reason={} attempts={} detail={}",
                id, dl.getReason(), dl.getAttempts(), dl.getDetail());
        assertEquals(com.github.feng111627.webhookingestdispatchjava.model.DecisionCode.RETRIES_EXHAUSTED,
                dl.getReason());
        assertEquals(3, dl.getAttempts(), "最多3次尝试（含首次）");
        assertEquals(3, ControllableHandler.invocations(id), "退避重试应真实执行3次");
        assertEquals(0, ControllableHandler.sideEffects(id), "全部失败期间不得提交副作用");

        // 死信查询接口应能看到
        HttpResponse<String> list = client.listDeadLetters();
        assertEquals(200, list.statusCode());
        assertTrue(list.body().contains(id));
        assertTrue(list.body().contains("RETRIES_EXHAUSTED"));
        log.info("[eventId={}] 死信列表查询命中: {}", id, list.body());

        // 让后续重放能成功（第4次调用达到阈值1 -> 立即成功）
        ControllableHandler.succeedAfterAttempt.put(id, 1);
        HttpResponse<String> replay = client.replay(id);
        log.info("[eventId={}] 重放结论 status={} body={}", id, replay.statusCode(), replay.body());
        assertEquals(200, replay.statusCode());
        assertTrue(replay.body().contains("REPLAY_SUCCEEDED"));

        // 重放成功后从死信移除
        Await.until(() -> deadLetterRepository.find(id).isEmpty(), WAIT);
        HttpResponse<String> listAfter = client.listDeadLetters();
        assertFalse(listAfter.body().contains(id), "重放成功后死信中应移除");

        // 关键：重放成功后再重放/再投递，不得产生第二次副作用
        int sideEffectsBefore = ControllableHandler.sideEffects(id);
        HttpResponse<String> replayAgain = client.replay(id);
        log.info("[eventId={}] 二次重放 status={} body={}", id, replayAgain.statusCode(), replayAgain.body());
        assertEquals(409, replayAgain.statusCode());
        assertTrue(replayAgain.body().contains("REPLAY_SKIPPED_DUPLICATE"));
        assertEquals(sideEffectsBefore, ControllableHandler.sideEffects(id),
                "重复重放不得产生重复副作用");

        HttpResponse<String> repost = client.postValid(body);
        assertEquals(200, repost.statusCode());
        assertTrue(repost.body().contains("DUPLICATE_EVENT"));
        assertEquals(sideEffectsBefore, ControllableHandler.sideEffects(id),
                "重放成功后再次投递也不得产生重复副作用");
        log.info("[eventId={}] 判定依据=重放成功后副作用计数恒定为{}，二次重放/再投递均幂等", id, sideEffectsBefore);
    }

    @Test
    void permanentFailure_goesDeadImmediatelyWithoutRetry() {
        String id = "evt-permanent";
        ControllableHandler.alwaysPermanent.add(id);
        String body = WebhookTestClient.body(id, ControllableHandler.TYPE);

        HttpResponse<String> resp = client.postValid(body);
        log.info("[eventId={}] 永久失败结论 status={} body={}", id, resp.statusCode(), resp.body());
        assertEquals(422, resp.statusCode());
        assertTrue(resp.body().contains("PERMANENT_FAILURE"));

        waitDeadLetter(id);
        var dl = deadLetterRepository.find(id).orElseThrow();
        assertEquals(com.github.feng111627.webhookingestdispatchjava.model.DecisionCode.PERMANENT_FAILURE,
                dl.getReason());
        assertEquals(1, dl.getAttempts(), "永久失败不应重试，只尝试1次");
        assertEquals(1, ControllableHandler.invocations(id));

        // 重放默认仍是永久失败（处理逻辑本身幂等且仍判定永久失败）
        HttpResponse<String> replay = client.replay(id);
        log.info("[eventId={}] 永久失败重放 status={} body={}", id, replay.statusCode(), replay.body());
        assertEquals(422, replay.statusCode());
        assertTrue(replay.body().contains("REPLAY_FAILED"));
    }

    @Test
    void unroutableEvent_isNotSilentlyDropped_butDeadLettered() {
        String id = "evt-unknown-type";
        String body = WebhookTestClient.body(id, "no.such.type.xyz");

        HttpResponse<String> resp = client.postValid(body);
        log.info("[eventId={}] 不可路由结论 status={} body={}", id, resp.statusCode(), resp.body());
        assertEquals(422, resp.statusCode());
        assertTrue(resp.body().contains("UNROUTABLE_EVENT"),
                "没有匹配处理逻辑必须显式失败，不能静默丢弃");

        waitDeadLetter(id);
        var dl = deadLetterRepository.find(id).orElseThrow();
        assertEquals(com.github.feng111627.webhookingestdispatchjava.model.DecisionCode.UNROUTABLE_EVENT,
                dl.getReason());
    }

    @Test
    void replayUnknownEvent_returnsNotFound() {
        HttpResponse<String> replay = client.replay("evt-does-not-exist");
        log.info("[eventId=evt-does-not-exist] 重放不存在死信 status={} body={}",
                replay.statusCode(), replay.body());
        assertEquals(404, replay.statusCode());
        assertTrue(replay.body().contains("NOT_FOUND"));
    }
}
