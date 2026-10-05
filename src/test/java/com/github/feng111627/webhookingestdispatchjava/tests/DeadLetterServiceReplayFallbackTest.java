package com.github.feng111627.webhookingestdispatchjava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.feng111627.webhookingestdispatchjava.dto.ReplayResponse;
import com.github.feng111627.webhookingestdispatchjava.model.EventRecord;
import com.github.feng111627.webhookingestdispatchjava.model.EventState;
import com.github.feng111627.webhookingestdispatchjava.model.FailureKind;
import com.github.feng111627.webhookingestdispatchjava.service.DeadLetterService;
import com.github.feng111627.webhookingestdispatchjava.service.DispatchService;
import com.github.feng111627.webhookingestdispatchjava.store.InMemoryEventStore;
import com.github.feng111627.webhookingestdispatchjava.trace.InMemoryTraceStore;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.server.ResponseStatusException;

/**
 * 重放失败边界的异常路径（无法通过 HTTP 稳定触发，直接实例化服务覆盖）：
 * 重放执行本身抛出未预期异常时，事件必须保守回到 DEAD（失败性质/尝试次数/最后原因可查），
 * 绝不能卡在 PROCESSING；之后的重放请求仍能正常执行。
 */
class DeadLetterServiceReplayFallbackTest {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterServiceReplayFallbackTest.class);

    private static final String EVENT_ID = "replay-fallback-1";

    private InMemoryEventStore store;
    private InMemoryTraceStore trace;

    @BeforeEach
    void seedDeadEvent() {
        store = new InMemoryEventStore();
        trace = new InMemoryTraceStore();
        store.claimIfAbsent(EVENT_ID, ignored -> {
            EventRecord r = new EventRecord();
            r.setEventType("order.flaky");
            r.setState(EventState.RECEIVED);
            r.setReceivedAt(Instant.now());
            r.setUpdatedAt(Instant.now());
            return r;
        });
        store.update(EVENT_ID, r -> {
            r.setState(EventState.DEAD);
            r.setFailureKind(FailureKind.RETRYABLE);
            r.setAttempts(3);
            r.setLastError("重试上限耗尽(3次): simulated downstream unavailable");
            return r;
        });
    }

    /** 分发器 stub：redispatch 抛未预期异常，模拟重放执行本身出错 */
    private DispatchService failingDispatch() {
        return new DispatchService(null, store, null, trace) {
            @Override
            public EventRecord redispatch(EventRecord record) {
                throw new IllegalStateException("simulated dispatch infrastructure failure");
            }
        };
    }

    /** 分发器 stub：redispatch 直接置成功（模拟修复后的正常链路） */
    private DispatchService okDispatch() {
        return new DispatchService(null, store, null, trace) {
            @Override
            public EventRecord redispatch(EventRecord record) {
                store.update(record.getEventId(), r -> {
                    r.setState(EventState.SUCCEEDED);
                    r.setFailureKind(FailureKind.NONE);
                    r.setAttempts(1);
                    r.setLastError("ok");
                    return r;
                });
                return store.find(record.getEventId()).orElseThrow();
            }
        };
    }

    @Test
    void replayExecutionThrows_eventBackToDead_andLaterReplayNotStuck() {
        DeadLetterService svc = new DeadLetterService(store, failingDispatch(), trace);

        // 重放执行异常：500，且事件必须回到 DEAD 而不是卡在 PROCESSING
        assertThatThrownBy(() -> svc.replay(EVENT_ID, null))
                .isInstanceOfSatisfying(ResponseStatusException.class, ex -> {
                    log.info("[case eventId={}] 重放执行异常 -> http={} reason={}",
                            EVENT_ID, ex.getStatusCode(), ex.getReason());
                    assertThat(ex.getStatusCode().value()).isEqualTo(500);
                });

        EventRecord after = store.find(EVENT_ID).orElseThrow();
        log.info("[case eventId={}] 异常兜底后状态 state={} failureKind={} attempts={} lastError={}",
                EVENT_ID, after.getState(), after.getFailureKind(), after.getAttempts(), after.getLastError());
        assertThat(after.getState()).isEqualTo(EventState.DEAD);
        assertThat(after.getFailureKind()).isEqualTo(FailureKind.RETRYABLE);
        assertThat(after.getLastError()).contains("重放执行异常");
        // 死信列表里仍能查到（未被卡没）
        assertThat(store.findDead()).anyMatch(r -> r.getEventId().equals(EVENT_ID));
        // 重放历史留下本次结论
        assertThat(after.getReplays()).hasSize(1);
        assertThat(after.getReplays().get(0).getResult()).isEqualTo("DEAD");

        // 异常批次结束后，再次重放（链路恢复）仍能正常执行，不被卡死
        DeadLetterService svc2 = new DeadLetterService(store, okDispatch(), trace);
        ReplayResponse resp = svc2.replay(EVENT_ID, null);
        log.info("[case eventId={}] 链路恢复后再次重放 -> status={} reason={}",
                EVENT_ID, resp.status(), resp.reason());
        assertThat(resp.status()).isEqualTo("SUCCEEDED");
        assertThat(store.find(EVENT_ID).orElseThrow().getState()).isEqualTo(EventState.SUCCEEDED);
    }
}
