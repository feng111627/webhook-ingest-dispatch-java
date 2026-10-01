package com.github.feng111627.webhookingestdispatchjava.tests.support;

import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试专用处理逻辑（test 目录下的 @Component，仅测试装配时存在）。
 * - 记录每个 eventId 的真实副作用次数，用于断言“重试真的重新执行、成功后不重复”；
 * - 通过静态开关控制在第几次成功，或是否抛异常 / 永久失败。
 */
@Component
public class ControllableHandler implements com.github.feng111627.webhookingestdispatchjava.handler.EventHandler {

    private static final Logger log = LoggerFactory.getLogger(ControllableHandler.class);

    public static final String TYPE = "test.controllable";

    /** 每个 eventId 成功前允许的暂时失败次数：调用次数 <= failUntilAttempt 时失败。 */
    public static final ConcurrentMap<String, Integer> succeedAfterAttempt = new ConcurrentHashMap<>();
    public static final java.util.Set<String> throwOnce = ConcurrentHashMap.newKeySet();
    public static final java.util.Set<String> alwaysPermanent = ConcurrentHashMap.newKeySet();
    public static final ConcurrentMap<String, AtomicInteger> invocationCount = new ConcurrentHashMap<>();
    /** 只在“成功提交副作用”时递增，用于验证重放不产生重复副作用。 */
    public static final ConcurrentMap<String, AtomicInteger> sideEffectCount = new ConcurrentHashMap<>();

    public static void reset() {
        succeedAfterAttempt.clear();
        throwOnce.clear();
        alwaysPermanent.clear();
        invocationCount.clear();
        sideEffectCount.clear();
    }

    public static int invocations(String eventId) {
        return invocationCount.getOrDefault(eventId, new AtomicInteger()).get();
    }

    public static int sideEffects(String eventId) {
        return sideEffectCount.getOrDefault(eventId, new AtomicInteger()).get();
    }

    @Override
    public String supportedType() {
        return TYPE;
    }

    @Override
    public HandlerResult handle(WebhookEvent event) {
        String id = event.getEventId();
        int attempt = invocationCount.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();

        if (throwOnce.remove(id)) {
            log.info("[eventId={}] ControllableHandler 第{}次 判定=THROW 依据=模拟处理逻辑抛异常", id, attempt);
            throw new IllegalStateException("simulated downstream crash");
        }
        if (alwaysPermanent.contains(id)) {
            log.info("[eventId={}] ControllableHandler 第{}次 判定=PERMANENT 依据=测试标记为永久失败", id, attempt);
            return HandlerResult.permanentFailure("marked permanent by test");
        }
        Integer threshold = succeedAfterAttempt.get(id);
        if (threshold != null && attempt >= threshold) {
            sideEffectCount.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            log.info("[eventId={}] ControllableHandler 第{}次 判定=SUCCESS 依据=达到恢复阈值，副作用已提交", id, attempt);
            return HandlerResult.ok("recovered on attempt " + attempt);
        }
        log.info("[eventId={}] ControllableHandler 第{}次 判定=TRANSIENT 依据=未达到恢复阈值", id, attempt);
        return HandlerResult.transientFailure("not yet recovered, attempt " + attempt);
    }
}
