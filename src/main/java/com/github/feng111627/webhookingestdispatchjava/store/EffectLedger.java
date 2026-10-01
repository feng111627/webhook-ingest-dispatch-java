package com.github.feng111627.webhookingestdispatchjava.store;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 副作用台账：每一种真正对外产生影响的动作，成功前以稳定的 effectKey 登记一次。
 *
 * <p>重复事件、死信重放只要命中台账，就必须跳过真实副作用，从而保证
 * "重放成功后不产生重复副作用"。
 */
@Component
public class EffectLedger {

    private static final Logger log = LoggerFactory.getLogger(EffectLedger.class);

    private final ConcurrentHashMap<String, Boolean> effects = new ConcurrentHashMap<>();

    /**
     * 幂等登记。
     *
     * @return true 表示首次登记（调用方应真正执行副作用）；false 表示已登记（调用方必须跳过）
     */
    public boolean recordIfAbsent(String effectKey) {
        boolean[] firstTime = {true};
        effects.compute(effectKey, (key, current) -> {
            if (current != null) {
                firstTime[0] = false;
                return current;
            }
            firstTime[0] = true;
            return Boolean.TRUE;
        });
        if (firstTime[0]) {
            log.info("[effect key={}] 首次登记：允许执行真实副作用", effectKey);
        } else {
            log.info("[effect key={}] 命中台账：跳过重复副作用", effectKey);
        }
        return firstTime[0];
    }

    public boolean exists(String effectKey) {
        return effects.containsKey(effectKey);
    }

    public Set<String> keys() {
        return Set.copyOf(effects.keySet());
    }
}
