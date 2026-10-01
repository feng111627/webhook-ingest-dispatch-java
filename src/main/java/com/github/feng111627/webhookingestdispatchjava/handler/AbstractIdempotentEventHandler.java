package com.github.feng111627.webhookingestdispatchjava.handler;

import com.github.feng111627.webhookingestdispatchjava.model.FailureType;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 幂等处理基类：真正的业务副作用只对每个 eventId 成功执行一次。
 *
 * 语义：
 * - doHandleOnce 返回成功后才把 eventId 记为“已应用”；因此处理失败时
 *   退避重试会真正重新执行业务逻辑，而不是被错误短路。
 * - 一旦成功，后续重试、死信重放或重复投递再进入该处理逻辑，直接返回
 *   “副作用已应用”的成功结论，杜绝重复副作用。
 * - 处理逻辑内部也应尽量把副作用设计成按 eventId 幂等（如唯一约束/upsert）。
 */
public abstract class AbstractIdempotentEventHandler implements EventHandler {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Set<String> appliedEventIds = ConcurrentHashMap.newKeySet();

    @Override
    public final HandlerResult handle(WebhookEvent event) {
        if (appliedEventIds.contains(event.getEventId())) {
            log.info("[eventId={}] type={} 判定=IDEMPOTENT_SKIP 依据=该事件副作用已成功执行过，跳过",
                    event.getEventId(), supportedType());
            return HandlerResult.ok("idempotent skip: side effect already applied");
        }
        HandlerResult result = doHandleOnce(event);
        // 只有成功才标记已应用；失败（暂时/永久）保留重试与重放重新执行的可能。
        if (result.success() && appliedEventIds.add(event.getEventId())) {
            log.info("[eventId={}] type={} 判定=SIDE_EFFECT_APPLIED 依据=副作用首次成功执行",
                    event.getEventId(), supportedType());
        }
        return result;
    }

    /** 每个 eventId 在该处理逻辑内真正成功只会生效一次的业务逻辑。 */
    protected abstract HandlerResult doHandleOnce(WebhookEvent event);
}
