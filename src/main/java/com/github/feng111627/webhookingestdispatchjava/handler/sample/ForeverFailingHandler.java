package com.github.feng111627.webhookingestdispatchjava.handler.sample;

import com.github.feng111627.webhookingestdispatchjava.handler.AbstractIdempotentEventHandler;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.springframework.stereotype.Component;

/**
 * payment.failed：每次都返回暂时失败，用于演示退避重试耗尽后进入死信。
 * 注意：失败不算“副作用完成”，因此这里不使用基类的一次性短路
 * （幂等短路发生在 doHandleOnce 成功落库的场景）；
 * 为让每次重试都真正执行，本类直接实现 EventHandler。
 */
@Component
public class ForeverFailingHandler implements com.github.feng111627.webhookingestdispatchjava.handler.EventHandler {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(ForeverFailingHandler.class);

    @Override
    public String supportedType() {
        return "payment.failed";
    }

    @Override
    public HandlerResult handle(WebhookEvent event) {
        log.info("[eventId={}] type=payment.failed 判定=TRANSIENT 依据=下游支付通道暂不可用（模拟）",
                event.getEventId());
        return HandlerResult.transientFailure("payment gateway temporarily unavailable");
    }
}
