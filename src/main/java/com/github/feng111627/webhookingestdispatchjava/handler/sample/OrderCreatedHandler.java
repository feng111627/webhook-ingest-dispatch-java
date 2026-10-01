package com.github.feng111627.webhookingestdispatchjava.handler.sample;

import com.github.feng111627.webhookingestdispatchjava.handler.AbstractIdempotentEventHandler;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.springframework.stereotype.Component;

/**
 * order.created：正常处理逻辑。基类保证同一 eventId 的副作用只执行一次。
 */
@Component
public class OrderCreatedHandler extends AbstractIdempotentEventHandler {

    @Override
    public String supportedType() {
        return "order.created";
    }

    @Override
    protected HandlerResult doHandleOnce(WebhookEvent event) {
        // 这里是“下单成功”的真实副作用（演示中仅记录日志）。
        log.info("[eventId={}] type=order.created 执行业务副作用：订单已受理 payload={}",
                event.getEventId(), event.getPayload());
        return HandlerResult.ok("order accepted");
    }
}
