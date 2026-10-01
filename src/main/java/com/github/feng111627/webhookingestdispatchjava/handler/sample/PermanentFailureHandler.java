package com.github.feng111627.webhookingestdispatchjava.handler.sample;

import com.github.feng111627.webhookingestdispatchjava.handler.AbstractIdempotentEventHandler;
import com.github.feng111627.webhookingestdispatchjava.model.HandlerResult;
import com.github.feng111627.webhookingestdispatchjava.model.WebhookEvent;
import org.springframework.stereotype.Component;

/**
 * invoice.rejected：业务上无法恢复的错误，直接判定永久失败，不再重试。
 *
 * 为支持死信“重放成功”的端到端演示/测试，读取负载里的可选字段
 * payload.replayShouldSucceed：为 true 时重放返回成功。该字段是后续新增字段，
 * 读取采用可选方式，字段缺失时维持原有永久失败行为，证明前向兼容。
 */
@Component
public class PermanentFailureHandler extends AbstractIdempotentEventHandler {

    @Override
    public String supportedType() {
        return "invoice.rejected";
    }

    @Override
    protected HandlerResult doHandleOnce(WebhookEvent event) {
        boolean shouldSucceed = event.getPayload() != null
                && event.getPayload().path("replayShouldSucceed").asBoolean(false);
        if (shouldSucceed) {
            log.info("[eventId={}] type=invoice.rejected 判定=PERMANENT_RECOVERED 依据=开关要求本次成功",
                    event.getEventId());
            return HandlerResult.ok("invoice issue resolved on replay");
        }
        log.info("[eventId={}] type=invoice.rejected 判定=PERMANENT 依据=发票抬头校验不通过，无法自动恢复",
                event.getEventId());
        return HandlerResult.permanentFailure("invoice title invalid; cannot auto-recover");
    }
}
