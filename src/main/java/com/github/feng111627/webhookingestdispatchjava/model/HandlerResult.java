package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 事件处理逻辑返回的处理结论。
 */
public record HandlerResult(boolean success, FailureType failureType, String reason) {

    public static HandlerResult ok() {
        return new HandlerResult(true, null, "processed");
    }

    public static HandlerResult ok(String reason) {
        return new HandlerResult(true, null, reason);
    }

    public static HandlerResult transientFailure(String reason) {
        return new HandlerResult(false, FailureType.TRANSIENT, reason);
    }

    public static HandlerResult permanentFailure(String reason) {
        return new HandlerResult(false, FailureType.PERMANENT, reason);
    }
}
