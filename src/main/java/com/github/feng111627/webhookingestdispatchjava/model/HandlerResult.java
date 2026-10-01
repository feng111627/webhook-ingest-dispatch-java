package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 事件处理器的单次处理结论。
 */
public record HandlerResult(Outcome outcome, String reason) {

    public enum Outcome {
        SUCCESS,
        RETRYABLE,
        PERMANENT
    }
}
