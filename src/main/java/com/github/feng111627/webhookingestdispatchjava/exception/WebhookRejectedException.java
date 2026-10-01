package com.github.feng111627.webhookingestdispatchjava.exception;

/** 入口处被拒绝的请求：携带明确原因与 HTTP 状态码 */
public class WebhookRejectedException extends RuntimeException {

    private final RejectReason reason;
    private final int httpStatus;

    public WebhookRejectedException(RejectReason reason, String message, int httpStatus) {
        super(message);
        this.reason = reason;
        this.httpStatus = httpStatus;
    }

    public RejectReason getReason() {
        return reason;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}
