package com.github.feng111627.webhookingestdispatchjava.exception;

/**
 * 入口校验拒绝原因（签名问题与时间戳问题必须可区分）。
 */
public enum RejectReason {
    BAD_REQUEST,
    TIMESTAMP_MISSING,
    TIMESTAMP_EXPIRED,
    SIGNATURE_MISSING,
    SIGNATURE_INVALID,
    UNSUPPORTED_EVENT_TYPE
}
