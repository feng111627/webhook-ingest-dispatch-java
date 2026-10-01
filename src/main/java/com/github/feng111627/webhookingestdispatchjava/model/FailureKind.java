package com.github.feng111627.webhookingestdispatchjava.model;

/** 失败性质：未失败 / 可重试的暂时失败 / 不可重试的永久失败 */
public enum FailureKind {
    NONE,
    RETRYABLE,
    PERMANENT
}
