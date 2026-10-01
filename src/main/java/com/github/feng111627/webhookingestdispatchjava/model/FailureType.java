package com.github.feng111627.webhookingestdispatchjava.model;

/** 失败类型：暂时失败可重试；永久失败不再重试。 */
public enum FailureType {
    TRANSIENT,
    PERMANENT
}
