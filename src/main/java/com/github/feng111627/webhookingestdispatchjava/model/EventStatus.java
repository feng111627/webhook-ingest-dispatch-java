package com.github.feng111627.webhookingestdispatchjava.model;

/** 事件生命周期状态。 */
public enum EventStatus {
    RECEIVED,
    PROCESSING,
    SUCCEEDED,
    DEAD,
    REPLAYED
}
