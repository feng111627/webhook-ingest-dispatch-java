package com.github.feng111627.webhookingestdispatchjava.model;

/** 事件在链路中的状态机：接收 -> 处理中 -> 成功 / 死信 */
public enum EventState {
    RECEIVED,
    PROCESSING,
    SUCCEEDED,
    DEAD
}
