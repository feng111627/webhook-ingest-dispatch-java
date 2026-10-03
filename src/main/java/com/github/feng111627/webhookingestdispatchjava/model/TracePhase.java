package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 判定轨迹的阶段（按事件串起从接收到重放的每一步）。
 */
public enum TracePhase {
    RECEIVED,
    REJECTED,
    DUPLICATE,
    ATTEMPT,
    BACKOFF,
    DEAD,
    SUCCEEDED,
    REPLAY_REQUESTED,
    REPLAY_REJECTED,
    REPLAY_SUCCEEDED,
    REPLAY_FAILED
}
