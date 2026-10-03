package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 事件轨迹的步骤类型：从接收、判定、每次尝试与退避、进入死信到重放，每一步都可按 eventId 串查。
 */
public enum TraceStep {
    /** 首次接收并占位成功 */
    RECEIVED,
    /** 命中重复（含在途并发重复），不再分发 */
    DUPLICATE,
    /** 入口/重放请求被明确拒绝（携带判定依据） */
    REJECTED,
    /** 一次处理尝试的结论（SUCCESS/RETRYABLE/PERMANENT） */
    ATTEMPT,
    /** 暂时失败后的退避等待 */
    BACKOFF,
    /** 进入死信 */
    DEAD_LETTERED,
    /** 处理成功（首次或重放） */
    SUCCEEDED,
    /** 重放请求到达（无论是否被受理） */
    REPLAY_REQUESTED,
    /** 重放抢占成功，开始执行（记录本次实际使用的负载来源） */
    REPLAY_STARTED
}
