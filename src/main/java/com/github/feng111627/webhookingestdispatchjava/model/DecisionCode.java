package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 全链路统一的判定结论码。每个码固定绑定一个 HTTP 状态，
 * 保证接收、分发、死信、重放各环节对同一结论的表达一致。
 */
public enum DecisionCode {

    /** 首次接收并处理成功。 */
    ACCEPTED(200),
    /** 重复事件：按成功返回，但不重复分发。 */
    DUPLICATE_EVENT(200),
    /** 签名校验不通过。 */
    INVALID_SIGNATURE(401),
    /** 时间戳过期或超出容忍窗口。 */
    TIMESTAMP_EXPIRED(401),
    /** 缺少必要请求头或请求头无法解析。 */
    MISSING_HEADERS(400),
    /** 请求体不是合法 JSON 或缺少必填字段。 */
    INVALID_PAYLOAD(400),
    /** 没有匹配的事件处理逻辑，不静默丢弃，进入死信。 */
    UNROUTABLE_EVENT(422),
    /** 处理逻辑判定为永久失败，不再重试，进入死信。 */
    PERMANENT_FAILURE(422),
    /** 本次处理暂时失败，已安排退避重试。 */
    RETRYING(202),
    /** 重试次数用尽仍失败，进入死信。 */
    RETRIES_EXHAUSTED(202),
    /** 事件已进入死信（查询/日志用）。 */
    DEAD_LETTERED(200),
    /** 重放成功。 */
    REPLAY_SUCCEEDED(200),
    /** 重放失败（永久失败或仍无处理逻辑）。 */
    REPLAY_FAILED(422),
    /** 重放请求重复到达，已有重放在进行中或已完成。 */
    REPLAY_SKIPPED_DUPLICATE(409),
    /** 死信不存在。 */
    NOT_FOUND(404);

    private final int httpStatus;

    DecisionCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}
