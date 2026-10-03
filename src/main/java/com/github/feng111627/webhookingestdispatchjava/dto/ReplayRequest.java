package com.github.feng111627.webhookingestdispatchjava.dto;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 死信重放请求体（可选）。
 *
 * @param payload 人工修正后的负载；缺省/null 表示沿用原始负载重放
 */
public record ReplayRequest(JsonNode payload) {
}
