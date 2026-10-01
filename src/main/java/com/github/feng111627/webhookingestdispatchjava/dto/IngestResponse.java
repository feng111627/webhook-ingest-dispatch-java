package com.github.feng111627.webhookingestdispatchjava.dto;

/**
 * 接收接口的统一返回：无论首次成功还是重复事件，HTTP 都是 2xx。
 */
public record IngestResponse(String eventId, String status, String reason, int attempts) {
}
