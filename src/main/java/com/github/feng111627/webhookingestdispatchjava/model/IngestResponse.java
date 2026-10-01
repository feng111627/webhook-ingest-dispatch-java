package com.github.feng111627.webhookingestdispatchjava.model;

/**
 * 接收接口返回体：携带统一判定码与可解释的原因，便于调用方排障。
 */
public record IngestResponse(String eventId, DecisionCode decision, String reason) {
}
