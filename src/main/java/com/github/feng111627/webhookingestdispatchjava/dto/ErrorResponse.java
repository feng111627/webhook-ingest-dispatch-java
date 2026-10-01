package com.github.feng111627.webhookingestdispatchjava.dto;

/** 统一错误体，code 用于机读判定，message 用于人读解释 */
public record ErrorResponse(String code, String message) {
}
