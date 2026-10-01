package com.github.feng111627.webhookingestdispatchjava.web;

import com.github.feng111627.webhookingestdispatchjava.dto.ErrorResponse;
import com.github.feng111627.webhookingestdispatchjava.exception.WebhookRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/**
 * 统一异常 -> 结构化错误响应：code 机读、message 人读，保证判定结论一致、可解释。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /** 入口校验拒绝（签名/时间戳/坏请求/不支持的类型） */
    @ExceptionHandler(WebhookRejectedException.class)
    public ResponseEntity<ErrorResponse> handleRejected(WebhookRejectedException ex) {
        log.warn("请求被拒绝 code={} http={} message={}", ex.getReason(), ex.getHttpStatus(), ex.getMessage());
        return ResponseEntity.status(ex.getHttpStatus())
                .body(new ErrorResponse(ex.getReason().name(), ex.getMessage()));
    }

    /** 死信重放等管理接口的 404/409 等 */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ErrorResponse> handleStatus(ResponseStatusException ex) {
        String code = "HTTP_" + ex.getStatusCode().value();
        return ResponseEntity.status(ex.getStatusCode())
                .body(new ErrorResponse(code, ex.getReason()));
    }

    /** 兜底：未预期错误不暴露堆栈，返回统一结构 */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        log.error("未预期异常", ex);
        return ResponseEntity.internalServerError()
                .body(new ErrorResponse("INTERNAL_ERROR", "服务内部错误，请查看服务端日志"));
    }
}
