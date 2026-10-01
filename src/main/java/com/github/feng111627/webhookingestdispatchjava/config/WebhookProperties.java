package com.github.feng111627.webhookingestdispatchjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Webhook 链路可调参数（骨架阶段仅定义类型）。
 */
@ConfigurationProperties(prefix = "webhook")
public class WebhookProperties {

    /** HMAC 签名密钥 */
    private String secret = "test-secret";

    /** 时间戳允许的偏移秒数（过去/未来对称） */
    private long timestampToleranceSeconds = 300;

    /** 单次分发最多尝试次数（含首次） */
    private int maxAttempts = 3;

    /** 退避基础间隔毫秒，第 n 次重试前等待 base * 2^(n-1) */
    private long backoffBaseMillis = 50;

    public String getSecret() { return secret; }
    public void setSecret(String secret) { this.secret = secret; }

    public long getTimestampToleranceSeconds() { return timestampToleranceSeconds; }
    public void setTimestampToleranceSeconds(long timestampToleranceSeconds) {
        this.timestampToleranceSeconds = timestampToleranceSeconds;
    }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getBackoffBaseMillis() { return backoffBaseMillis; }
    public void setBackoffBaseMillis(long backoffBaseMillis) {
        this.backoffBaseMillis = backoffBaseMillis;
    }
}
