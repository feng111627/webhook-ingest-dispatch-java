package com.github.feng111627.webhookingestdispatchjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Webhook 链路配置：签名密钥/时间戳容忍度、重试退避参数。
 */
@ConfigurationProperties(prefix = "webhook")
public class WebhookProperties {

    private Security security = new Security();
    private Retry retry = new Retry();

    public Security getSecurity() {
        return security;
    }

    public void setSecurity(Security security) {
        this.security = security;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public static class Security {
        /** HMAC-SHA256 签名密钥。 */
        private String secret = "changeit";
        /** 时间戳允许的偏差秒数（绝对值，过期或未来时间戳均拒绝）。 */
        private long timestampToleranceSeconds = 300;

        public String getSecret() {
            return secret;
        }

        public void setSecret(String secret) {
            this.secret = secret;
        }

        public long getTimestampToleranceSeconds() {
            return timestampToleranceSeconds;
        }

        public void setTimestampToleranceSeconds(long timestampToleranceSeconds) {
            this.timestampToleranceSeconds = timestampToleranceSeconds;
        }
    }

    public static class Retry {
        /** 最大处理次数（含首次处理），超过后进入死信。 */
        private int maxAttempts = 3;
        /** 首次重试退避毫秒数。 */
        private long initialBackoffMs = 1000;
        /** 退避倍数。 */
        private double backoffMultiplier = 2.0;
        /** 退避上限毫秒数。 */
        private long maxBackoffMs = 30000;

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getInitialBackoffMs() {
            return initialBackoffMs;
        }

        public void setInitialBackoffMs(long initialBackoffMs) {
            this.initialBackoffMs = initialBackoffMs;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }

        public long getMaxBackoffMs() {
            return maxBackoffMs;
        }

        public void setMaxBackoffMs(long maxBackoffMs) {
            this.maxBackoffMs = maxBackoffMs;
        }
    }
}
