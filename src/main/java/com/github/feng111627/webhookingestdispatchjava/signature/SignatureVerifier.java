package com.github.feng111627.webhookingestdispatchjava.signature;

import com.github.feng111627.webhookingestdispatchjava.config.WebhookProperties;
import com.github.feng111627.webhookingestdispatchjava.model.DecisionCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;

/**
 * 签名与时间戳校验。
 * 签名方案：hex( HMAC-SHA256(secret, timestamp + "." + rawBody) )，十六进制小写。
 *
 * 判定顺序（结论可解释、可区分）：
 * 1. 缺头/时间戳非数字 -> MISSING_HEADERS（400）
 * 2. 签名不匹配       -> INVALID_SIGNATURE（401）
 * 3. 时间戳超出容忍窗 -> TIMESTAMP_EXPIRED（401）
 * 先验签名再验时间戳，避免过期请求通过时间戳探测侧信道；
 * 但两类失败使用不同判定码，调用方可以明确区分。
 */
@Component
public class SignatureVerifier {

    private static final Logger log = LoggerFactory.getLogger(SignatureVerifier.class);
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final WebhookProperties properties;

    public SignatureVerifier(WebhookProperties properties) {
        this.properties = properties;
    }

    /** 校验结果：统一判定码 + 可解释原因。 */
    public record Verification(DecisionCode code, String reason, Long timestamp) {
        public boolean valid() {
            return code == DecisionCode.ACCEPTED;
        }
    }

    public Verification verify(String signatureHeader, String timestampHeader, String rawBody) {
        if (signatureHeader == null || signatureHeader.isBlank()
                || timestampHeader == null || timestampHeader.isBlank()) {
            log.info("签名校验 判定=MISSING_HEADERS 依据=缺少 {} 或 {} 请求头",
                    "X-Webhook-Signature", "X-Webhook-Timestamp");
            return new Verification(DecisionCode.MISSING_HEADERS,
                    "missing signature/timestamp header", null);
        }

        final long timestamp;
        try {
            timestamp = Long.parseLong(timestampHeader.trim());
        } catch (NumberFormatException e) {
            log.info("签名校验 判定=MISSING_HEADERS 依据=时间戳头不是Unix秒: {}", timestampHeader);
            return new Verification(DecisionCode.MISSING_HEADERS,
                    "timestamp header is not a unix epoch second", null);
        }

        String expected = sign(properties.getSecurity().getSecret(), timestamp, rawBody);
        if (!constantTimeEquals(expected, normalize(signatureHeader))) {
            log.info("签名校验 timestamp={} 判定=INVALID_SIGNATURE 依据=重算签名{}与请求签名不一致",
                    timestamp, expected);
            return new Verification(DecisionCode.INVALID_SIGNATURE,
                    "signature does not match HMAC-SHA256(timestamp.body)", timestamp);
        }

        long ageSeconds = Duration.between(Instant.ofEpochSecond(timestamp), Instant.now()).getSeconds();
        long tolerance = properties.getSecurity().getTimestampToleranceSeconds();
        if (Math.abs(ageSeconds) > tolerance) {
            log.info("签名校验 timestamp={} 判定=TIMESTAMP_EXPIRED 依据=时间偏差{}s超过容忍{}s",
                    timestamp, ageSeconds, tolerance);
            return new Verification(DecisionCode.TIMESTAMP_EXPIRED,
                    "timestamp " + ageSeconds + "s outside tolerance " + tolerance + "s", timestamp);
        }

        log.debug("签名校验 timestamp={} 判定=ACCEPTED 依据=签名匹配且时间偏差{}s在容忍{}s内",
                timestamp, ageSeconds, tolerance);
        return new Verification(DecisionCode.ACCEPTED, "signature and timestamp valid", timestamp);
    }

    /** 计算签名，供生产校验与测试/本地脚本复用，保证算法唯一来源。 */
    public static String sign(String secret, long timestamp, String rawBody) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            byte[] digest = mac.doFinal((timestamp + "." + rawBody).getBytes(StandardCharsets.UTF_8));
            return toHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("HMAC-SHA256 计算失败", e);
        }
    }

    private static String normalize(String signatureHeader) {
        String v = signatureHeader.trim();
        // 兼容 "sha256=<hex>" 形式
        if (v.startsWith("sha256=")) {
            v = v.substring("sha256=".length());
        }
        return v.toLowerCase();
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
