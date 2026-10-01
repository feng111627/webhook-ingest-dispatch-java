package com.github.feng111627.webhookingestdispatchjava.security;

import com.github.feng111627.webhookingestdispatchjava.config.WebhookProperties;
import com.github.feng111627.webhookingestdispatchjava.exception.RejectReason;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 校验 X-Webhook-Timestamp（Unix 秒）与 X-Webhook-Signature。
 *
 * <p>签名算法：HMAC-SHA256，签名内容为 {@code timestamp + "." + rawBody}，输出小写 hex。
 *
 * <p>判定顺序固定（各处结论保持一致的关键）：
 * <ol>
 *   <li>时间戳缺失/无法解析 → {@link RejectReason#TIMESTAMP_MISSING}</li>
 *   <li>时间戳超出容忍窗口（过去或未来）→ {@link RejectReason#TIMESTAMP_EXPIRED}</li>
 *   <li>签名头缺失 → {@link RejectReason#SIGNATURE_MISSING}</li>
 *   <li>签名不匹配 → {@link RejectReason#SIGNATURE_INVALID}</li>
 *   <li>全部通过 → {@link Optional#empty()}</li>
 * </ol>
 * 时间戳不合法时不再继续验签，避免把时间戳问题误报成签名问题。
 */
@Component
public class SignatureVerifier {

    private static final Logger log = LoggerFactory.getLogger(SignatureVerifier.class);
    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final WebhookProperties properties;

    public SignatureVerifier(WebhookProperties properties) {
        this.properties = properties;
    }

    /**
     * 校验请求头。
     *
     * @param eventId 事件标识（请求头校验阶段可能还无法解析出事件 id，允许为 null）
     * @return 校验通过返回 Optional.empty()；否则返回拒绝原因
     */
    public Optional<RejectReason> verify(String eventId, String timestampHeader,
                                         String signatureHeader, String rawBody) {
        // 1. 时间戳必须存在且是 Unix 秒
        if (timestampHeader == null || timestampHeader.isBlank()) {
            log.info("[verify eventId={}] REJECT: X-Webhook-Timestamp 缺失 -> TIMESTAMP_MISSING", eventId);
            return Optional.of(RejectReason.TIMESTAMP_MISSING);
        }
        final long timestampSeconds;
        try {
            timestampSeconds = Long.parseLong(timestampHeader.trim());
        } catch (NumberFormatException ex) {
            log.info("[verify eventId={}] REJECT: X-Webhook-Timestamp 不是Unix秒(value={}) -> TIMESTAMP_MISSING",
                    eventId, timestampHeader);
            return Optional.of(RejectReason.TIMESTAMP_MISSING);
        }

        // 2. 时间戳新鲜度：与当前时间的绝对偏差不超过容忍窗口（过去/未来对称处理）
        Instant ts = Instant.ofEpochSecond(timestampSeconds);
        long driftSeconds = Duration.between(ts, Instant.now()).getSeconds();
        long tolerance = properties.getTimestampToleranceSeconds();
        if (Math.abs(driftSeconds) > tolerance) {
            log.info("[verify eventId={}] REJECT: 时间戳过期/超前(ts={}, drift={}s, tolerance={}s) -> TIMESTAMP_EXPIRED",
                    eventId, ts, driftSeconds, tolerance);
            return Optional.of(RejectReason.TIMESTAMP_EXPIRED);
        }

        // 3. 签名头必须存在
        if (signatureHeader == null || signatureHeader.isBlank()) {
            log.info("[verify eventId={}] REJECT: X-Webhook-Signature 缺失 -> SIGNATURE_MISSING", eventId);
            return Optional.of(RejectReason.SIGNATURE_MISSING);
        }

        // 4. 签名必须匹配（常量时间比较，防止时序侧信道）
        String expected = sign(timestampHeader.trim(), rawBody == null ? "" : rawBody);
        String actual = signatureHeader.trim().toLowerCase();
        if (!constantTimeHexEquals(expected, actual)) {
            log.info("[verify eventId={}] REJECT: 签名不匹配(expected={}, actual={}) -> SIGNATURE_INVALID",
                    eventId, expected, actual);
            return Optional.of(RejectReason.SIGNATURE_INVALID);
        }

        log.info("[verify eventId={}] PASS: 时间戳新鲜(drift={}s)且签名匹配", eventId, driftSeconds);
        return Optional.empty();
    }

    /** 按签名规范计算 hex 签名，测试工具复用同一实现，保证规则只有一处。 */
    public String sign(String timestamp, String rawBody) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(properties.getSecret().getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            byte[] data = (timestamp + "." + (rawBody == null ? "" : rawBody)).getBytes(StandardCharsets.UTF_8);
            return toHex(mac.doFinal(data));
        } catch (Exception ex) {
            throw new IllegalStateException("HMAC 签名计算失败", ex);
        }
    }

    private static boolean constantTimeHexEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
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
