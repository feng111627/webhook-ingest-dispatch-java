package com.github.feng111627.webhookingestdispatchjava.dispatch;

import com.github.feng111627.webhookingestdispatchjava.config.WebhookProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 退避重试调度：指数退避 initial * multiplier^(failedAttempt-1)，并以 maxBackoffMs 封顶。
 * 第 n 次“处理尝试”失败后，若 n 小于最大尝试次数，则安排第 n+1 次；
 * 是否还有重试机会由 {@link #hasMoreAttempts(int)} 统一判定，上层据此进入死信。
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final WebhookProperties properties;
    private final ScheduledExecutorService executor;

    public RetryScheduler(WebhookProperties properties) {
        this.properties = properties;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "webhook-retry");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * @param failedAttempt 刚失败的尝试序号（首次处理为 1）
     * @return 是否还存在下一次重试机会（failedAttempt &lt; maxAttempts）
     */
    public boolean hasMoreAttempts(int failedAttempt) {
        return failedAttempt < properties.getRetry().getMaxAttempts();
    }

    public int maxAttempts() {
        return properties.getRetry().getMaxAttempts();
    }

    /**
     * 计算第 failedAttempt 次失败后，下一次重试的退避毫秒数。
     */
    public long backoffMillis(int failedAttempt) {
        WebhookProperties.Retry cfg = properties.getRetry();
        double raw = cfg.getInitialBackoffMs()
                * Math.pow(cfg.getBackoffMultiplier(), Math.max(0, failedAttempt - 1));
        long delay = Math.min(cfg.getMaxBackoffMs(), (long) raw);
        return Math.max(0L, delay);
    }

    /**
     * @param failedAttempt 刚失败的尝试序号（首次处理为 1）
     */
    public void scheduleWithBackoff(String eventId, int failedAttempt, Runnable retryTask) {
        long delay = backoffMillis(failedAttempt);
        log.info("[eventId={}] 判定=RETRYING 依据=第{}次暂时失败，{}ms后进行第{}/{}次尝试",
                eventId, failedAttempt, delay, failedAttempt + 1, maxAttempts());
        executor.schedule(retryTask, delay, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
