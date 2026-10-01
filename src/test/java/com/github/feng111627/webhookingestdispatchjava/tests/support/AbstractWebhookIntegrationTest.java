package com.github.feng111627.webhookingestdispatchjava.tests.support;

import com.github.feng111627.webhookingestdispatchjava.dedup.ProcessedEventRepository;
import com.github.feng111627.webhookingestdispatchjava.dlq.DeadLetterRepository;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * 集成测试基类：启动真实 HTTP 服务（随机端口），
 * 重试参数调到极小（首退避 50ms、最多 3 次），便于在测试内等待异步重试落定。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "webhook.security.secret=test-secret",
                "webhook.security.timestamp-tolerance-seconds=60",
                "webhook.retry.max-attempts=3",
                "webhook.retry.initial-backoff-ms=50",
                "webhook.retry.backoff-multiplier=2.0",
                "webhook.retry.max-backoff-ms=100"
        })
public abstract class AbstractWebhookIntegrationTest {

    protected static final String SECRET = "test-secret";
    protected static final long TOLERANCE_SECONDS = 60;

    @LocalServerPort
    protected int port;

    @Autowired
    protected ProcessedEventRepository eventRepository;

    @Autowired
    protected DeadLetterRepository deadLetterRepository;

    protected WebhookTestClient client;

    @BeforeEach
    void setUpBase() {
        eventRepository.clear();
        deadLetterRepository.clear();
        ControllableHandler.reset();
        client = new WebhookTestClient("http://localhost:" + port, SECRET);
    }

    protected String url() {
        return "http://localhost:" + port;
    }
}
