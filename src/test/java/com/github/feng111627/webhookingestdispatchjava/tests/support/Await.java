package com.github.feng111627.webhookingestdispatchjava.tests.support;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/** 测试中等待异步重试链落定的简单轮询工具。 */
public final class Await {

    private Await() {
    }

    public static void until(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        AssertionError last = new AssertionError("condition not met before timeout");
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (AssertionError e) {
                last = e;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw last;
    }
}
