package com.example.replicator.util;

import com.example.replicator.config.ReplicatorProperties;
import org.slf4j.Logger;

import java.util.concurrent.Callable;

public class Retryer {
    private final int maxAttempts;
    private final long backoffMs;

    public Retryer(ReplicatorProperties.Retry retry) {
        this.maxAttempts = retry.getMaxAttempts();
        this.backoffMs = retry.getBackoffMs();
    }

    public <T> T call(String operation, Logger log, Callable<T> callable) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return callable.call();
            } catch (Exception e) {
                last = e;
                log.warn("{} failed attempt={}/{} error={}", operation, attempt, maxAttempts, e.getMessage());
                if (attempt < maxAttempts) {
                    Thread.sleep(backoffMs);
                }
            }
        }
        throw last;
    }

    public void run(String operation, Logger log, ThrowingRunnable runnable) throws Exception {
        call(operation, log, () -> {
            runnable.run();
            return null;
        });
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }
}
