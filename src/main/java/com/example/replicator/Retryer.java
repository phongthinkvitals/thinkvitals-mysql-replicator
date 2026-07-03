package com.example.replicator;

import org.slf4j.Logger;

import java.util.concurrent.Callable;

class Retryer {
    private final int maxAttempts;
    private final long backoffMs;

    Retryer(ReplicatorProperties.Retry retry) {
        this.maxAttempts = retry.getMaxAttempts();
        this.backoffMs = retry.getBackoffMs();
    }

    <T> T call(String operation, Logger log, Callable<T> callable) throws Exception {
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

    void run(String operation, Logger log, ThrowingRunnable runnable) throws Exception {
        call(operation, log, () -> {
            runnable.run();
            return null;
        });
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
