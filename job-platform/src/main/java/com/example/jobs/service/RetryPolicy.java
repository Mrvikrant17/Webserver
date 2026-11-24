package com.example.jobs.service;

import com.example.jobs.handler.NonRetryableException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Exponential backoff with jitter + failure classification. */
@Component
public class RetryPolicy {

    private final long baseMs;
    private final long maxMs;
    private final double jitter;

    public RetryPolicy(@Value("${app.retry.base-delay-ms:2000}") long baseMs,
                       @Value("${app.retry.max-delay-ms:300000}") long maxMs,
                       @Value("${app.retry.jitter:0.2}") double jitter) {
        this.baseMs = baseMs;
        this.maxMs = maxMs;
        this.jitter = jitter;
    }

    /** Delay before the next attempt, given the attempt number that just failed (1-based). */
    public Duration delayFor(int failedAttempt) {
        long exp = Math.min(maxMs, baseMs * (1L << Math.min(Math.max(failedAttempt - 1, 0), 20)));
        double factor = 1 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitter;
        return Duration.ofMillis(Math.min(maxMs, Math.round(exp * factor)));
    }

    /** Transient infrastructure errors are retried; errors that cannot succeed on retry are not. */
    public boolean isRetryable(Throwable t) {
        return !(t instanceof NonRetryableException
                || t instanceof IllegalArgumentException
                || t instanceof JsonProcessingException);
    }
}
