package com.example.jobs.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.jobs.handler.NonRetryableException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private final RetryPolicy noJitter = new RetryPolicy(1000, 30_000, 0.0);

    @Test
    void backoffGrowsExponentiallyAndIsCapped() {
        assertTrue(noJitter.delayFor(1).toMillis() == 1000);
        assertTrue(noJitter.delayFor(2).toMillis() == 2000);
        assertTrue(noJitter.delayFor(3).toMillis() == 4000);
        assertTrue(noJitter.delayFor(10).toMillis() == 30_000);
    }

    @Test
    void classifiesFailures() {
        assertFalse(noJitter.isRetryable(new NonRetryableException("bad input")));
        assertFalse(noJitter.isRetryable(new IllegalArgumentException("bad")));
        assertTrue(noJitter.isRetryable(new IOException("timeout")));
    }
}
