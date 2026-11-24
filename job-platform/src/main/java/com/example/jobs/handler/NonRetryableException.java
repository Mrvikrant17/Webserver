package com.example.jobs.handler;

/** Throw for failures that will never succeed on retry (bad input, unknown type, business rule violation). */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
