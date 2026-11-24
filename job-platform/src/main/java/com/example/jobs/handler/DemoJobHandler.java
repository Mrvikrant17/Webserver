package com.example.jobs.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Demo handler to exercise every failure path. Payload examples:
 *   {"behavior":"ok"}
 *   {"behavior":"transient","failUntilAttempt":3}   -> fails (retryable) until attempt 3
 *   {"behavior":"permanent"}                          -> non-retryable, goes straight to DLQ
 *   {"behavior":"slow","sleepMs":8000}
 *   {"behavior":"crash"}                              -> kills the JVM on attempt 1 (crash-recovery demo)
 */
@Component
public class DemoJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(DemoJobHandler.class);
    private final ObjectMapper mapper;

    public DemoJobHandler(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String type() {
        return "demo";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
        JsonNode p = mapper.readTree(ctx.payload());
        String behavior = p.path("behavior").asText("ok");
        log.info("Running demo job, behavior={}, attempt={}", behavior, ctx.attempt());
        switch (behavior) {
            case "ok" -> { /* success */ }
            case "slow" -> Thread.sleep(p.path("sleepMs").asLong(5000));
            case "transient" -> {
                if (ctx.attempt() < p.path("failUntilAttempt").asInt(3)) {
                    throw new IOException("simulated transient failure");
                }
            }
            case "permanent" -> throw new NonRetryableException("simulated permanent failure");
            case "crash" -> {
                if (ctx.attempt() == 1) {
                    log.error("Simulating a hard crash");
                    Runtime.getRuntime().halt(1); // no shutdown hooks, no ack, no DB update
                }
            }
            default -> throw new NonRetryableException("unknown behavior " + behavior);
        }
    }
}
