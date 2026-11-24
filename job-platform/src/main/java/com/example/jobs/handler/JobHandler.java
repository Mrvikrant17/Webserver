package com.example.jobs.handler;

/**
 * Implement one per job type. Handlers MUST be idempotent: delivery is at-least-once,
 * so a handler can run more than once for the same job (e.g. crash after the side effect, before the DB commit).
 * Use ctx.jobId() as the idempotency key towards downstream systems.
 */
public interface JobHandler {
    String type();

    void handle(JobContext ctx) throws Exception;
}
