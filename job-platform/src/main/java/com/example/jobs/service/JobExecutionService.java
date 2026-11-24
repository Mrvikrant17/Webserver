package com.example.jobs.service;

import static com.example.jobs.config.RabbitConfig.DEAD_KEY;
import static com.example.jobs.config.RabbitConfig.DLX;
import static com.example.jobs.config.RabbitConfig.JOBS_EXCHANGE;
import static com.example.jobs.config.RabbitConfig.RUN_KEY;

import com.example.jobs.domain.Job;
import com.example.jobs.repo.ExecutionRepository;
import com.example.jobs.repo.JobRepository;
import com.example.jobs.repo.OutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Worker-side state transitions. Every method is ONE database transaction that changes the job row,
 * the execution history, and (when a new message is needed) the outbox, atomically.
 * The RabbitMQ ack happens only AFTER these commit.
 */
@Service
public class JobExecutionService {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionService.class);

    private final JobRepository jobs;
    private final ExecutionRepository executions;
    private final OutboxRepository outbox;
    private final RetryPolicy retryPolicy;
    private final MeterRegistry metrics;
    private final Duration lease;

    public JobExecutionService(JobRepository jobs, ExecutionRepository executions, OutboxRepository outbox,
                               RetryPolicy retryPolicy, MeterRegistry metrics,
                               @Value("${app.worker.lease-seconds:60}") long leaseSeconds) {
        this.jobs = jobs;
        this.executions = executions;
        this.outbox = outbox;
        this.retryPolicy = retryPolicy;
        this.metrics = metrics;
        this.lease = Duration.ofSeconds(leaseSeconds);
    }

    public Duration lease() {
        return lease;
    }

    /** Empty = duplicate delivery / not yet due / cancelled / already finished: caller just acks and moves on. */
    @Transactional
    public Optional<Job> claim(UUID jobId, String workerId) {
        Optional<Job> claimed = jobs.claim(jobId, workerId, lease);
        claimed.ifPresent(j -> executions.start(j.id(), j.attempts(), workerId));
        return claimed;
    }

    @Transactional
    public void succeed(Job job, String workerId) {
        if (jobs.markSucceeded(job.id(), workerId, job.attempts()) == 0) {
            log.warn("Lost lease before completing job {}; result discarded (another attempt owns it)", job.id());
            return;
        }
        executions.finish(job.id(), job.attempts(), "SUCCEEDED", null, null);
        metrics.counter("jobs.outcome", "outcome", "succeeded", "type", job.type()).increment();
    }

    @Transactional
    public void fail(Job job, String workerId, Throwable error, boolean retryable) {
        String message = truncate(error.getMessage());
        String errorClass = error.getClass().getName();
        boolean exhausted = job.attempts() >= job.maxAttempts();

        if (!retryable || exhausted) {
            if (jobs.markDead(job.id(), workerId, job.attempts(), message) == 0) {
                log.warn("Lost lease before dead-lettering job {}", job.id());
                return;
            }
            executions.finish(job.id(), job.attempts(),
                    retryable ? "FAILED_EXHAUSTED" : "FAILED_PERMANENT", errorClass, message);
            outbox.add(job.id(), DLX, DEAD_KEY, Instant.now());
            metrics.counter("jobs.outcome", "outcome", "dead", "type", job.type()).increment();
            log.error("Job {} moved to DLQ after attempt {}: {}", job.id(), job.attempts(), message);
        } else {
            Instant next = Instant.now().plus(retryPolicy.delayFor(job.attempts()));
            if (jobs.markForRetry(job.id(), workerId, job.attempts(), next, message) == 0) {
                log.warn("Lost lease before scheduling retry of job {}", job.id());
                return;
            }
            executions.finish(job.id(), job.attempts(), "FAILED_RETRYABLE", errorClass, message);
            outbox.add(job.id(), JOBS_EXCHANGE, RUN_KEY, next);   // delivered once available_at passes
            metrics.counter("jobs.outcome", "outcome", "retried", "type", job.type()).increment();
            log.warn("Job {} failed on attempt {}/{}; retry at {}: {}",
                    job.id(), job.attempts(), job.maxAttempts(), next, message);
        }
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 2000 ? s.substring(0, 2000) : s;
    }
}
