package com.example.jobs.worker;

import static com.example.jobs.config.RabbitConfig.DEAD_KEY;
import static com.example.jobs.config.RabbitConfig.DLX;
import static com.example.jobs.config.RabbitConfig.JOBS_EXCHANGE;
import static com.example.jobs.config.RabbitConfig.RUN_KEY;

import com.example.jobs.domain.Job;
import com.example.jobs.repo.ExecutionRepository;
import com.example.jobs.repo.JobRepository;
import com.example.jobs.repo.OutboxRepository;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Worker crash recovery. A job stuck in RUNNING with an expired lease means its worker died
 * (or stalled so long its heartbeat stopped). Requeue it, or dead-letter it if attempts are exhausted.
 * Safe to run on every instance: SKIP LOCKED partitions the work.
 */
@Component
@ConditionalOnProperty(name = "app.reaper.enabled", havingValue = "true", matchIfMissing = true)
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    private final JobRepository jobs;
    private final ExecutionRepository executions;
    private final OutboxRepository outbox;
    private final TransactionTemplate tx;
    private final MeterRegistry metrics;

    public LeaseReaper(JobRepository jobs, ExecutionRepository executions, OutboxRepository outbox,
                       TransactionTemplate tx, MeterRegistry metrics) {
        this.jobs = jobs;
        this.executions = executions;
        this.outbox = outbox;
        this.tx = tx;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${app.reaper.interval-ms:10000}")
    public void reap() {
        try {
            tx.executeWithoutResult(status -> {
                List<Job> expired = jobs.lockExpiredLeases(50);
                for (Job job : expired) {
                    String reason = "Lease expired; worker " + job.lockedBy() + " presumed crashed";
                    executions.finish(job.id(), job.attempts(), "LEASE_EXPIRED", "WorkerCrash", reason);
                    if (job.attempts() >= job.maxAttempts()) {
                        jobs.kill(job.id(), reason);
                        outbox.add(job.id(), DLX, DEAD_KEY, Instant.now());
                        log.error("Job {} dead-lettered after lease expiry on final attempt", job.id());
                    } else {
                        jobs.requeue(job.id(), Instant.now(), reason);
                        outbox.add(job.id(), JOBS_EXCHANGE, RUN_KEY, Instant.now());
                        log.warn("Job {} requeued after lease expiry (attempt {}/{})",
                                job.id(), job.attempts(), job.maxAttempts());
                    }
                    metrics.counter("jobs.lease.expired").increment();
                }
            });
        } catch (Exception e) {
            log.warn("Reaper iteration failed: {}", e.getMessage());
        }
    }
}
