package com.example.jobs.service;

import static com.example.jobs.config.RabbitConfig.JOBS_EXCHANGE;
import static com.example.jobs.config.RabbitConfig.RUN_KEY;

import com.example.jobs.domain.Job;
import com.example.jobs.domain.JobExecution;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.repo.ExecutionRepository;
import com.example.jobs.repo.JobRepository;
import com.example.jobs.repo.OutboxRepository;
import com.example.jobs.web.SubmitRequest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Client- and admin-facing operations. */
@Service
public class JobService {

    private static final int DEFAULT_MAX_ATTEMPTS = 5;
    private static final int DEFAULT_REPLAY_ATTEMPTS = 3;

    private final JobRepository jobs;
    private final ExecutionRepository executions;
    private final OutboxRepository outbox;

    public JobService(JobRepository jobs, ExecutionRepository executions, OutboxRepository outbox) {
        this.jobs = jobs;
        this.executions = executions;
        this.outbox = outbox;
    }

    /**
     * Durable submission. The job row and its outbox row commit together, or not at all.
     * If the app dies right after commit, the relay still publishes later. Nothing is lost.
     */
    @Transactional
    public Job submit(SubmitRequest req) {
        UUID id = UUID.randomUUID();
        Instant runAt = Instant.now().plusSeconds(req.delaySeconds() == null ? 0 : req.delaySeconds());
        String payload = req.payload() == null ? "{}" : req.payload().toString();
        int maxAttempts = req.maxAttempts() == null ? DEFAULT_MAX_ATTEMPTS : req.maxAttempts();

        boolean inserted = jobs.insert(id, req.type(), payload, maxAttempts, req.idempotencyKey(), runAt);
        if (!inserted) {
            // Same idempotency key submitted before: return the original job instead of creating a duplicate.
            return jobs.findByIdempotencyKey(req.idempotencyKey()).orElseThrow();
        }
        outbox.add(id, JOBS_EXCHANGE, RUN_KEY, runAt);
        return jobs.findById(id).orElseThrow();
    }

    public Job get(UUID id) {
        return jobs.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    public List<JobExecution> history(UUID id) {
        get(id);
        return executions.findByJob(id);
    }

    public List<Job> list(JobStatus status, int limit) {
        return jobs.list(status, Math.min(Math.max(limit, 1), 500));
    }

    /** Controlled replay of a DEAD job: grants fresh attempts and re-enqueues via the outbox. */
    @Transactional
    public Job replay(UUID id, Integer extraAttempts) {
        get(id);
        int extra = extraAttempts == null ? DEFAULT_REPLAY_ATTEMPTS : extraAttempts;
        if (jobs.replay(id, extra) == 0) {
            throw new IllegalStateException("Only DEAD jobs can be replayed");
        }
        outbox.add(id, JOBS_EXCHANGE, RUN_KEY, Instant.now());
        return get(id);
    }

    @Transactional
    public int replayAllDead(int limit) {
        List<Job> dead = jobs.list(JobStatus.DEAD, limit);
        dead.forEach(j -> replay(j.id(), null));
        return dead.size();
    }

    @Transactional
    public Job cancel(UUID id) {
        get(id);
        if (jobs.cancel(id) == 0) {
            throw new IllegalStateException("Only PENDING jobs can be cancelled");
        }
        return get(id);
    }
}
