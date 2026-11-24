package com.example.jobs.repo;

import com.example.jobs.domain.Job;
import com.example.jobs.domain.JobStatus;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * All state transitions are conditional UPDATEs. The WHERE clause is the concurrency control:
 * if it matches 0 rows, somebody else got there first (or we lost our lease) and the caller backs off.
 */
@Repository
public class JobRepository {

    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return false if a job with the same idempotency key already exists. */
    public boolean insert(UUID id, String type, String payload, int maxAttempts, String idempotencyKey, Instant runAt) {
        return jdbc.update("""
                INSERT INTO jobs (id, type, payload, status, max_attempts, idempotency_key, next_run_at)
                VALUES (?, ?, ?::jsonb, 'PENDING', ?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """, id, type, payload, maxAttempts, idempotencyKey, Timestamp.from(runAt)) == 1;
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = ?", Job::map, id).stream().findFirst();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT * FROM jobs WHERE idempotency_key = ?", Job::map, key).stream().findFirst();
    }

    public List<Job> list(JobStatus status, int limit) {
        if (status == null) {
            return jdbc.query("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", Job::map, limit);
        }
        return jdbc.query("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                Job::map, status.name(), limit);
    }

    public long countByStatus(JobStatus status) {
        Long n = jdbc.queryForObject("SELECT count(*) FROM jobs WHERE status = ?", Long.class, status.name());
        return n == null ? 0 : n;
    }

    /**
     * Atomic claim. Only ONE worker can flip PENDING -> RUNNING, so duplicate deliveries are harmless.
     * next_run_at <= now() stops a stale duplicate message from running a job before its backoff has elapsed.
     */
    public Optional<Job> claim(UUID id, String workerId, Duration lease) {
        return jdbc.query("""
                UPDATE jobs
                   SET status = 'RUNNING', attempts = attempts + 1, locked_by = ?,
                       lease_expires_at = now() + make_interval(secs => ?), updated_at = now()
                 WHERE id = ? AND status = 'PENDING' AND next_run_at <= now()
                RETURNING *
                """, Job::map, workerId, (double) lease.toSeconds(), id).stream().findFirst();
    }

    public int heartbeat(UUID id, String workerId, int attempt, Duration lease) {
        return jdbc.update("""
                UPDATE jobs SET lease_expires_at = now() + make_interval(secs => ?), updated_at = now()
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ? AND attempts = ?
                """, (double) lease.toSeconds(), id, workerId, attempt);
    }

    // The three terminal-for-this-attempt transitions are "fenced" on (worker, attempt),
    // so a zombie worker whose lease expired cannot overwrite a newer attempt's result.

    public int markSucceeded(UUID id, String workerId, int attempt) {
        return jdbc.update("""
                UPDATE jobs SET status = 'SUCCEEDED', completed_at = now(), updated_at = now(),
                       locked_by = NULL, lease_expires_at = NULL, last_error = NULL
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ? AND attempts = ?
                """, id, workerId, attempt);
    }

    public int markForRetry(UUID id, String workerId, int attempt, Instant nextRunAt, String error) {
        return jdbc.update("""
                UPDATE jobs SET status = 'PENDING', next_run_at = ?, last_error = ?, updated_at = now(),
                       locked_by = NULL, lease_expires_at = NULL
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ? AND attempts = ?
                """, Timestamp.from(nextRunAt), error, id, workerId, attempt);
    }

    public int markDead(UUID id, String workerId, int attempt, String error) {
        return jdbc.update("""
                UPDATE jobs SET status = 'DEAD', last_error = ?, updated_at = now(), completed_at = now(),
                       locked_by = NULL, lease_expires_at = NULL
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ? AND attempts = ?
                """, error, id, workerId, attempt);
    }

    // ---- reaper (crash recovery) ----

    /** Must be called inside a transaction; rows stay locked until commit. SKIP LOCKED lets several reapers coexist. */
    public List<Job> lockExpiredLeases(int limit) {
        return jdbc.query("""
                SELECT * FROM jobs
                 WHERE status = 'RUNNING' AND lease_expires_at < now()
                 ORDER BY lease_expires_at
                 LIMIT ? FOR UPDATE SKIP LOCKED
                """, Job::map, limit);
    }

    public void requeue(UUID id, Instant runAt, String error) {
        jdbc.update("""
                UPDATE jobs SET status = 'PENDING', next_run_at = ?, last_error = ?, updated_at = now(),
                       locked_by = NULL, lease_expires_at = NULL
                 WHERE id = ?
                """, Timestamp.from(runAt), error, id);
    }

    public void kill(UUID id, String error) {
        jdbc.update("""
                UPDATE jobs SET status = 'DEAD', last_error = ?, updated_at = now(), completed_at = now(),
                       locked_by = NULL, lease_expires_at = NULL
                 WHERE id = ?
                """, error, id);
    }

    // ---- admin ----

    /** Replay a DEAD job, granting extra attempts on top of those already used (history numbering stays continuous). */
    public int replay(UUID id, int extraAttempts) {
        return jdbc.update("""
                UPDATE jobs SET status = 'PENDING', max_attempts = attempts + ?, next_run_at = now(),
                       completed_at = NULL, updated_at = now()
                 WHERE id = ? AND status = 'DEAD'
                """, extraAttempts, id);
    }

    public int cancel(UUID id) {
        return jdbc.update("""
                UPDATE jobs SET status = 'CANCELLED', updated_at = now(), completed_at = now()
                 WHERE id = ? AND status = 'PENDING'
                """, id);
    }
}
