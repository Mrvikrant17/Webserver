package com.example.jobs.repo;

import com.example.jobs.domain.OutboxEvent;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Always call inside the same transaction that changes the job row. That is the whole point of the pattern. */
    public void add(UUID jobId, String exchange, String routingKey, Instant availableAt) {
        jdbc.update("""
                INSERT INTO outbox_events (job_id, exchange, routing_key, payload, available_at)
                VALUES (?, ?, ?, ?, ?)
                """, jobId, exchange, routingKey, jobId.toString(), Timestamp.from(availableAt));
    }

    /** Locks due, unpublished rows. Must run inside a transaction. */
    public List<OutboxEvent> lockDue(int limit) {
        return jdbc.query("""
                SELECT id, exchange, routing_key, payload FROM outbox_events
                 WHERE published_at IS NULL AND available_at <= now()
                 ORDER BY available_at, id
                 LIMIT ? FOR UPDATE SKIP LOCKED
                """, OutboxEvent::map, limit);
    }

    public void markPublished(long id) {
        jdbc.update("UPDATE outbox_events SET published_at = now() WHERE id = ?", id);
    }

    public long countUnpublished() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE published_at IS NULL", Long.class);
        return n == null ? 0 : n;
    }
}
