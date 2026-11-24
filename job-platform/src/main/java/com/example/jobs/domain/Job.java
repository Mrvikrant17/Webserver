package com.example.jobs.domain;

import com.fasterxml.jackson.annotation.JsonRawValue;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public record Job(
        UUID id,
        String type,
        @JsonRawValue String payload,
        JobStatus status,
        int attempts,
        int maxAttempts,
        Instant nextRunAt,
        String lockedBy,
        Instant leaseExpiresAt,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant completedAt) {

    public static Job map(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                rs.getString("payload"),
                JobStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                ts(rs, "next_run_at"),
                rs.getString("locked_by"),
                ts(rs, "lease_expires_at"),
                rs.getString("last_error"),
                ts(rs, "created_at"),
                ts(rs, "updated_at"),
                ts(rs, "completed_at"));
    }

    private static Instant ts(ResultSet rs, String col) throws SQLException {
        Timestamp t = rs.getTimestamp(col);
        return t == null ? null : t.toInstant();
    }
}
