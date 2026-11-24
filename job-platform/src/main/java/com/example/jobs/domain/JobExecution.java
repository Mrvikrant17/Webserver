package com.example.jobs.domain;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public record JobExecution(long id, UUID jobId, int attempt, String workerId, Instant startedAt,
                           Instant finishedAt, String outcome, String errorClass, String errorMessage) {

    public static JobExecution map(ResultSet rs, int rowNum) throws SQLException {
        Timestamp fin = rs.getTimestamp("finished_at");
        return new JobExecution(rs.getLong("id"), rs.getObject("job_id", UUID.class), rs.getInt("attempt"),
                rs.getString("worker_id"), rs.getTimestamp("started_at").toInstant(),
                fin == null ? null : fin.toInstant(), rs.getString("outcome"),
                rs.getString("error_class"), rs.getString("error_message"));
    }
}
