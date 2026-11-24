package com.example.jobs.repo;

import com.example.jobs.domain.JobExecution;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ExecutionRepository {

    private final JdbcTemplate jdbc;

    public ExecutionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void start(UUID jobId, int attempt, String workerId) {
        jdbc.update("INSERT INTO job_executions (job_id, attempt, worker_id, outcome) VALUES (?, ?, ?, 'RUNNING')",
                jobId, attempt, workerId);
    }

    public void finish(UUID jobId, int attempt, String outcome, String errorClass, String errorMessage) {
        jdbc.update("""
                UPDATE job_executions SET finished_at = now(), outcome = ?, error_class = ?, error_message = ?
                 WHERE job_id = ? AND attempt = ? AND outcome = 'RUNNING'
                """, outcome, errorClass, errorMessage, jobId, attempt);
    }

    public List<JobExecution> findByJob(UUID jobId) {
        return jdbc.query("SELECT * FROM job_executions WHERE job_id = ? ORDER BY attempt, id",
                JobExecution::map, jobId);
    }
}
