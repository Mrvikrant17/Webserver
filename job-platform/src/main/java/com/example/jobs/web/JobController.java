package com.example.jobs.web;

import com.example.jobs.domain.Job;
import com.example.jobs.domain.JobExecution;
import com.example.jobs.domain.JobStatus;
import com.example.jobs.service.JobService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Job submit(@Valid @RequestBody SubmitRequest request) {
        return service.submit(request);
    }

    @GetMapping("/{id}")
    public Job get(@PathVariable UUID id) {
        return service.get(id);
    }

    @GetMapping("/{id}/executions")
    public List<JobExecution> history(@PathVariable UUID id) {
        return service.history(id);
    }

    @GetMapping
    public List<Job> list(@RequestParam(required = false) JobStatus status,
                          @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, limit);
    }
}
