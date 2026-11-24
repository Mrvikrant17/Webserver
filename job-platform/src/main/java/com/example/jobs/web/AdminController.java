package com.example.jobs.web;

import com.example.jobs.domain.Job;
import com.example.jobs.service.JobService;
import java.util.Map;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Secure these endpoints (Spring Security, network policy) before exposing them anywhere real. */
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final JobService service;

    public AdminController(JobService service) {
        this.service = service;
    }

    @PostMapping("/jobs/{id}/replay")
    public Job replay(@PathVariable UUID id, @RequestParam(required = false) Integer extraAttempts) {
        return service.replay(id, extraAttempts);
    }

    @PostMapping("/dlq/replay")
    public Map<String, Integer> replayAll(@RequestParam(defaultValue = "100") int limit) {
        return Map.of("replayed", service.replayAllDead(limit));
    }

    @PostMapping("/jobs/{id}/cancel")
    public Job cancel(@PathVariable UUID id) {
        return service.cancel(id);
    }
}
