package com.example.jobs.config;

import com.example.jobs.domain.JobStatus;
import com.example.jobs.repo.JobRepository;
import com.example.jobs.repo.OutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Gauges are computed from the DB at scrape time, so they are correct across all instances. */
@Configuration
public class MetricsConfig {

    @Bean
    MeterBinder jobGauges(JobRepository jobs, OutboxRepository outbox) {
        return registry -> {
            for (JobStatus status : JobStatus.values()) {
                Gauge.builder("jobs.by.status", () -> jobs.countByStatus(status))
                        .tag("status", status.name())
                        .register(registry);
            }
            Gauge.builder("outbox.unpublished", outbox::countUnpublished).register(registry);
        };
    }
}
