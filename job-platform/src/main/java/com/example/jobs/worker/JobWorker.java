package com.example.jobs.worker;

import com.example.jobs.config.RabbitConfig;
import com.example.jobs.domain.Job;
import com.example.jobs.handler.JobContext;
import com.example.jobs.handler.JobHandlerRegistry;
import com.example.jobs.repo.JobRepository;
import com.example.jobs.service.JobExecutionService;
import com.example.jobs.service.RetryPolicy;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Consumer with MANUAL acks. Ack happens only after the DB transaction recording the outcome has committed.
 *
 * Failure windows:
 *  - Crash before claim            -> message unacked, broker redelivers, normal processing.
 *  - Crash after claim, mid-handler -> message redelivered but claim fails (job is RUNNING);
 *                                      lease expires, reaper requeues the job. Handler runs again (at-least-once).
 *  - Crash after DB commit, before ack -> redelivery; claim fails (job already SUCCEEDED/PENDING-not-due); ack + skip.
 */
@Component
public class JobWorker {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private final JobExecutionService execution;
    private final JobRepository jobs;
    private final JobHandlerRegistry handlers;
    private final RetryPolicy retryPolicy;
    private final MeterRegistry metrics;
    private final String workerId = hostname() + "-" + UUID.randomUUID().toString().substring(0, 8);
    private final ScheduledExecutorService heartbeats = Executors.newScheduledThreadPool(2);

    public JobWorker(JobExecutionService execution, JobRepository jobs, JobHandlerRegistry handlers,
                     RetryPolicy retryPolicy, MeterRegistry metrics) {
        this.execution = execution;
        this.jobs = jobs;
        this.handlers = handlers;
        this.retryPolicy = retryPolicy;
        this.metrics = metrics;
    }

    @RabbitListener(queues = RabbitConfig.JOBS_QUEUE, autoStartup = "${app.worker.enabled:true}")
    public void onMessage(String body, Channel channel,
                          @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        UUID jobId;
        try {
            jobId = UUID.fromString(body.trim());
        } catch (IllegalArgumentException e) {
            log.error("Poison message, sending to DLQ: '{}'", body);
            channel.basicReject(deliveryTag, false);   // requeue=false -> dead-lettered to jobs.dlq
            return;
        }

        try {
            process(jobId);
            channel.basicAck(deliveryTag, false);
        } catch (Exception infra) {
            // DB/infra failure while claiming or recording the outcome. Hand the message back to the broker.
            log.error("Infrastructure failure processing job {}; requeueing message", jobId, infra);
            channel.basicNack(deliveryTag, false, true);
            sleepQuietly(1000); // avoid a hot redelivery loop while the DB is down
        }
    }

    private void process(UUID jobId) {
        Optional<Job> claimed = execution.claim(jobId, workerId);
        if (claimed.isEmpty()) {
            log.info("Skipping message for job {}: not claimable (duplicate, not yet due, cancelled or finished)", jobId);
            metrics.counter("jobs.skipped").increment();
            return;
        }

        Job job = claimed.get();
        MDC.put("jobId", job.id().toString());
        MDC.put("attempt", String.valueOf(job.attempts()));
        MDC.put("workerId", workerId);
        try {
            long latencyMs = Math.max(0, Duration.between(job.nextRunAt(), Instant.now()).toMillis());
            metrics.timer("jobs.queue.latency").record(Duration.ofMillis(latencyMs));

            long hbSeconds = Math.max(1, execution.lease().toSeconds() / 3);
            ScheduledFuture<?> heartbeat = heartbeats.scheduleAtFixedRate(() -> {
                try {
                    if (jobs.heartbeat(job.id(), workerId, job.attempts(), execution.lease()) == 0) {
                        log.warn("Heartbeat rejected: lease for job {} was lost", job.id());
                    }
                } catch (Exception e) {
                    log.warn("Heartbeat failed for job {}: {}", job.id(), e.getMessage());
                }
            }, hbSeconds, hbSeconds, TimeUnit.SECONDS);

            Throwable failure = null;
            Timer.Sample sample = Timer.start(metrics);
            try {
                handlers.get(job.type()).handle(new JobContext(job.id(), job.type(), job.payload(), job.attempts()));
            } catch (Exception e) {
                failure = e;
            } finally {
                heartbeat.cancel(false);
                sample.stop(metrics.timer("jobs.execution.duration", "type", job.type(),
                        "result", failure == null ? "success" : "failure"));
            }

            if (failure == null) {
                execution.succeed(job, workerId);
            } else {
                execution.fail(job, workerId, failure, retryPolicy.isRetryable(failure));
            }
        } finally {
            MDC.clear();
        }
    }

    @PreDestroy
    void shutdown() {
        heartbeats.shutdownNow();
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "worker";
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
