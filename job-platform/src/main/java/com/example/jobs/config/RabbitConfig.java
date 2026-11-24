package com.example.jobs.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topology:
 *   jobs.exchange --job.run--> jobs.queue --(reject / poison)--> jobs.dlx --jobs.dead--> jobs.dlq
 *
 * Delayed retries are NOT done with TTL queues. They are scheduled in Postgres (outbox available_at),
 * which avoids head-of-line blocking of per-message TTLs and keeps retry state in one place.
 * Jobs that exhaust retries are published to jobs.dlx by the outbox; unparseable messages reach it via
 * the queue's dead-letter arguments.
 */
@Configuration
public class RabbitConfig {

    public static final String JOBS_EXCHANGE = "jobs.exchange";
    public static final String JOBS_QUEUE = "jobs.queue";
    public static final String RUN_KEY = "job.run";

    public static final String DLX = "jobs.dlx";
    public static final String DLQ = "jobs.dlq";
    public static final String DEAD_KEY = "jobs.dead";

    @Bean
    DirectExchange jobsExchange() {
        return new DirectExchange(JOBS_EXCHANGE, true, false);
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange(DLX, true, false);
    }

    @Bean
    Queue jobsQueue() {
        return QueueBuilder.durable(JOBS_QUEUE)
                .deadLetterExchange(DLX)
                .deadLetterRoutingKey(DEAD_KEY)
                .build();
    }

    @Bean
    Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ).build();
    }

    @Bean
    Binding jobsBinding() {
        return BindingBuilder.bind(jobsQueue()).to(jobsExchange()).with(RUN_KEY);
    }

    @Bean
    Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with(DEAD_KEY);
    }
}
