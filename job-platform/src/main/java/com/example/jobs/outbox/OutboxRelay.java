package com.example.jobs.outbox;

import com.example.jobs.domain.OutboxEvent;
import com.example.jobs.repo.OutboxRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves committed outbox rows to RabbitMQ.
 *
 * Failure windows:
 *  - Crash BEFORE publish          -> row still unpublished, next poll publishes it.
 *  - Crash AFTER publish, BEFORE commit of published_at -> message is published AGAIN (at-least-once).
 *    The worker's atomic claim makes the duplicate harmless.
 *  - Broker down / nack / confirm timeout -> exception, loop stops, row stays unpublished, retried next poll.
 *
 * Several relay instances can run at once: FOR UPDATE SKIP LOCKED gives each a disjoint batch.
 */
@Component
@ConditionalOnProperty(name = "app.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final RabbitTemplate rabbit;
    private final TransactionTemplate tx;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outbox, RabbitTemplate rabbit, TransactionTemplate tx,
                       @Value("${app.relay.batch-size:100}") int batchSize) {
        this.outbox = outbox;
        this.rabbit = rabbit;
        this.tx = tx;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${app.relay.poll-interval-ms:500}")
    public void relay() {
        try {
            tx.executeWithoutResult(status -> {
                List<OutboxEvent> batch = outbox.lockDue(batchSize);
                for (OutboxEvent event : batch) {
                    try {
                        publishAndWaitForConfirm(event);
                        outbox.markPublished(event.id());
                    } catch (Exception e) {
                        log.warn("Publish of outbox event {} failed, will retry next poll: {}", event.id(), e.getMessage());
                        break; // keep already-published rows committed, leave the rest for the next poll
                    }
                }
            });
        } catch (Exception e) {
            log.warn("Outbox relay iteration failed: {}", e.getMessage());
        }
    }

    private void publishAndWaitForConfirm(OutboxEvent e) {
        rabbit.invoke(ops -> {
            ops.convertAndSend(e.exchange(), e.routingKey(), e.payload());
            ops.waitForConfirmsOrDie(5_000);   // broker has taken responsibility (persisted) for the message
            return null;
        });
    }
}
