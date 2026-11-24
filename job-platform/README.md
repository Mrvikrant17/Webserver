# Reliable Job Processing Platform

Java 17 · Spring Boot 3.4 · RabbitMQ · PostgreSQL

## Run
```bash
docker compose up -d
mvn spring-boot:run
# horizontal scaling: more instances on other ports (each runs API + worker + relay + reaper)
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081
```
Split roles with `app.worker.enabled`, `app.relay.enabled`, `app.reaper.enabled`.

## Try it
```bash
# happy path
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo","payload":{"behavior":"ok"}}'

# transient failure: retries with backoff, succeeds on attempt 3
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo","payload":{"behavior":"transient","failUntilAttempt":3}}'

# permanent failure: straight to DLQ (status DEAD)
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo","payload":{"behavior":"permanent"}}'

# idempotent submission: same key twice returns the same job
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo","idempotencyKey":"order-42","payload":{}}'

# crash recovery: JVM is killed mid-job; start it again, reaper requeues after the lease expires
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"type":"demo","payload":{"behavior":"crash"}}'

# inspect
curl localhost:8080/api/jobs/<id>
curl localhost:8080/api/jobs/<id>/executions
curl 'localhost:8080/api/jobs?status=DEAD'

# admin
curl -X POST localhost:8080/api/admin/jobs/<id>/replay
curl -X POST localhost:8080/api/admin/dlq/replay
curl -X POST localhost:8080/api/admin/jobs/<id>/cancel
```
Metrics: `localhost:8080/actuator/prometheus` (`jobs_outcome_total`, `jobs_queue_latency_seconds`,
`jobs_execution_duration_seconds`, `jobs_by_status`, `outbox_unpublished`, `jobs_lease_expired_total`).
RabbitMQ UI: http://localhost:15672 (guest/guest).

## Failure windows and what closes them
| Window | Mechanism |
|---|---|
| Job saved, publish to broker fails (or app dies between) | Transactional outbox: job + outbox row commit atomically; relay publishes later |
| Relay publishes, crashes before marking row published | Duplicate message; atomic claim makes it harmless |
| Worker crashes before ack | Manual ack, broker redelivers |
| Worker crashes after DB commit, before ack | Redelivery; claim fails (job finished), ack and skip |
| Worker crashes mid-handler | Lease expires, reaper requeues; handler must be idempotent |
| Zombie worker finishes after lease was taken over | Fenced updates on (worker, attempt) reject its write |
| Stale duplicate arrives before backoff elapsed | Claim requires `next_run_at <= now()` |
| Poison / unparseable message | Rejected without requeue, dead-lettered to `jobs.dlq` |
| Permanent error or retries exhausted | Job marked DEAD, message published to `jobs.dlq` via outbox |

## Known limits (good interview talking points)
- Delivery is at-least-once. Handlers must be idempotent; use `jobId` as the key for downstream calls.
- The relay holds a DB transaction open while publishing a batch. Fine at this scale; at larger scale publish
  outside the lock, or use CDC (Debezium) instead of polling.
- Outbox rows and `job_executions` grow forever. Add a retention job.
- Admin endpoints have no auth. Add Spring Security.
- Unroutable publishes are still confirmed by the broker; enable `mandatory` + returns if exchanges can change.
