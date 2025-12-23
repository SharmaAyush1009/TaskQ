# Distributed Task Queue System

A Redis + PostgreSQL based asynchronous task processing system designed to ensure reliable execution, idempotency, retry safety, and horizontal scalability.

## Architecture Overview

### Components:
- **API Server**: Accepts task creation requests and enqueues tasks
- **Redis Queue**: Acts as the coordination layer for distributing tasks
- **PostgreSQL**: Source of truth for task state, execution logs, and idempotency
- **Workers**: Stateless processes that execute tasks in parallel
- **Reaper**: Recovers tasks stuck due to worker crashes

## Core Design Principles

### 1. Asynchronous Execution
The API immediately enqueues tasks and returns, keeping user-facing latency low.

### 2. Idempotency
Duplicate requests are prevented using:
- `idempotency_key` at task creation
- Execution logs (`email_logs`, `payment_logs`, `operation_logs`) to prevent re-processing

### 3. Reliability & Recovery
- Tasks are atomically claimed using conditional updates
- Crashed or stuck tasks are detected and safely re-queued
- Exponential backoff retries prevent overload during transient failures

## Task Lifecycle

Non-retryable failures skip retries and are immediately marked `DEAD`.

## Error Classification

Not all failures should be retried.

### Retryable Errors
- Temporary network issues
- External service failures
- Timeouts

### Non-Retryable Errors
- Invalid payloads
- Schema constraint violations
- Unknown task types

**Improvement Applied**: Non-retryable errors are detected in the worker and immediately dead-lettered, preventing wasted retries and preserving worker capacity.

## Performance Benchmarks (Local, Controlled)

### Database Query Optimization

**Query:**
```sql
SELECT id, status, created_at
FROM tasks
WHERE status = 'PENDING'
ORDER BY created_at
LIMIT 100;
```

- Before indexing: 4.5ms, sequential scan + sort
- After indexing: 0.2ms, index scan
- Improvement: ~95%

### API Ingress Latency

- Mean latency: ~4ms
- Measured using repeated HTTP POST requests to /tasks
- Execution is asynchronous; workers are not on the request path

### Redis Queue Throughput

- Sustained 40k+ enqueues/sec
- Measures Redis ingestion capacity, not end-to-end execution

### Multi-Worker Scaling

- Multiple worker processes can be started simultaneously
- Workers compete on the same Redis queue
- Redis guarantees each task is delivered to only one worker
- PostgreSQL enforces correctness and idempotency
- Scaling is achieved by adding more worker processes without changing API logic

### Failure Scenarios Tested

- Duplicate task submission → safely deduplicated
- Worker crash mid-task → recovered by reaper
- Invalid task payload → immediately dead-lettered
- Retry exhaustion → task marked DEAD

## Why PostgreSQL + Redis?

- Redis: Fast coordination, blocking queue semantics
- PostgreSQL: Strong consistency, constraints, auditing, idempotency enforcement

This separation keeps the system fast and correct.

## Known Limitations

- Benchmarks are local and synthetic
- Network latency, TLS, and resource contention will increase absolute numbers in deployment
- Architecture is designed to preserve correctness and scalability under real-world conditions

## Future Work
- Dockerized deployment
- Metrics export (Prometheus)
- Priority queues
- Rate-limited retries
