# TaskQ — Fault-Tolerant Task Processing System

TaskQ separates HTTP task submission from background execution using Node.js, Express.js, PostgreSQL, and Redis. PostgreSQL arbitrates duplicate submissions and competing worker claims; Redis distributes task IDs; a recovery process requeues stalled work and scheduled retries.

The implementation demonstrates concurrency control, persistent task state, and recovery across independent processes. Email, payment, image, and report handlers simulate work and record execution logs; external service integrations are not implemented.

## Key Architectural Features

- **Asynchronous task queueing.** The API persists a task, pushes its ID to a Redis list, and returns without waiting for execution. Workers consume through blocking `BRPOP` calls. Separate Redis connections keep blocking reads independent of other commands.
- **Fault tolerance.** Failed tasks receive exponential-backoff timestamps. A reaper scans for eligible pending tasks and recovers tasks left `IN_PROGRESS` beyond a ten-minute threshold. Exhausted retries and explicitly non-retryable failures become `DEAD` tasks that can be manually requeued.
- **Data integrity and relational storage.** PostgreSQL stores task payloads, status, results, failure counts, retry timestamps, and handler execution logs. A unique constraint on `(type, idempotency_key)` enforces submission deduplication at the database boundary.
- **Concurrency and performance.** `INSERT ... ON CONFLICT ... RETURNING` removes the submission check-then-insert race. A conditional `UPDATE ... RETURNING` arbitrates worker claims. Handler logs provide another idempotency layer when completed work is encountered again. These controls have different scopes; they do not make every cross-system operation atomic.

## System Architecture Diagram

```text
 Client
   |
   | POST /tasks { type, payload, idempotency_key }
   v
 Express API
   | Validate task type, required payload fields, and idempotency key
   v
 PostgreSQL: tasks
   | Atomic INSERT ... ON CONFLICT (type, idempotency_key) DO NOTHING
   |
   +-- Conflict --> Read existing task --> 200 { id, status, result,
   |                                            duplicate: true }
   |
   +-- Inserted --> PENDING task + returned ID
                       |
                       | Separate writes: LPUSH, then SADD
                       v
               Redis
               task_queue      : list of task IDs
               task_queue_set  : advisory queue-membership set
                       |
                       +--> API returns 201 { id, status: PENDING,
                       |                     duplicate: false }
                       |
                       | BRPOP from shared list
                       v
               Worker 1 ... Worker N
               One active task per worker process
                       |
                       | Remove set membership; load task and check state
                       v
               PostgreSQL: atomic conditional claim
               PENDING / FAILED --> IN_PROGRESS
                       |
                       +-- No row returned --> Skip execution
                       |
                       v
               Handler: check execution log --> Simulate work --> Write log
                       |
                       +-- Success --> Persist result; mark SUCCESS
                       |
                       +-- Retryable failure, budget remains
                       |      --> PENDING + attempts + retry_at
                       |
                       +-- Non-retryable / exhausted --> DEAD
                                                          |
                                                POST /tasks/:id/retry
                                                          |
                                                PENDING --> Redis

 Recovery loop: starts immediately, then scans every 5 minutes
 -------------------------------------------------------------------------
 PostgreSQL IN_PROGRESS older than 10 min --> PENDING; increment attempts
                                           --> Redis
 PostgreSQL PENDING older than 1 min, retry due, absent from membership set
                                           --> Redis
```

PostgreSQL is the authority for task state and claim eligibility. Redis carries IDs rather than task payloads. `LPUSH` with `BRPOP` provides FIFO dequeue order for queued entries; concurrent execution and retries do not preserve completion order.

### Task lifecycle and failure handling

| Trigger | Persisted transition | Behavior |
| --- | --- | --- |
| Valid, new submission | New row → `PENDING` | Persist first, then enqueue the ID. |
| Duplicate `(type, idempotency_key)` | No new row | Return the existing task; do not enqueue another copy through this path. |
| Successful worker claim | `PENDING` or `FAILED` → `IN_PROGRESS` | Execute only if the conditional update returns a row. `FAILED` is accepted by the claim query, but current failure paths write `PENDING` or `DEAD`. |
| Handler completes | `IN_PROGRESS` → `SUCCESS` | Store the handler result. Later deliveries of a successful task are skipped. |
| Retryable failure with attempts remaining | `IN_PROGRESS` → `PENDING` | Increment `attempts`, record `last_error`, and set `retry_at`. |
| Non-retryable failure or exhausted attempts | `IN_PROGRESS` → `DEAD` | Stop automatic handler retries. `DEAD` is a database state, not a separate Redis dead-letter queue. |
| Stale task detected by reaper | `IN_PROGRESS` → `PENDING` | Increment `attempts` and enqueue for recovery. |
| Manual retry | `DEAD` → `PENDING` | Reset attempts, error, and retry timestamp; enqueue again. Other states return HTTP 404. |

Backoff is `2^attempts` seconds after incrementing the failure count: 2, 4, 8 seconds, and so on, while `attempts < max_attempts`. This is an eligibility timestamp, not an exact execution schedule. The reaper runs every five minutes and only selects pending rows whose `updated_at` is more than one minute old, so actual retries can occur minutes later. Backoff has no jitter or explicit cap.

The worker immediately marks `NonRetryableError`, PostgreSQL not-null violations (`23502`), and unique violations (`23505`) as `DEAD`. Other handler errors follow the retry budget. The API rejects unsupported task types and invalid required fields with HTTP 400; an unknown type encountered directly by a worker follows the ordinary retry path.

## Race Condition Resolution (Deep Dive)

### Submission: replace check-then-insert with an atomic database decision

A Time-of-Check to Time-of-Use (TOCTOU) race occurs when a process reads shared state, then acts on the assumption that the state has not changed. Concurrent requests can interleave in a check-then-insert design:

```text
Request A                         Request B
---------                         ---------
SELECT key --> absent
                                  SELECT key --> absent
INSERT task A
                                  INSERT task B
Enqueue A                         Enqueue B
```

Without a database uniqueness constraint, both inserts can succeed. With a constraint but an unhandled conflict, a legitimate duplicate request can become an error. An application-level precheck alone resolves neither outcome.

TaskQ makes the insert itself decide whether the task is new:

```sql
-- Enforced by db/migrations/add_unique_idempotency.sql:
UNIQUE (type, idempotency_key)

-- Used by POST /tasks:
INSERT INTO tasks (type, payload, status, idempotency_key, created_at, updated_at)
VALUES ($1, $2, 'PENDING', $3, NOW(), NOW())
ON CONFLICT (type, idempotency_key) DO NOTHING
RETURNING id;
```

PostgreSQL arbitrates concurrent inserts against the unique constraint. Only the request receiving an inserted ID proceeds to enqueue. A request receiving no row reads the existing task and returns it with `duplicate: true`. That follow-up read retrieves the response; it does not decide whether another insert is allowed.

Idempotency is scoped to **task type plus key**. The same key can identify different task types. Reusing a key with a different payload for the same type returns the original task; the API does not compare payloads or reject the mismatch.

### Execution: make the state transition the claim

Queue entries can reappear through recovery, manual retries, or duplicate enqueue operations. A worker's preliminary read is therefore insufficient to establish ownership. The decisive operation is:

```sql
UPDATE tasks
SET status = 'IN_PROGRESS', updated_at = NOW()
WHERE id = $1 AND status IN ('PENDING', 'FAILED')
RETURNING *;
```

For competing claims on the same eligible row, PostgreSQL serializes the updates. The winner changes the state; a competing update that sees `IN_PROGRESS` returns no row and skips execution. Each worker process executes one task at a time; parallelism comes from running additional processes against the same queue.

### Reliability boundaries and engineering tradeoffs

| Boundary | Implemented protection | Remaining limitation |
| --- | --- | --- |
| PostgreSQL → Redis | Persist before enqueue; reaper looks for older eligible pending rows absent from the membership set. | No transactional outbox or cross-store transaction. A crash or Redis error can leave a persisted task unenqueued, including after an HTTP 500. Recovery depends on the reaper and consistent membership metadata. |
| Redis list → membership set | Set membership helps the reaper avoid redundant enqueue operations. | `LPUSH`, `SADD`, `SREM`, and membership checks are separate commands. The list and set can diverge; the set is advisory, not an atomic deduplication guarantee. |
| Dequeue → database claim | Pending rows remain in PostgreSQL if a worker dies after `BRPOP`. | Redis list consumption has no acknowledgement or processing list. Recovery comes from database reconciliation rather than a broker acknowledgement protocol. |
| Worker → handler effect → execution log | Handlers consult `email_logs`, `payment_logs`, or `operation_logs` before repeating work. | Effect, log insertion, and task completion are separate operations. A crash between them can repeat work. External effects would require provider idempotency or another atomic coordination mechanism. |
| Long-running task → recovery | Reaper recovers `IN_PROGRESS` rows older than ten minutes. | No heartbeat or fencing token distinguishes a dead worker from a slow one. A live task exceeding the threshold can overlap with a recovered attempt; completion writes are not fenced by ownership. |
| Repeated worker crashes → retry budget | Zombie recovery increments `attempts`. | The zombie recovery query does not enforce `max_attempts`; repeated crashes are not bounded by the handler-failure retry policy. |

These mechanisms support recovery and deduplication, but do not establish end-to-end exactly-once execution under arbitrary failures. The Redis persistence and replication policy is also outside this repository.

## Performance & Benchmarking Metrics

The following figures are maintainer-reported measurements supplied for this project. Raw results, hardware specifications, and a saved run configuration are not checked into the repository; these numbers have not been independently reproduced here.

| Metric | Reported result | Scope |
| --- | --- | --- |
| Enqueue throughput | ~2,000 tasks/second | Task submission throughput; not completed handler executions per second. |
| Average API latency | 20 ms | Task submission request latency; excludes background execution. |
| p99 API latency | 53 ms | Task submission tail latency. |
| Execution accuracy | Exactly-once execution reported as verified across 1,500+ tasks with 4 concurrent workers | Observation from the reported test run; not a guarantee across crash windows or external side effects. |

The latency and throughput figures should be read as separate measurements; the repository does not establish that they came from the same run. Simulated handlers wait approximately 1.5–4 seconds per task, so enqueue throughput is distinct from worker processing capacity.

### Available validation scripts

Run these against a configured local API, PostgreSQL, Redis, workers, and reaper. They create tasks and execution logs; use a disposable database.

| Command | What it exercises |
| --- | --- |
| `node benchmark-api.js` | 500 submissions in batches of 50; reports mean, p95, p99, errors, and duplicate responses. |
| `node test-enqueue-throughput.js` | 1,000 submissions in batches of 100; reports successful enqueue requests per second. |
| `node test-idempotency.js` | 50 concurrent submissions sharing one key; reports unique task IDs and duplicate responses. |
| `node test-processing-throughput.js` | Polls aggregate task states from the last ten minutes and reports completion throughput from the polling start. It does not isolate a benchmark run or audit duplicate effects. |
| `node test-retry-endpoint.js` | Creates an intentionally failing task, waits for `DEAD`, then requests a manual retry. |
| `node test-crash-recovery.js` | Provides manual crash-test instructions and checks task states afterward. Its two-minute assumptions do not match the current ten-minute zombie threshold and five-minute scan interval; adjust the procedure before using it. |

These are standalone integration scripts, not a complete automated correctness suite. Some report failures without a failing exit code, and none provides the saved 1,500-task, four-worker execution audit. `npm test` is currently a placeholder.

## Tech Stack

- **JavaScript / Node.js** — CommonJS modules; independently launched API, worker, and reaper processes.
- **Express.js 5** — Task submission, inspection, listing, manual retry, statistics, and health endpoints.
- **PostgreSQL / `pg`** — Persistent task state, execution logs, unique constraints, conditional updates, and connection pooling.
- **Redis / `redis`** — FIFO task-ID list, blocking consumption, and an advisory membership set.
- **dotenv** — Environment-based connection configuration.
- **Axios** — HTTP requests in benchmark and integration scripts.

### Running the processes

Install dependencies with `npm ci`. Configure `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, and `REDIS_URL` in a local `.env`; `PORT` defaults to `3000`.

**Database prerequisite:** the repository includes an incremental constraint/index migration, but no initial schema for `tasks`, `email_logs`, `payment_logs`, or `operation_logs`. Provision those tables, their required columns, and task defaults such as `attempts` and `max_attempts` before applying [the migration](db/migrations/add_unique_idempotency.sql). The repository alone does not currently provide a complete fresh-database bootstrap.

Start each process in a separate terminal after PostgreSQL and Redis are available:

```sh
node api/server.js
node worker/worker.js
node worker/reaper.js
```

Repeat `node worker/worker.js` in additional terminals for concurrent workers. Submit a task:

```sh
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":{"to":"engineer@example.com","subject":"TaskQ demo"},"idempotency_key":"demo-email-001"}'
```

Inspect the returned ID with `GET /tasks/:id`. `GET /tasks` supports `status`, `type`, and `limit` filters; `GET /stats` aggregates tasks created in the last hour. `GET /health` is a static process-health response, not a dependency readiness check. Authentication is not implemented, and the worker's shutdown handlers exit immediately rather than draining active work.
