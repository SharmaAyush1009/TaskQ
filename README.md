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

The results below were measured locally on **2026-11-03 (Asia/Kolkata)**. Raw reports, task IDs, configuration, and environment details are saved in [data/benchmark-20261103](data/benchmark-20261103). They replace the unverified historical claims of ~2,000 enqueues/second, 20 ms mean, 53 ms p99, and exactly-once execution across 1,500+ tasks. Those earlier runs cannot be reconstructed from the repository.

| Metric | Measured result | Scope and evidence |
| --- | --- | --- |
| Enqueue throughput | **3,425.93 new tasks/second** | 60,000 unique email submissions in 17.513 seconds, concurrency 50; 100 additional warm-up submissions excluded. [Report](data/benchmark-20261103/enqueue-long.json) |
| Mean / p95 / p99 API latency | **14.59 / 21.02 / 28.44 ms** | Same 60,000-request run; client-observed HTTP submission latency, including persistence and enqueue, excluding task execution. |
| Errors / duplicates | **0 / 0** | Same run; all 60,000 returned task IDs were also found in PostgreSQL. |
| Four-worker email completion throughput | **2.39 tasks/second** | 32 tasks completed in 13.374 seconds, including worker startup and polling overhead. Each handler includes a 1.5-second simulated delay. [Report](data/benchmark-20261103/processing.json) |
| Submission idempotency | **1 new task + 49 duplicate responses; 0 errors** | 50 concurrent submissions sharing one key, all returning one task ID. [Report](data/benchmark-20261103/idempotency.json) |
| Controlled execution audit | **32/32 SUCCESS; one email-log row per task** | Four-worker simulated-email run; no crash injection or external email delivery. This does not establish exactly-once external effects. [Report](data/benchmark-20261103/processing.json) |

Environment: Intel Core i5-12450H, 12 logical CPUs, 15.68 GiB RAM, Windows, Node.js 22.13.0, PostgreSQL 18.1 on Windows, and Redis 8.0.5 in WSL Ubuntu. API and load generator ran on the same machine. PostgreSQL used the supplied [benchmark fixture](benchmarks/schema.sql) plus the existing migration, with `fsync`, `synchronous_commit`, and `full_page_writes` enabled. Redis snapshotting and AOF were disabled. API logging remained enabled and was redirected to files. No workers or reaper ran during enqueue measurements, so the reported rate is submission capacity while the queue grows. This is a short local benchmark, not a production capacity guarantee or a reproduction of the original missing schema/environment.

The unchanged original batch scripts measured **1,866 enqueues/second** and, in a separate 500-request latency run, **30.52 ms mean / 125 ms p99** in this setup. Their output is retained in [original throughput output](data/benchmark-20261103/original-test-enqueue-throughput.js.txt) and [original latency output](data/benchmark-20261103/original-benchmark-api.js.txt). The corrected default entry points measured 2,136.10/sec at 1,000 requests/concurrency 100 and 1,803.98/sec at 500 requests/concurrency 50; [their reports](data/benchmark-20261103) record the matching latency values. These short runs differ in workload, warm-up, and load generation, so their numbers should not be combined into one benchmark result.

### Concurrency and the observed limit

Two 5,000-request runs per concurrency level, with constant concurrency, keep-alive, and 100 excluded warm-up requests, produced the following ranges. [Raw summary](data/benchmark-20261103/summary.json)

| Concurrent requests | New tasks/second | Mean HTTP latency | p99 HTTP latency |
| --- | --- | --- | --- |
| 10 | 2,214–2,363 | 4.23–4.51 ms | 7.54–9.19 ms |
| 50 | 3,244–3,362 | 14.83–15.36 ms | 20.82–26.28 ms |
| 100 | 3,285–3,450 | 28.78–30.23 ms | 37.62–59.07 ms |
| 200 | 2,852–3,255 | 60.54–69.22 ms | 85.60–117.50 ms |

There is **no 2,000/sec cap in the code**. In these tests, additional concurrency beyond 50–100 mostly increased waiting and latency. A separate [instrumented diagnostic run](data/benchmark-20261103/profile.json), at concurrency 100, observed a ten-connection PostgreSQL pool with up to 86 requests waiting. Mean pool acquisition took 21.40 ms within a mean 25.69 ms database-query span; Redis `LPUSH` and `SADD` spans averaged 4.31 and 4.93 ms. The API event loop was busy for 99.25% of the diagnostic interval. These measurements identify pool waiting and API event-loop pressure; instrumentation adds overhead, and they do not isolate a single root cause or prove that increasing the pool alone will help. The two sequential Redis operations and per-task logging also remain part of the request path.

Worker capacity is separate: one 1.5-second email at a time gives an ideal ceiling of 0.67 emails/second per worker, or 2.67 with four workers, before overhead. The API does not apply backlog-based admission control. Slow completion therefore grows the queue rather than imposing a fixed enqueue cap.

### What changed, and exactly what the timer measures

The measured application submission path was not optimized to produce the new figures. The benchmark changed from short, fixed batches to continuously replenished concurrency, with an explicit warm-up, a longer measurement, unique run IDs, stricter response accounting, and saved evidence. The corrected client explicitly configures persistent HTTP connections; the tested Node.js 22 global HTTP agent already enables keep-alive by default, so connection reuse alone is not an established explanation for the difference. The database schema and environment were also newly specified because the original setup was unavailable. The earlier ~2K/sec result is therefore not proven false: the original throughput script measured 1,866/sec in the new setup. The new result must not be described as a measured percentage improvement over the historical figure, because the test conditions differ.

For the 60,000-request result, the client first sent 100 real warm-up `POST /tasks` requests and waited for them to finish. Those submissions and the health check were excluded from measurement. It then started a monotonic timer, kept up to 50 measured HTTP requests in flight, replaced each finished request with the next one, and stopped the timer after the final measured response. Concurrency 50 means up to 50 outstanding requests, not 50 total requests or 50 distinct users. All 60,000 measured responses were HTTP 201 with `duplicate: false` and distinct task IDs. The later database audit found all of those IDs; the audit itself was outside the timer. There were 60,100 task-submission requests in this particular test including warm-up.

```text
Timed request path:
client serialization/send -> HTTP -> Express parsing/validation
-> PostgreSQL connection wait + unique-checked INSERT/commit
-> Redis LPUSH -> Redis SADD -> per-task logging
-> HTTP response -> client receives/parses response

Enqueue throughput = 60,000 new tasks / 17.5134933 wall-clock seconds
                   = 3,425.93 new tasks/second

Mean HTTP latency  = sum of 60,000 individual request durations / 60,000
                   = 14.59 milliseconds

Concurrency sanity check: 50 / 0.01459 seconds ~= 3,427 responses/second
```

The final line is an approximate consistency check for continuously occupied concurrency; it is not an independent measurement of maximum capacity. Requests overlap, so adding all their individual durations does not give the elapsed wall-clock time. Client scheduling, serialization, network communication, API work, database waiting/commit, both Redis commands, and response handling contribute to the measured interval. Background handler execution does not: no workers ran against this queue during the submission test. All 60,000 measured tasks were accepted and queued, not completed. The separate four-worker test completed a different 32-task cohort in 13.374 seconds, or 2.39 completions/second.

A claim such as "50K+ Redis enqueues/sec" needs its own measurement boundary and configuration. Timing Redis commands directly, pipelining or batching IDs, and timing this application's HTTP-plus-PostgreSQL-plus-Redis path are different experiments. Redis's standalone maximum was not measured here. Even one application task issues two Redis commands in addition to its database write. At concurrency 50, 50,000 submissions/sec would require an average response time of about 1 ms; with the observed 14.59 ms mean, the same arithmetic would require roughly 730 requests in flight *if latency stayed constant*. The concurrency sweep shows that latency does not stay constant as concurrency increases. Pool waits, API event-loop pressure, Windows/WSL communication, and competition with the local load generator require further isolated experiments before attributing the plateau to one component. The ~3.4K/sec result is an observation for this configuration, not a Redis limit or an application-wide fixed cap.

### Available validation scripts

Run these against a configured local API, PostgreSQL, Redis, workers, and reaper. They create tasks and execution logs; use a disposable database.

| Command | What it exercises |
| --- | --- |
| `node benchmark-api.js` | 500 measured submissions at concurrency 50; 100 excluded warm-up submissions; saves throughput, mean/p95/p99, errors, duplicates, environment, and task IDs as JSON. |
| `node test-enqueue-throughput.js` | 1,000 measured submissions at concurrency 100, plus 100 excluded warm-up submissions; counts only unique HTTP 201 responses marked `duplicate: false` as new enqueues. |
| `node test-idempotency.js` | 50 concurrent submissions sharing one key; requires one creation, 49 duplicates, no errors, and one task ID to pass. Checks submission deduplication, not effect safety. |
| `node test-processing-throughput.js <enqueue-report.json>` | Observes only task IDs from a valid enqueue report. Subtracts successes already present at the baseline query; reports new completions per observation second. Refuses an already-completed cohort, fails on missing IDs or DEAD tasks, and times out after 120 seconds by default. |
| `node test-retry-endpoint.js` | Creates an intentionally failing task, waits for `DEAD`, then requests a manual retry. |
| `node test-crash-recovery.js` | Provides manual crash-test instructions and checks task states afterward. Its two-minute assumptions do not match the current ten-minute zombie threshold and five-minute scan interval; adjust the procedure before using it. |

The API metric scripts use continuously replenished concurrency, HTTP keep-alive, monotonic timing, random run IDs, and five-second request timeouts. `BASE_URL`, `BENCH_N`, `BENCH_CONCURRENCY`, `BENCH_WARMUP`, `BENCH_TIMEOUT_MS`, and `BENCH_OUTPUT` configure the run; generated reports default to `data/benchmarks/`. Mean is the arithmetic average; p95/p99 use the nearest-rank percentile of successful HTTP responses. A p99 of 28.44 ms means at least 99% of the sampled successful requests finished within that time. Latency excludes errors and task execution; error counts must be read alongside it. A failed or duplicate-containing run exits nonzero; no successful samples produce unavailable latency rather than `NaN`.

The original enqueue script counted duplicate HTTP 200 responses as new enqueues. The original processing script divided a rolling ten-minute success count by time since polling began, so old completions could inflate its rate. Both counting errors are corrected. `npm test` runs nine regression tests covering these cases, HTTP failures, warm-up exclusion, percentiles, bounded processing waits, and idempotency validation. The retry and manual crash scripts remain standalone checks with the limitations described above; these tests are not a complete distributed-failure suite.

### Checks actually completed in this benchmark session

This is an execution record for the saved results, not a claim that every available test was run.

| Check | Execution status | What it establishes |
| --- | --- | --- |
| Original API-latency and enqueue scripts | Ran successfully against the local fixture | Original batch-script outputs retained for comparison; does not reproduce the missing historical environment. |
| Corrected API-latency and enqueue entry points | Ran successfully | New-task response counting, HTTP latency, and saved reports for the default workloads. |
| Concurrency sweep and 60,000-request submission run | Passed | Eight 5,000-request runs and one 60,000-request run; returned IDs checked against PostgreSQL. Submission measurements only. |
| `node test-idempotency.js` | Passed | One creation and 49 duplicate responses, one returned task ID, no errors. |
| Processing measurement through `observeProcessing`, shared with `test-processing-throughput.js` | Passed | Separate 32-task, four-worker run; all tasks SUCCESS and one email-log row per task. The standalone processing CLI was not run through a positive complete-cohort case. |
| `npm test` | 9/9 passed | Regression checks for the measurement and idempotency-reporting code; not a full worker/reaper integration suite. |
| API unavailable / processing CLI missing report | Expected nonzero exits observed | Missing prerequisites no longer appear as successful metric runs. |
| JavaScript syntax / `git diff --check` | Passed | Source parses and changes have no whitespace errors; does not establish runtime correctness. |
| `node test-retry-endpoint.js` | **Not run; source reviewed** | Automatic retries through exhaustion and the manual retry endpoint remain unverified in this session. The script also logs post-retry expectations without asserting them. |
| `node test-crash-recovery.js` | **Not run; source reviewed** | Existing procedure ages tasks by two minutes and waits about two minutes; current zombie threshold is ten minutes with a five-minute scan interval. Its success criterion also does not require SUCCESS. |
| Redis/PostgreSQL outages, worker crash injection, and end-to-end exactly-once effects | **Not tested live** | No guarantee established for arbitrary failures or external side effects. Queue-membership races and ownership limitations described above remain. |

The temporary benchmark API, workers, PostgreSQL cluster on port 55432, and Redis instance on port 56379 were stopped after testing. A later check also found Ubuntu WSL stopped. The pre-existing PostgreSQL service on port 5432 remained running. Installed npm dependencies, the Redis package in Ubuntu, database fixture files, and reports remain on disk. These are observations from the benchmark session and follow-up, not a live service-status endpoint.

To reproduce the saved local suite, provision a fresh disposable PostgreSQL database with `benchmarks/schema.sql` followed by `db/migrations/add_unique_idempotency.sql`, configure the `DB_*` and `REDIS_URL` environment variables, and ensure the configured API port is free. Then run `node benchmarks/run-local.js` and `node benchmarks/run-extended.js`. The first runner performs the original and corrected short benchmarks, the concurrency sweep, diagnostics, and a four-worker audit; the second performs the 60,000-task run and submission-idempotency check. They start and stop their own API/worker processes, and leave the test data in the configured stores. The worker audit also requires an empty Redis database 1 on the disposable Redis instance. Original scripts are read from Git revision `dc4e3e90f2b0f41a69215f889938a650c7abf43e` (overridable with `BENCH_BASELINE_REF`); source snapshots are saved locally with their output.

## Tech Stack

- **JavaScript / Node.js** — CommonJS modules; independently launched API, worker, and reaper processes.
- **Express.js 5** — Task submission, inspection, listing, manual retry, statistics, and health endpoints.
- **PostgreSQL / `pg`** — Persistent task state, execution logs, unique constraints, conditional updates, and connection pooling.
- **Redis / `redis`** — FIFO task-ID list, blocking consumption, and an advisory membership set.
- **dotenv** — Environment-based connection configuration.
- **Axios** — HTTP requests in benchmark and integration scripts.

### Running the processes

Install dependencies with `npm ci`. Configure `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, and `REDIS_URL` in a local `.env`; `PORT` defaults to `3000`.

**Database prerequisite:** [benchmarks/schema.sql](benchmarks/schema.sql) provides a disposable initial schema for the measured test setup, inferred from the application queries. Apply it to a fresh database before [the migration](db/migrations/add_unique_idempotency.sql). It is a benchmark fixture, not a recovered copy of the unavailable original schema; review its types, defaults, and constraints before using it elsewhere.

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
