const axios = require("axios");
const http = require("node:http");
const https = require("node:https");
const { randomUUID } = require("node:crypto");
const { performance } = require("node:perf_hooks");
const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");

function positiveInteger(value, fallback, name, allowZero = false) {
  const number = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(number) || number < (allowZero ? 0 : 1)) {
    throw new Error(`${name} must be ${allowZero ? "a nonnegative" : "a positive"} integer`);
  }
  return number;
}

function latencySummary(samples) {
  if (samples.length === 0) return null;
  const sorted = [...samples].sort((a, b) => a - b);
  const percentile = (p) => sorted[Math.ceil(sorted.length * p) - 1];
  return {
    samples: sorted.length,
    mean: sorted.reduce((sum, value) => sum + value, 0) / sorted.length,
    p95: percentile(0.95), p99: percentile(0.99), min: sorted[0], max: sorted[sorted.length - 1],
  };
}

async function runApiMetrics(options = {}) {
  const count = positiveInteger(options.count, 1000, "count");
  const concurrency = positiveInteger(options.concurrency, 100, "concurrency");
  const warmup = positiveInteger(options.warmup, 100, "warmup", true);
  const timeoutMs = positiveInteger(options.timeoutMs, 5000, "timeoutMs");
  const baseUrl = options.baseUrl || "http://localhost:3000";
  const runId = options.runId || `bench-${randomUUID()}`;
  const httpAgent = new http.Agent({ keepAlive: true, maxSockets: concurrency });
  const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: concurrency });
  const client = axios.create({ baseURL: baseUrl, timeout: timeoutMs, httpAgent, httpsAgent });
  const successes = [];
  const errors = [];
  let duplicates = 0;
  let successfulResponses = 0;
  const taskIds = new Set();

  async function phase(total, prefix, measured) {
    let next = 0;
    await Promise.all(Array.from({ length: Math.min(total, concurrency) }, async () => {
      while (next < total) {
        const index = next++;
        const requestStart = performance.now();
        try {
          const response = await client.post("/tasks", {
            type: "send_email",
            payload: { to: `bench-${index}@example.com`, subject: "Benchmark" },
            idempotency_key: `${prefix}-${index}`,
          });
          const { id, duplicate } = response.data || {};
          const created = response.status === 201 && duplicate === false;
          const existing = response.status === 200 && duplicate === true;
          if ((!created && !existing) || id === undefined || id === null) {
            throw new Error(`Unexpected task response: HTTP ${response.status}`);
          }
          if (measured) {
            if (created && taskIds.has(String(id))) throw new Error(`Repeated created task ID: ${id}`);
            successfulResponses++;
            successes.push(performance.now() - requestStart);
            if (existing) duplicates++;
            else taskIds.add(String(id));
          }
        } catch (error) {
          const detail = { index, status: error.response?.status || null,
            code: error.code || null, message: error.response?.data?.error || error.message };
          if (!measured) throw new Error(`Warm-up failed: ${detail.message}`);
          errors.push(detail);
        }
      }
    }));
  }

  try {
    await client.get("/health");
    await phase(warmup, `${runId}-warmup`, false);
    const startedAt = new Date().toISOString();
    const started = performance.now();
    await phase(count, runId, true);
    const durationSeconds = (performance.now() - started) / 1000;
    return {
      runId, startedAt, finishedAt: new Date().toISOString(),
      valid: errors.length === 0 && duplicates === 0 && taskIds.size === count,
      configuration: { baseUrl, count, concurrency, warmup, timeoutMs,
        loadModel: "constant concurrency with HTTP keep-alive", taskType: "send_email" },
      environment: { node: process.version, platform: os.platform(), release: os.release(),
        cpu: os.cpus()[0]?.model, logicalCpus: os.cpus().length, totalMemoryBytes: os.totalmem() },
      attempted: count, successfulResponses, newTasks: taskIds.size, duplicates,
      errors: errors.length, errorSamples: errors.slice(0, 10), durationSeconds,
      newTasksPerSecond: taskIds.size / durationSeconds,
      latencyMs: latencySummary(successes),
      latencyScope: "Successful task-submission HTTP responses; excludes warm-up and handler execution",
      taskIds: [...taskIds],
    };
  } finally {
    httpAgent.destroy();
    httpsAgent.destroy();
  }
}

async function cli(defaults) {
  const output = process.env.BENCH_OUTPUT;
  let report;
  try {
    report = await runApiMetrics({
      count: process.env.BENCH_N ?? defaults.count,
      concurrency: process.env.BENCH_CONCURRENCY ?? defaults.concurrency,
      warmup: process.env.BENCH_WARMUP,
      timeoutMs: process.env.BENCH_TIMEOUT_MS,
      baseUrl: process.env.BASE_URL,
    });
    console.log(`New tasks: ${report.newTasks}/${report.attempted}`);
    console.log(`Duplicates: ${report.duplicates}; errors: ${report.errors}`);
    console.log(`Duration: ${report.durationSeconds.toFixed(3)}s`);
    console.log(`Enqueue throughput: ${report.newTasksPerSecond.toFixed(2)} new tasks/sec`);
    if (report.latencyMs) {
      const latency = report.latencyMs;
      console.log(`HTTP latency: mean ${latency.mean.toFixed(2)}ms, p95 ${latency.p95.toFixed(2)}ms, p99 ${latency.p99.toFixed(2)}ms`);
    } else console.log("HTTP latency: unavailable (no successful responses)");
    if (!report.valid) {
      console.error("INVALID benchmark: errors, duplicates, or missing unique task IDs; see report.");
      process.exitCode = 1;
    }
  } catch (error) {
    report = { valid: false, measured: false, timestamp: new Date().toISOString(),
      error: error.message, code: error.code || null };
    console.error(`Benchmark could not run (${error.code || "ERROR"}): ${error.message}. API: ${process.env.BASE_URL || "http://localhost:3000"}`);
    process.exitCode = 1;
  }
  const destination = output || path.join("data", "benchmarks", `${report.runId || `failed-${Date.now()}`}.json`);
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  fs.writeFileSync(destination, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Report: ${destination}`);
}

module.exports = { runApiMetrics, latencySummary, positiveInteger, cli };
