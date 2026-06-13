const axios = require("axios");

async function run() {
  const BASE_URL = "http://localhost:3000";
  const N = 500;
  const CONCURRENCY = 50;
  const times = [];
  const errors = [];

  console.log(`Running ${N} requests at concurrency ${CONCURRENCY}...\n`);

  for (let i = 0; i < N; i += CONCURRENCY) {
    const batch = [];
    for (let j = i; j < Math.min(i + CONCURRENCY, N); j++) {
      const start = Date.now();
      batch.push(
        axios.post(`${BASE_URL}/tasks`, {
          type: "send_email",
          payload: { to: `bench-${j}@example.com`, subject: "Benchmark" },
          idempotency_key: `bench-run-${Date.now()}-${j}`
        })
        .then(res => {
          times.push({ duration: Date.now() - start, duplicate: res.data.duplicate });
        })
        .catch(err => {
          errors.push({ index: j, status: err.response?.status, message: err.response?.data?.error });
        })
      );
    }
    await Promise.all(batch);
  }

  if (errors.length > 0) {
    console.log(`Errors: ${errors.length}`);
    console.log(errors.slice(0, 5)); // show first 5
  }

  const durations = times.map(t => t.duration).sort((a, b) => a - b);
  const avg = durations.reduce((a, b) => a + b, 0) / durations.length;
  const p95 = durations[Math.floor(durations.length * 0.95)];
  const p99 = durations[Math.floor(durations.length * 0.99)];
  const duplicates = times.filter(t => t.duplicate).length;

  console.log(`Completed: ${times.length}/${N}`);
  console.log(`Duplicates (should be 0): ${duplicates}`);
  console.log(`Mean latency: ${avg.toFixed(2)}ms`);
  console.log(`p95: ${p95}ms`);
  console.log(`p99: ${p99}ms`);
  console.log(`Min: ${durations[0]}ms`);
  console.log(`Max: ${durations[durations.length - 1]}ms`);
}

run();