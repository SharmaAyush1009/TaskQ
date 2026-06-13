const axios = require("axios");

async function run() {
  const N = 1000;
  const CONCURRENCY = 100;
  const start = Date.now();
  const errors = [];

  for (let i = 0; i < N; i += CONCURRENCY) {
    const batch = [];
    for (let j = i; j < Math.min(i + CONCURRENCY, N); j++) {
      batch.push(
        axios.post("http://localhost:3000/tasks", {
          type: "send_email",
          payload: { to: `throughput-${j}@example.com`, subject: "Throughput" },
          idempotency_key: `throughput-${Date.now()}-${j}`
        }).catch(err => errors.push(err.response?.data?.error))
      );
    }
    await Promise.all(batch);
  }

  const duration = (Date.now() - start) / 1000;
  console.log(`Enqueued: ${N - errors.length}/${N}`);
  console.log(`Errors: ${errors.length}`);
  console.log(`Duration: ${duration.toFixed(2)}s`);
  console.log(`Enqueue throughput: ${((N - errors.length) / duration).toFixed(0)} tasks/sec`);
}

run();