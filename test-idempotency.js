const axios = require("axios");

async function run() {
  const KEY = `idem-test-${Date.now()}`;
  const CONCURRENCY = 50;

  console.log(`Firing ${CONCURRENCY} concurrent requests with same idempotency_key...`);

  const requests = Array.from({ length: CONCURRENCY }, () =>
    axios.post("http://localhost:3000/tasks", {
      type: "send_email",
      payload: { to: "test@example.com", subject: "Idempotency test" },
      idempotency_key: KEY
    }).catch(err => ({ error: err.response?.data }))
  );

  const results = await Promise.all(requests);
  const successful = results.filter(r => r.data || r.status);
  const ids = new Set(results.filter(r => r.data?.id).map(r => r.data.id));

  console.log(`Unique task IDs created: ${ids.size} (should be 1)`);
  console.log(`Duplicates flagged: ${results.filter(r => r.data?.duplicate).length}`);

  if (ids.size === 1) {
    console.log("PASS — idempotency constraint working correctly");
  } else {
    console.log("FAIL — multiple tasks created for same idempotency_key");
    console.log([...ids]);
  }

  process.exit(0);
}

run();