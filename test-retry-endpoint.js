const axios = require("axios");

async function run() {
  const BASE = "http://localhost:3000";

  // Create a test_failure task (it will go dead after max_attempts)
  const create = await axios.post(`${BASE}/tasks`, {
    type: "test_failure",
    payload: {},
    idempotency_key: `retry-test-${Date.now()}`
  });

  const start = Date.now();
  const taskId = create.data.id;
  console.log(`Created task ${taskId}, waiting for it to go DEAD...`);
  console.log(`This takes ~15 minutes because reaper runs every 5 minutes\n`);

  let task;
  const deadline = Date.now() + 20 * 60 * 1000; // 20 minute timeout

  while (Date.now() < deadline) {
    await new Promise(res => setTimeout(res, 5000)); // poll every 5 seconds
    const check = await axios.get(`${BASE}/tasks/${taskId}`);
    task = check.data;
    const elapsed = Math.round((Date.now() - start) / 1000);
    process.stdout.write(`\r[${elapsed}s] Status: ${task.status}, Attempts: ${task.attempts}   `);
    if (task.status === 'DEAD') {
      console.log('\n');
      break;
    }
  }

  if (task.status !== 'DEAD') {
    console.log("Task never went DEAD — check max_attempts and worker");
    process.exit(1);
  }

  console.log("\nTask is DEAD. Testing retry endpoint...");
  const retry = await axios.post(`${BASE}/tasks/${taskId}/retry`);
  console.log(`Retry response:`, retry.data);

  // Verify it went back to PENDING
  const check = await axios.get(`${BASE}/tasks/${taskId}`);
  console.log(`Status after retry: ${check.data.status} (should be PENDING)`);
  console.log(`Attempts after retry: ${check.data.attempts} (should be 0)`);

  process.exit(0);
}

run().catch(err => {
  console.error(err.response?.data || err.message);
  process.exit(1);
});