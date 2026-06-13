const axios = require("axios");
const pool = require("./db/db");

async function run() {
  const BASE = "http://localhost:3000";

  // Enqueue 5 tasks
  const ids = [];
  for (let i = 0; i < 5; i++) {
    const res = await axios.post(`${BASE}/tasks`, {
      type: "send_email",
      payload: { to: `crash-test-${i}@example.com`, subject: "Crash test" },
      idempotency_key: `crash-${Date.now()}-${i}`
    });
    ids.push(res.data.id);
  }

  console.log(`Created tasks: ${ids.join(', ')}`);
  console.log("Now manually kill the worker with: kill -9 <worker_pid>");
  console.log("Then simulate stuck IN_PROGRESS by running:");
  console.log(`UPDATE tasks SET status='IN_PROGRESS', updated_at=NOW()-INTERVAL '2 minutes' WHERE id IN (${ids.join(',')});`);
  console.log("\nWaiting 2 minutes for reaper to recover them...\n");

  await new Promise(res => setTimeout(res, 130000)); // wait slightly over reaper cycle

  const result = await pool.query(
    `SELECT id, status FROM tasks WHERE id = ANY($1)`,
    [ids]
  );

  const recovered = result.rows.filter(r => r.status !== 'IN_PROGRESS');
  console.log(`Recovered: ${recovered.length}/${ids.length}`);
  result.rows.forEach(r => console.log(`  Task ${r.id}: ${r.status}`));

  if (recovered.length === ids.length) {
    console.log("PASS — reaper recovered all stuck tasks");
  } else {
    console.log("FAIL — some tasks still stuck");
  }

  process.exit(0);
}

run();