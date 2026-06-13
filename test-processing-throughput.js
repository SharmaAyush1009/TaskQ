const pool = require("./db/db");

async function run() {
  const TARGET = 200; // adjust based on how many you just enqueued

  console.log("Waiting for worker to process tasks...");
  const start = Date.now();

  while (true) {
    const result = await pool.query(
      `SELECT 
        COUNT(*) FILTER (WHERE status = 'SUCCESS') as done,
        COUNT(*) FILTER (WHERE status = 'PENDING' OR status = 'IN_PROGRESS') as remaining,
        COUNT(*) FILTER (WHERE status = 'DEAD') as dead
       FROM tasks
       WHERE created_at > NOW() - INTERVAL '10 minutes'`
    );

    const { done, remaining, dead } = result.rows[0];
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    process.stdout.write(`\r[${elapsed}s] Done: ${done} | Remaining: ${remaining} | Dead: ${dead}   `);

    if (parseInt(remaining) === 0) {
      const duration = (Date.now() - start) / 1000;
      console.log(`\n\nAll tasks processed in ${duration.toFixed(2)}s`);
      console.log(`Processing throughput: ${(parseInt(done) / duration).toFixed(0)} tasks/sec`);
      console.log(`Dead tasks: ${dead}`);
      break;
    }

    await new Promise(res => setTimeout(res, 500));
  }

  process.exit(0);
}

run();