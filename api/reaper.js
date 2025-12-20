require("dotenv").config();
const pool = require("../db/db");
const { commandClient } = require("../queue/redis");

const ZOMBIE_THRESHOLD_MINUTES = 1;
const CHECK_INTERVAL_MINUTES = 5;

async function reapZombieTasks() {
  console.log(`\n[${new Date().toLocaleTimeString()}] Scanning for zombie tasks...`);

  try {
    // Find stuck IN_PROGRESS tasks
    const zombieResult = await pool.query(
      `UPDATE tasks
       SET status = 'PENDING', 
           attempts = attempts + 1,
           updated_at = NOW()
       WHERE status = 'IN_PROGRESS'
         AND updated_at < NOW() - INTERVAL '${ZOMBIE_THRESHOLD_MINUTES} minutes'
       RETURNING id, type, updated_at`
    );

    // Also check for PENDING tasks not in queue
    const pendingResult = await pool.query(
      `SELECT id, type FROM tasks 
       WHERE status = 'PENDING' 
       AND updated_at < NOW() - INTERVAL '1 minute'
       ORDER BY created_at ASC`
    );

    let totalRequeued = 0;

    if (zombieResult.rowCount > 0) {
      console.log(`Found ${zombieResult.rowCount} zombie task(s):`);

      for (const task of zombieResult.rows) {
        const age = Math.round((Date.now() - new Date(task.updated_at)) / 1000 / 60);
        console.log(`   - Task ${task.id} (${task.type}) - stale for ${age} minutes`);
        
        await commandClient.lPush('task_queue', task.id.toString());
        console.log(` Re-queued`);
        totalRequeued++;
      }
    }

    if (pendingResult.rowCount > 0) {
      console.log(`Found ${pendingResult.rowCount} pending task(s) to re-queue:`);

      for (const task of pendingResult.rows) {
        await commandClient.lPush('task_queue', task.id.toString());
        console.log(`Re-queued task ${task.id} (${task.type})`);
        totalRequeued++;
      }
    }

    if (totalRequeued === 0) {
      console.log(`No tasks need re-queuing`);
    } else {
      console.log(`\nTotal re-queued: ${totalRequeued}`);
    }
  } catch (err) {
    console.error(`Reaper error:`, err.message);
  }
}

async function startReaper() {
  console.log("════════════════════════════════════════");
  console.log("Zombie Task Reaper Started");
  console.log(`Check interval: Every ${CHECK_INTERVAL_MINUTES} minutes`);
  console.log(`Zombie threshold: ${ZOMBIE_THRESHOLD_MINUTES} minutes`);
  console.log(`Started at: ${new Date().toLocaleString()}`);
  console.log("════════════════════════════════════════");

  // Run immediately on start
  await reapZombieTasks();

  // Then run periodically
  setInterval(reapZombieTasks, CHECK_INTERVAL_MINUTES * 60 * 1000);
}

process.on('SIGINT', () => {
  console.log("\n\nReaper shutting down...");
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log("\n\nReaper shutting down...");
  process.exit(0);
});

startReaper();