require("dotenv").config();
const { commandClient, blockingClient} = require("../queue/redis");
const pool = require("../db/db");


function isNonRetryableError(err) {
  const msg = err.message || "";

  return (
    msg.includes("recipient") ||
    msg.includes("null value") ||
    msg.includes("violates not-null constraint") ||
    msg.includes("Unknown task type")
  );
}

// IDEMPOTENT TASK HANDLERS

const taskHandlers = {

  // EMAIL HANDLER (with idempotency)

  send_email: async (payload, taskId) => {
    // Check if already sent
    const existing = await pool.query(
      'SELECT * FROM email_logs WHERE task_id = $1',
      [taskId]
    );

    if (existing.rows.length > 0) {
      console.log(`Email already sent (found in logs)`);
      return { 
        status: 'already_sent', 
        sentAt: existing.rows[0].sent_at,
        recipient: existing.rows[0].recipient
      };
    }

    console.log(`Sending email to ${payload.to}`);
    console.log(`Subject: ${payload.subject || 'No subject'}`);
    
    // Simulate email sending
    await new Promise(res => setTimeout(res, 1500));
    
    // Real implementation HERE!!
    // can use sendgrid etc logic here

    // Record that we sent it (CRITICAL for idempotency!)
    await pool.query(
      `INSERT INTO email_logs (task_id, recipient, subject, sent_at)
       VALUES ($1, $2, $3, NOW())`,
      [taskId, payload.to, payload.subject || '']
    );

    console.log(`Email sent successfully`);
    return { status: 'sent', to: payload.to };
  },

  // PAYMENT HANDLER (with idempotency)
  
  process_payment: async (payload, taskId) => {
    // Check if already processed
    const existing = await pool.query(
      'SELECT * FROM payment_logs WHERE task_id = $1',
      [taskId]
    );

    if (existing.rows.length > 0) {
      console.log(`Payment already processed`);
      return {
        status: 'already_processed',
        transactionId: existing.rows[0].transaction_id,
        amount: existing.rows[0].amount
      };
    }

    console.log(`Processing payment: $${payload.amount}`);
    console.log(`Customer: ${payload.customerId || 'N/A'}`);
    
    // Simulate payment processing
    await new Promise(res => setTimeout(res, 2000));
    
    const transactionId = `txn_${taskId}_${Date.now()}`;
    
    // TODO: Real implementation with idempotency key
    // const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
    // const charge = await stripe.charges.create({
    //   amount: payload.amount,
    //   currency: 'usd',
    //   customer: payload.customerId
    // }, {
    //   idempotencyKey: `task-${taskId}` // ← Stripe's built-in deduplication
    // });

    // Record the payment
    await pool.query(
      `INSERT INTO payment_logs (task_id, transaction_id, amount, processed_at)
       VALUES ($1, $2, $3, NOW())`,
      [taskId, transactionId, payload.amount]
    );

    console.log(`Payment processed`);
    return { 
      status: 'processed', 
      transactionId,
      amount: payload.amount
    };
  },

  // IMAGE HANDLER
  
  resize_image: async (payload, taskId) => {
    // Check if already processed
    const existing = await pool.query(
      `SELECT * FROM operation_logs 
       WHERE task_id = $1 AND operation_type = 'resize_image'`,
      [taskId]
    );

    if (existing.rows.length > 0) {
      console.log(`Image already resized`);
      return JSON.parse(existing.rows[0].operation_key);
    }

    console.log(`Resizing image: ${payload.url}`);
    console.log(`Sizes: ${payload.sizes.join(', ')}px`);
    
    // Simulate image processing
    await new Promise(res => setTimeout(res, 3000));
    
    // Real logic HERE!!
    

    const result = {
      original: payload.url,
      resized: payload.sizes.map(s => `resized_${taskId}_${s}.jpg`)
    };

    // Record operation
    await pool.query(
      `INSERT INTO operation_logs (task_id, operation_type, operation_key, completed_at)
       VALUES ($1, 'resize_image', $2, NOW())`,
      [taskId, JSON.stringify(result)]
    );

    console.log(`Image resized`);
    return result;
  },

  // REPORT HANDLER
  generate_report: async (payload, taskId) => {
    const existing = await pool.query(
      `SELECT * FROM operation_logs 
       WHERE task_id = $1 AND operation_type = 'generate_report'`,
      [taskId]
    );

    if (existing.rows.length > 0) {
      console.log(`Report already generated`);
      return JSON.parse(existing.rows[0].operation_key);
    }

    console.log(`Generating report for user ${payload.userId}`);
    
    await new Promise(res => setTimeout(res, 4000));
    
    const result = {
      reportUrl: `reports/user_${payload.userId}_${taskId}.pdf`,
      generatedAt: new Date().toISOString()
    };

    await pool.query(
      `INSERT INTO operation_logs (task_id, operation_type, operation_key, completed_at)
       VALUES ($1, 'generate_report', $2, NOW())`,
      [taskId, JSON.stringify(result)]
    );

    console.log(`Report generated`);
    return result;
  },

  // test failure
  test_failure: async (payload, taskId) => {
    console.log(`Simulating failure...`);
    await new Promise(res => setTimeout(res, 500));
    throw new Error("Intentional failure for testing retries");
  }
};

// WORKER LOGIC (IDEMPOTENT)

async function processTask(taskId) {
  // STEP 1: Fetch current task state
  const checkResult = await pool.query(
    'SELECT * FROM tasks WHERE id = $1',
    [taskId]
  );

  if (checkResult.rows.length === 0) {
    console.log(`Task ${taskId} not found`);
    return;
  }

  const task = checkResult.rows[0];

  // ✅ ADD THIS: Check retry_at
  if (task.retry_at && new Date(task.retry_at) > new Date()) {
    const waitSeconds = Math.ceil((new Date(task.retry_at) - new Date()) / 1000);
    console.log(`Task ${taskId} not ready yet (retry in ${waitSeconds}s), re-queueing`);
    
    // Put back in queue
    await commandClient.lPush('task_queue', taskId.toString());
    return;
  }
  
  // IDEMPOTENCY CHECK 1: Already completed?
  if (task.status === 'SUCCESS') {
    console.log(`Task ${taskId} already completed, skipping\n`);
    return;
  }

  // IDEMPOTENCY CHECK 2: Currently being processed by another worker?
  if (task.status === 'IN_PROGRESS') {
    const ageMinutes = (Date.now() - new Date(task.updated_at)) / 1000 / 60;
    if (ageMinutes < 10) {
      console.log(`Task ${taskId} currently being processed by another worker, skipping\n`);
      return;
    }
    // If older than 10 minutes, it's probably a zombie - continue to claim
  }

  // STEP 2: Try to atomically claim the task
  const claimResult = await pool.query(
    `UPDATE tasks
     SET status = 'IN_PROGRESS', updated_at = NOW()
     WHERE id = $1 AND status IN ('PENDING', 'FAILED')
     RETURNING *`,
    [taskId]
  );

  if (claimResult.rowCount === 0) {
    console.log(`Task ${taskId} already claimed by another worker\n`);
    return;
  }

  const claimedTask = claimResult.rows[0];
  const startTime = Date.now();
  
  console.log(`\n[Task ${claimedTask.id}] Started`);
  console.log(`Type: ${claimedTask.type}`);
  console.log(`Attempt: ${claimedTask.attempts + 1}/${claimedTask.max_attempts}`);

  try {
    // STEP 3: Find and execute the handler
    const handler = taskHandlers[claimedTask.type];
    
    if (!handler) {
      throw new Error(
        `Unknown task type: ${claimedTask.type}. ` +
        `Available: ${Object.keys(taskHandlers).join(', ')}`
      );
    }

    // Execute handler (passes taskId for idempotency)
    const result = await handler(claimedTask.payload, claimedTask.id);
    
    const duration = Date.now() - startTime;

    // STEP 4: Mark as successful
    await pool.query(
      `UPDATE tasks
       SET status = 'SUCCESS', 
           result = $2,
           updated_at = NOW()
       WHERE id = $1`,
      [claimedTask.id, JSON.stringify(result)]
    );

    console.log(`[Task ${claimedTask.id}] Completed in ${duration}ms\n`);

  } catch (err) {
    const duration = Date.now() - startTime;
    const newAttempts = claimedTask.attempts + 1;

    if (isNonRetryableError(err)) {
      await pool.query(
        `UPDATE tasks
        SET status = 'DEAD',
            attempts = $2,
            last_error = $3,
            updated_at = NOW()
        WHERE id = $1`,
        [claimedTask.id, newAttempts, err.message]
      );

      console.error(`💀 [Task ${claimedTask.id}] Non-retryable error`);
      console.error(`   Error: ${err.message}\n`);
      return;
    }
    // Check if we should retry
    if (newAttempts < claimedTask.max_attempts) {
      // Exponential backoff: 2^attempts seconds
      // Attempt 1: 2s, Attempt 2: 4s, Attempt 3: 8s
      const backoffSeconds = Math.pow(2, newAttempts);
      const retryAt = new Date(Date.now() + backoffSeconds * 1000);

      await pool.query(
        `UPDATE tasks
         SET status = 'PENDING',
             attempts = $2,
             last_error = $3,
             retry_at = $4,
             updated_at = NOW()
         WHERE id = $1`,
        [claimedTask.id, newAttempts, err.message, retryAt]
      );

      console.log(`🔄 [Task ${claimedTask.id}] Will retry in ${backoffSeconds}s`);
      console.log(`   Attempt ${newAttempts}/${claimedTask.max_attempts}`);
      console.log(`   Error: ${err.message}\n`);

      // // Re-queue after delay
      // setTimeout(async () => {
      //   await redis.lPush('task_queue', claimedTask.id.toString());
      //   console.log(`📨 [Task ${claimedTask.id}] Re-queued for retry\n`);
      // }, backoffSeconds * 1000);
      // ⚠️ IMPORTANT: Don't use await inside setTimeout!
      // Re-queue after delay (non-blocking)
      setTimeout(() => {
        commandClient.lPush('task_queue', claimedTask.id.toString())
          .then(() => {
            console.log(`📨 [Task ${claimedTask.id}] Re-queued for retry\n`);
          })
          .catch((redisErr) => {
            console.error(`❌ Failed to re-queue task ${claimedTask.id}:`, redisErr.message);
          });
      }, backoffSeconds * 1000);

    } else {
      // Max retries exceeded - mark as DEAD
      await pool.query(
        `UPDATE tasks
         SET status = 'DEAD',
             attempts = $2,
             last_error = $3,
             updated_at = NOW()
         WHERE id = $1`,
        [claimedTask.id, newAttempts, err.message]
      );

      console.error(`💀 [Task ${claimedTask.id}] Max retries exceeded`);
      console.error(`   Final error: ${err.message}\n`);
    }
  }
}

async function startWorker() {
  console.log("════════════════════════════════════════");
  console.log("Task Worker Started");
  console.log(`Supported task types:`);
  Object.keys(taskHandlers).forEach(type => {
    console.log(`   - ${type}`);
  });
  console.log(`Started at: ${new Date().toLocaleString()}`);
  console.log("════════════════════════════════════════\n");
  console.log("Waiting for tasks...\n");

  while (true) {
    try {
      // Block until a task is available, use blocking client
      const task = await blockingClient.brPop("task_queue", 0);
      await processTask(task.element);
      
    } catch (err) {
      console.error("Worker error:", err.message);
      console.error("Retrying in 5 seconds...\n");
      
      // Brief pause before retrying
      await new Promise(res => setTimeout(res, 5000));
    }
  }
}

// Graceful shutdown
process.on('SIGINT', () => {
  console.log("\n\nShutdown signal received");
  console.log("Worker stopping after current task...");
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log("\n\nShutdown signal received");
  console.log("Worker stopping after current task...");
  process.exit(0);
});

// Start the worker
startWorker();