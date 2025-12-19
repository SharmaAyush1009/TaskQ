require("dotenv").config();
const redis = require("../queue/redis");
const pool = require("../db/db");

// Task Handlers
const taskHandlers = {
  send_email: async (payload) => {
    console.log(`Sending email to ${payload.to}`);
    console.log(`Subject: ${payload.subject || 'No subject'}`);
    
    // Simulate email sending delay
    await new Promise(res => setTimeout(res, 1500));
    
    // TODO: Real implementation
    // logic to send mail using service like sendgrid,etc
    
    console.log(`Email sent to ${payload.to}`);
    return { sent: true, to: payload.to };
  },

  resize_image: async (payload) => {
    console.log(`Resizing image: ${payload.url}`);
    console.log(`Target sizes: ${payload.sizes.join(', ')}px`);
    
    // Simulate image processing delay
    await new Promise(res => setTimeout(res, 3000));
    
    // TODO: Real implementation
    // resize logic
    
    console.log(`Image resized successfully`);
    return { 
      original: payload.url, 
      resized: payload.sizes.map(s => `resized_${s}.jpg`)
    };
  },

  generate_report: async (payload) => {
    console.log(`Generating report for user ${payload.userId}`);
    console.log(`Report type: ${payload.reportType || 'standard'}`);
    
    // Simulate report generation delay
    await new Promise(res => setTimeout(res, 4000));
    
    // TODO: Real implementation
    // real logic to do so

    console.log(`Report generated for user ${payload.userId}`);
    return { 
      reportUrl: `reports/user_${payload.userId}_report.pdf`,
      generatedAt: new Date().toISOString()
    };
  },

  process_payment: async (payload) => {
    console.log(`Processing payment: $${payload.amount}`);
    console.log(`Customer: ${payload.customerId}`);
    
    // Simulate payment processing delay
    await new Promise(res => setTimeout(res, 2000));
    
    // TODO: Real implementation
    // can use stripe logic
    
    console.log(`Payment processed: $${payload.amount}`);
    return { 
      transactionId: `txn_${Date.now()}`,
      amount: payload.amount,
      status: 'completed'
    };
  },

  send_notification: async (payload) => {
    console.log(`Sending notification to user ${payload.userId}`);
    console.log(`Message: ${payload.message}`);
    
    await new Promise(res => setTimeout(res, 1000));
    
    // TODO: Real implementation
    // send notification logic
    
    console.log(`Notification sent`);
    return { delivered: true, userId: payload.userId };
  }
};

// WORKER LOGIC (Generic - Works for All Tasks)

async function processTask(taskId) {
  // Step 1: Atomically claim the task
  const result = await pool.query(
    `UPDATE tasks
     SET status = 'IN_PROGRESS', updated_at = NOW()
     WHERE id = $1 AND status = 'PENDING'
     RETURNING *`,
    [taskId]
  );

  // Another worker already claimed it
  if (result.rowCount === 0) {
    console.log(`Task ${taskId} already claimed by another worker`);
    return;
  }

  const task = result.rows[0];
  const startTime = Date.now();
  
  console.log(`\n[Task ${task.id}] Started: ${task.type}`);

  try {
    // Step 2: Find the handler for this task type
    const handler = taskHandlers[task.type];
    
    if (!handler) {
      throw new Error(`Unknown task type: ${task.type}. Available types: ${Object.keys(taskHandlers).join(', ')}`);
    }

    // Step 3: Execute the handler with the task payload
    const handlerResult = await handler(task.payload);
    
    const duration = Date.now() - startTime;

    // Step 4: Mark task as successful
    await pool.query(
      `UPDATE tasks
       SET status = 'SUCCESS', 
           updated_at = NOW(),
           result = $2
       WHERE id = $1`,
      [task.id, JSON.stringify(handlerResult)]
    );

    console.log(`[Task ${task.id}] Completed in ${duration}ms\n`);

  } catch (err) {
    const duration = Date.now() - startTime;
    
    // Step 5: Mark task as failed
    await pool.query(
      `UPDATE tasks
       SET status = 'FAILED',
           attempts = attempts + 1,
           last_error = $2,
           updated_at = NOW()
       WHERE id = $1`,
      [task.id, err.message]
    );

    console.error(`[Task ${task.id}] Failed after ${duration}ms`);
    console.error(`Error: ${err.message}\n`);
  }
}

async function startWorker() {
  console.log("Task Worker Started");
  console.log(`Available task types: ${Object.keys(taskHandlers).join(', ')}`);
  console.log(`Started at: ${new Date().toLocaleString()}`);
  console.log("════════════════════════════════════════\n");
  console.log("Waiting for tasks...\n");

  while (true) {
    try {
      // Block until a task is available
      const task = await redis.brPop("task_queue", 0);
      await processTask(task.element);
      
    } catch (err) {
      console.error("Worker error:", err.message);
      console.error("Retrying in 5 seconds...\n");
      
      // Brief pause before retrying to avoid tight error loops
      await new Promise(res => setTimeout(res, 5000));
    }
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log("\n\nShutdown signal received (SIGINT)");
  console.log("Worker will stop after completing current task...");
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log("\n\nShutdown signal received (SIGTERM)");
  console.log("Worker will stop after completing current task...");
  process.exit(0);
});

// Start the worker
startWorker();