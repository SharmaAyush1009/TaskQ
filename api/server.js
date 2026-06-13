require("dotenv").config();

const express = require("express");
const pool = require("../db/db");
const { commandClient } = require("../queue/redis");

const app = express();
app.use(express.json());

const taskValidators = {
  send_email: (payload) => {
    if (!payload.to || typeof payload.to !== 'string') 
      return 'payload.to (recipient email) is required for send_email';
    return null;
  },
  process_payment: (payload) => {
    if (typeof payload.amount !== 'number') 
      return 'payload.amount is required for process_payment';
    return null;
  },
  resize_image: (payload) => {
    if (!payload.url || !Array.isArray(payload.sizes)) 
      return 'payload.url and payload.sizes[] are required for resize_image';
    return null;
  },
  generate_report: (payload) => {
    if (!payload.userId) 
      return 'payload.userId is required for generate_report';
    return null;
  },
  test_failure: () => null  // no required fields
};

app.post("/tasks", async (req, res) => {
  const { type, payload, idempotency_key } = req.body;

  if (!type || !payload) {
    return res.status(400).json({ error: "type and payload are required" });
  }

  if (!taskValidators[type]) {
    return res.status(400).json({ 
      error: `Unknown task type: ${type}. Valid types: ${Object.keys(taskValidators).join(', ')}` 
    });
  }

  const validationError = taskValidators[type](payload);
  if (validationError) {
    return res.status(400).json({ error: validationError });
  }

  if (!idempotency_key) {
    return res.status(400).json({ error: "idempotency_key is required for duplicate prevention" });
  }

  try {
    const result = await pool.query(
      `INSERT INTO tasks (type, payload, status, idempotency_key, created_at, updated_at)
       VALUES ($1, $2, 'PENDING', $3, NOW(), NOW())
       ON CONFLICT (type, idempotency_key) DO NOTHING
       RETURNING id`,
      [type, JSON.stringify(payload), idempotency_key]
    );

    if (result.rows.length === 0) {
      const existing = await pool.query(
        `SELECT id, status, result FROM tasks WHERE type = $1 AND idempotency_key = $2`,
        [type, idempotency_key]
      );
      return res.json({ 
        id: existing.rows[0].id, 
        status: existing.rows[0].status, 
        result: existing.rows[0].result, 
        duplicate: true 
      });
    }

    const taskId = result.rows[0].id;
    await commandClient.lPush("task_queue", taskId.toString());
    await commandClient.sAdd("task_queue_set", taskId.toString());
    console.log(`Created new task ${taskId} (${type})`);

    res.status(201).json({ id: taskId, status: "PENDING", duplicate: false });
  } catch (err) {
    console.error("Error creating task:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Get task status

app.get("/tasks/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const result = await pool.query(
      "SELECT * FROM tasks WHERE id = $1",
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Task not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error fetching task:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post('/tasks/:id/retry', async (req, res) => {
  const { id } = req.params;

  try {
    const result = await pool.query(
      `UPDATE tasks
       SET status = 'PENDING',
           attempts = 0,
           last_error = NULL,
           retry_at = NULL,
           updated_at = NOW()
       WHERE id = $1 AND status = 'DEAD'
       RETURNING id, type, status`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ 
        error: 'Task not found or not in DEAD status. Only DEAD tasks can be retried.' 
      });
    }

    await commandClient.lPush('task_queue', id.toString());
    await commandClient.sAdd('task_queue_set', id.toString());

    res.json({ id: result.rows[0].id, status: 'PENDING', message: 'Task requeued for retry' });
  } catch (err) {
    console.error('Error retrying task:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Get statistics
app.get("/stats", async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT 
        status,
        COUNT(*) as count,
        AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) as avg_duration_seconds
      FROM tasks
      WHERE created_at > NOW() - INTERVAL '1 hour'
      GROUP BY status
    `);

    res.json({
      last_hour: stats.rows,
      timestamp: new Date()
    });
  } catch (err) {
    console.error("Error fetching stats:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});

//list tasks
app.get("/tasks", async (req, res) => {
  try {
    const rawLimit = parseInt(req.query.limit);
    const limit = (!isNaN(rawLimit) && rawLimit > 0 && rawLimit <= 100) ? rawLimit : 20;
    const status = req.query.status;
    const type = req.query.type;

    let query = "SELECT * FROM tasks WHERE 1=1";
    const params = [];

    if (status) {
      params.push(status);
      query += ` AND status = $${params.length}`;
    }

    if (type) {
      params.push(type);
      query += ` AND type = $${params.length}`;
    }

    params.push(limit);
    query += ` ORDER BY created_at DESC LIMIT $${params.length}`;

    const result = await pool.query(query, params);
    
    res.json({
      tasks: result.rows,
      count: result.rows.length,
      filters: { status, type, limit }
    });
  } catch (err) {
    console.error("Error listing tasks:", err);
    res.status(500).json({ error: "Internal server error" });
  }
});
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy' });
});
// Start server

const PORT = process.env.PORT || 3000;

async function verifyConnections() {
  try {
    await pool.query('SELECT 1');
    console.log('Postgres connection verified');
  } catch (err) {
    console.error('Cannot connect to Postgres:', err.message);
    process.exit(1);
  }

  try {
    await commandClient.ping();
    console.log('Redis connection verified');
  } catch (err) {
    console.error('Cannot connect to Redis:', err.message);
    process.exit(1);
  }
}


verifyConnections().then(() => {
  app.listen(PORT, () => {
    console.log(`Task Queue API Server`);
    console.log(`Listening on port ${PORT}`);
    console.log(`Started at: ${new Date().toLocaleString()}`);
  });
});