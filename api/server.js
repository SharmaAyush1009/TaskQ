require("dotenv").config();

const express = require("express");
const pool = require("../db/db");
const { commandClient } = require("../queue/redis");

const app = express();
app.use(express.json());

app.post("/tasks", async (req, res) => {
  const { type, payload, idempotency_key } = req.body;

  // Validation
  if (!type || !payload) {
    return res.status(400).json({ 
      error: "type and payload are required" 
    });
  }

  if (!idempotency_key) {
    return res.status(400).json({ 
      error: "idempotency_key is required for duplicate prevention" 
    });
  }
  try {
    // check if task already exists with this idempotency key
    const existing = await pool.query(
      `SELECT id, status, result, created_at FROM tasks
      WHERE type = $1 AND idempotency_key = $2`,
      [type, idempotency_key]
    );

    if (existing.rows.length > 0) {
      console.log(`Duplicate request detected: returning existing task ${existing.rows[0].id}`);
      
      return res.json({
        id: existing.rows[0].id,
        status: existing.rows[0].status,
        result: existing.rows[0].result,
        duplicate: true
      });
    }

    // create new task
    const result = await pool.query(
      `INSERT INTO tasks (type, payload, status, idempotency_key, created_at, updated_at)
      VALUES ($1, $2, 'PENDING', $3, NOW(), NOW())
      RETURNING id`,
      [type, payload, idempotency_key]
    );

    const taskId = result.rows[0].id;

    // push to redis queue
    await commandClient.lPush("task_queue", taskId.toString());
    console.log(`Created new task ${taskId} (${type})`);
    
    res.status(201).json({ 
      id: taskId, 
      status: "PENDING", 
      duplicate: false 
    });
  } catch(err){
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

// ============================================
// LIST TASKS
// ============================================

app.get("/tasks", async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 20;
    const status = req.query.status;
    const type = req.query.type;

    let query = "SELECT * FROM tasks WHERE 1=1";
    const params = [];

    if (status) {
      params.push(status);
      query += ` AND status = ${params.length}`;
    }

    if (type) {
      params.push(type);
      query += ` AND type = ${params.length}`;
    }

    params.push(limit);
    query += ` ORDER BY created_at DESC LIMIT ${params.length}`;

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

// Start server

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Task Queue API Server`);
  console.log(`Listening on port ${PORT}`);
  console.log(`Started at: ${new Date().toLocaleString()}`);
});
