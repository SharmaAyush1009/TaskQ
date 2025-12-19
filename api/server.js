require("dotenv").config();

const express = require("express");
const pool = require("../db/db");
const redis = require("../queue/redis");

const app = express();
app.use(express.json());

app.post("/tasks", async (req, res) => {
  const { type, payload, idempotency_key } = req.body;

  if (!type || !payload || !idempotency_key) {
    return res.status(400).json({
      error: "type, payload, and idempotency_key required"
    });
  }

  const existing = await pool.query(
    `SELECT id, status FROM tasks
     WHERE type = $1 AND idempotency_key = $2`,
    [type, idempotency_key]
  );

  if (existing.rows.length > 0) {
    return res.json({
      taskId: existing.rows[0].id,
      status: existing.rows[0].status,
      duplicate: true
    });
  }

  const result = await pool.query(
    `INSERT INTO tasks (type, payload, status, idempotency_key)
     VALUES ($1, $2, 'PENDING', $3)
     RETURNING id`,
    [type, payload, idempotency_key]
  );

  const taskId = result.rows[0].id;
  await redis.lPush("task_queue", taskId.toString());

  res.json({ taskId, status: "PENDING", duplicate: false });
});

app.listen(process.env.PORT, () => {
  console.log(`API running on port ${process.env.PORT}`);
});
