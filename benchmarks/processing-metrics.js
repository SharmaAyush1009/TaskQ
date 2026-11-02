const { performance } = require("node:perf_hooks");
const { positiveInteger } = require("./api-metrics");

async function observeProcessing(pool, input, options = {}) {
  if (!input.valid || !Array.isArray(input.taskIds) || input.taskIds.length === 0) {
    throw new Error("Supply a valid enqueue report containing taskIds");
  }
  const ids = [...new Set(input.taskIds.map(String))];
  const timeoutMs = positiveInteger(options.timeoutMs, 120000, "timeoutMs");
  const pollMs = positiveInteger(options.pollMs, 250, "pollMs");
  const query = "SELECT id, status FROM tasks WHERE id = ANY($1::bigint[])";
  const started = performance.now();
  let rows = (await pool.query(query, [ids])).rows;
  if (rows.length !== ids.length) throw new Error("Some benchmark task IDs are missing from the database");
  const baseline = new Set(rows.filter(row => row.status === "SUCCESS").map(row => String(row.id)));
  if (baseline.size === ids.length) throw new Error("All tasks already succeeded; start polling earlier or submit a larger run");
  const observationStarted = performance.now();
  while (true) {
    const successful = rows.filter(row => row.status === "SUCCESS");
    const dead = rows.filter(row => row.status === "DEAD").length;
    const remaining = rows.length - successful.length - dead;
    if (remaining === 0) {
      const seconds = (performance.now() - observationStarted) / 1000;
      const completed = successful.filter(row => !baseline.has(String(row.id))).length;
      return { runId: input.runId, valid: dead === 0,
        scope: "New SUCCESS transitions observed after baseline query for this run's task IDs; not handler-only service time",
        totalTasks: ids.length, baselineSuccesses: baseline.size, newSuccesses: completed,
        dead, observationSeconds: seconds, completionsPerSecond: completed / seconds };
    }
    if (performance.now() - started >= timeoutMs) throw new Error(`Timed out with ${remaining} unfinished tasks, ${dead} DEAD`);
    await new Promise(resolve => setTimeout(resolve, pollMs));
    rows = (await pool.query(query, [ids])).rows;
    if (rows.length !== ids.length) throw new Error("Benchmark task IDs disappeared during observation");
  }
}

module.exports = { observeProcessing };
