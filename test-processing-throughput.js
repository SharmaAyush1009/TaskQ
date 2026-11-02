require("dotenv").config();
const fs = require("node:fs");
const { Pool } = require("pg");
const { observeProcessing } = require("./benchmarks/processing-metrics");

async function run() {
  const source = process.argv[2];
  if (!source) throw new Error("Usage: node test-processing-throughput.js <enqueue-report.json>");
  const input = JSON.parse(fs.readFileSync(source, "utf8"));
  const pool = new Pool({ host: process.env.DB_HOST, port: process.env.DB_PORT,
    user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_NAME,
    connectionTimeoutMillis: 5000, query_timeout: 5000 });
  try {
    const result = await observeProcessing(pool, input, {timeoutMs: process.env.PROCESSING_TIMEOUT_MS});
    const destination = source.replace(/\.json$/i, "") + ".processing.json";
    fs.writeFileSync(destination, `${JSON.stringify(result, null, 2)}\n`);
    console.log(JSON.stringify(result, null, 2));
    console.log(`Report: ${destination}`);
    if (!result.valid) process.exitCode = 1;
  } finally {
    await pool.end();
  }
}
run().catch(error => { console.error(error.message); process.exitCode = 1; });
