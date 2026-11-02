// Local evidence runner. Requires a disposable database and Redis instance.
// Configuration is inherited; this runner never provisions or clears stores.
require("dotenv").config();
const fs = require("node:fs");
const path = require("node:path");
const { spawn, execFileSync } = require("node:child_process");
const { Pool } = require("pg");
const { createClient } = require("redis");
const axios = require("axios");
const { runApiMetrics } = require("./api-metrics");
const { observeProcessing } = require("./processing-metrics");

const directory = process.env.BENCH_DIR || "data/benchmark-20261103";
const baselineRevision = process.env.BENCH_BASELINE_REF || "dc4e3e90f2b0f41a69215f889938a650c7abf43e";
fs.mkdirSync(directory, {recursive:true});
const baseUrl = `http://127.0.0.1:${process.env.PORT || 3000}`;
const children = new Set();
const pool = new Pool({host:process.env.DB_HOST,port:process.env.DB_PORT,user:process.env.DB_USER,
  password:process.env.DB_PASSWORD,database:process.env.DB_NAME,connectionTimeoutMillis:5000});
const redis = createClient({url:process.env.REDIS_URL,socket:{reconnectStrategy:false,connectTimeout:5000}});
redis.on("error", error => console.error(error.message));
const save = (name, data) => fs.writeFileSync(path.join(directory,name), JSON.stringify(data,null,2)+"\n");

function launch(file, name, env = process.env, profile = false) {
  const descriptor = fs.openSync(path.join(directory, `${name}.log`), "a");
  const args = profile ? ["--require", "./benchmarks/profile-api.js", file] : [file];
  const child = spawn(process.execPath, args, {env, windowsHide:true,
    stdio:["ignore",descriptor,descriptor,"ipc"]});
  fs.closeSync(descriptor);
  children.add(child);
  child.on("error", error => console.error(`${name}: ${error.message}`));
  child.on("exit", () => children.delete(child));
  return child;
}

async function stop(child) {
  if (child.exitCode !== null) return;
  await new Promise(resolve => {child.once("exit", resolve);child.kill();});
}

async function ready(child) {
  for (let attempt=0;attempt<100;attempt++) {
    if (child.exitCode !== null) throw new Error("API stopped; inspect its log");
    try {await axios.get(`${baseUrl}/health`,{timeout:500});return;}
    catch {await new Promise(resolve => setTimeout(resolve,100));}
  }
  throw new Error("API readiness timeout");
}

async function message(child, request) {
  return new Promise((resolve,reject) => {
    const timer=setTimeout(()=>{child.off("message",receive);reject(new Error("Profiler timeout"));},5000);
    function receive(result){clearTimeout(timer);resolve(result);}
    child.once("message",receive);
    child.send(request);
  });
}

async function audit(report) {
  if (!report.valid) throw new Error("Invalid enqueue run; inspect its errors and duplicates");
  const result = await pool.query("SELECT COUNT(*)::integer AS count FROM tasks WHERE id = ANY($1::bigint[])",[report.taskIds]);
  report.audit = {persistedTasks:result.rows[0].count, queueLength:await redis.lLen("task_queue")};
  if (report.audit.persistedTasks !== report.newTasks) throw new Error("Persisted task count disagrees with benchmark");
}

async function main() {
  // Refuse to send measurements to an unrelated process already using the port.
  let occupied=false;
  try {await axios.get(`${baseUrl}/health`,{timeout:500});occupied=true;}catch{}
  if(occupied) throw new Error(`An API is already listening at ${baseUrl}; choose another PORT`);
  await pool.query("SELECT 1");
  await redis.connect();
  const summary = {timestamp:new Date().toISOString(),baselineRevision,schema:"benchmarks/schema.sql + db/migrations/add_unique_idempotency.sql",
    databaseVersion:(await pool.query("SELECT version() AS version")).rows[0].version,
    databaseSettings:(await pool.query("SELECT name, setting FROM pg_settings WHERE name IN ('fsync','synchronous_commit','full_page_writes','shared_buffers')")).rows,
    redisServer:(await redis.info("server")).split("\r\n").filter(line=>/^(redis_version|os):/.test(line)),
    redisPersistence:await redis.configGet(["save","appendonly"]),
    workersDuringEnqueue:0,apiLogging:"unchanged; stdout written to file",sweeps:[]};
  let api=launch("api/server.js","api");
  await ready(api);

  // Preserve and execute the repository's original batch-based scripts.
  for(const file of ["benchmark-api.js","test-enqueue-throughput.js"]) {
    let source=execFileSync("git",["show",`${baselineRevision}:${file}`],{encoding:"utf8",windowsHide:true});
    source=source.replaceAll("http://localhost:3000",baseUrl);
    const filename=path.join(directory,`original-${file}`);
    fs.writeFileSync(filename,source);
    const output=execFileSync(process.execPath,[filename],{encoding:"utf8",windowsHide:true,timeout:120000});
    fs.writeFileSync(path.join(directory,`original-${file}.txt`),output);
    console.log(`Original ${file}:\n${output}`);
  }

  // Run the corrected public entry points as well as the longer sweep.
  for(const file of ["benchmark-api.js","test-enqueue-throughput.js"]) {
    const outputFile=path.join(directory,`corrected-${file}.json`);
    const output=execFileSync(process.execPath,[file],{encoding:"utf8",windowsHide:true,timeout:120000,
      env:{...process.env,BASE_URL:baseUrl,BENCH_OUTPUT:outputFile}});
    console.log(`Corrected ${file}:\n${output}`);
    const report=JSON.parse(fs.readFileSync(outputFile,"utf8"));
    await audit(report);
    fs.writeFileSync(outputFile,JSON.stringify(report,null,2)+"\n");
  }
  for (const concurrency of [10,50,100,200]) {
    for(let repeat=1;repeat<=2;repeat++) {
      const report=await runApiMetrics({baseUrl,count:5000,concurrency,warmup:100});
      await audit(report);
      const filename=`enqueue-c${concurrency}-r${repeat}.json`;
      save(filename,report);
      summary.sweeps.push({file:filename,concurrency,repeat,newTasksPerSecond:report.newTasksPerSecond,
        meanMs:report.latencyMs?.mean,p99Ms:report.latencyMs?.p99,valid:report.valid});
      save("summary.json",summary);
      console.log(`Concurrency ${concurrency}, repeat ${repeat}: ${report.newTasksPerSecond.toFixed(2)}/sec; p99 ${report.latencyMs?.p99.toFixed(2)}ms`);
    }
  }
  await stop(api);
  api=launch("api/server.js","api-profile",process.env,true);
  await ready(api);
  await message(api,"reset");
  const profiled=await runApiMetrics({baseUrl,count:5000,concurrency:100,warmup:100});
  profiled.profile=await message(api,"snapshot");
  save("profile.json",profiled);
  await stop(api);

  // An empty queue on a distinct Redis DB isolates the short worker run.
  const workerUrl=new URL(process.env.REDIS_URL);
  workerUrl.pathname="/1";
  const workerEnv={...process.env,REDIS_URL:workerUrl.toString()};
  const workerRedis=createClient({url:workerEnv.REDIS_URL,socket:{reconnectStrategy:false}});
  workerRedis.on("error",()=>{});
  await workerRedis.connect();
  try {
    if(await workerRedis.dbSize() !== 0) throw new Error("Processing fixture requires empty Redis DB 1");
    api=launch("api/server.js","api-processing",workerEnv);
    await ready(api);
    const cohort=await runApiMetrics({baseUrl,count:32,concurrency:8,warmup:0});
    save("processing-enqueue.json",cohort);
    const observation=observeProcessing(pool,cohort,{timeoutMs:60000});
    const workers=Array.from({length:4},(_,i)=>launch("worker/worker.js",`worker-${i+1}`,workerEnv));
    const result=await observation;
    const logs=await pool.query(`SELECT t.id, t.status, COUNT(e.task_id)::integer AS email_logs
      FROM tasks t LEFT JOIN email_logs e ON e.task_id=t.id
      WHERE t.id=ANY($1::bigint[]) GROUP BY t.id,t.status`,[cohort.taskIds]);
    result.workers=4;
    result.effectAudit={tasks:logs.rowCount,allSuccessful:logs.rows.every(row=>row.status==="SUCCESS"),
      exactlyOneEmailLogPerTask:logs.rows.every(row=>row.email_logs===1),
      scope:"Controlled simulated-email run; not proof of exactly-once external effects under crashes"};
    save("processing.json",result);
    if(!result.valid || !result.effectAudit.allSuccessful || !result.effectAudit.exactlyOneEmailLogPerTask) {
      throw new Error("Worker audit failed; inspect processing.json");
    }
    console.log(`Four-worker processing: ${result.completionsPerSecond.toFixed(2)}/sec; ${result.newSuccesses} completions`);
    for(const worker of workers) await stop(worker);
    await stop(api);
  } finally {await workerRedis.quit();}
  save("summary.json",summary);
}

main().catch(error=>{console.error(error);process.exitCode=1;}).finally(async()=>{
  for(const child of [...children]) await stop(child);
  if(redis.isOpen) await redis.quit();
  await pool.end();
});
