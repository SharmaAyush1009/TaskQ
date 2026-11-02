// Follow-up evidence run against the same disposable stores as run-local.js.
require("dotenv").config();
const fs = require("node:fs");
const { spawn, execFileSync } = require("node:child_process");
const axios = require("axios");
const { Pool } = require("pg");
const { runApiMetrics } = require("./api-metrics");

async function main() {
  const directory=process.env.BENCH_DIR || "data/benchmark-20261103";
  const baseUrl=`http://127.0.0.1:${process.env.PORT || 3000}`;
  fs.mkdirSync(directory,{recursive:true});
  let occupied=false;
  try {await axios.get(`${baseUrl}/health`,{timeout:500});occupied=true;}catch{}
  if(occupied) throw new Error(`An API is already listening at ${baseUrl}; choose another PORT`);
  const descriptor=fs.openSync(`${directory}/api-extended.log`,"a");
  const child=spawn(process.execPath,["api/server.js"],{env:process.env,windowsHide:true,
    stdio:["ignore",descriptor,descriptor]});
  fs.closeSync(descriptor);
  const pool=new Pool({host:process.env.DB_HOST,port:process.env.DB_PORT,user:process.env.DB_USER,
    password:process.env.DB_PASSWORD,database:process.env.DB_NAME,connectionTimeoutMillis:5000});
  try {
    let ready=false;
    for(let attempt=0;attempt<100;attempt++) {
      if(child.exitCode!==null) throw new Error("API exited; inspect log");
      try {await axios.get(`${baseUrl}/health`,{timeout:500});ready=true;break;}
      catch {await new Promise(resolve=>setTimeout(resolve,100));}
    }
    if(!ready) throw new Error("API readiness timeout");
    if(process.env.BENCH_SKIP_LONG !== "1") {
      const report=await runApiMetrics({baseUrl,count:60000,concurrency:50,warmup:100});
      const row=await pool.query("SELECT COUNT(*)::integer AS count FROM tasks WHERE id=ANY($1::bigint[])",[report.taskIds]);
      report.audit={persistedTasks:row.rows[0].count};
      fs.writeFileSync(`${directory}/enqueue-long.json`,JSON.stringify(report,null,2)+"\n");
      if(!report.valid || report.audit.persistedTasks!==report.newTasks) throw new Error("Long benchmark failed validation; inspect report");
      console.log(JSON.stringify({newTasks:report.newTasks,seconds:report.durationSeconds,
        throughput:report.newTasksPerSecond,latency:report.latencyMs,valid:report.valid},null,2));
    }
    console.log(execFileSync(process.execPath,["test-idempotency.js"],{encoding:"utf8",windowsHide:true,
      env:{...process.env,BASE_URL:baseUrl,IDEMPOTENCY_OUTPUT:`${directory}/idempotency.json`}}));
  } finally {
    if(child.exitCode===null) await new Promise(resolve=>{child.once("exit",resolve);child.kill();});
    await pool.end();
  }
}
main().catch(error=>{console.error(error);process.exitCode=1;});
