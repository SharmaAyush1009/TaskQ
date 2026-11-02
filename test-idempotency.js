const fs = require("node:fs");
const path = require("node:path");
const { runIdempotency } = require("./benchmarks/idempotency-metrics");
async function run() {
  const report=await runIdempotency(process.env.BASE_URL);
  const destination=process.env.IDEMPOTENCY_OUTPUT || path.join("data","benchmarks",`${report.idempotencyKey}.json`);
  fs.mkdirSync(path.dirname(destination),{recursive:true});
  fs.writeFileSync(destination,JSON.stringify(report,null,2)+"\n");
  console.log(JSON.stringify(report,null,2));
  console.log(`Report: ${destination}`);
  if(!report.valid) process.exitCode=1;
}
run().catch(error=>{console.error(error.message);process.exitCode=1;});
