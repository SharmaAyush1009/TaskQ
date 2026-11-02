const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");
const { runApiMetrics, latencySummary } = require("./api-metrics");
const { observeProcessing } = require("./processing-metrics");
const { runIdempotency } = require("./idempotency-metrics");

async function withServer(handler, run) {
  const server = http.createServer((req, res) => {
    res.setHeader("Content-Type", "application/json");
    if (req.url === "/health") return res.end('{}');
    req.resume();
    req.on("end", () => handler(req, res));
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  try { await run(`http://127.0.0.1:${server.address().port}`); }
  finally { await new Promise(resolve => server.close(resolve)); }
}

test("duplicate responses never count as new enqueues", async () => {
  await withServer((req, res) => res.end(JSON.stringify({id:1,duplicate:true})), async baseUrl => {
    const result = await runApiMetrics({baseUrl, count:10, concurrency:3, warmup:0});
    assert.equal(result.newTasks, 0);
    assert.equal(result.duplicates, 10);
    assert.equal(result.newTasksPerSecond, 0);
    assert.equal(result.valid, false);
  });
});

test("all HTTP errors produce unavailable latency and an invalid run", async () => {
  await withServer((req, res) => {res.statusCode=500;res.end('{"error":"unavailable"}');}, async baseUrl => {
    const result = await runApiMetrics({baseUrl, count:5, concurrency:2, warmup:0});
    assert.equal(result.errors, 5);
    assert.equal(result.latencyMs, null);
    assert.equal(result.valid, false);
  });
});

test("unique creations count once and exclude warm-up requests", async () => {
  let id = 0;
  await withServer((req, res) => {res.statusCode=201;res.end(JSON.stringify({id:++id,duplicate:false}));}, async baseUrl => {
    const result = await runApiMetrics({baseUrl, count:10, concurrency:3, warmup:4});
    assert.equal(id, 14);
    assert.equal(result.newTasks, 10);
    assert.equal(result.latencyMs.samples, 10);
    assert.equal(result.valid, true);
  });
});

test("nearest-rank percentiles handle small samples and no successes", () => {
  assert.equal(latencySummary([]), null);
  const summary = latencySummary(Array.from({length:100}, (_, i) => i+1));
  assert.equal(summary.p95, 95);
  assert.equal(summary.p99, 99);
});

test("old completed tasks cannot become new processing throughput", async () => {
  const pool = {query:async () => ({rows:[{id:1,status:'SUCCESS'}]})};
  await assert.rejects(observeProcessing(pool,{valid:true,taskIds:[1]}), /already succeeded/);
});

test("processing excludes baseline successes and scopes every query to run IDs", async () => {
  let polls=0;
  const pool = {query:async (sql, params) => {
    assert.deepEqual(params, [['1','2']]);
    polls++;
    return {rows:[{id:1,status:'SUCCESS'},{id:2,status:polls===1?'PENDING':'SUCCESS'}]};
  }};
  const result = await observeProcessing(pool,{valid:true,taskIds:[1,2]}, {pollMs:1});
  assert.equal(result.baselineSuccesses,1);
  assert.equal(result.newSuccesses,1);
  assert.equal(result.valid,true);
});

test("processing fails on missing IDs and bounded waits", async () => {
  await assert.rejects(observeProcessing({query:async()=>({rows:[]})},{valid:true,taskIds:[1]}), /missing/);
  const pool = {query:async()=>({rows:[{id:1,status:'PENDING'}]})};
  await assert.rejects(observeProcessing(pool,{valid:true,taskIds:[1]},{timeoutMs:2,pollMs:2}), /Timed out/);
});

test("task status in response body cannot overwrite HTTP status in idempotency metrics", async () => {
  let requests=0;
  await withServer((req,res)=>{
    const duplicate=requests++ > 0;
    res.statusCode=duplicate?200:201;
    res.end(JSON.stringify({id:1,status:'PENDING',duplicate}));
  }, async baseUrl=>{
    const result=await runIdempotency(baseUrl);
    assert.equal(result.valid,true);
    assert.equal(result.created,1);
    assert.equal(result.duplicates,49);
  });
});

test("one successful ID with failed peer requests is not an idempotency pass", async () => {
  let requests=0;
  await withServer((req,res)=>{
    if(requests++===0){res.statusCode=201;res.end('{"id":1,"duplicate":false}');}
    else {res.statusCode=500;res.end('{"error":"database unavailable"}');}
  }, async baseUrl=>{
    const result=await runIdempotency(baseUrl);
    assert.equal(result.valid,false);
    assert.equal(result.errors,49);
  });
});
