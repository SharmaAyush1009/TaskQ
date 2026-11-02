// Optional benchmark-only preload: node --require ./benchmarks/profile-api.js api/server.js
const { performance, monitorEventLoopDelay } = require("node:perf_hooks");
const pool = require("../db/db");
const { commandClient } = require("../queue/redis");
const { latencySummary } = require("./api-metrics");
const delay = monitorEventLoopDelay({ resolution: 10 });
delay.enable();
let samples;
let cpu;
let utilization;
let maxPoolWaiting;
let started;
function reset() {
  samples = { databaseQueryMs: [], databaseAcquireMs: [], redisPushMs: [], redisSetMs: [] };
  cpu = process.cpuUsage();
  utilization = performance.eventLoopUtilization();
  maxPoolWaiting = 0;
  started = performance.now();
  delay.reset();
}
reset();
const connect = pool.connect;
pool.connect = function(callback) {
  if (typeof callback !== "function") return connect.call(this);
  const start = performance.now();
  const result = connect.call(this, (...args) => {
    samples.databaseAcquireMs.push(performance.now() - start);
    callback(...args);
  });
  maxPoolWaiting = Math.max(maxPoolWaiting, pool.waitingCount);
  return result;
};
function instrument(object, name, key) {
  const original = object[name];
  object[name] = async function(...args) {
    const start = performance.now();
    try { return await original.apply(this, args); }
    finally { samples[key].push(performance.now() - start); }
  };
}
instrument(pool, "query", "databaseQueryMs");
instrument(commandClient, "lPush", "redisPushMs");
instrument(commandClient, "sAdd", "redisSetMs");
process.on("message", message => {
  if (message === "reset") {reset();process.send?.({reset:true});}
  if (message === "snapshot") {
    const used = process.cpuUsage(cpu);
    const elapsedMs = performance.now() - started;
    process.send?.({ elapsedMs, cpuMs: (used.user + used.system) / 1000,
      cpuSingleCorePercent: (used.user + used.system) / (elapsedMs * 10),
      eventLoopUtilization: performance.eventLoopUtilization(utilization).utilization,
      eventLoopDelayMaxMs: delay.max / 1e6, poolMax: pool.options.max, maxPoolWaiting,
      stages: Object.fromEntries(Object.entries(samples).map(([key, values]) => [key, latencySummary(values)])) });
  }
});
