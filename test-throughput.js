const Redis = require("redis");

async function test() {
  const redis = Redis.createClient();
  await redis.connect();

  const N = 1000;
  const start = Date.now();

  for (let i = 0; i < N; i++) {
    await redis.lPush("task_queue", i.toString());
  }

  const duration = (Date.now() - start) / 1000;
  console.log("Throughput:", (N / duration).toFixed(0), "tasks/sec");

  await redis.quit();
}

test();
