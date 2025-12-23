const axios = require("axios");

async function run() {
  const N = 500;
  const times = [];

  for (let i = 0; i < N; i++) {
    const start = Date.now();
    await axios.post("http://localhost:3000/tasks", {
      type: "send_email",
      payload: {},
      idempotency_key: `bench-${i}`
    });
    times.push(Date.now() - start);
  }

  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  console.log("Mean API latency:", avg.toFixed(2), "ms");
}

run();
