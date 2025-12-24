require("dotenv").config();
const { createClient } = require("redis");

// Connection for BLOCKING operations (BRPOP)
const blockingClient = createClient({
  url: process.env.REDIS_URL,
});

// Connection for COMMAND operations (LPUSH, etc.)
const commandClient = createClient({
  url: process.env.REDIS_URL,
});

blockingClient.on("error", (err) => console.error("Redis Blocking Client Error:", err));
commandClient.on("error", (err) => console.error("Redis Command Client Error:", err));

// Connect both clients
(async () => {
  await blockingClient.connect();
  console.log("Redis blocking client connected");
  
  await commandClient.connect();
  console.log("Redis command client connected");
})();

module.exports = {
  blockingClient,
  commandClient
};