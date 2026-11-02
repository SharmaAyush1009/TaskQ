const axios = require("axios");
const { randomUUID } = require("node:crypto");

async function runIdempotency(baseUrl = "http://localhost:3000") {
  const key = `idem-${randomUUID()}`;
  const count = 50;
  const results = await Promise.all(Array.from({length:count}, async () => {
    try {
      const response = await axios.post(`${baseUrl}/tasks`, {
        type:"send_email", payload:{to:"test@example.com",subject:"Idempotency test"}, idempotency_key:key
      }, {timeout:5000});
      return {httpStatus:response.status, id:response.data.id, duplicate:response.data.duplicate};
    } catch (error) {
      return {error:error.response?.data?.error || error.message, code:error.code || null};
    }
  }));
  const hasId = result => result.id !== undefined && result.id !== null;
  const ids = new Set(results.filter(hasId).map(result => String(result.id)));
  const created = results.filter(result => result.httpStatus===201 && result.duplicate===false && hasId(result)).length;
  const duplicates = results.filter(result => result.httpStatus===200 && result.duplicate===true && hasId(result)).length;
  return { timestamp:new Date().toISOString(), idempotencyKey:key, requests:count,
    valid:created===1 && duplicates===count-1 && ids.size===1,
    created, duplicates, errors:count-created-duplicates, uniqueIds:[...ids],
    scope:"HTTP submission deduplication only; does not establish exactly-once handler effects",
    errorSamples:results.filter(result=>result.error).slice(0,5) };
}

module.exports = { runIdempotency };
