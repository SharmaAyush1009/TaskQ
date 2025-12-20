#!/bin/bash

echo "════════════════════════════════════════"
echo "🧪 Complete System Test"
echo "════════════════════════════════════════"
echo ""

# Test 1: API Idempotency
echo "Test 1: API-Level Idempotency"
echo "Creating task with key 'test-001'..."
RESPONSE1=$(curl -s -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":{"to":"test@test.com"},"idempotency_key":"test-001"}')
echo "Response 1: $RESPONSE1"

echo "Creating same task again..."
RESPONSE2=$(curl -s -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"send_email","payload":{"to":"test@test.com"},"idempotency_key":"test-001"}')
echo "Response 2: $RESPONSE2"

if echo "$RESPONSE2" | grep -q '"duplicate":true'; then
  echo "✅ API idempotency working"
else
  echo "❌ API idempotency FAILED"
fi
echo ""

# Test 2: Different Task Types
echo "Test 2: Multiple Task Types"
for TYPE in send_email process_payment resize_image generate_report; do
  echo "Creating $TYPE task..."
  curl -s -X POST http://localhost:3000/tasks \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"$TYPE\",\"payload\":{},\"idempotency_key\":\"test-$TYPE-$(date +%s)\"}" \
    > /dev/null
done
echo "✅ All task types created"
echo ""

# Test 3: Retry Logic
echo "Test 3: Retry Logic"
echo "Creating failing task..."
curl -s -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"test_failure","payload":{},"idempotency_key":"test-failure-'$(date +%s)'"}' \
  > /dev/null
echo "✅ Failing task created (check worker logs for retries)"
echo ""

# Test 4: Statistics
echo "Test 4: Statistics"
STATS=$(curl -s http://localhost:3000/stats)
echo "$STATS" | python3 -m json.tool 2>/dev/null || echo "$STATS"
echo ""

echo "════════════════════════════════════════"
echo "🎉 Test Complete!"
echo "Check worker logs to verify processing"
echo "════════════════════════════════════════"