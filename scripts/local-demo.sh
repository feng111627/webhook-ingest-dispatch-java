#!/usr/bin/env bash
# 本地端到端演示：签名校验 -> 接收/去重 -> 退避重试 -> 死信 -> 查询 -> 重放
# 依赖：bash、curl、openssl、python3。启动：mvn spring-boot:run（默认 8080）
set -u
BASE="${BASE:-http://localhost:8080}"
SECRET="${SECRET:-test-secret}"

sign() { # $1=timestamp $2=body
  printf '%s' "$1.$2" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.*= //'
}

post_event() { # $1=json-body
  local body="$1" ts sig
  ts="$(date +%s)"
  sig="$(sign "$ts" "$body")"
  curl -sS -X POST "$BASE/webhooks/events" \
    -H "Content-Type: application/json" \
    -H "X-Webhook-Timestamp: $ts" \
    -H "X-Webhook-Signature: $sig" \
    -d "$body"
  echo
}

echo "== 1) 正常事件 order.created =="
post_event '{"eventId":"evt-1001","eventType":"order.created","orderId":"O-1001","amountCents":19900}'

echo "== 2) 重复投递同一事件（期望 DUPLICATE，不再分发）=="
post_event '{"eventId":"evt-1001","eventType":"order.created","orderId":"O-1001","amountCents":19900}'

echo "== 3) 无处理器类型（期望 422 UNSUPPORTED_EVENT_TYPE，不静默丢弃）=="
post_event '{"eventId":"evt-1002","eventType":"billing.unknown"}'

echo "== 4) 签名错误（期望 401 SIGNATURE_INVALID）=="
curl -sS -X POST "$BASE/webhooks/events" -H "X-Webhook-Timestamp: $(date +%s)" \
  -H "X-Webhook-Signature: deadbeef" -H "Content-Type: application/json" \
  -d '{"eventId":"evt-1003","eventType":"order.created"}'; echo

echo "== 5) 时间戳过期（期望 401 TIMESTAMP_EXPIRED）=="
old_ts=$(( $(date +%s) - 600 ))
b='{"eventId":"evt-1004","eventType":"order.created"}'
curl -sS -X POST "$BASE/webhooks/events" -H "X-Webhook-Timestamp: $old_ts" \
  -H "X-Webhook-Signature: $(sign "$old_ts" "$b")" -H "Content-Type: application/json" -d "$b"; echo

echo "== 6) 暂时失败：退避重试3次后进入死信 =="
post_event '{"eventId":"evt-2001","eventType":"order.flaky","fail":"retryable"}'

echo "== 7) 永久失败：不重试，直接死信 =="
post_event '{"eventId":"evt-2002","eventType":"order.flaky","fail":"permanent"}'

echo "== 8) 查询死信列表 =="
curl -sS "$BASE/admin/dead-letters"; echo

echo "== 9) 重放 evt-2001（下游仍故障 -> 仍 DEAD）=="
curl -sS -X POST "$BASE/admin/dead-letters/evt-2001/replay"; echo

echo "== 10) 对非死信事件重放（期望 409）=="
curl -sS -o /dev/null -w "http=%{http_code}\n" -X POST "$BASE/admin/dead-letters/evt-1001/replay"

echo "== 11) 修正负载重放：上游把 fail 写错成 permanent，人工修正后重放 =="
post_event '{"eventId":"evt-3001","eventType":"order.flaky","fail":"permanent","orderId":"O-WRONG","amountCents":-5}'
echo "-- 携带修正负载重放（fail=ok，修正订单号与金额）--"
curl -sS -X POST "$BASE/admin/dead-letters/evt-3001/replay" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"eventId":"evt-3001","eventType":"order.flaky","fail":"ok","orderId":"O-FIXED","amountCents":19900}}'; echo

echo "== 12) 事件详情：原始负载保留 + 本次重放实际使用的修正负载 =="
curl -sS "$BASE/admin/events/evt-3001"; echo

echo "== 13) 事件轨迹：从接收到重放成功的完整判定链 =="
curl -sS "$BASE/admin/events/evt-3001/trace"; echo

echo "== 14) 非法修正负载（eventId 不一致，期望 400）=="
curl -sS -o /dev/null -w "http=%{http_code}\n" -X POST "$BASE/admin/dead-letters/evt-2002/replay" \
  -H "Content-Type: application/json" \
  -d '{"payload":{"eventId":"someone-else","fail":"ok"}}'

echo "== 15) 并发重放同一死信（5 个请求同时打，期望恰好 1 个执行、其余一致的 409）=="
post_event '{"eventId":"evt-4001","eventType":"order.flaky","fail":"permanent"}'
for i in 1 2 3 4 5; do
  curl -sS -X POST "$BASE/admin/dead-letters/evt-4001/replay" \
    -H "Content-Type: application/json" \
    -d '{"payload":{"eventId":"evt-4001","fail":"ok","sleepMillis":1500}}' \
    > "/tmp/replay-$i.json" &
done
wait
for i in 1 2 3 4 5; do echo "  请求$i -> $(cat /tmp/replay-$i.json)"; done
echo "-- 重放历史（期望恰好一条）--"
curl -sS "$BASE/admin/events/evt-4001"; echo
