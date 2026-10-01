#!/usr/bin/env bash
# 本地端到端演示：从外部事件进入 -> 校验 -> 分发 -> 成功/重试/死信 -> 查询 -> 重放。
# 依赖：bash + curl + python3（仅用于按 HMAC-SHA256 生成签名）。
set -uo pipefail

BASE="${BASE:-http://localhost:8080}"
SECRET="${SECRET:-local-dev-secret}"
TS_TOLERANCE="${TS_TOLERANCE:-300}"

# sign <timestamp> <body> -> hex HMAC-SHA256(secret, timestamp + "." + body)
sign() {
  local ts="$1" body="$2"
  SECRET="$SECRET" TS="$ts" BODY="$body" python3 - <<'PY'
import hashlib, hmac, os
msg = (os.environ["TS"] + "." + os.environ["BODY"]).encode()
print(hmac.new(os.environ["SECRET"].encode(), msg, hashlib.sha256).hexdigest())
PY
}

# send <eventId> <eventType> [extraJson] [timestamp] 
send() {
  local id="$1" type="$2" extra="${3:-}" ts="${4:-$(date +%s)}"
  local body
  if [ -n "$extra" ]; then
    body="{\"eventId\":\"$id\",\"eventType\":\"$type\",$extra}"
  else
    body="{\"eventId\":\"$id\",\"eventType\":\"$type\"}"
  fi
  local sig; sig="$(sign "$ts" "$body")"
  echo ">>> POST /webhooks/events eventId=$id type=$type"
  curl -s -o /tmp/wh_body -w "HTTP %{http_code}\n" -X POST "$BASE/webhooks/events" \
    -H 'Content-Type: application/json' \
    -H "X-Webhook-Signature: $sig" -H "X-Webhook-Timestamp: $ts" \
    -d "$body"
  echo "    $(cat /tmp/wh_body)"; echo
}

echo "================ 1) 正常事件：order.created =================="
send "demo-1" "order.created"

echo "================ 2) 重复事件（返回成功但不再分发） ================"
send "demo-1" "order.created"

echo "================ 3) 签名不合法（INVALID_SIGNATURE 401） =========="
ts=$(date +%s)
curl -s -o /tmp/wh_body -w "HTTP %{http_code}\n" -X POST "$BASE/webhooks/events" \
  -H 'Content-Type: application/json' \
  -H "X-Webhook-Signature: wrong-signature" -H "X-Webhook-Timestamp: $ts" \
  -d '{"eventId":"demo-bad","eventType":"order.created"}'
echo "    $(cat /tmp/wh_body)"; echo

echo "================ 4) 时间戳过期（TIMESTAMP_EXPIRED 401） =========="
old=$(( $(date +%s) - TS_TOLERANCE - 120 ))
send "demo-old" "order.created" "" "$old"

echo "================ 5) 不可路由（UNROUTABLE_EVENT 422，进入死信） ===="
send "demo-unknown" "no.such.type"

echo "================ 6) 暂时失败 -> 重试耗尽 -> 死信（payment.failed） =="
send "demo-pay" "payment.failed"
echo "    （等待退避重试耗尽：约 1s + 2s = 3 秒后进入死信）"
sleep 4

echo "================ 7) 永久失败立即进死信（invoice.rejected） ========"
send "demo-inv" "invoice.rejected"

echo "================ 8) 查询死信列表 ================================"
curl -s "$BASE/webhooks/dead-letters"; echo; echo

echo "================ 9) 死信重放（demo-unknown 仍不可路由 -> 失败） ===="
curl -s -o /tmp/wh_body -w "HTTP %{http_code}\n" -X POST "$BASE/webhooks/dead-letters/demo-unknown/replay"
echo "    $(cat /tmp/wh_body)"; echo

echo "================ 10) 携带新增可选字段重放 demo-inv，重放成功 ======"
body='{"eventId":"demo-inv","eventType":"invoice.rejected","replayShouldSucceed":true}'
ts=$(date +%s); sig="$(sign "$ts" "$body")"
curl -s -o /tmp/wh_body -w "HTTP %{http_code}\n" -X POST "$BASE/webhooks/dead-letters/demo-inv/replay" \
  -H 'Content-Type: application/json' -H "X-Webhook-Signature: $sig" -H "X-Webhook-Timestamp: $ts" \
  -d "$body"
echo "    $(cat /tmp/wh_body)"; echo

echo "================ 11) 再次重放 demo-inv（幂等跳过，409） =========="
curl -s -o /tmp/wh_body -w "HTTP %{http_code}\n" -X POST "$BASE/webhooks/dead-letters/demo-inv/replay"
echo "    $(cat /tmp/wh_body)"; echo

echo "完成。可用：curl -s $BASE/webhooks/dead-letters 查看剩余死信。"
