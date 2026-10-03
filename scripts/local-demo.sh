#!/usr/bin/env bash
# 本地端到端演示：签名校验 -> 接收/去重 -> 退避重试 -> 死信 -> 查询
#   -> 重放(原始负载仍失败) -> 修正负载重放成功 -> 事件详情(原始负载保留+重放留档)
#   -> 判定轨迹 -> 并发/幂等 -> 无类型轨迹
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

replay() { # $1=eventId  [$2=修正负载json(省略=沿用原始负载)]
  local eid="$1"
  if [ $# -ge 2 ]; then
    curl -sS -X POST "$BASE/admin/dead-letters/$eid/replay" \
      -H "Content-Type: application/json" -d "$2"
  else
    curl -sS -X POST "$BASE/admin/dead-letters/$eid/replay"
  fi
  echo
}

echo "== 1) 正常事件 order.created =="
post_event '{"eventId":"evt-1001","eventType":"order.created","orderId":"O-1001","amountCents":19900}'

echo "== 2) 重复投递同一事件（期望 DUPLICATE，不再分发）=="
post_event '{"eventId":"evt-1001","eventType":"order.created","orderId":"O-1001","amountCents":19900}'

echo "== 3) 无处理器类型（期望 422 UNSUPPORTED_EVENT_TYPE，不静默丢弃，但仍有轨迹）=="
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

echo "== 6) 暂时失败：退避重试3次后进入死信（上游字段写错演示：fail=retryable）=="
post_event '{"eventId":"evt-2001","eventType":"order.flaky","fail":"retryable","orderId":"O-WRONG","amountCents":-1}'

echo "== 7) 永久失败：不重试，直接死信 =="
post_event '{"eventId":"evt-2002","eventType":"order.flaky","fail":"permanent"}'

echo "== 8) 查询死信列表 =="
curl -sS "$BASE/admin/dead-letters"; echo

echo "== 9) 不带负载重放 evt-2001（下游仍故障 -> 仍 DEAD，返回 replayId/payloadSource=ORIGINAL）=="
replay evt-2001

echo "== 10) 带人工修正负载重放 evt-2001（fail 改为 ok，并修正业务字段 -> SUCCEEDED）=="
replay evt-2001 '{"eventId":"evt-2001","eventType":"order.flaky","fail":"ok","orderId":"O-FIXED","amountCents":19900,"fixMeta":{"by":"ops","refs":["t-1","t-2"]}}'

echo "== 11) 事件详情：原始负载保留(O-WRONG/retryable) + 两次重放留档(一次ORIGINAL失败/一次CORRECTED成功) =="
curl -sS "$BASE/admin/dead-letters/evt-2001"; echo

echo "== 12) 判定轨迹：接收->尝试/退避->死信->重放被拒/请求->重放成功，每步带 eventId 与判定依据 =="
curl -sS "$BASE/admin/dead-letters/evt-2001/trace"; echo

echo "== 13) 无处理器被拒事件也有轨迹（evt-1002：RECEIVED -> REJECTED/UNSUPPORTED_EVENT_TYPE）=="
curl -sS "$BASE/admin/dead-letters/evt-1002/trace"; echo

echo "== 14) 对已成功事件重放（期望 409，非 DEAD 不允许）=="
curl -sS -o /dev/null -w "http=%{http_code}\n" -X POST "$BASE/admin/dead-letters/evt-2001/replay"

echo "== 15) 非法修正负载（不是 JSON 对象，期望 400 BAD_REQUEST，事件状态不变）=="
curl -sS -X POST "$BASE/admin/dead-letters/evt-2002/replay" -H "Content-Type: application/json" -d 'not-json'; echo

echo "== 16) 并发重放同一条重试型死信 evt-2003：只有一个真正执行，其余 409 =="
post_event '{"eventId":"evt-2003","eventType":"order.flaky","fail":"retryable"}' >/dev/null
for i in $(seq 1 8); do
  ( curl -sS -o /dev/null -w "%{http_code} " -X POST "$BASE/admin/dead-letters/evt-2003/replay" ) &
done
wait; echo

echo "== 17) 查询未知事件轨迹（期望 404）=="
curl -sS -o /dev/null -w "http=%{http_code}\n" "$BASE/admin/dead-letters/no-such-event/trace"
