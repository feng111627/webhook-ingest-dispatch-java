# Webhook 接收与分发链路

一个可在本地完整跑通的 Webhook 接收 → 校验 → 去重 → 按类型分发 → 失败重试 → 死信 → 查询/重放 的 Spring Boot 服务。
全链路使用一套统一的**判定码（DecisionCode）**，每个判定码固定绑定一个 HTTP 状态，
接收、分发、死信、重放各环节对同一结论的表达完全一致，且响应体和日志都带 `eventId` 与判定依据，可解释、可追溯。

## 一、接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/webhooks/events` | 接收事件（需签名/时间戳头） |
| GET  | `/webhooks/dead-letters` | 查询全部死信 |
| POST | `/webhooks/dead-letters/{eventId}/replay` | 重放死信，body 可空；可传更新后的 JSON 负载 |

事件请求体（JSON）：

```json
{
  "eventId": "evt-123",
  "eventType": "order.created",
  "occurredAt": "2026-10-02T10:00:00Z",
  "其它任意字段": "..."
}
```

- `eventId`、`eventType` 必填；负载以 `JsonNode` 承载，**后续新增字段不会破坏已有处理逻辑**（前向兼容）。
- `occurredAt` 可缺省，缺省取请求头时间戳。

必需请求头：

- `X-Webhook-Timestamp`：Unix 秒。
- `X-Webhook-Signature`：`hex( HMAC-SHA256(secret, timestamp + "." + rawBody) )`，小写十六进制，也兼容 `sha256=<hex>` 形式。

## 二、校验规则

判定顺序固定（两类失败明确区分，分别拒绝）：

1. 缺少签名/时间戳头，或时间戳非数字 → `MISSING_HEADERS`（**400**）
2. 签名重算不一致（含错误密钥） → `INVALID_SIGNATURE`（**401**）
3. 签名正确但时间偏差超过容忍窗口（过去或未来） → `TIMESTAMP_EXPIRED`（**401**）
4. 请求体不是合法 JSON 对象，或缺 `eventId`/`eventType` → `INVALID_PAYLOAD`（**400**）

配置项：

| 配置 | 默认值 | 含义 |
|------|--------|------|
| `webhook.security.secret` | `local-dev-secret` | HMAC 密钥，生产用环境变量覆盖 |
| `webhook.security.timestamp-tolerance-seconds` | `300` | 允许的绝对时间偏差（秒） |

## 三、去重与并发规则

- 以 `eventId` 为全局幂等键，通过 `ConcurrentHashMap.putIfAbsent` **原子占位**。
- 同一 `eventId`：
  - **串行重复**：按成功返回 `DUPLICATE_EVENT`（**200**），但**不再分发**。
  - **并发重复**：只有一个请求真正首次分发，其余请求等待首次尝试落定后返回 `DUPLICATE_EVENT`（**200**），不重复分发。
- 已进入死信的事件再次投递，同样不会重复分发（避免与重放相互覆盖），请走重放接口。

## 四、分发、重试与死信规则

按 `eventType` 在 `EventHandlerRegistry` 中查找处理逻辑：

- 命中：调用处理逻辑，处理逻辑返回三态结论：
  - **成功**：`ACCEPTED`（**200**）。
  - **暂时失败 TRANSIENT**（含处理逻辑抛出的异常）：按指数退避重试，首次返回 `RETRYING`（**202**）。
  - **永久失败 PERMANENT**：不重试，直接进入死信 `PERMANENT_FAILURE`（**422**）。
- 未命中任何处理逻辑：**不静默丢弃**，进入死信 `UNROUTABLE_EVENT`（**422**）。

退避重试：

- 尝试次数从 1 开始，**包含首次处理**；处理逻辑抛异常按暂时失败处理。
- 退避：`delay = min(maxBackoff, initialBackoff * multiplier^(failedAttempt-1))`。
- 达到最大尝试次数仍暂时失败：进入死信 `RETRIES_EXHAUSTED`（死信记录 attempts 即真实尝试次数）。

| 配置 | 默认值 | 测试值 |
|------|--------|--------|
| `webhook.retry.max-attempts` | `3` | `3` |
| `webhook.retry.initial-backoff-ms` | `1000` | `50` |
| `webhook.retry.backoff-multiplier` | `2.0` | `2.0` |
| `webhook.retry.max-backoff-ms` | `30000` | `100` |

### 幂等副作用

处理逻辑可继承 `AbstractIdempotentEventHandler`：**只有业务逻辑成功后**才记录“副作用已应用”，
因此失败会被真正重试；一旦成功，后续重试、重复投递、死信重放再进入该逻辑都会短路为成功，杜绝重复副作用。

## 五、死信查询与重放

- `GET /webhooks/dead-letters` 返回死信视图：`eventId/eventType/reason/attempts/deadAt/detail`。
- `POST /webhooks/dead-letters/{eventId}/replay`：
  - 复用与首次处理完全相同的分发/重试路径，保证判定一致；
  - 成功 → `REPLAY_SUCCEEDED`（**200**），并从死信中移除；
  - 暂时失败 → `RETRYING`（**202**），重新挂上退避重试链；
  - 永久失败/不可路由/重试耗尽 → `REPLAY_FAILED`（**422**），事件仍留在死信；
  - 事件不存在于死信且未成功过 → `NOT_FOUND`（**404**）；
  - 已成功或已有重放进行中 → `REPLAY_SKIPPED_DUPLICATE`（**409**），不产生重复副作用；
  - 并发重放通过 CAS 抢占 `replayInFlight`，只有一个请求真正执行；
  - 可在 body 传更新后的 JSON 对象（**eventType 不允许借重放改变**），用于上游修复后带新增字段重放。

## 六、判定码与 HTTP 状态总表

| DecisionCode | HTTP | 含义 |
|--------------|------|------|
| `ACCEPTED` | 200 | 首次接收并处理成功 |
| `DUPLICATE_EVENT` | 200 | 重复事件，成功返回但不再分发 |
| `RETRYING` | 202 | 暂时失败，已安排退避重试 |
| `REPLAY_SUCCEEDED` | 200 | 死信重放成功 |
| `DEAD_LETTERED` | 200 | 已进入死信（日志/查询用） |
| `MISSING_HEADERS` | 400 | 缺头或时间戳不可解析 |
| `INVALID_PAYLOAD` | 400 | 非法 JSON / 缺必填字段 |
| `INVALID_SIGNATURE` | 401 | 签名不合法 |
| `TIMESTAMP_EXPIRED` | 401 | 时间戳超出容忍窗口 |
| `UNROUTABLE_EVENT` | 422 | 无匹配处理逻辑，进入死信 |
| `PERMANENT_FAILURE` | 422 | 永久失败，不重试，进入死信 |
| `REPLAY_FAILED` | 422 | 重放失败 |
| `REPLAY_SKIPPED_DUPLICATE` | 409 | 重放重复/进行中，跳过 |
| `NOT_FOUND` | 404 | 死信不存在 |
| `RETRIES_EXHAUSTED` | 202 | 重试耗尽（进入死信的原因码） |

## 七、内置示例处理逻辑

| eventType | 行为 | 用途 |
|-----------|------|------|
| `order.created` | 幂等成功 | 正常链路 / 重复并发 |
| `payment.failed` | 始终暂时失败 | 演示退避重试耗尽进死信 |
| `invoice.rejected` | 默认永久失败；负载新增可选字段 `replayShouldSucceed=true` 时成功 | 永久失败 & 带新字段重放 |

## 八、本地验证

### 方式一：一键脚本（推荐）

```bash
mvn -DskipTests package
java -jar target/webhook-ingest-dispatch-java-1.0-SNAPSHOT.jar &   # 默认 8080
bash demo/local_demo.sh
```

脚本会依次演示：正常、重复、坏签名、过期时间戳、不可路由、暂时失败重试到死信、
永久失败、死信列表查询、重放失败、携带新增字段重放成功、重复重放幂等跳过。

服务端日志会对每一步打印 `eventId`、`判定=XXX` 与 `依据=...`。

### 方式二：跑测试

```bash
mvn test
```

覆盖场景（共 21 个用例）：

- `SignatureAndTimestampIntegrationTest`：签名正确/错误密钥/错误签名/过期/未来/边界/缺头/时间戳格式/非法 JSON/缺字段。
- `IdempotencyConcurrencyIntegrationTest`：串行重复、12 线程并发同一事件只处理一次。
- `RetryDeadLetterReplayIntegrationTest`：暂时失败重试成功、异常按暂时失败、重试耗尽进死信、
  永久失败立即进死信、死信查询、重放成功且不产生重复副作用、不可路由进死信、重放未知事件。
- `PayloadForwardCompatibilityTest`：新增未知字段不破坏旧逻辑；新增可选字段支持死信重放恢复。

### 方式三：手工 curl

```bash
TS=$(date +%s)
BODY='{"eventId":"e1","eventType":"order.created"}'
SIG=$(SECRET=local-dev-secret TS=$TS BODY=$BODY python3 - <<'PY'
import hashlib,hmac,os
print(hmac.new(os.environ["SECRET"].encode(),
   (os.environ["TS"]+"."+os.environ["BODY"]).encode(), hashlib.sha256).hexdigest())
PY
)
curl -i -X POST localhost:8080/webhooks/events \
  -H 'Content-Type: application/json' \
  -H "X-Webhook-Signature: $SIG" -H "X-Webhook-Timestamp: $TS" -d "$BODY"

curl -s localhost:8080/webhooks/dead-letters
curl -s -X POST localhost:8080/webhooks/dead-letters/<eventId>/replay
```

## 九、代码结构

```
config/      WebhookProperties（密钥、时间戳容忍、退避参数）
signature/   SignatureVerifier（HMAC-SHA256 签名 + 时间戳校验）
model/       WebhookEvent / DecisionCode / HandlerResult / 各响应视图 / 状态枚举
dedup/       EventRecord、ProcessedEventRepository（原子去重与并发合并）
handler/     EventHandler、AbstractIdempotentEventHandler、Registry、sample/*
dispatch/    EventRouter（按类型路由）、RetryScheduler（指数退避）、EventDispatcher（分发/重放核心）
dlq/         DeadLetterRecord、DeadLetterRepository
service/     WebhookIngestService（接收编排）、DeadLetterService（查询/重放）
web/         WebhookController、DeadLetterController
demo/        local_demo.sh 本地端到端脚本
```

> 说明：当前台账（去重表、死信表）为内存实现，适合本地与测试演示；
> 生产可替换为数据库/Redis 实现而不改动分发与判定逻辑。
