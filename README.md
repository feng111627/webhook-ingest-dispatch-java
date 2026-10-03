# Webhook 接收与分发链路（Java / Spring Boot）

本地即可跑通 **外部事件进入 → 签名/时间戳校验 → 幂等接收 → 按类型分发 → 退避重试 → 成功 / 死信 → 死信查询、带修正负载重放 → 全链路判定轨迹查询** 的完整过程。
链路上每一个判定结论都有**唯一的错误码/状态码、响应说明以及带事件标识（eventId）的日志**，各处一致、可解释。

> 存储为进程内内存实现（`ConcurrentHashMap`），零外部依赖，重启清空，适合本地演示与测试。

---

## 1. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/webhooks/events` | 接收事件（需签名头） |
| GET | `/admin/dead-letters` | 查询死信列表 |
| POST | `/admin/dead-letters/{eventId}/replay` | 重放（**请求体可带人工修正负载**） |
| GET | `/admin/dead-letters/{eventId}` | 事件详情：原始负载 + 状态 + 历次重放留档 |
| GET | `/admin/dead-letters/{eventId}/trace` | 该事件的完整判定轨迹 |

### 1.1 事件格式

请求体为 JSON 对象，必填 `eventId`、`eventType`，其余为业务负载：

```json
{
  "eventId": "evt-1001",
  "eventType": "order.created",
  "orderId": "O-1001",
  "amountCents": 19900
}
```

必须携带的请求头：

| 头 | 含义 |
| --- | --- |
| `X-Webhook-Timestamp` | Unix 秒 |
| `X-Webhook-Signature` | `hex( HMAC_SHA256(secret, timestamp + "." + rawBody) )`（小写 hex） |

内置演示处理器：

- `order.created`：正常处理器，只读取 `orderId`/`amountCents`，成功时登记一次幂等副作用（模拟"创建发货单"）。
- `order.flaky`：故障演示处理器，由负载字段 `fail` 控制：
  - `"retryable"`：始终暂时失败 → 退避重试到上限 → 死信（修复后重放可成功）；
  - `"permanent"`：永久失败 → **不重试**，首次即进入死信；
  - `"retry-then-ok"`：前 `failUntil` 次（默认 2）暂时失败，之后成功；
  - 其它/缺省（如 `"ok"`）：直接成功。

---

## 2. 校验规则（判定顺序全局唯一）

服务端严格按以下顺序判定，保证同一请求在任何环节得到的结论一致：

| 顺序 | 判定 | HTTP | 错误码 |
| --- | --- | --- | --- |
| 1 | 请求体为空 / 不是合法 JSON / 不是 JSON 对象 | 400 | `BAD_REQUEST` |
| 2 | `X-Webhook-Timestamp` 缺失或不是 Unix 秒 | 401 | `TIMESTAMP_MISSING` |
| 3 | 时间戳与服务器时间偏差超过容忍窗口（过去或未来，默认 300s） | 401 | `TIMESTAMP_EXPIRED` |
| 4 | `X-Webhook-Signature` 缺失 | 401 | `SIGNATURE_MISSING` |
| 5 | 签名不匹配（密钥错误/负载被篡改） | 401 | `SIGNATURE_INVALID` |
| 6 | 缺少 `eventId` / `eventType` | 400 | `BAD_REQUEST` |
| 7 | 没有匹配的事件处理器 | 422 | `UNSUPPORTED_EVENT_TYPE` |
| 8 | 重复事件（含正在处理中的并发重复） | 200 | `status=DUPLICATE` |
| 9 | 首次事件处理成功 | 200 | `status=SUCCEEDED` |
| 10 | 首次事件重试耗尽 / 永久失败 | 200 | `status=DEAD` |

要点：

- **签名不合法与时间戳过期严格区分**：时间戳先校验，时间戳问题不会被误报成签名问题。
- **无处理器的事件不会被静默丢弃**：直接 422 拒绝，且不写入事件存储；但**仍会写一条判定轨迹**（RECEIVED → REJECTED），事后可按 eventId 查到被拒依据。
- **重复请求按成功返回**（200 + `DUPLICATE`），调用方无需重试；服务端不会再次分发。
- 去重由 `InMemoryEventStore.claimIfAbsent` 的 `ConcurrentHashMap.compute` 原子占位保证——
  无论串行重复还是并发重复，只有第一个请求进入分发，其余全部走 DUPLICATE。

错误响应统一结构：

```json
{ "code": "TIMESTAMP_EXPIRED", "message": "时间戳超出容忍窗口，请检查时钟或重放窗口" }
```

---

## 3. 重试与死信规则

处理器每次返回三种结论之一：

| 处理器结论 | 含义 | 链路动作 |
| --- | --- | --- |
| `SUCCESS` | 处理成功 | 事件置为 `SUCCEEDED` |
| `RETRYABLE` | 暂时失败（如下游超时） | 按指数退避重试 |
| `PERMANENT` | 永久失败（如业务规则拒绝） | **立即进入死信，不再重试** |

- 最大尝试次数（含首次）：`webhook.max-attempts`，默认 **3**。
- 退避：第 n 次失败后等待 `webhook.backoff-base-millis * 2^(n-1)`，默认 **50ms → 100ms → 200ms…**。
- 重试上限耗尽后：事件置为 `DEAD`，`failureKind=RETRYABLE`，记录最后错误原因与尝试次数。
- 处理器抛出未预期异常时保守视为 `RETRYABLE`。
- 死信记录包含：`eventId / eventType / failureKind / attempts / lastError / receivedAt / updatedAt`。

### 3.1 死信重放（支持人工修正负载）

- `GET /admin/dead-letters`：只列出 `DEAD` 事件。
- `POST /admin/dead-letters/{eventId}/replay`：复用同一条分发链路（含退避重试）。
  - **请求体为空**：沿用事件接收时的**原始负载**（`payloadSource=ORIGINAL`）。
  - **请求体为 JSON 对象**：作为本次重放的**人工修正负载**（`payloadSource=CORRECTED`），
    本次分发实际处理的就是这份内容（例如把写错的订单号、金额改成正确值）。
  - 事件不存在 → 404；事件不是 DEAD 状态（成功/处理中）→ 409；修正负载不合法 → 400（见 3.2）。
  - 重放成功：从死信列表消失；重放仍失败：继续保留在死信，
    响应与详情里都能看到 **失败性质（failureKind）、本次尝试次数（attempts）、最后一次失败原因（lastError）**。

**修正负载的核心语义：只作用于"这一次"重放，原始负载永不被覆盖或丢失。**

- 事件存储中的 `payload/rawBody` 始终是**接收入库时的原始内容**，任何重放都不回写它。
- 每次重放单独留档（见 3.3），其中 `CORRECTED` 重放会完整保存本次使用的修正负载。
- 因此事后可以精确核对：**这次重放到底基于哪份负载成功/失败、相对原始内容改了什么**。
- 修正负载依旧走原来的分发与退避重试链路，不是旁路逻辑。

响应示例：

```json
{
  "eventId": "evt-2001", "status": "SUCCEEDED",
  "reason": "重放成功(replayId=rp-...)，本次使用人工修正负载；原始负载保留未改；副作用受幂等台账保护，无重复副作用",
  "attempts": 1, "effects": 0,
  "replayId": "rp-5e4dbaa8-...", "payloadSource": "CORRECTED"
}
```

### 3.2 修正负载校验规则

在抢占死信之前先校验，非法请求直接 **400 `BAD_REQUEST`**，不改变事件状态、不产生重放留档：

1. 请求体非空时必须是**合法 JSON 对象**（数组、字符串、数字等一律拒绝）；
2. 若给出 `eventId`，必须与路径上的 `{eventId}` 一致——**禁止借重放串改事件身份**；
3. 若给出 `eventType`，必须与原事件类型一致（大小写不敏感）——**禁止借重放把事件路由到别的处理器**；
4. `eventId`/`eventType` 缺省时由服务端按原事件自动补齐；
5. 其它业务字段（含嵌套对象、数组）允许自由修正，处理器只读取自己关心的字段。

### 3.3 重放并发与幂等语义

- 重放在**记录锁内**把状态 `DEAD → PROCESSING` 做**原子抢占**。
  同一条死信被多个人/多个脚本（带或不带修正负载）**同时**触发时：
  - **恰好一个**请求抢占成功并真正执行分发；
  - 其余全部得到 **409**，且结论一致（"该事件已有重放在执行 / 当前状态非 DEAD"），
    **不会**出现同一事件分发两次或副作用执行两遍。
- 抢占失败者即使携带了不同的修正负载，也**不会**触发第二次分发，也不产生重放留档。
- 重放**仍失败**时，事件回到 `DEAD`（而不是卡在 PROCESSING），之后仍可再次重放；
  每次重放（无论成功失败）都有独立 `replayId` 与留档。
- **副作用不重复**：真实副作用以稳定 key（`处理器前缀:eventId`）登记在 `EffectLedger`，
  已完成的副作用在重试/重放/重复投递命中台账时直接跳过。
- 成功后再对该事件重放 → 409（只有 DEAD 允许重放）。

### 3.4 事件详情与重放留档查询

`GET /admin/dead-letters/{eventId}`（事件不存在 → 404）返回：

- 原始接收负载 `originalPayload`（**永不被任何重放覆盖**）；
- 当前 `state / failureKind / attempts / lastError`；
- `replays`：历次重放数组，每条含
  `replayId / seq / payloadSource(ORIGINAL|CORRECTED) / correctedPayload /
   submittedAt / finishedAt / status(SUCCEEDED|DEAD|PROCESSING) / failureKind / attempts / lastError`。

对照 `originalPayload` 与某次重放的 `correctedPayload`，即可核对"改了什么、这次基于什么成功"。

### 3.5 判定轨迹（按 eventId 串起全过程）

`GET /admin/dead-letters/{eventId}/trace` 返回该事件严格按 `seq` 升序的每一步轨迹。
每条轨迹都带 **eventId、阶段 phase、判定依据 decision、当时接口结论 apiConclusion**，
以及可选的尝试序号、失败性质、负载来源、replayId。事件与轨迹都不存在时返回 404。

覆盖的阶段：

| phase | 含义 | 典型 apiConclusion |
| --- | --- | --- |
| `RECEIVED` | JSON 解析通过、进入判定 | `null`（尚未结论） |
| `REJECTED` | 校验/无处理器被拒（无类型被拒也记录） | `TIMESTAMP_EXPIRED` / `SIGNATURE_INVALID` / `UNSUPPORTED_EVENT_TYPE` / `BAD_REQUEST` |
| `DUPLICATE` | 命中幂等占位，不再分发 | `DUPLICATE` |
| `ATTEMPT` | 第 n/m 次尝试及处理器结论 | 重放中为 `REPLAY_RUNNING` |
| `BACKOFF` | 暂时失败后的指数退避 | 重放中为 `REPLAY_RUNNING` |
| `DEAD` | 首次分发失败进入死信 | `DEAD` |
| `SUCCEEDED` | 首次分发成功 | `SUCCEEDED` |
| `REPLAY_REQUESTED` | 重放抢占成功，开始执行 | `REPLAY_RUNNING` |
| `REPLAY_REJECTED` | 重放被拒（404/409/400） | `HTTP_404` / `HTTP_409` / `BAD_REQUEST` |
| `REPLAY_SUCCEEDED` / `REPLAY_FAILED` | 某次重放成功 / 仍失败 | `REPLAY_SUCCEEDED` / `REPLAY_FAILED` |

轨迹与接口当时返回的结论一一对应（`apiConclusion` 即接口状态/错误码），方便对账。

### 3.6 前向兼容：负载新增字段不破坏轨迹与处理

- 处理器只读取自己关心的已知字段；负载新增字段（**含嵌套对象、数组、任意层级**）一律忽略，
  不会破坏既有处理逻辑（见 `extraFutureFields_doNotBreakExistingHandler`）。
- 判定轨迹是独立于负载的旁路记录，不解析也不依赖业务字段，新增字段不会影响轨迹生成
  （见 `deadThenCorrectedReplay_fullTrace...andFutureFieldsSafe`）。
- 修正负载同样允许携带未知嵌套/数组字段，仅透传给本次处理器读取已知字段。

---

## 4. 配置项（`application.properties`）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `server.port` | 8080 | 本地端口（测试用随机端口覆盖） |
| `webhook.secret` | `test-secret` | HMAC 密钥 |
| `webhook.timestamp-tolerance-seconds` | 300 | 时间戳容忍窗口（秒，过去/未来对称） |
| `webhook.max-attempts` | 3 | 最大尝试次数（含首次） |
| `webhook.backoff-base-millis` | 50 | 退避基数（毫秒） |

---

## 5. 本地验证方法

### 5.1 运行测试

```bash
mvn test
```

共 **30** 个用例（正常与异常路径均有断言；日志均带 eventId 与每步判定依据）：

- `SignatureAndTimestampIntegrationTest`（10）：合法请求、时间戳缺失/非法/过期/超前/窗口边界、
  签名缺失/错误密钥/负载篡改、坏 JSON/缺字段；
- `DeduplicationIntegrationTest`（4）：串行重复、**16 线程并发重复**（恰好 1 次 SUCCEEDED、
  其余 DUPLICATE、副作用恰好 1 次）、无处理器类型 422、不同 eventId 互不影响；
- `RetryDeadLetterReplayIntegrationTest`（6）：暂时失败重试到上限入死信、永久失败只尝试 1 次、
  死信查询、404/409、重放成功且副作用不重复、先失败后成功、负载新增字段前向兼容、死信后重复投递；
- `CorrectedPayloadReplayIntegrationTest`（2，新增）：
  修正负载重放按修正内容成功且**原始负载保留**、详情可见每次实际负载；
  非法修正（非 JSON / eventType 串改 / eventId 不一致）→ 400 且死信不变，之后仍可修正成功；
- `ConcurrentReplayIntegrationTest`（3，新增）：
  **16 线程全修正负载并发**（恰好 1 个执行、其余 409、副作用 1 次、留档 1 条）、
  **带/不带修正负载混合并发**（恰好 1 个执行、其余一致 409，按赢家分支断言）、
  重放仍失败留死信且原因可解释、之后修正重放成功、成功后再重放 409；
- `EventTraceIntegrationTest`（4，新增）：
  成功事件轨迹有序、无类型被拒轨迹与 422 对得上、重复轨迹与 DUPLICATE 对得上、
  死信→修正重放成功的完整轨迹（含 BACKOFF/DEAD/REPLAY_*）且未知事件 404、
  未知嵌套/数组字段不破坏轨迹与处理。

### 5.2 一键 curl 演示

```bash
# 终端 A
mvn spring-boot:run

# 终端 B
./scripts/local-demo.sh
```

脚本依次演示：正常事件 → 重复投递（DUPLICATE）→ 不支持类型（422）→ 签名错误（401）
→ 时间戳过期（401）→ 暂时失败重试入死信 → 永久失败直入死信 → 查询死信
→ **空体重放仍失败（ORIGINAL）** → **带人工修正负载重放成功（CORRECTED）**
→ **事件详情（原始负载保留 + 两次重放留档）** → **判定轨迹** → 无类型被拒轨迹
→ 成功后重放 409 → 非法修正负载 400 → **8 并发重放（1 个 200、其余 409）** → 未知轨迹 404。

手工发起一次修正负载重放：

```bash
curl -sS -X POST http://localhost:8080/admin/dead-letters/evt-2001/replay \
  -H "Content-Type: application/json" \
  -d '{"eventId":"evt-2001","eventType":"order.flaky","fail":"ok","orderId":"O-FIXED","amountCents":19900}'

curl -sS http://localhost:8080/admin/dead-letters/evt-2001        # 原始负载 + 重放留档
curl -sS http://localhost:8080/admin/dead-letters/evt-2001/trace  # 全链路判定轨迹
```

### 5.3 手工计算签名

```bash
TS=$(date +%s)
BODY='{"eventId":"evt-1","eventType":"order.created"}'
SIG=$(printf '%s' "$TS.$BODY" | openssl dgst -sha256 -hmac test-secret -hex | sed 's/^.*= //')
curl -sS -X POST http://localhost:8080/webhooks/events \
  -H "Content-Type: application/json" \
  -H "X-Webhook-Timestamp: $TS" -H "X-Webhook-Signature: $SIG" -d "$BODY"
```

观察服务端日志：每条日志都带 `eventId` 与判定依据，例如
`[dispatch eventId=evt-2001] 第1次尝试 RETRYABLE -> 50ms 后退避重试`，
轨迹存储日志形如 `[trace eventId=evt-2001] #7/DEAD 重试3次仍失败，进入死信：failureKind=RETRYABLE...`。

---

## 6. 代码结构

```
config/      WebhookProperties            可调参数
security/    SignatureVerifier            时间戳 + HMAC 签名校验（规则唯一实现）
model/       EventRecord/EventState/...   领域模型与状态机
             ReplayAttempt                单次重放留档（负载来源/修正负载/结论）
             TraceEntry/TracePhase        判定轨迹条目与阶段
handler/     EventHandler + Registry      按类型路由；impl 下两个演示处理器
store/       InMemoryEventStore           原子占位去重；记录状态（更新在记录锁内）
             EffectLedger                 副作用幂等台账
             InMemoryTraceStore           按事件有序追加/查询判定轨迹（事件级锁，seq 递增）
service/     WebhookIngestService         接收编排（判定顺序见第 2 节）
             DispatchService              退避重试 / 永久失败 / 死信；支持本次负载覆盖(不回写)
             DeadLetterService            死信查询、修正负载校验、原子抢占重放与留档
             EventQueryService            判定轨迹查询
web/         Controller + 全局异常处理     结构化响应
```
