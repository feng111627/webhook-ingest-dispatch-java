# Webhook 接收与分发链路（Java / Spring Boot）

本地即可跑通 **外部事件进入 → 签名/时间戳校验 → 幂等接收 → 按类型分发 → 退避重试 → 成功 / 死信 → 死信查询与重放** 的完整过程。
链路上每一个判定结论都有**唯一的错误码/状态码、响应说明以及带事件标识（eventId）的日志**，各处一致、可解释。

> 存储为进程内内存实现（`ConcurrentHashMap`），零外部依赖，重启清空，适合本地演示与测试。

---

## 1. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/webhooks/events` | 接收事件（需签名头） |
| GET | `/admin/dead-letters` | 查询死信列表 |
| POST | `/admin/dead-letters/{eventId}/replay` | 重放指定死信（可携带修正负载，见 3.1） |
| GET | `/admin/events/{eventId}` | 事件详情：原始负载 + 全部重放历史 |
| GET | `/admin/events/{eventId}/trace` | 事件轨迹：每一步判定与依据（见 3.3） |

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
  - `"sleep"`：沉睡后成功（等价于成功 + `sleepMillis`）。
  - 任何模式都可额外携带 `sleepMillis`（毫秒）：先沉睡再判定结论，
    用于确定性地制造"处理中"窗口，验证并发重放互斥（含仍会失败的场景）。

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
- **无处理器的事件不会被静默丢弃**：直接 422 拒绝，且不写入存储。
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
  - **不带请求体**：沿用原始负载重放（`payloadSource=ORIGINAL`）。
  - **携带修正负载**：请求体 `{"payload": {...}}`，本次重放以人工修正后的内容走原分发与重试链路
    （`payloadSource=CORRECTED`）。规则：
    - 修正负载必须是 JSON 对象，否则 400；
    - 修正负载若带 `eventId`，必须与路径中的 eventId 一致，否则 400（防止改错事件）；
    - 分发路由仍按事件入库时的 `eventType`，修正负载里的 `eventType` 不改变路由；
    - **原始负载永不被覆盖**：每次重放实际使用的负载来源与修正原文都追加进重放历史，
      通过 `GET /admin/events/{eventId}` 可查 `originalPayload` 与每次重放的
      `payloadSource` / `correctedPayload` / 结论，便于事后核对"改了什么、基于什么成功"。
  - 事件不存在 → 404；事件不是 DEAD 状态（成功/处理中）→ 409。
  - 重放成功：从死信列表消失。
  - 重放仍失败：继续保留在死信，`failureKind/attempts/lastError` 更新，轨迹与详情均可解释；
    此时单独再发起一次重放不受之前并发请求留下的状态影响，可正常执行。
- **并发与幂等语义**：状态判定与 `DEAD → PROCESSING` 抢占在**记录锁内原子完成（唯一判定点）**，
  同一事件同一时刻**只有一个重放真正执行**，绝不会出现同一事件被分发两次：
  - 并发/重复点击（无论是否携带修正负载）打到同一死信时，恰好一个请求抢占成功并执行，
    其余请求在锁内读到 `PROCESSING`，一律得到**完全一致的** 409 结论
    （轨迹决策码 `REPLAY_IN_PROGRESS`）：
    ```json
    { "code": "HTTP_409", "message": "该事件已有重放在执行（当前状态 PROCESSING）：同一死信并发重放只会有一个真正执行，本次请求未被执行，请待其完成后查询结果" }
    ```
  - 重放已结束、事件处于非 DEAD 终态（如 SUCCEEDED）时再重放，得到 409
    （轨迹决策码 `REPLAY_NOT_DEAD`）：`事件当前状态为 SUCCEEDED，只有 DEAD 状态允许重放`。
  - 拒绝消息中的状态一律取自锁内快照，与事件当时的真实状态严格一致，
    不会出现"当前状态为 DEAD，只有 DEAD 状态允许重放"这类自相矛盾的措辞。
  - 重放失败回到 DEAD 后允许再次重放（顺序重放是合法的，并发重放才互斥）。
- **重放不产生重复副作用**：真实副作用以稳定 key（`处理器前缀:eventId`）登记在 `EffectLedger`，
  已完成的副作用在重试/重放/重复投递命中台账时直接跳过。

### 3.3 事件轨迹（按 eventId 串查判定过程）

`GET /admin/events/{eventId}/trace` 返回该事件从接收到当前的全部判定步骤，按时间升序：

| step | 含义 |
| --- | --- |
| `RECEIVED` | 首次接收并占位成功 |
| `DUPLICATE` | 命中重复（含在途并发），不再分发 |
| `REJECTED` | 被明确拒绝（签名/时间戳/无处理器/重放不被受理等），decision 携带具体原因码 |
| `ATTEMPT` | 一次处理尝试的结论（SUCCESS/RETRYABLE/PERMANENT） |
| `BACKOFF` | 暂时失败后的退避等待（含等待时长） |
| `DEAD_LETTERED` | 进入死信（PERMANENT 或 RETRYABLE_EXHAUSTED） |
| `REPLAY_REQUESTED` / `REPLAY_STARTED` | 重放请求到达 / 抢占成功（记录本次负载来源） |
| `SUCCEEDED` | 处理成功 |

重放相关的 `REJECTED` 决策码（与接口当时的 409/404 结论一一对应）：

| decision | 含义 |
| --- | --- |
| `EVENT_NOT_FOUND` | 事件不存在（404） |
| `REPLAY_BAD_PAYLOAD` / `REPLAY_EVENT_ID_MISMATCH` | 修正负载非法（400） |
| `REPLAY_IN_PROGRESS` | 已有重放在执行，并发重放只会有一个真正执行（409） |
| `REPLAY_NOT_DEAD` | 事件当前不是 DEAD 状态（409），detail 中的状态取自锁内快照 |

- 每条轨迹都带 `eventId`、机读 `decision` 与人读 `detail`（判定依据），
  与接口当时返回的结论一一对应（如接口返回 `DEAD` ⇔ 轨迹含 `DEAD_LETTERED`）。
- 被 422 拒绝的事件**不入库但轨迹可查**（详情 404、轨迹 200），判定不会"查不到"。
- 完全没有轨迹的 eventId 查询返回 404。

### 3.2 前向兼容

处理器只读取自己关心的已知字段，负载新增字段（含嵌套对象/数组）一律忽略，
不会破坏既有处理逻辑；轨迹记录、死信重放、修正负载重放对新增字段同样透明
（修正负载可以携带未来新增字段，原样参与分发与留档）。
见测试 `extraFutureFields_doNotBreakExistingHandler` 与 `newFieldsInPayload_doNotBreakTraceOrHandling`。

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

覆盖场景（34 个用例，正常与异常路径均有断言，日志均带 eventId 与判定依据）：

- `SignatureAndTimestampIntegrationTest`（10）：合法请求、时间戳缺失/非法/过期/超前/窗口边界、
  签名缺失/错误密钥/负载篡改、坏 JSON/缺字段；
- `DeduplicationIntegrationTest`（4）：串行重复、**16 线程并发重复**（断言恰好 1 次 SUCCEEDED、
  其余 DUPLICATE、副作用恰好 1 次）、无处理器类型 422、不同 eventId 互不影响；
- `RetryDeadLetterReplayIntegrationTest`（6）：暂时失败重试到上限入死信、永久失败只尝试 1 次、
  死信查询、404/409、重放成功且副作用不重复、先失败后成功、负载新增字段前向兼容、死信后重复投递；
- `ReplayWithCorrectedPayloadIntegrationTest`（4）：修正负载重放成功且原始负载保留可查、
  修正后仍失败留死信可解释、不带负载重放走原始负载、非法修正负载（非对象/eventId 不一致）400；
- `ConcurrentReplayIntegrationTest`（4）：**8 线程并发重放恰好 1 个执行**、其余 409 结论逐字节一致、
  重放历史恰好一条、副作用恰好一次、成功后再重放 409 且消息状态与真实状态一致；
  **混合（带/不带修正负载）并发重放**：赢家在途期间其余请求一律同一个 409（REPLAY_IN_PROGRESS），
  全局恰好 1 个 SUCCEEDED；**并发重放仍失败**：事件留死信（failureKind/attempts/lastError 可查），
  并发结束后单独重放不受影响可成功；重放仍失败留死信可解释。
  并发用例通过 `sleepMillis` 撑开执行窗口 + 轮询确认在途，**可反复连跑不偶发失败**：
  ```bash
  for i in $(seq 1 10); do mvn -q test -Dtest=ConcurrentReplayIntegrationTest || break; done
  ```
- `EventTraceIntegrationTest`（5）：全生命周期轨迹与接口结论一一对应、
  被拒事件不入库但轨迹可查、未知事件轨迹 404、新增嵌套/数组字段不破坏轨迹与处理、
  重放失败的轨迹可解释。

### 5.2 一键 curl 演示

```bash
# 终端 A
mvn spring-boot:run

# 终端 B
./scripts/local-demo.sh
```

脚本依次演示：正常事件 → 重复投递（DUPLICATE）→ 不支持类型（422）→ 签名错误（401）
→ 时间戳过期（401）→ 暂时失败重试入死信 → 永久失败直入死信 → 查询死信 → 重放 → 409
→ 修正负载重放成功 → 事件详情（原始负载 vs 修正负载）→ 事件轨迹
→ **5 路并发重放同一死信**（恰好 1 个执行、其余一致的 409、重放历史恰好一条）。

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
`[dispatch eventId=evt-2001] 第1次尝试 RETRYABLE -> 50ms 后退避重试`。

---

## 6. 代码结构

```
config/      WebhookProperties            可调参数
security/    SignatureVerifier            时间戳 + HMAC 签名校验（规则唯一实现）
model/       EventRecord/EventState/...   领域模型与状态机
handler/     EventHandler + Registry      按类型路由；impl 下两个演示处理器
store/       InMemoryEventStore           原子占位去重；记录状态
             EffectLedger                 副作用幂等台账
service/     WebhookIngestService         接收编排（判定顺序见第 2 节）
             DispatchService              退避重试 / 永久失败 / 死信（分发用生效负载）
             DeadLetterService            死信查询、修正负载重放、重放历史
trace/       InMemoryTraceStore           事件轨迹（只追加，按 eventId 串查）
web/         Controller + 全局异常处理     结构化响应
```
