# async-event-pipeline-go

一条**完全本地、无外部中间件、无需联网**的异步事件投递与消费链路（Go 标准库实现）。
它面向“单进程 + 本地磁盘”的场景，演示并保证：

- 可靠投递与**幂等消费**（重复投递业务效果只生效一次）；
- 处理失败可重试，失败尝试**不留下半完成状态**；
- 重试耗尽或业务明确拒绝时进入可识别的**失败终态（dead-letter）**，并保留可区分原因；
- **同一业务键严格按产生顺序处理，不同业务键并行**；
- 消费进度持久化，**进程重启后从检查点恢复，结果与顺序执行一致**；
- 回退到已确认位置、回退到日志之外的不可恢复位置会被**显式拒绝**，不会静默从中间继续。

## 目录结构

```
internal/event/     事件信封（Event / Delivery）
internal/store/     本地持久化：events.jsonl(WAL) + state.json(原子提交) + dead.jsonl(死信投影)
internal/pipeline/  生产/消费运行时：顺序 worker、并发信号量、重试退避、去重、检查点、seek 校验
internal/business/  示例领域：账户开户/存款/取款 + 确定性故障注入
cmd/demo/           本地端到端演示
```

## 本地验证

要求 Go 1.25+（仅用标准库，无第三方依赖）。

```bash
# 全部单元测试（含重复投递 / 失败重试 / 并发顺序 / 重启恢复 / seek 拒绝）
go test ./...

# 带竞态检测重复运行
go test -race -count=3 ./...

# 端到端演示（数据落在 ./.demo-data）
go run ./cmd/demo

# 复用同一数据目录，验证重启恢复（余额只应各增加 7，已确认事件不重复生效）
go run ./cmd/demo -restart
```

测试日志（`-v`）会打印每个关键判定的 **事件标识 event_id、处理进度 checkpoint/attempt 与
判定依据 decision（commit / retry / dead_fatal / dead_retry_exhausted / reject_backward /
reject_out_of_range / dedup）**。

## 持久化布局（均为本地文件）

| 文件 | 角色 |
| --- | --- |
| `events.jsonl` | 仅追加（append + fsync）的事件日志，是事件顺序与序列号的权威来源 |
| `state.json` | 消费状态：每键检查点、已处理事件 ID 集合、业务 KV、跳过审计、死信记录；以“临时文件 + fsync + rename”原子替换 |
| `dead.jsonl` | 死信的人类可读投影，权威数据在 `state.json`，打开存储时自动重建/对账 |

## 投递与处理语义

### 1. 幂等发布与“效果恰好一次”

- 每个事件由调用方提供**全局唯一 `ID`**。`Publish` 对同一 ID 只追加一次；重复发布返回原有事件及
  `Dup=true`，不产生新序列号、不产生第二次业务效果。
- 每个业务键维护单调的 `KeySeq`（产生顺序）与一个已确认检查点 `checkpoint`。
- 处理器通过事务对象 `Tx` 读写业务 KV：**所有写操作先缓存在内存**，处理器返回成功后，才把
  “业务变更 + 去重标记 + 检查点推进”作为**一次原子状态提交**落盘。处理器返回错误时缓冲被丢弃，
  因而中途失败不会留下半完成状态。
- 由于业务效果与检查点在同一个原子 rename 中可见，投递语义为 **at-least-once delivery，
  effect-level exactly-once**：即使事件被重复投递，业务效果也只生效一次。

### 2. 顺序与并发

- dispatcher 对**每个业务键至多启动一个 worker**，同一键的事件严格按 `KeySeq` 逐条处理；
  结构上保证同键不会乱序。
- 不同键的 worker 由全局信号量（`Config.Concurrency`）限流并行执行。
- 发布、消费、重试、seek 并发发生时，存储层互斥 + 越序提交拒绝
  （只允许提交 `checkpoint+1`）共同保证不丢、不重、不乱序。

### 3. 失败、重试与失败终态

处理器返回的错误分为两类：

- `pipeline.Retry(err)`（或任意未分类的普通 error）：视为**瞬时失败**，按指数退避
  （`BackoffInitial → BackoffMax`）在**同键原位置**重新投递，`Delivery.Attempt` 递增；
  重试期间不推进检查点，因此该键后续事件被有序阻塞，但不影响其他键。
- `pipeline.Fatal(err)`：视为**不可恢复的业务拒绝**，立即进入死信。
- 瞬时失败在 `MaxAttempts` 次后仍未成功，则以 **retry-exhausted（`fatal=false`）** 进入死信。

死信记录 `store.DeadRecord` 包含：事件标识与序列号、`Attempts`、`Fatal` 以及字符串 `Reason`。
示例领域给出互不混淆的区分码：`ALREADY_OPEN`、`ACCOUNT_NOT_OPEN`、`INSUFFICIENT_FUNDS`、
`BAD_COMMAND`、`TRANSIENT_FAILURE`。进入死信的事件会推进该键检查点，因此毒消息不会永久阻塞同键
后续事件，后续事件仍按序处理。

### 4. 进度、重启恢复与重放

- 消费进度按键持久化在 `state.json`。进程重启后打开存储：重放 `events.jsonl` 重建索引与序列号，
  从每键检查点之后继续投递；已确认事件不会被再次处理。
- “从更早位置重放”通过**再次发布相同事件 ID** 实现：它是幂等 no-op；而真正未处理的旧事件在
  重启后会被重新投递，并通过原子事务提交保证与一次性顺序执行结果完全一致。
- `Seek(key, toSeq)` 只允许**向前**把检查点推进到日志中已存在的位置，被越过的事件 ID 会记录在
  跳过审计中；以下操作被显式拒绝并返回哨兵错误，绝不静默继续：
  - `toSeq <= 当前检查点`：`pipeline.ErrSeekBackwards`（回退到已确认位置之前）；
  - `toSeq > 该键已知最大序列号`：`pipeline.ErrSeekOutOfRange`（不可恢复位置）。

### 5. 崩溃安全说明

- 事件追加采用 append + fsync，失败会截断回半行，WAL 始终可重放。
- 状态提交采用同目录临时文件 + fsync + rename 原子替换；崩溃只会看到“旧状态”或“新状态”，
  不会看到半截状态。新状态先落盘，内存态随后更新；提交失败时内存与磁盘不会分叉。
- 死信记录是原子状态的一部分；`dead.jsonl` 仅为投影，若崩溃发生在投影追加之前，重新打开时
  会依据 `state.json` 重建。

## 关键配置（`pipeline.Config`）

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `MaxAttempts` | 5 | 单事件最大处理尝试次数，耗尽后死信 |
| `BackoffInitial` / `BackoffMax` | 10ms / 1s | 指数退避上下限 |
| `Concurrency` | 4 | 不同业务键的最大并行度 |
| `PollInterval` | 5ms | dispatcher 扫描持久化日志的间隔 |
| `Logger` | stderr 文本 slog | 决策日志（事件 ID、进度、判定依据） |
