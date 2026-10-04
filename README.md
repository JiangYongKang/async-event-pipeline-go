# async-event-pipeline-go

一条**纯本地、零外部依赖**的异步事件投递与消费链路。不使用 Kafka / Redis /
数据库或任何需要联网的服务，只依赖本地文件：

- `events.log`：append-only 事件日志（定长头 + 长度前缀 + CRC32 帧）；
- `state.snapshot.json`：消费进度、幂等表、死信列表与业务效果的**原子快照**。

它要解决的问题是：当**重复投递、处理失败、进程重启/重放**都可能发生时，
如何让最终业务状态可预期、可解释——事件不丢、业务效果不重复、同键顺序不乱、
失败有可区分的终态与原因、进度回退被明确拒绝。

## 投递语义

| 维度 | 保证 |
| --- | --- |
| 事件投递 | **at-least-once（至少一次）**：`Publish` 返回前事件已写入日志并 `fsync`；崩溃不丢，重试/重启可能让同一事件被处理多次。 |
| 业务效果 | **exactly-once（恰好一次生效）**：以事件 `ID` 为幂等键，成功记录与业务效果在**同一个原子快照事务**中落盘；同一 ID 的重复投递只记录重复标记，不再施加效果。 |
| 处理中途失败 | **无半完成状态**：Handler 始终在最新业务状态的**克隆**上执行，失败（业务错误、提交失败、panic、崩溃）直接丢弃克隆；事件保持未终态并按退避重试。 |
| 消费顺序 | 同一业务键 `Key` 严格按日志顺序（= 产生顺序）串行处理；不同 `Key` 之间受 `Parallelism` 许可控制并行推进。 |
| 进度持久化 | 已提交水位 `committed_offset`（已连续完成的最大偏移+1）随每次终态提交原子落盘；重启从水位继续，结果与一口气顺序执行一致。 |

## 完成判定与“等真正处理完”（WaitUntilIdle / Progress）

`WaitForOffset(n)` 只表达“等到水位推进到 n”。当你复用一个已有历史的
数据目录、或重启后继续追加事件时，用“本次发布条数”去等会**立即误返回**
（历史水位早就超过了该数字），导致“最终状态已打印、新批次其实还没消费完”。
为此新增两个面向“确实处理完了”的能力：

### `WaitUntilIdle(ctx) error`

等到**调用那一刻已经发布（fsync 入日志）**的全部事件都到达终态
（成功处理或进入死信都算）并持久化后才返回：

- 水位**连续**覆盖到调用时刻的日志末尾（`committed == 当时 log_end`），
  中间没有空洞；
- 没有任何事件仍在 `queued / processing / retrying`，也没有事件“已发布但
  dispatcher 还没读到”；
- 完成判定逐偏移核对终态记录（成功/死信/重复别名），不仅看一个水位数字，
  因此重复投递占用的偏移也必须可查，杜绝“水位到了但中间有偏移没有终态”
  的假完成。

边界与两种状态：

- **运行中调用**：阻塞等待消费者真正处理完；返回后立刻查看状态，看到的
  就是完整处理后的结果。调用之后由其他 goroutine **新追加**的事件不属于
  本次等待范围（等待目标在进入等待时捕获）；要把新批次算进来，在追加完成
  后**再调用一次**即可，历史与新批次都不丢。
- **停机后调用**：以持久化日志/快照做一次性判定。已全部终态 → 立即返回
  `nil`；尚有未终态事件 → 返回包装了 `ErrPipelineNotRunning` 的错误，错误
  信息直接附带差距计数（停机状态不会自行推进，先 `Start` 再等待）。
- `ctx` 超时/取消返回 `ctx` 的错误，此时**不代表已完成**。
- `Drain`（优雅停机）内部使用同一判定：它先捕获日志末尾，再等待该位置
  之前所有事件——包括“已 fsync 但 dispatcher 尚未读到”的事件——都到终态，
  不会在停机时漏掉未读事件。

### `Progress() ProgressReport`：可解释的进度说明

逐偏移汇总当前状态，给出：

| 字段 | 含义 |
| --- | --- |
| `log_end` / `committed_offset` | 已持久化事件总数（下一条偏移）/ 已连续终态水位 |
| `processed` / `dead` | 终态=成功 / 终态=死信的**偏移数**（重复投递按其偏移计入） |
| `processing` | 正在执行 Handler |
| `waiting_retry` | 瞬态失败后退避中，等待下一次重试（含下次尝试时间与原因） |
| `queued` | 已派发入键队列，但还没开始处理（等同键前序或并行许可） |
| `not_dispatched` | 已发布（在日志里）但 dispatcher 还没读到；停机时未消费事件标记为 `stopped` |
| `in_flight` | processing + waiting_retry + queued |
| `complete` | 完成结论：全部已发布偏移均有终态，且无任何未完成计数 |
| `gaps` | 水位之后尚未终态的位置明细（offset/event/key/阶段/尝试次数/重试原因，抽样最多 30 条；计数仍全量） |
| `keys[]` | **每个业务键**各自的 total/processed/dead/剩余（再细分 processing/retry/queued/unread） |
| `basis` | 人读判定依据：最后终态到哪个位置、有没有缺口、首个缺口是谁、dispatcher 读到哪 |

没等够时，`complete=false` 且 `basis` + `gaps` + 每键剩余会直接告诉你差在
哪个偏移、处于什么阶段，而不是对着总数猜。

## 失败终态（死信）与可区分原因

瞬态错误按指数退避重试，超过 `MaxAttempts` 后进入死信；不可重试错误立即进入
死信。三类原因（`FailureKind`）在死信记录与日志中可明确区分：

| Kind | 含义 | 是否重试 |
| --- | --- | --- |
| `transient` | 普通错误/包装错误，重试次数耗尽 | 是，直到 `MaxAttempts` |
| `permanent` | 业务明确判定不可处理（`pipeline.PermanentError`） | 否，立即死信 |
| `invalid` | 事件无法解析/接受的毒丸消息（`pipeline.InvalidError`） | 否，立即死信 |

每条死信保留：事件 ID、键、偏移、`kind`、`reason`、尝试次数、首次失败时间、
终态时间。事件进入死信**也会推进水位**（死信是一种终态），且其失败的业务
克隆被丢弃，不产生半成品效果。

## 并发模型

```
Publish ──fsync──▶ events.log（单 writer 互斥，偏移单调）
                         │
                  单 dispatcher goroutine（顺序读日志：先回放后实时）
                         │  按 Key 分入 FIFO 键队列
            ┌────────────┼────────────────────┐
         key A 串行    key B 串行   ...   Parallelism 个跨键许可并行
         worker        worker
                         │
        StateStore（互斥 + 乐观版本号 state_version）
        业务效果克隆 + 幂等记录 + 水位推进 ⇒ 原子快照（tmp+fsync+rename）
```

- 同键顺序：每个键一个队列，同一时刻只处理队首；前一条不到终态，后一条不开始。
- 跨键并行：`Parallelism` 个许可，派发顺序更早的键优先获得执行机会。
- 无丢失/无重复生效的并发要点：
  - dispatcher 是日志的唯一顺序读取者，事件入队顺序即产生顺序；
  - 即使同一事件 ID 的两个副本被并发分发、都在提交前通过幂等快判，提交层
    仍以事件 ID 做最终裁决：第二个被判为重复，效果不二次生效，只在自己的
    offset 上登记并推进水位；
  - 跨键并行提交采用**乐观并发控制**：基于过期状态版本的提交返回
    `ErrStaleState`，运行时在最新克隆上重新施加事件效果（reapply），
    不覆盖其他键刚提交的效果；
  - 水位只能连续推进：前方 offset 未到终态时，后面的事件即使先处理完也
    不会让水位越过空洞，避免“从中间静默继续”。

## 进度恢复、重放与回退拒绝

- **重启恢复**：打开日志时校验每一帧（magic/版本/长度/CRC/JSON）。
  - 末尾撕裂帧（崩溃于写入中途）→ 自动截断到最后完整帧并继续；
  - 中间帧损坏 → 返回 `ErrLogCorrupt`，不静默跳过、不丢数据；
  - 状态快照采用 `临时文件 + fsync + rename + 目录 fsync`，崩溃后要么看到
    旧快照要么看到新快照，不存在半写入。
- **`Seek(ctx, offset)`（运行中，只允许向前的同步屏障）**
  - `offset < 已提交水位` → `ErrRollbackRejected`，不允许把已确认进度静默回退；
  - `offset > 日志末尾` → `ErrOffsetBeyondEnd`，不允许越过尚未存在的事件；
  - 合法目标：阻塞等待对应区间全部处理完成，**不跳过任何事件**。
- **`Replay(ctx, 0)`（仅停机状态，显式重放）**
  - 回退到日志最早可恢复位置之前（`< 0`）→ `ErrOffsetBeforeEarliest`；
  - 当前没有中段快照边界，回退到“日志中间”→ 明确拒绝（`ErrOffsetBeforeEarliest`），
    不支持从中间位置静默重放；
  - 合法重放会清空进度/幂等/死信/业务效果快照，从日志头顺序重新处理全部事件。
    由于处理幂等，最终业务状态与首次顺序执行一致；死信按当前 Handler 语义
    重新判定。运行中调用 Replay 返回 `ErrPipelineRunning`。

## 对已有数据目录与旧状态文件的兼容

- 复用同一数据目录（重启后续跑、或运行中/重启后继续追加）是一等支持场景：
  日志 offset 单调延续，新事件紧接历史偏移入列，享有与历史事件完全相同的
  可靠投递、ID 幂等与同键顺序保证；`WaitUntilIdle` 覆盖历史 + 新批次。
- 状态快照新增了可空字段 `offset_aliases`（重复投递偏移 → 首次终态记录的
  事件 ID）。早期版本不保存该字段，重复投递的偏移只存在于内存索引、靠水位
  掩盖；**旧目录无需迁移**：`Open` 会读取 `events.log` 对逐偏移终态索引做一次
  对账（`ReconcileOffsets`），自动补齐缺失的重复偏移别名并原子写回，之后
  `Progress`/`WaitUntilIdle` 对每个偏移都成立。快照版本号不变。

## 本地验证

要求 Go 1.25+，全程离线：

```bash
# 1. 一键演示（正常/重复/重试/永久失败/非法负载，含进度与判定依据日志）
go run ./cmd/demo -dir ./.demo-data -clean

# 重启恢复 + 在同一目录上继续追加新一批事件（每轮事件 ID 不同）
go run ./cmd/demo -dir ./.demo-data

# 再次复用同一目录：历史 + 两次新批次都应被处理干净，complete=true
go run ./cmd/demo -dir ./.demo-data

# 停机后显式全量重放（最终状态应与重放前一致）
go run ./cmd/demo -dir ./.demo-data -replay

# 2. 全量单测（含 -race 竞态检测）
go test ./... -race -count=1
go test ./... -race -count=5        # 稳定性压测
go test ./pipeline/ -run 'Idle|Progress|Drain|Legacy|Restart|Crash' -v   # 只看完成判定相关
go test ./pipeline/ -v              # 查看逐条事件日志
```

手工确认“确实处理完”的推荐顺序（demo 也是这么做的）：

1. 运行中发布完一批后调用 `WaitUntilIdle(ctx)`，而不是按“本次条数”等待；
2. 它返回后立即调用 `Progress()`，确认 `complete=true`、
   `committed_offset == log_end`、`processing/waiting_retry/queued/not_dispatched`
   全部为 0、每键 `remaining=0`，`basis` 显示“last terminal offset … no gaps”；
3. `Drain` 停机后可再次调用 `WaitUntilIdle`（停机判定）确认落盘状态完整；
   若没等够，`complete=false` 且 `gaps` 直接指出第一个缺口事件与所处阶段。

测试覆盖的场景：

1. `TestDuplicateDelivery`：同一事件重复投递，效果只一次；
2. `TestFailureRetryAndDeadLetter`：瞬态重试成功、瞬态耗尽死信、永久失败、
   非法负载，三类死信原因可区分，失败事件不留业务效果；
3. `TestConcurrentProcessing`：多键高并发 + 并发重复投递，最终每键计数精确；
4. `TestPerKeyOrdering`：同键偏移序列严格递增，跨键可并行；
5. `TestRestartRecovery` / `TestCrashMidRetryThenRecover`：优雅重启与
   “重试中途崩溃”后从持久化水位/日志恢复；
6. `TestReplayAndRejectRollback`：回退拒绝、越界拒绝、显式重放结果一致；
7. `TestEventLogTornWriteTruncation` / `TestEventLogMidCorruptionRejected`：
   撕裂写截断与中间帧损坏报错；
8. `TestHandlerPanicRecovered`：业务 panic 作为瞬态错误重试，不卡死键队列。
9. `TestWaitUntilIdleAcrossRestartsAndAppends`：连续 3 轮复用同一目录，
   恢复后追加新批次，历史+新批次全部终态、计数与死信可对账；
10. `TestIdleAfterCrashRestartThenAppend`（包内）：硬崩溃于重试中途后重启，
    在历史之上追加事件（含重复），`WaitUntilIdle` 覆盖全部 5 条偏移；
11. `TestProgressReflectsUnfinishedWhileRetrying` /
    `TestWaitUntilIdleReturnsAfterRetryRecovers`：重试中进度如实显示
    `retrying`/每键剩余/缺口且 `complete=false`，恢复后才完成；等待期间
    不会提前返回；
12. `TestIdleNoFalsePositiveUnderConcurrency`：多键高并发混跑 + 并发等待者/
    观察器，数千次 `complete=true` 结论逐一与“水位、逐偏移终态、日志末尾”
    交叉核对，不出现“没处理完却报告完成”的假阳性；
13. `TestOfflineWaitUntilIdleSemantics`：停机后未消费→`ErrPipelineNotRunning`
    并给出差距；处理完停机→离线立即确认完成；
14. `TestDrainCoversUnreadPublished`：发布后立即停机（含 dispatcher 尚未读入
    的事件），重复多轮，`Drain` 必须等全部已发布事件终态；
15. `TestLegacySnapshotReconciledOnOpen`：旧格式快照（无重复偏移别名）重新
    打开时依据日志对账自愈，不迁移即可得到正确完成结论；
16. `TestWaitUntilIdleBoundaryAcrossAppends`：等待调用之后追加的事件不属本次
    等待，再次等待覆盖新批次，两批都不丢。

测试日志中每条事件都打印 **事件标识（id/key/offset）、处理进度
（committed 水位）与判定依据（verdict + basis）**，例如：

```
event=fail-2 offset=3 key=tenant-a verdict=retry attempt=2/5 progress=3 basis=transient-error(...)
event=fail-2 offset=3 key=tenant-a verdict=processed attempts=3 progress=5 basis=handler-success+atomic-snapshot(v=4)
event=perm    offset=4 key=tenant-b verdict=dead kind=permanent reason="..." basis=permanent-error
```

## 作为库使用

```go
cfg := pipeline.DefaultConfig("./data")      // 可调整 Parallelism/MaxAttempts/退避
p, _ := pipeline.Open(cfg, myHandler)        // myHandler 实现 pipeline.Handler
_ = p.Start(context.Background())

off, _ := p.Publish(pipeline.Event{
    ID: "order-123", Key: "tenant-7", Type: "inc", Payload: []byte("orders"),
})

_ = p.WaitForOffset(ctx, off+1)              // 等待该事件终态落盘
if err := p.WaitUntilIdle(ctx); err != nil { // 等“当前已发布事件全部终态”（重启/追加都适用）
    // 停机且尚有未终态事件时返回 ErrPipelineNotRunning；可用 Progress() 看差距
}
report := p.Progress()                       // 成功/死信/处理中/重试/排队/未读、每键剩余、判定依据
_ = report.Complete
snap := p.Snapshot()                         // committed 水位、幂等表、死信、业务状态
_ = p.Drain(ctx)                             // 优雅停机：在途与已发布未读事件都处理到终态
_ = p.Close()
```

业务方实现 `pipeline.Handler`：

```go
type Handler interface {
    Handle(ctx context.Context, ev StoredEvent, state BusinessState) error
}
```

- 返回 `nil`：成功；
- 返回 `pipeline.TransientError(...)` 或普通 error：按退避重试；
- 返回 `pipeline.PermanentError(...)` / `pipeline.InvalidError(...)`：立即死信；
- 自定义业务状态实现 `BusinessState`（`Clone()` 深拷贝），或直接使用内置
  `*pipeline.KVState` 与 `pipeline.CountHandler`。

## 设计边界（有意为之的取舍）

- 单进程、本地文件：不提供跨进程/跨机器的分布式协调；`Pipeline` 假定同一
  数据目录同一时刻只有一个消费者（日志写入由单进程内互斥保护）。
- 不做日志截断/压缩：`Earliest()` 恒为 0，因此 `Replay` 的可恢复位置即日志头；
  这使“回退到不可恢复位置必须拒绝”的语义简单且可验证。
- 每次终态提交整快照落盘：状态文件大小随终态事件数增长，适合本地开发/测试/
  单节点轻量场景，不追求高吞吐。
