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

## 完成判定、等待语义与进度说明

`WaitForOffset(n)` 只保证“水位到达调用时指定的偏移”，无法回答“**现在是不是
真的全部处理完了**”：重启恢复时 dispatcher 可能还没追上日志、等待期间又有
新事件追加、或还有事件停在重试退避里。为此提供两个新能力：

### `WaitUntilIdle(ctx)` —— 等“真正处理完”再返回

无论运行中还是停机后，调用后阻塞直到满足**全部**条件：

1. 当前已 `Publish`（即已 fsync 入日志）的每条事件都到达**终态**
   （成功处理或进入死信，重复投递副本也算）；
2. 没有事件仍在执行 Handler/提交（`handling`）、退避等待重试（`retrying`）、
   在键队列里排队等前序同键事件（`waiting`）；
3. 没有事件“已发布但 dispatcher 尚未读到”（`unread`）——这正是重启恢复或
   运行中追加新批次时最容易被漏掉的一类；
4. 提交水位**连续**推进到日志末尾，中间没有任何缺口（`gap`）。

判定不是只看一个水位数字，而是对日志中的**每个偏移逐一归类**，恒等式成立：

```
logEndOffset = succeeded + dead + handling + retrying + waiting + unread
```

等待期间新 `Publish` 的事件自动纳入等待范围（判定始终对照“调用期间不断增长
的日志末尾”，而不是调用开始时的旧末尾），因此：

- 重启后继续用、在旧历史上追加新事件：同一套机制覆盖新批次；
- 运行过程中又追加一批：等到这批也终态才返回。

返回后立刻查看状态，看到的就是完整结果——不存在“以为等到了、其实还有事件
没消费”。停机状态下：若已全部终态立即返回 `nil`；若尚有未完成事件则返回
`ErrPipelineNotRunning`（停机不可能自行产生进展，需先 `Start` 恢复消费）。

### `Progress()` —— 可解释的进度说明

返回一份快照（`Progress`），给出：

| 字段 | 含义 |
| --- | --- |
| `LogEndOffset` | 已发布事件总数（判定对照的“终点”） |
| `DispatchedOffset` | dispatcher 已顺序读到的位置 |
| `CommittedOffset` | 已连续终态的水位 |
| `Succeeded` / `Dead` | 已成功 / 已死信的投递偏移数 |
| `Handling` / `Retrying` / `Waiting` / `Unread` | 正在处理 / 等待重试 / 排队 / 尚未读到 |
| `GapOffsets` | 水位前方仍无终态记录的偏移（缺口），直接说明“差在哪” |
| `Keys` | 每个业务键的 `published / terminal / remaining` |
| `Complete` / `Basis` | 是否完成，以及人读的判定依据 |

`Progress` 实现了 `String()`，可直接多行打印。未等够时一眼能看到是卡在
重试、排队还是 dispatcher 还没读到，而不用对着总数猜。

## 对已有数据目录与状态的兼容

- **快照文件格式不变**：仍是原来的 `state.snapshot.json`，旧目录可直接打开，
  无需迁移。
- **打开时自动对账（reconcile，自愈）**：幂等表按事件 ID 存储，而同一 ID 的
  重复投递副本各占一个日志偏移；这类“重复偏移”的终态登记此前只存在于
  运行期内存索引，重启后从幂等表重建时会丢失，在水位前方形成**伪空洞**
  （水位虽已推进，但按偏移查询会误判该投递未终态，导致空闲判定永远不满足）。
  现在 `Open` 会在消费者启动前用“日志 + 幂等表”无损重建偏移终态索引并重新
  原子落盘：日志在、且该 ID 已有终态，就补回该偏移的终态引用。因此**复用旧
  目录、追加新事件同样可靠**，新事件与历史事件享有一致的可靠投递、幂等与
  同键顺序保证。
- 投递语义（at-least-once / 业务效果 exactly-once / 同键顺序 / 死信三分类 /
  回退拒绝）均不变；新能力是只读观测与更强的“处理完”判定，不改变处理路径。

## 本地验证

要求 Go 1.25+，全程离线：

```bash
# 1. 一键演示（正常/重复/重试/永久失败/非法负载，含进度与判定依据日志）
go run ./cmd/demo -dir ./.demo-data -clean

# 重启恢复 + 在旧进度上追加全新一批事件（复用同一数据目录）
go run ./cmd/demo -dir ./.demo-data

# 再次复用：重投相同 ID 演示跨重启幂等（计数不增长），且立即判定空闲
go run ./cmd/demo -dir ./.demo-data

# 停机后显式全量重放（最终状态应与重放前一致）
go run ./cmd/demo -dir ./.demo-data -replay

# 2. 全量单测（含 -race 竞态检测）
go test ./... -race -count=1
go test ./... -race -count=5        # 稳定性压测
go test ./pipeline/ -v              # 查看逐条事件日志
```

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
8. `TestHandlerPanicRecovered`：业务 panic 作为瞬态错误重试，不卡死键队列；
9. `TestWaitUntilIdleCoversAppendedBatch`：等待进行中并发追加新批次，
   `WaitUntilIdle` 返回后全部终态、数字不再变化（无假阳性）；
10. `TestProgressExplainsOutstandingWork`：进度说明如实区分
    handling/retrying/waiting/unread、缺口偏移与每键剩余，未完成时
    `Complete=false`；
11. `TestResumeThenAppendReliable`：连续多轮复用同一目录，旧历史 + 新追加
    批次（含跨重启同 ID 重复、同键衔接、重试事件）全部可靠处理到位；
12. `TestStoppedIncompleteRejectsFalseIdle`：硬崩溃于重试中途后，停机态
    `Progress` 把未完成事件计入 unread 并指出缺口，`WaitUntilIdle` 拒绝
    假报完成；`Start` 恢复后收敛到完整；
13. `TestIdleNoFalsePositiveUnderMixedLoad`：连续多轮、多键、高并发混跑
    （发布未结束即开始等待），每轮进度恒等式成立、每键计数精确，
    绝不“没处理完却报告完成”。

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

_ = p.WaitForOffset(ctx, off+1)              // 只等这一条到终态
// 或者：等“当前已发布的事件全部真正处理完”（含运行中追加的新批次）。
if err := p.WaitUntilIdle(ctx); err != nil { // 停机且未完成 -> ErrPipelineNotRunning，先 Start
	_ = err
}
fmt.Print(p.Progress().String())             // 成功/死信/处理中/重试/排队/未读、缺口、每键剩余
snap := p.Snapshot()                         // committed 水位、幂等表、死信、业务状态
_ = p.Drain(ctx)                             // 优雅停机：在途事件处理到终态
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
