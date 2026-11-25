# 会话 Fork 与子 Agent 独立调度：设计评审稿

2026-11-25。对照基线为实验分支 `perf/session-projection-tail` 的 `14f81fe2`。本文供共同审校，阶段、表名、策略和指标均是提案。批准之后才写 ADR、改代码或安排发布。

**建议拍板的模型**：每个逻辑 Agent 有一个可独立领取的 Session，一个 Session 可以先后执行多个 Turn；同一份已提交历史可以被多个 Session 只读引用。参考 Temporal，为每个分支存一份短的祖先区间清单。正文长期只保留在其原物理流上。网页 Fork 和工具 Fork 共用冻结历史的原语，分别创建空闲 Session 和带首任务的 Session。

第一次审校已确认工具 Fork 的冻结点与网页 Fork 的第一版入口。父 Turn 的 Stop 是否传播给本轮子 Turn 待定，详见末节。需要追问“打开页面要查什么”时看第 5 节，需要判断“可不可以上线”时看第 7、8 节。

## 1. 先用一个例子定语义

父会话 P 写了 A、B、C、D、E。C 是一个已完成且封口的 Turn 的可见末尾，用户在网页点 C，得到 Q。Q 有自己的地址，此时没有工作，没人领取它。后来用户发 F，Q 才有第一次 Turn。Q 执行到 F 中途，调用 `subagent(name="payments", context="branch", task="检查退款")`，得到子会话 R。R 继承 Q 当时指定的已提交历史，另有第一项任务；它可以马上被另一台 Worker 领取。若用 `fresh`，同样创建 R 和第一项任务，继承段清单为空。

| 对象 | 负责回答 | 例子 |
|---|---|---|
| Session | 哪个 Agent 可以接收输入、被独立领取、停止或恢复？ | P、Q、R 各有稳定 ID，运行时各有自己的写入代际与租约 |
| Turn | 这个 Agent 的哪一次工作？由什么输入启动？ | F 是 Q 的一轮；“检查退款”是 R 的一轮。初期可沿用现有表名 `runs` 和公开 Run ID |
| 历史流 Flow | 这批正文实际追加在哪里？ | P 的 A…E 只写在 P 当时的 Flow。Q、R 的新内容各写入自己的 Flow |
| 历史来源与委派关系 | 能读到哪段旧历史？谁能管理这个子 Agent？ | Q 是网页上的平级会话，R 是 Q 的直接子 Agent。若在 R 中选祖先的 B 再 Fork，目的会话的内容截止 P 的 B |

目标负载包括多年使用的会话、历史位置上反复人工 Fork、大量同级委派，以及未来允许的多层委派。读一个会话的成本应取决于它自己的可见段和本页/本次模型需要的内容。正常消息追加无需更新其他分支。分叉时允许写少量描述历史范围和原生文档的元数据，历史消息、图片和附件正文均共享。

## 2. 调研结论与现有实现的差距

| 样本 | 核对到的实现 | 采用的部分及代价 |
|---|---|---|
| Temporal Server v1.32.2，MIT | Fork 构造包含祖先分支及节点范围的元数据。落点在祖先时裁掉后面的区间。SQL 后端插入分支记录；倒序读定位到区间，分页令牌携带区间进度。删除会检查其他分支还引用到哪里。 | 作为主要参考。每个目的 Flow 登记一份短清单，允许重复几个范围描述。Temporal 的节点可包含一批事件，Pi 的合法 Entry 与提交边界需要另外处理。[Fork 与删除](https://github.com/temporalio/temporal/blob/v1.32.2/common/persistence/history_manager.go#L38-L199) · [逆向读取](https://github.com/temporalio/temporal/blob/v1.32.2/common/persistence/history_manager.go#L670-L723) · [SQL 存储](https://github.com/temporalio/temporal/blob/v1.32.2/common/persistence/sql/history_store.go#L295-L329) |
| Neon，Apache-2.0 | 子时间线记录父时间线与分叉 LSN，读取父数据受该位置限制；回收保留子分支依赖的旧版本。 | 借鉴固定截止点；父指针方案保留作低创建成本的对照。数据库页的读取数据不能换算成聊天分页延迟。[存储设计](https://github.com/neondatabase/neon/blob/fa504217c61bbcaf5c512d75830564541f917f8f/docs/pageserver-storage.md#L304-L355) |
| Git | 分支引用已有不可变提交。删除分支引用和回收底层对象有不同的生命周期。 | 用户删除窗口可以先完成逻辑删除。后台确认所有可达引用后再释放正文。PG 继续管理事务和索引。[Git 分支](https://git-scm.com/book/en/v2/Git-Branching-Branches-in-a-Nutshell) · [Git GC](https://git-scm.com/docs/git-gc) |
| Pi Durable 1.0.0，MIT，仓库固定版本 | 原生 `conversation.fork(at)` 在同一物理 Session 中创建 Conversation。原生 `asOf` 文档的来源是所选 Entry 所属 Conversation 及其提交，`current` 文档的来源是被 Fork 的 Conversation 在创建时的当前状态。Pi 自己处理 context edit、compaction 和工具配对。 | 目标是跨物理 Session。借用现有独立历史导入与 Storage 适配器，对照原生结果；独立子 Session 不应由父 Harness 内的原生子任务和目标 Worker 同时执行。[Pi 源码](https://github.com/earendil-works/pi/blob/a13d35a742c6ef8462812a28fbe1d8c8b7431c32/packages/durable/src/session/forks.ts) · [仓库的独立 Session 导入](../../packages/pi-session-postgres/src/durable/history-import.ts) |

**已有证据的范围。** 旧共享目录用 1 GiB、35,330 条 Entry 的夹具，单次首次 Fork 765 ms，冷读聊天历史 496 ms，冷读原生上下文 114 ms，均未调用模型。新 Run 级原型的 128 段测试，向上翻页时选择可见 Run 的语句约用 0.86 ms PG CPU。这些是不同实现、不同指标；目标方案的端到端性能待测。已有平衡目录、冻结 base 的 Run 级实现都可以作对照。[旧共享目录实测](../reports/single-path-shared-forks-20261030.md) · [Run 分页实测](../reports/flow-page-selector-20261124.md) · [既有选型](../adr/0286-shared-history-root-prototype.md)

**两个实际差异决定了改动范围。** 当前 #52 named executor 登记的是子 Run，利用父 Worker 的管理接口放置原生工作，父子共享一个物理写入者。另外，当前 `context="branch"` 取的是本轮父输入之前的锚点：父 Agent 在本轮读文件、跑测试得到的结果，不会自动被孩子继承。改成“委派前最近一次完整交互”会改变模型看到的内容。位置必须在工具意图中固定；收到重试时取父会话的最新头会造成漂移。[当前工具契约](../SUBAGENTS.md#direct-task) · [锚点来源](../../packages/sandbox-supervisor/src/durable/agent-session.ts) · [当前子执行器与回包](../reports/named-subagent-executor-20261123.md)

## 3. 建议的数据结构与索引

以下是逻辑字段。优先映射到已有关系、索引及幂等回执，最终 DDL 取决于原型实测。

| 关系 | 最少需要的内容与约束 |
|---|---|
| `sessions` | 租户、稳定 ID、当前可写 `flow_id`、视图版本、调度状态、Workspace/目录引用。Web Fork 记录来源以供导航；委派另有管理父 Session、稳定名字。管理父与历史来源是独立字段。租约仍由既有 PG 所有权机制签发。 |
| `runs`（逻辑 Turn） | 稳定 Turn ID、执行 Session、写入 Flow、队列位置、输入请求 ID/指纹、待放置输入、状态和时间。对租户/Session/请求去重；按 Session 和排队位置找下一轮。同一 Session 最多一轮活动，领取/续租主体是 Session。 |
| `flows` | 稳定物理历史身份、所属租户、只读或可写状态、当前所有者、原生 ID 分配与提交进度的既有依据。删除窗口后仍可保留源 Flow。 |
| `flow_segments` | 每条记录为一段**继承前缀的引用**：目的 Flow、段序、来源物理 Flow、来源 Conversation、可见 Entry 上下界、原始提交上界。当前 Flow 的可写尾段是隐含的。主键按租户/目的 Flow/段序，另建按租户/来源 Flow 的反向索引供 GC。Temporal 在 SQL 行里存序列化清单；这里建议用短关系行，以便定点找到消费者和做 FK 保护。 |
| `messages` 与既有原生文档/附件 | 每条新正文归一个物理 Flow、Conversation、Turn。保留原 Entry ID、原提交序号和批内位置。按源 Flow/Conversation/Entry 做范围和点读，按 Turn 取可见头部及依赖，按源位置用现有唯一键抑制重放。配置、附件和摘要各按其原有语义保存。分支数量不进入 Projector 的普通写入路径。 |
| 委派操作与等待 | 复用 named executor 的幂等创建、请求回执、待交付结果，目标改成稳定子 Session 和本次 Turn。名字在租户和管理父 Session 内唯一；重复请求返回同一孩子和已冻结的历史/授权，不同参数继续报告冲突。 |

**索引如何选。** 首版至少检查四条真实查询的计划：(1) 按租户/就绪状态/排队次序批量领 Session，租约的当前所有者唯一；(2) 按目的 Flow/段序从游标倒读，按来源 Flow/边界反查仍活着的引用；(3) 按来源 Flow/Conversation/原生 Entry 找锚点、文档版本和可见压缩标记；(4) 按来源 Flow/Conversation/展示位置取本页，再按已选 ID 精确取正文。历史读取与 Worker 原生上下文的排序各依其既有契约。已有等价索引就复用，把额外索引的写入和 WAL 成本列进对照，避免为每一种界面操作各建一份正文索引。

**用户首条消息。** Session API 在事务里登记 Turn、待执行输入和就绪状态。Pi 接受输入后，经 Session Kafka 与 Projector 落一条正式用户 Entry。用原输入 ID 对应到 Turn。首版在同一 PG 事务里写正式记录和附件引用、登记其位置并释放暂存正文，保留指纹供重试；即便异步投影的事务在故障后丢失，这几项一起回退，已同步接单的暂存输入仍可重建。崩溃前后只能有一条模型可见输入。已接受的 steer/follow-up 仍进目标 Session 的 Pi Inbox。历史读取需要保留被引用 Turn 的最小头部（含原首输入位置）；禁止删除源 Session 时通过外键连带删掉共享正文、Turn 头或附件。对于在 Turn 中途截止的分支，仅显示可见前缀，不泄漏源 Turn 后来才产生的最终状态、标题或结果。[现有历史读取入口](../../packages/runtime-core/src/root-message-history.ts)

**为什么同时保存 Entry 和提交界限。** Entry 按 Pi 顺序读；提交界限规定建分支当时哪些记录已经生效。预留 ID 100、101，101 先提交；这时建分支，100 后提交，旧分支仍看不到 100。相同提交内的文档按原子提交解释，`ordinal` 标识批内写入。每段携带自己的来源和上界，跨物理 Session 不比较裸 ID 或不同 Kafka 分区的 offset。新 Worker 用独立的写入序号和 ID 分配状态；用已捕获的分配高水位保守预留继承的原生 ID 空间（含文档），避免为求最大 ID 扫描终生正文。不同孩子可以分配相同的本地数字，物理流隔离它们。[已有 ID 与版本约束](../adr/0287-single-path-shared-forks.md#acceptance) · [配置可见性](../adr/0345-ordered-agent-configuration.md)

## 4. 创建、领取和结果回到父 Agent

**统一创建操作。** 输入包含目标归属、幂等身份、来源窗口版本、精确历史锚点、启动选项及可选第一项任务。先验证租户、对象权限、锚点在请求者可见的这条路径内。锁定来源视图版本，依段序取其清单，保留锚点之前的段并截短包含锚点的一段；落点若在祖先，舍去中间分支后续的全部段。插入目标 Session、新 Flow、冻结段和必要的原生基线。网页 Fork 提交后返回新地址，状态为空闲。委派在同一事务插入名字/操作回执和第一轮输入，并令新 Session ready。`fresh` 走同样流程，继承段数为零。事务成本随实际段数与文档基线增长；不扫描或复制每条历史消息，不批量保活、不启动沙箱、不发模型请求。

**两种入口。** 网页从 Session API 直达 PG，经幂等键返回目的 Session。子 Agent 沿父 Worker 已校验的工具意图 → 父 Session 的 Kafka 分区 → Projector 准入与派发许可 → named executor → PG 创建事务。父 Worker 将锚点与调用身份随同原意图提交。执行器在创建事务中验证原控制意图仍可执行；父 Turn 已关闭接单、已撤销或越权的迟到首创不能开新工作。父工具等待超时本身不能证明创建失败。子 Session 一旦建立，其历史引用不随父 Worker 重启而改写。执行器在创建落库之后确认接单，按现有工具契约在结果或问题就绪时给父调用回包；超时向模型报告未确认，允许按名查询。停止或重复的父调用不会自动二次创建。每个孩子独立领取，但父 Pi 当前的同步工具调用仍遵守等待结果或问题的契约；独立租约不使这次调用自动提前返回。子完成走**子 Session 路由到的 Kafka 分区及其 Projector**；从已确认的子 Turn 终态幂等唤醒等待者，再经现有回包路径由父 Pi 写正式 Tool Result。父历史只收父调用的结果，不吸入子私有正文。

**允许独立领取的时刻。** PG 内登记可领取 Session，沿用定向唤醒、批量领取和 Worker 空位。Worker 领取 Session（可用既有 `SKIP LOCKED` 队列模式），锁内选择该 Session 的最早可执行 Turn、分配 epoch，把任务和归属原子返回。Worker 可同时运行多个 Session，空闲孩子没有租约。沿用每个 Worker boot 的 Kafka producer；各 Session 独立 fencing、分区有序、故障封口和恢复。父子的分区可以相同或不同，系统不能利用二者 Kafka offset 的数值先后来判断因果。PG 所有权、按 boot 合批续租、Projector 的接受视图和本地失效停止仍须保持一致；专门测从“每个家族一份活跃租约”改为“每个活跃 Agent 一份”带来的行更新/WAL，批量 SQL 减少往返不等于减少行写入。[现有租约实现](../../packages/runtime-core/src/session-lease-coordinator.ts) · [定向唤醒实验](../reports/directed-worker-wake-20261119.md) · [PG 队列读取](https://www.postgresql.org/docs/17/sql-select.html#SQL-FOR-UPDATE-SHARE)

**防止子 Worker 抢跑。** 既有正文投影允许异步提交。第一版把分支元数据、历史引用、回执、子就绪状态放在同一 PG 事务域，用同步提交。只接受已通过 fencing、在 PG 能精确读取且达到所需提交界限的来源；对于委派，还要校验对应父意图已投影。PG 尚未追到则保留/重试同一控制请求，不能截到“上一次能看见的消息”。在同一 PG 实例，同步事务刷 WAL 到自身提交位置，也覆盖更早的本地 WAL。崩溃测试须验证；不同实例、异步 standby 没有这项跨实例保证。同点并发创建还需测同步刷盘对异步投影的影响。未来若拆控制库和正文库，先由历史权威同步确认幂等冻结凭据并持有引用，再由控制库使 Session ready，失败时依据创建回执核对并释放孤立引用；该跨库协议另设验收闸门。[PG 异步提交的持久性语义](https://www.postgresql.org/docs/17/wal-async-commit.html)

**避免“所有父亲都在等，孩子却没有槽位”。** 独立领取后，哪怕释放了模型并发许可，64 个正在等孩子的父 Session 仍可能占完 64 个 Worker 执行槽。建议为**只剩可恢复委派等待**的父 Turn 提供停驻：先登记持久等待和准确工具身份，经过有序写入边界停止这个 writer 的新工作，释放执行槽。待办仍是同一 Turn，主动停驻不能走“Worker 失联，Bash 补 unknown”的路径。结果、问题、Stop 或原期限到达都能唤醒同一 Turn；先登记等待再检查已存在结果，避免先完成后睡下。若还有模型或不可安全挂起的工具在跑，不能强行丢弃它们。原型必须证明在总容量 1、全部父任务等待、以及混合并行工具条件下孩子能取得容量；混合情形如需执行容量预留/准入策略，应先提交证据评审。通过前不要开放无界嵌套。独立租约也不自动给子 Agent 开放 `subagent` 工具。[当前等待和结果机制](../reports/named-subagent-executor-20261123.md)

## 5. 从浏览器的 Session ID 读到正文

以 R 的继承清单 `[P:A…C, Q:F1…F2]` 和本地消息 `R1,R2` 为例。P 后来的 D、E，Q 后来的 F3，以及 R 的兄弟均不在范围内。

1. **最新一页。** 带租户身份用 `session_id=R` 读可写 Flow 和版本。优先从 R 本地的展示索引倒序取轻量位置描述，满足条数和字节预算时停止。本地不足，再按段序倒查 `flow_segments`，从 Q 的 F2、P 的 C 向前取。只取入选的正文，工具行有需要才按已知 ID 补配对依赖。首次请求优先满足本地尾段，已有足够内容时不应查询祖先正文。读到继承 Turn 的首输入或工具参数，要用其物理来源，仍向网页返回当前窗口 R。消息级可见性要先于 Turn 展示；不能先 JOIN 一个大 Run 的全部正文再 LIMIT。
2. **向上翻页。** 游标标识目的 Session、视图版本、段序、物理来源、段内位置与方向；服务端重新校验归属和范围。在上一页停下的段内用索引续读，耗尽该段才换段。仅有新尾部追加时旧的向上游标继续有效；裁剪或重绑历史视图后明确返回游标已过时，让客户端重新定位。查询元数据与取正文本页共用一个读快照。首屏、上一页、单条定位、导航与导出共用同一可见范围判定。[已有的按预算取正文](../../packages/runtime-core/src/root-message-history.ts) · [现有分页对照](../reports/flow-page-selector-20261124.md)
3. **实时输出。** 保留当前 Session API/Projector 的 PG 已提交覆盖位置和 SSE 接续协议。R 只订阅 R 当前写入者的实时输出；继承段只提供冻结内容。刷新时交叠去重，不能丢掉“最后一条正文已经写 Kafka、但尚未跟 PG 页接上”的区间。历史 Fork 不要求把父亲的 `pi.live` 或 SSE 重放进新 Session。
4. **Worker 冷开。** 从冻结源和本地尾段中选最近一个**在该路径可见**的 Pi Compaction，读摘要、它指向的保留区间及后续有效 Entry，再由原生 Pi 处理 edits、遗漏/替换、工具顺序和上下文。父亲在分叉点之后做的摘要对子会话不可见；完全无摘要时要承担真实模型上下文的读取成本。首版比较按段索引反向找标记和冻结时保存“最近可见标记的来源＋保留起点”两种做法。允许缓存这个可验证的定位信息，避免引入第二份摘要正文。[Pi 上下文代码](https://github.com/earendil-works/pi/blob/a13d35a742c6ef8462812a28fbe1d8c8b7431c32/packages/durable/src/harness/context.ts) · [现有冷读实验](../reports/candidate-context-and-prune-20261123.md)

一个完整例子：目的 R 的元数据给出本地流 r 和只读区间 P[A…C]、Q[F1…F2]。假设每页放三条，PG 从 r 取到 R2、R1，只需再从 Q 取 F2，网页按时间顺序显示 F2、R1、R2。下一页从 F2 之前继续，取 F1、C、B；这两页都没有读取 P 的 D、E。若用户在孙会话 R 内对祖先 B 再 Fork，新会话只有 P[A…B] 和空本地尾段。即便从未提交第一轮，这个新会话的引用也已经保护 B。

查询形状示意。参数均由本次授权后的可见段产生；下式是选位置的逻辑，不是可执行迁移。完整实现还要支持展示顺序、保留头部和跨页工具依赖。

```sql
-- 已读完自己的尾段，下一步从最近的继承段继续
SELECT segment_no, source_flow_id, source_conversation_id,
       first_entry_id, last_entry_id, through_commit
FROM flow_segments
WHERE tenant_id = $tenant AND destination_flow_id = $flow
  AND segment_no <= $resume_segment
ORDER BY segment_no DESC
LIMIT $segment_budget;

-- Pi 原生历史的一段。网页使用自己的展示位置索引，带相同的范围约束
SELECT entry_id, commit_seq, ordinal
FROM messages
WHERE tenant_id = $tenant AND physical_flow_id = $source
  AND conversation_id = $lane
  AND entry_id BETWEEN $lo AND $hi AND commit_seq <= $frozen_commit
  AND entry_id < $before_entry
ORDER BY entry_id DESC
LIMIT $remaining;
```

**数据代价。** 用 D 表示目标点实际继承的段数，B 表示历史正文总字节。短清单 Fork 创建约写 O(D) 个范围及原生基线，正文复制目标为 0 B。已存在的兄弟数量不应进入创建和正常追加的查询；唯一索引查找、锁等待和 WAL 仍有真实成本。多年会话的一次读取只取命中的段、位置描述和入选正文；过滤历史撤销、跨段配对或大附件会增加工作量，应用 EXPLAIN 和实际返回字节验证。跨祖先再次 Fork 应截到所选段；空的本地尾段不额外生成继承段。海量孩子首次冷开会重复读取热门祖先、发送模型上下文，要分别测 PG 读取与模型请求，不能用“零正文复制”推导出“启动免费”。

## 6. 原生 Pi、Workspace、Stop 和删除

- **新建与恢复。** 创建子 Session 时挂只读继承历史和小型启动基线；使用现有跨 Session 导入/Storage 适配器作为语义对照。Pi 的 ID 可用于 context edit、摘要保留点、工具配对和文档引用，不能简单重编所有消息号。为新 Session 固定分配高水位和版本命名空间，先验证继承祖先再二次 Fork 时也没有碰撞。旧源流的写入凭证、活动 Task、Inbox、进行中的 Bash、模型连接和回包等待都属于原 Agent。重新接手**已有**子 Session 时，恢复其自己的活动 Turn、Inbox、任务和未结算操作。现有新原型普通 Bash 在故障封口后补 unknown；其他工具按各自的恢复政策处理，回包的七天窗口沿用已讨论的规则。[Bash 接手](../adr/0359-sealed-bash-unknown.md) · [回包保留](../adr/0361-stop-context-and-mailbox-retention.md)
- **配置与能力。** 对 `pi.agent` 采用锚点的原子提交版本；对标为 `current` 的文档，冻结发起 Fork 的 Conversation 在本次切面上的版本；`initial` 走新 Conversation 初始化。子会话获得自己的运行状态。冻结的历史配置不能扩大当前模型、工具、Workspace 权限。保留文件目录/共享或临时计算资源的产品选项和续聊授权交集；不复制 Workspace 文件、不按历史地址路由新工具，不继承旧 VM 的运行承诺。第一轮拿当前资源管理结果形成环境提示。两个独立 Worker 同时用同一 Workspace 时，分别记录和撤销各自的绑定，仅取消本 Session 的操作；要把“两个 Session 共用一台 Guest”的跨分区路由、保活和停止纳入测试。[文档 Fork](../adr/0345-ordered-agent-configuration.md) · [计算代际](../adr/0347-conversation-compute-continuity.md) · [授权交集](../adr/0351-serving-delegated-history.md)
- **会话互通。** `list/status/wait/send/cancel` 按管理父子关系和租户权限查稳定 Session，`send` 向已授权的当前 Turn 投 steer/follow-up；已错过的投递返回明确结果。显式继续已完成的孩子，在同一子 Session 创建下一 Turn。历史上看到以前的子 Agent 名字不会让网页 Fork 获得它的管理权。子结果已经成为父 Tool Result 时，该结果属于父历史，可随父路径继承。附件、粘贴原件和图片通过可见 Entry 的持久对象引用保留，下载走当前会话鉴权。继承历史进入 `/status` 估算；`/usage` 按实际产生请求的 Session 统计，重复 Fork 不会重复计费。[用量口径](../adr/0342-context-status-and-gateway-usage.md)
- **父等待与停止：原提案中的级联选项，待审。** 父 Turn 正常收尾继续等它所管理、仍未结束的本轮委派；只等待时可以停驻。用户 Stop 父 Turn，记录停止意图，并按委派关系向这些子 Turn 发幂等 Stop；若未来放开嵌套则逐层传播。父 Stop 和迟到创建通过同一管理侧关闭状态串行化，已创建者纳入取消，尚未准入者拒绝。递送可以重试；某一 Guest 失联不阻塞用户永久等待，也不能宣称物理进程退出。父 Worker 故障时，在另一台 Worker 上执行的子 Session 继续；若父子恰好在同一故障机器上，各自封口并按自己的 Session 恢复。对单个子 Turn 按 Stop 只停止该项并向等待方报告。网页 Fork 为平级，父 Stop 不影响它。父自然结束先关闭本轮新增委派的准入，再确认已登记的受管子 Turn 均已收尾，避免“检查为空”和晚到创建竞态。是否允许有意脱离父 Turn 的后台子任务是另一个产品决定。Temporal 也把父执行关闭后的子行为建模为单独策略。[Temporal Parent Close Policy](https://docs.temporal.io/parent-close-policy) · [现有原型的关闭检查](../../scripts/experiments/minimal-session-storage/delegated-runs.sql)
- **保留历史与回收字节。** 用户删除窗口或裁剪后续，要求该目标 Session 满足既有静默/封口前置条件，并验证期望视图版本。裁剪保留同一网页地址，切到新可写 Flow 和裁剪点的段清单，不原地改共享正文。删除/裁剪事务同步更新可见根、撤掉旧写入资格、登记后台清理；按既有策略保留必要的审计和去重。GC 按来源 Flow 的反向引用，保护仍被其他 Session、冻结文档、附件、活跃读取或正在恢复的任务引用的区间。删除 P 后，R 可以继续读 C；无人引用的 D、E 以后分批回收。删 R 后再释放原公共区间。在同一 Flow 内允许保守延迟回收，长期目标是独有后缀最终释放。不要为一次删除同步扫描整棵家族，也不要给普通消息追加逐分支计数。
- **防止创建和 GC 交错。** Fork、裁剪按确定顺序锁目标窗口/源 Flow 的保护行，在事务内复查锚点可见性和版本，插入全部区间引用及附件/文档保留凭据；多个合法读者可持共享锁。GC 对准备回收的 Flow 取冲突锁，在拿锁后的有效快照**重新**查根和反向区间引用，分批处理。FK 防止删除仍被引用的 Flow；删除其中一些消息行还必须检查区间覆盖。GC 先赢则过时请求失败并可重新选点；Fork 先赢则 GC 保护新引用。把 PG 内行清理与对象存储物理删除分开，复用现有对象删除确认。历史链接可以指向已删除的来源说明或墓碑，不能依赖仍可打开父窗口。建议“删除父 Session”默认只删所选窗口，独立孩子的会话仍受自己的 ACL 约束；批量删除整棵委派树应是显式操作。

## 7. 故障与验收清单

| 注入点或竞态 | 必须观察到的结果 |
|---|---|
| 同一网页请求重复；同一名字、相同或不同参数并发创建；父工具回包丢失 | 相同操作最多建一个孩子和一个首输入；重试回同一身份。名字冲突可查原身份。创建事务回滚后不留下可领取的空壳；已删除的旧身份不能被迟到的原操作重新创建。 |
| 父流 Kafka 已接受但 PG 还未物化；PG 异步提交后崩溃；子分区先恢复 | 只有冻结源与委派准入可以证明时才调度孩子；恢复按分区进度重放，没有半份历史、幽灵子会话或重复派发。回放不能创建已过期的父意图。 |
| 旧 writer 在封口后迟到；父/子分别失联；两台 Worker 竞争同一子 Session | 每个 Session 只有一个被 Projector 接受的写入代际。同一 Turn 可由继任者继续，旧写入无法改变另一个 Session。父子分别恢复各自的控制等待、普通工具和 Inbox。 |
| 已写子结果，父刚要停驻或已换 Worker；子发 supervisor 问题 | 等待者最终见到正确交付或确切的未知/超时；不漏唤醒，不重开已结束 Turn，不重做外部工具。不将控制面的接单 ACK 写成最终 Tool Result。 |
| P、Q、R 多次混合 Fork；在 R 内选 P 的 B；父分叉点之后产出或压缩 | 各分支的网页历史、模型上下文与逐条构造的可见路径相等；新分支不含 Q/R 后缀、兄弟内容、父 Inbox 或未结算的等待。对退回队列、图片、上下文编辑和历史配置做同样断言。 |
| 并发 Fork／删源／裁剪／GC；请求重试；打开旧分页游标 | 事务要么完整成功，要么报告锚点失效。已接受分支可冷打开。旧游标不越界。验证先删父、先删子、最后一个引用消失，分别检查行与对象字节。 |
| `fresh`、已封口手动 Fork、共享 Workspace、沙箱中途回收、子权限缩小 | 新会话不接管旧进程、取消目标、续租权或凭证。首请求环境信息准确，历史链接不扩大权限。 |
| 所有可用 Worker 槽位被父占用；Stop 与孩子完成同时发生 | 在无新增机器的情况下仍有可证明的进展；原终态与通知幂等，父 Stop 不误取消平级 Fork。禁止在热路径查另一台 Worker 的租约来弥补协议缺失。 |

**性能实验。** 在同一硬件/电源模式、相同数据与索引、相同 RF3 Kafka 和 PG 池配置下串行交替 A/B，至少三轮，分别跑冷缓存和热缓存。第一组与现有 Run 级/目录读对照；独立调度的代价另开一组，不把两项差异相互抵消。固定模拟模型、50 ms 接单延迟的 20 工具录制负载供投影对照；同步记录可见消息/s、P95/P99、积压增长及停止输入后的清空时间。分子只含按同一口径计数的用户可见正文、工具调用和结果，另报原始 Kafka 记录/s。此前在该开发机的 475 条/s 输入下测到约 473–476 条/s，1,200 条/s 的实验已有积压；均只作同负载参照。[投影测量口径与原始表](../reports/bash-intent-log-20261125.md#cost-of-each-change)

| 主负载 | 具体做法 | 单独报告 |
|---|---|---|
| 多年主线、按历史位置人工 Fork | 十万条以上记录；10 MiB、100 MiB、1 GiB；近端、远端、压缩前后及已继承的历史点；多次裁剪与继续 | 创建事务延迟/锁等待/SQL 行数/WAL；正文读取和复制字节；首屏及连续翻十页的 HTTP P95/P99、PG CPU/页与实际取出的正文量 |
| 宽扇出 | 从同一冻结点同时登记 1、10、100、1,000 个孩子；另测多个父源分散创建。独立测试“只登记”“等容量领取”“首次 Pi 上下文可用”；不伪造 1,000 台沙箱 | 同点竞争、实际段引用数量、每个首输入只放置一次、唤醒放大、租约更新行/s、限流后的进展。模型时延/流量分开报 |
| 实际混合深度 | 1、4、16 层交替人工/工具 Fork，每段几十到几千条消息；加空子分支、从孙会话回祖先旧点、同级兄弟持续写 | 初次和第二页分别访问多少段、普通 append SQL/行、稳定吐量、冷恢复耗时分解、活跃上下文大小。128 层作为回归，1,000 层单消息链只做压力边界 |
| 回收与竞争 | 一边投影一边真实浏览器向上滚；后台分批回收独有后缀；读者进行中删父；少量长尾图片/大工具结果 | 用户删除返回时间与实际释放时间分报，GC 对写入与读 P99 的影响，附件残留/误删，跨租户隔离 |

审校建议的硬条件是：公共正文复制 0 字节；正常追加的写集合不随已有分支数扩大；分页正文量受页面预算约束（必要的少量精确依赖单列）；冷恢复读取量能用可见上下文和真实任务依赖解释；所有并发与崩溃用例通过。延迟预算需在第一轮成对基线后共同确定。若出现稳定的两位数百分比回归或持续新增积压，先交出分项归因，沿用既有上线暂停约定。这些是未来实验，不是本文的实测成绩。

## 8. 实施顺序、可撤回的边界与审校决定

| 阶段 | 评审通过后才做的工作 | 交付/是否继续的依据 |
|---|---|---|
| 0 | 把两种入口、锚点、父关闭策略、权威事务边界写成新的候选 ADR。固定本分支对照和原始 trace，列出合同差异 | 用户审定下列决定；现有公开 API 的默认行为有明确映射 |
| 1 | 先做独立 schema 和 PG 分支适配器，接入真实 Pi 的只读继承＋本地追加。复用现有 Storage 符合性测试、引用/图片保留测试 | 两级 Fork、祖先截短、乱序 ID、文档三策略、压缩、冷开、剪枝 GC 通过。另做一项小探针：单槽位父等待子能否安全停驻；不可行先复审 |
| 2 | 接网页 API、游标/导航/首输入、SSE 首屏接续和同一 Flow 的权限过滤。先跑长期主线及混合分支读写对照 | PG 读取与正文预算有分项，主线普通写入不额外维护子分支。保留冻结 base、旧共享目录作为只读实验对照 |
| 3 | 把领取单位改为 Session，复用定向唤醒、租约和封口。接新 executor 入库、独立子 Worker、等待/唤醒、跨 Session 通知和 Stop | 两个独立 Worker 进程、同/异 Kafka 分区分别跑 `fresh`/`branch`，看真实模型输入（可用本地录制模型）；共享沙箱、回包丢失、父/子单独故障、容量 1 到满载全部通过 |
| 4 | 跑上表宽扇出、读写混合和 GC，复核现有 `/status`、`/usage`、附件、页面及 Guest 端到端。核对 Session 独立租约的 PG 成本 | 提交结果和差异；正文零复制不抵消领取长尾或冷读放大。达到商定闸门再提交发布申请 |
| 5 | 单独审查 matched cutover：暂停新接单、排空或有序封口全部旧 writer，处理在途委派/回包，建立新读写边界再放行 | 对现有数据列出可转换/可重建/需明确清除三类。核对原生 ID、只读可见性、对象引用、账号和配置。允许的开发期清理放到单独发布步骤。旧协议不能消费新格式。新 writer 已产生数据后，只允许经验证的转换/前滚或隔离新数据，禁止简单切回旧二进制读取新数据 |

主要涉及的代码边界如下，供分阶段评审：

- 存储与生命周期：[当前 Flow 读取](../../scripts/experiments/minimal-session-storage/flow-base-admission.sql)、[历史导入](../../packages/pi-session-postgres/src/durable/history-import.ts)、[PG 目录适配器](../../packages/pi-session-postgres/src/durable/postgres-history-directory.ts)、[原生 Storage 测试](../../packages/pi-session-postgres/test/durable-storage-conformance.test.ts)。
- 会话读取：[Session API](../../packages/control-plane/src/conversation-reader.ts)、[正文与工具配对](../../packages/runtime-core/src/root-message-history.ts)、[上下文读取](../../packages/control-plane/src/projected-context.ts)。
- 调度、委派与执行：[租约协调](../../packages/runtime-core/src/session-lease-coordinator.ts)、[子 Agent 登记](../../packages/control-plane/src/named-subagent-store.ts)、[执行与交付](../../packages/control-plane/src/named-subagent-executor.ts)、[Worker Session host](../../packages/pi-session-postgres/src/durable/session-host.ts)、[Worker 冻结调用参数](../../packages/supervisor-host/src/tools/worker-tool-runtime.ts)。沿用执行侧取消原操作与沙箱路由，独立 Agent 也不持有沙箱管理凭据。

### 第一次审校结果

1. **已确认：工具 Fork 的冻结点。** 选择“本轮中发起委派的那次模型请求，进入模型时已有的稳定历史”。例如父亲先拿到 `npm test` 的正式失败结果，下一次请求决定分工：孩子看得到本轮用户输入和测试失败，看不到这次尚未成对的委派调用，也不接管同批其他工具。冻结该次请求的原生范围边界，不能把 HTTP 重试、父补充消息或晚到的结果带进来。验收时与目前的“本轮输入前”对照，检查父/子实际请求内容。
2. **已确认：网页 Fork 的入口。** 第一阶段沿用原型的入口限制，只允许在可见且已完成封口的普通 Turn 末尾点击（源会话后来正在运行另一轮也可以选更早的已封口点）；允许选主会话或有权访问的子会话，Fork 得到平级 Agent。底层从第一天就用消息锚点，保留已纳入该 Turn 的 steer/follow-up。以后若开放 Turn 内单条已提交消息，要另定未配对工具、可见头部和网页按钮语义；这不阻碍工具从活跃父亲的稳定位置 Fork。
3. **待审：父 Turn 的 Stop 范围。** 选项一只打断当前父 Turn 及其等待，已启动的子 Turn 继续。选项二同时向本轮仍在运行的受管子 Turn 发送 Stop（第 6 节原提案）。两者都须防止迟到结果改写已中断的父 Turn。父 Turn 自然完成、Worker 故障和删除会话各自有独立语义。

Codex CLI 普通 Esc 的源码对照：终端为当前 thread 和 turn 发出中断请求，Core 取出该 Session 的 active turn 做取消，V2 的 wait_agent 订阅活动通知。整棵运行时的挂起/关闭另有入口。[终端路由](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/tui/src/app/thread_routing.rs#L789-L812) · [Core 中断](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/core/src/tasks/mod.rs#L679-L731) · [等待工具](https://github.com/openai/codex/blob/rust-v0.172.0/codex-rs/core/src/tools/handlers/multi_agents_v2/wait.rs#L68-L97)。

隔离复验使用真实 Codex CLI 0.167.1、0.172.0 的 App Server，各跑一次。用本地模拟模型触发父线程调用 `spawn_agent`，让子线程的原始模型请求保持打开。确认父线程已开始 `wait_agent` 后，向父线程及其 Turn ID 发送终端 Esc 使用的 `turn/interrupt`。两个版本均先报告父 Turn `interrupted`；之后才放行孩子的响应，原子 Turn 提交了最终回答并报告 `completed`。键盘映射查源码，运行时行为走实际 RPC。此项核对覆盖已启动的普通多 Agent V2 子线程。
