# #52 · Worker 与 Projector 同时丢失后的原工具恢复

## 实施范围

在性能分支把原型交接接入正式的 Session Projector 与 Worker 会话宿主入口。隔离部署使用实际 Session HTTP API、RunExecutor、PG 租约与过期扫描、Kafka 三副本 Session 日志、固定版本 Pi Harness、Cube VM 和 Guest HTTPS/Kafka 回包。模型使用确定性回复；旧 Worker、新 Worker、旧 Projector 均有独立进程。替换的 Projector 在监督进程中启动，其原生状态、路由缓存、消费客户端均重新创建。跨进程保留的是测试断言、真实外部 Guest 和发布后的数据库/日志，未把旧 Projector 的原生状态或路由缓存交给新实例。

- **写入。** 已获准的原生批次按次序更新 Projector 的当前家族状态。Entry 正文和 Inbox 内容沿用原型原表。恢复封口时，当前 Conversation、Task、Submission、其他原生文档及展示序号保存在 Run 的一个可空交接字段里。写入核对当前 Run、租户、writer、代次、封口请求和原分区，校验待回包操作与原 Task，并要求同步 PG 提交。候选分区进度、旧身份退出及同一 Run 回到队列在一个控制事务提交。普通提交没有为这项交接另加 PG 写请求。Run 完成时回收交接数据。
- **冷读。** Worker B 由正常领取入口取得新身份，通过 `PostgresDurableSessionHost.open` 的候选存储装配启动。在一个只读、可重复读的 PG 快照中，核对当前身份、已封口释放的旧身份和原位点，读取正文、当前 Inbox、交接数据和原待回包引用。Pi 的提交序号、保留了但未写入的原生 ID、子会话归属及排队请求 ID 得以恢复。B 只为原操作重新监听其回包 Topic，沿用原绝对截止时间。B 的冷读角色无权查询旧原生表。原 Session Kafka 前缀在 B 启动前已从 broker 移除。
- **故障边界。** 测试先等 VM 中出现执行标记并确认子 Inbox 的编辑，然后 SIGKILL A 和独立 Projector。新 Projector 用同一消费组重建。第一次封口在写好候选快照后强制抛错。重试通过另一 PG 连接看到 Run 尚未入队、仍带旧 lease、快照为空、候选游标未越过封口；第二次提交才放行。网页验收读取父、子会话的实际 HTTP 历史，另在候选 PG 中独立核对子 Agent 的规范 Bash 结果与排队输入的放置条数。

## 四种故障时序的实测

| 场景 | Run 终态 | 原 Bash 实际执行 / B 重新派发 | B 冷读至可接手 | 其中恢复原回包邮箱 |
|---|---|---:|---:|---:|
| B 开始等待后 Guest 才回包 | completed | 1 / 0 | 3.127 秒 | 3.097 秒 |
| B 接手前已到两份相同回包 | completed | 1 / 0 | 4.088 秒 | 4.058 秒 |
| 等到原截止时间，真实结果迟到 | completed | 1 / 0 | 3.085 秒 | 3.054 秒 |
| 封口后、B 领取前点 Stop | cancelled | 1 / 0 | 0.029 秒 | 0.000 秒 |

每种时序一个样本。前两例父模型收到子 Agent 从真实 VM 读出的随机值，子会话只有一条最终 Bash 结果。原截止时间过后按既有探针策略形成 UNKNOWN，迟到结果未覆盖它。Stop 例 B 没有请求模型，待发输入由 Pi 收为 unanswered，只向原 Guest 发了一次取消。四例均核验原生排队请求 ID、原 Topic 与截止时间或原操作取消目标，以及最终界面历史。冷读计时从已领取的 B 开始恢复到原生状态和回包监听就绪；不包括 Worker 进程启动、其自身邮箱的初始连接、等待旧租约到期。本组交接状态的 UTF-8 JSON 均为 7,120 字节；这个数字并非 PG 物理占盘或 WAL 大小。

[本轮脱敏汇总](atomic-family-handoff-20261122.json)。曾在调试时跨两个未匹配的原型修订运行进程，该轮因缺少新的 head 坐标被拒绝。表中采用冻结同一份源码后的完整四例。

## 冷读取的范围和发布边界

封口保存了各 Conversation 原生 head marker 的编号与保留下界。Worker 按活跃任务及其创建的子 Conversation 选择上下文，沿继承关系使用创建时的截止位置，并单独读取待执行工作直接引用的较老 Entry。PG 回归覆盖了父会话最新压缩点、子会话继承的更早压缩点、压缩前的工具引用及不活跃旁路。故意删除仍被引用的 Entry 时，读取失败。测试也覆盖错误身份、错误分区、悬空回包引用、原生 ID 保留空洞，以及包含 NUL 和未配对代理项的当前文档。

当前范围是 fresh physical Session 中首个根 Run 的首次 writer 交接。测试组合使用正常的 Worker 会话宿主及其发布与租约逻辑，Workspace 工具适配器、模型端点和 Worker 管理调用由隔离夹具提供。公开历史接口、普通原生接纳与控制面仍有服务版投影参与。实际双进程故障样本是父子会话、单个执行中的 Bash；复杂多工具等待、完整 Pi 文档生命周期、长会话中的连续多代交接及加入当前状态折叠后的常驻内存和吞吐仍需验证。没有切换线上默认存储，也没有将旧的独立性能样本当成这条组合链路的容量。

## 如何复现

在开发集群旁串行运行 [atomic-handoff 场景](../../scripts/experiments/minimal-session-storage/README.md#atomic-handoff-after-loss-of-the-worker-and-session-projector-52)。日常传输回归 `npm run test:transport` 包含隔离 PG 中的交接入库、归属、指针、压缩范围和特殊字符检查。正式默认存储继续运行自身的 Pi storage conformance 和 Worker 生命周期套件。本次未执行新旧版本性能对比。

最终四例均通过，同一源版本的上一组 Bash 字段恢复回归也通过。独立查询 Cube 后，具有本实验工作负载标记的 VM 数为零。RF3/PG 传输套件通过 22 项；另行串行运行的 PG 交接、默认存储一致性及 Worker 运行时三个套件通过 82 项（含重叠的交接测试）。三个涉及的 TypeScript 包和文档检查通过。
