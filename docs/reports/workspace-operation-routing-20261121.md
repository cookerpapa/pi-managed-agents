# #52 · Worker 按操作编号执行与取消

原型 Worker 在 Pi 提交完整工具意图时公布操作编号、Pi 任务编号和原回包位置。参数来自同一次原生提交。Stop 写入同一条 Session Kafka 链路，取消描述只有 action、operationId 和 taskId。A 启动、B 接手、原生状态交接以及 Worker 的会话日志冷读均不传沙箱绑定。B 继续监听原回包 Topic，沿用截止时间，也可以直接按原操作发起取消。

## 执行侧如何找旧目标

执行侧按 Run、Conversation 和 Session Kafka 分区保存资源选择生效的位置。举例，在位置 10 开始使用甲，在位置 40 改用乙，位置 20 登记的命令就一直属于甲。已有待回包记录保留了该命令的位置。执行侧用操作和任务编号核对原记录，再用原位置查到当时的 Guest 绑定和 writer。目标不再可达时，物理取消可以未确认；原生 Stop 可以收尾。禁止转向当前 Guest 补发命令。

只在首次建立或切换绑定时同步提交路由边界。原型使用隔离 PG 中的 workspace_route_spans 表模拟资源管理的持久记录；同一绑定上的后续调用使用分区所有者的内存视图，原有异步投影与 Kafka 外部效果许可仍各有职责。取消与原生终态同批提交时，Run 会短暂保留原操作位置，以便移除公共 waiter 后仍能核对取消。Run 收尾清理这些位置与绑定记录。

## 隔离联调结果

| 场景 | Run 终态 | 原 Guest 实际执行 | 物理取消请求 | B 新发 Bash |
|---|---|---:|---:|---:|
| 原 Worker 正常完成 | completed | 1 | 0 | — |
| 原 Worker 正在执行时点 Stop | cancelled | 1 | 1 | — |
| 新 Worker 挂回原邮箱后才回包 | completed | 1 | 0 | 0 |
| 同一回包在接手前已到两份 | completed | 1 | 0 | 0 |
| 超出原期限，迟到回包随后送达 | completed | 1 | 0 | 0 |
| A 已失联，当前选择切到另一 VM，再由 B 处理 Stop | cancelled | 1 | 1 | 0 |

两类 Stop 都得到已确认的原生取消，Stop 之后模型调用为零。重绑定的场景用新路由对象从 PG 找回旧目标，检查到新 VM 没有运行这次命令。正常工具回包和 Worker 的原生交接记录均不含执行侧绑定。四种交接都在移除已封口的原 Session Kafka 前缀后启动 B。每个场景一个样本，本轮未测峰值吞吐。

## 验证边界

测试中的 Pi、Worker 进程、PG、Kafka、Cube VM、固定 Guest 和 HTTPS 回包服务均使用实际实现，模型固定返回测试任务。执行适配器从独立资源登记处选择已准备好的 Guest。切换资源后重新创建路由对象，清空内存；Stop 场景另备一台实际 VM，原操作的回包和执行标记均从第一台核对。新 VM 的执行标记须为空。此项验证覆盖执行路由冷启动；整个集群同时宕机的恢复另计。

超时探针也以操作 ID 定位原 Guest。物理探针的 HTTPS/Kafka 回包进入执行侧的测试邮箱，Worker 通过夹具已有的进程管理通道收到与探针和操作匹配的就绪结论。普通工具的真实结果仍直接进入原 Worker 回包 Topic。正式服务的探针、图片和 Workflow 协议未在本次迁移；其默认 Session Projector 解码与部署不变。

独立 PG 检查对相同分区设置两个边界，立即停止并重启临时 PostgreSQL，确认两个绑定生效位置幸存。然后从空的候选投影重放较早的工具意图，以已发放的 Kafka 许可为对照，断言没有新派发。稍后的新操作送往新绑定，旧操作的取消用原绑定；丢失取消响应不阻塞原生收尾。这个重放控制模拟缺失的候选投影，未测真实生产流量下的 WAL 丢失数量或吞吐。

另复跑 4 个 retained-log 场景、3 个 Bash 字段场景、32 项原型辅助测试和 21 项 PG/Kafka 传输集成测试。runtime-core 类型检查通过。测试期间出现过会自动重连的通用 consumer.failed 日志，未从该日志识别具体客户端；上述场景均达成最终断言。独立查询确认实验标记的存活 VM 与临时 PG/Kafka 容器均为零。

[架构决定](../adr/0344-execution-owned-operation-routing-study.md) · [路由与作用域回归](../../scripts/experiments/minimal-session-storage/workspace-routing-recovery.mjs) · [执行侧路由](../../scripts/experiments/minimal-session-storage/workspace-operation-router.mjs)
