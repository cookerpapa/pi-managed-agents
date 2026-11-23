# 子 Run 登记前后的直接委派恢复

后续已在候选分支实施任务身份方案。[实施与验收](task-keyed-delegation-20261123.md)。以下保留改动前的对照。

#52 候选版的上线门槛是：父 Agent 封口交接后，新 Worker 能从 PG 恢复尚在等待的直接委派，并且同一次委派只对应一个子 Run。普通 Bash 的原回包恢复另有真实 Cube 验收。这次单独检查 `subagent` 的调用编号。

## 三个对照

使用固定 Pi Durable、确定性模型和正式 Worker 的 `subagent` 工具准备逻辑。父 Agent 发出一次“检查测试”的委派。测试捕获 Pi 等待结果时的原生提交，通过候选 Projector 写入隔离的 PG，执行正式的 `saveSealedFamily` 和 `readSealedFamily`。所有对照使用相同的 Session、原生任务、模型 tool call ID、回包 Topic 和封口边界。

| 时机 | 观察 |
| --- | --- |
| 子 Run 尚未登记，Worker 准备生成编号 A | 新 Worker 读到运行中的原生任务、模型调用 ID 和工具参数。此时还没有子 Run 可查。 |
| 换成另一次合法准备生成的编号 B，仍停在同一处 | 两次 PG 冷读结果和根 Run 的投影检查点完全相同，原请求里的操作编号不同。 |
| 用原请求先在旧 Worker 所属代际登记，再封口、交接 | 旧代际再次登记被拒绝。新代际使用原编号重试，返回已有子 Run，数据库里只有一行。 |

前两个是对同一条原生提交的独立对照。编号 A 和 B 没有同时发进同一个线上会话。第三个对照在子 Run 登记后、子原生 Conversation 建立前中断，覆盖创建边界。故障点由测试精确控制，封口写入和接手读取使用真实 PG。这个实验没有模拟整机断电。

单独正向验证了 Kafka。取其中一份原生提交，发往三副本临时 Topic，用候选 Projector 的实际恢复位置启动新消费者。本轮从分区位置 7 开始，在位置 8 读到原操作编号、回包 Topic 和参数，对应 Pi 第 9 次提交。读取器只取证，不派活。生产中的 Worker 冷恢复仍使用 PG，当前候选没有把这份 Kafka 引用交给替换 Worker。子 Run 登记前，单凭模型 tool call ID 推不出 Worker 随机生成的操作编号。

## 前置验证

- 浏览器经 Session API 打开子 Agent 的历史。它能显示同一子 Agent 两次执行的消息，兄弟 Agent 的正文不会混入。另有租户隔离、分页及失效游标测试。
- 显式继续 A 时，新调用使用 A 上次 Workspace 授权与当前调用方授权的交集。分别验证保留读取和工具集合无交集两种情况。
- 使用正式 Worker 工具准备、控制工具派发器、控制器和原 Kafka 邮箱验证了 `list/status/wait` 的结果。本轮验证范围不含整套正式服务上的子 Run 创建和发消息。

## 上线判据

有两个可验证的方向。沿用随机操作编号时，可以在现有故障交接快照里交付仍未结算的直接委派引用。另一个方向是以物理 Session、根 Run 和原生工具任务的稳定组合作为创建子 Run 的幂等身份，各 Worker 独立登记自己的回包等待。后者还要验证继承上下文的位置、重放校验和原生调用只认领一个子 Run 的约束。

本轮没有选择或实施这两种方案。正式切换前还需覆盖封口前 Projector 自身重启、连续交接、Stop 与迟到回包，验证不会重复创建子 Run，再复测相同的 20 工具混合负载。

复现候选浏览器、Kafka 与 PG 验收：

```sh
PI_CLOUD_DELEGATED_BROWSER=1 npm run test:transport
```

[直接委派对照](../../scripts/experiments/minimal-session-storage/direct-delegation-source.integration.spec.mjs) · [已有真实 Cube 交接验收](later-root-handoff-20261122.md) · [候选子会话设计](../adr/0351-serving-delegated-history.md)
