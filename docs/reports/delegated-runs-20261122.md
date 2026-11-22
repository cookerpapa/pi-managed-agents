# 根会话与子 Run 原型

在 #52 性能分支上，使用独立 PG schema 对照另一种表示。每个用户会话在 `sessions` 中只有
根记录。只有根 Run 有队列位置、租约和 Worker 槽位。一次委派在 `runs` 有一行，第一次
委派的 Run ID 同时作为稳定的子 Agent 地址。每条子 Run 记录调用它的 Run 和原生工具任务。
继续已有 Agent 时，还记录上一次子 Run 和确切的历史 Entry。任务原文在登记时就可用于列表。
完整正文和 Inbox 沿用现有 Projector，同一物理 Session 的写入由根 Run 的身份统一接纳。
[决策](../adr/0349-delegated-runs-prototype.md)

| 实验动作 | 核对的事实 |
| --- | --- |
| R2 委派 A 和 B | A 首次继承冻结的父上下文，B 首次从独立任务开始。状态为 preparing 时即可列出任务。再领队列为零行。 |
| A 对 B 先 steer 后 follow-up | 实际 Pi 的当前 Inbox 接纳两种输入，检查了 B 后续的模型请求。重试同一投递得到原回执。结束后迟到的信息返回 missed。 |
| R3 明确继续 A | 新建 Harness 和内存 Storage，从 PG 冷读。新 Run 的调用方是 R3，前序工作是 A1，子 Agent ID 相同。实际模型请求包含 A1 的私有回答，排除了 B 的私有回答。主线只看自己已收到的结果。 |
| 查看 A 自己的记录 | 按稳定 Agent ID 读取自己两次工作的正文；两个子 Run 共用一个地址。 |
| 启动与收尾竞态 | 重复登记同一调用返回原 Run，变更参数被拒绝；忙碌子 Agent 拒绝显式继续。根 Run 尝试带着未收尾的子工作正常完成时被拒绝。旧写入者无法登记新工作。 |
| SIGKILL 正在等待的 Worker | 独立 Node 进程被强杀。替换进程使用同一根 Run、递增 epoch、PG 封口快照和原等待编号恢复。验证首次委派与“上一根 Run 已经有子 Agent、本轮继续后再崩溃”两个场景。每次原任务只有一条子 Run 和一条首输入。 |

第一组有 1 行 `sessions` 和 5 行 `runs`（R2、A1、B1、R3、A2），只有 2 次根领取，数据库里没有
`subagent_conversations`。在两个互相独立的验证层运行：实际 RF3 Kafka 可以传输主场景的
原生提交至现有候选 Projector；强杀实验以 IPC 控制精确时序，沿用正常的 PG 封口写入、读取及 Pi 重新接管机制。
模型为可控响应，等待工具为有原编号的可重复挂接测试工具，故障夹具显式安排数据库的租约交接。
这些断言未测试额外的物理沙箱执行或 HTTP/UI 切换。当前工具通过进程内控制适配器调用，尚未接入正式的工具目录和派发 RPC。

## 有很多历史子 Agent 的冷启动

普通根 Run 在全新 Worker 上开始时，这一版会预装可继续的子对话及其继承所需的历史区间。
即使这次只继续主线，历史子对话也有读取成本。在隔离 PG 中，每条对话放入 32 条正文，
每条文本为 512 字节，未触发压缩；每档暖身一次，再用 5 份全新的内存 Storage 读取同一数据库。

| 历史子 Agent 数 | 实际装入正文数（含主线） | 耗时中位数 | 五次中的最慢值 |
| ---: | ---: | ---: | ---: |
| 1 | 64 | 7.2 ms | 7.7 ms |
| 100 | 3,232 | 39.8 ms | 45.6 ms |
| 500 | 16,032 | 161.0 ms | 168.5 ms |

这些数值包括查询 PG、解析正文和重建 Pi 的内存 Storage。PG 为暖缓存，Worker 内存是冷的，
没有模型请求，也没有网页渲染。这里不能推断完整服务的吞吐。要把同一会话用于大规模研究，
下一项实验应是根 Worker 只加载主线，等工具明确要继续 A，再加载 A 所需的历史；同时比较原有上下文、
压缩锚点和重复交接的正确性，以及 100/500 个闲置兄弟的冷读耗时。
本次完整传输回归 20 个文件、33 项用例通过。

## 复现

```sh
node scripts/run-with-test-postgres.mjs -- node_modules/.bin/vitest run \
  scripts/experiments/minimal-session-storage/delegated-{runs,handoff}.integration.spec.mjs \
  --maxWorkers=1 --no-file-parallelism
npm run test:transport
node scripts/run-with-test-postgres.mjs -- node --import tsx \
  scripts/experiments/minimal-session-storage/delegated-cold-study.mjs
```

普通发送只在目标 Conversation 忙碌时把 Submission 和 Inbox 项一同提交；调用者在原生工具任务内
记住实际投递的子 Run 和操作 ID。显式继续使用 Pi 的 fork 接口，让新的工作归属于本次调用者的
活跃 generation，并从已结束的子 Run 的最后一个 Entry 接着工作。旧的子 Run 仍有自己的终态。

正式部署仍使用已有的会话关系。组织工具授权、Workspace 分配、浏览器导航以及完整混合负载
仍需在新表示上接入验收。本轮不构建或发布服务镜像。
