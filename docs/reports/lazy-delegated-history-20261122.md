# 主线冷启动与子 Agent 按需续聊

在 #52 的根会话原型上，普通接手先用 PG 的已结算位置读取主线的上下文后缀、Pi 配置和空 Inbox；同时校验正文条数与字节数。明确继续 A 时，再从 A 上次结束的位置读取它自己的消息，沿创建时冻结的继承关系取需要的祖先片段。首次独立启动的子 Agent 没有祖先片段。读取者采用同一物理 Session 的原生 Entry ID、提交位置和分配上界，没有新增持久表、索引或历史副本。[设计决策](../adr/0350-lazy-delegated-history.md)

## 同机对照

每个历史子 Agent 和主线各有 32 条未压缩的正文，每条文本 512 字节。旧代码取自 `154a480e`；两种实现分别创建相同形状的独立 PG 数据库。每档暖身一次，之后用五份全新的 Worker 内存 Storage 读取暖缓存 PG，表中是客户端耗时中位数。最后一列从“明确要继续”的读取动作开始计时；启动计时包含向 PG 核对当前根 Run 身份和读取历史。Worker 领取队列、模型推理、外部网络和页面渲染不在这组计时里。

| 历史子 Agent | 已存正文 | 旧版冷启动 | 按需版冷启动 | 随后加载一个独立子 Agent |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 64 | 6.9 ms | 4.5 ms | 4.7 ms |
| 100 | 3,232 | 38.8 ms | 4.7 ms | 5.3 ms |
| 500 | 16,032 | 150.7 ms | 6.1 ms | 7.8 ms |

旧版会装入上表全部正文。按需版三档在根启动时均加载 32 条，用到指定子 Agent 时另加载 32 条。同一源的并发请求只进行一次成功读取。已加载的分支保留到当前 Storage 关闭，实际用过许多分支的长 Run 仍需承担这些分支的内存成本。

## PG 仍有读放大

对实际发出的两条正文查询执行 `EXPLAIN (ANALYZE, BUFFERS)`。500 子 Agent 的样本中，主线与子 Agent 正文查询都返回 32 行，累计共享缓冲页访问分别为 **1,604 和 1,613**。这个指标包含重复访问，不能直接当作物理磁盘读取次数。

在同一个临时库添加按租户、物理 Session、Conversation、Entry ID 排列的正文部分索引，重复执行相同的 SQL，访问数降到 **7 和 14**。只测试了索引的读效果。正式采用前应测混合 Projector 写入吞吐、WAL 和索引体积。原型 schema 和线上索引沿用现有版本。

## 正确性与故障

- 父 Agent 在 R2 派 A 继承上下文，另派 B 独立工作。父会话之后用真实 Pi 做压缩。R3 换冷 Worker，第一次主线请求包含新摘要；明确叫回 A 的模型请求仍包含分支时的父输入和 A 的私有回答。B 的私有回答与父会话后来产生的摘要均未混入 A。
- 强杀正在等工具的 Worker，替换 Worker 从 PG 封口恢复原等待编号。上一轮已完成 A、B 的场景中，本轮只继续 A，再次强杀后仅挂回 A 的历史。对 B 的检查同时覆盖封口恢复查询返回的正文与额外按需读取的分支。模型及回包时序受控。
- 混合原生提交中包括主线、两条分别加载的子线及孙子线。与 Pi 原生 MemoryStorage 对照提交顺序、提交前不可见性、原始 Entry ID、逐条分页、上下文头标记和历史工具配置。失败读取可重试。
- 完整传输回归 20 个文件、33 项用例通过。另有独立的内存 Storage 对照用例。本轮只改隔离原型；正式服务切换和整体读写混压仍是单独的发布门槛。

复现读测与回归：

```sh
node scripts/run-with-test-postgres.mjs -- node --import tsx \
  scripts/experiments/minimal-session-storage/delegated-cold-study.mjs --compare-index
node_modules/.bin/vitest run scripts/experiments/minimal-session-storage/delegated-memory.test.mjs
npm run test:transport
```
