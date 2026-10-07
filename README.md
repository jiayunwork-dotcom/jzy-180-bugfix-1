# 来料检验后端服务（OC/ASN/AOQ/AOQL/ATI、风险、方案设计、GB/T 2828.1 转移状态机与历史重算）

Go 1.23 + Echo + PostgreSQL 16，纯 HTTP/JSON，无前端。

## 1. 目录结构（按职责分包）

```
cmd/server/            服务入口（等待数据库、优雅退出）
internal/dist/         二项 / 超几何 / 泊松分布；对数域组合数（log-gamma）与 LogSumExp
internal/sampling/     OC、Pa、ASN、AOQ/AOQL、ATI、α/β 风险、入参校验
internal/design/       一次/二次方案反向搜索（有上限，无可行解显式报错）
internal/statemachine/ GB/T 2828.1 转移状态机：纯函数折叠 (state,batch)->(state',记录)
internal/replay/       全量重放（参考实现）与从检查点起的后缀折叠（同一套纯函数）
internal/store/        PostgreSQL 持久化、行锁并发控制、增量重算与检查点
  plan_store.go / plan_replay.go / stream_store.go / batches_store.go /
  query_store.go / recompute.go / checkpoints.go / timeline.go / validate.go
internal/httpapi/      Echo 路由与 JSON 错误（字段级 422）
migrations/            建表 SQL（同时内嵌为 internal/store/schema.sql，启动时幂等执行）
docker-compose.yml     postgres:16-alpine + 本服务（golang:1.23-alpine 构建）
```

状态机与重算逻辑分别在 `statemachine/step.go`（判定+转移）、`statemachine/state.go`（状态/恢复）与
`replay/replay.go`（折叠/后缀）、`store/recompute.go`（检查点与落库），没有塞进单个大文件。

## 2. 快速开始

```bash
docker compose up --build
# 服务在 :8080；GET /healthz
```

本地直接运行：`DATABASE_URL=postgres://... go run ./cmd/server`（启动自动建表）。

## 3. 方案档与计算

方案档存库，可新建/修改/删除/按 id 与按名查询（`GET /api/plans/by-name?name=...`）。
一次方案 `{kind:"single",n,c}`；二次方案
`{kind:"double",n1,c1,r1,n2,c2}`，约束 `c1<r1≤c2+1`（另校验 `c1≤c2`、`c2<n1+n2`）。
`lot_size` 给定时用超几何分布，缺省用二项分布；`force_poisson:true` 用泊松近似，
响应中 `distribution.approximate=true`，且任一阶段 `n·p>5` 时在 `distribution.warnings` 给出警告。

- `POST /api/calculate/pa` `{plan|plan_id, p}`：Pa；二次方案同时给 ASN
- `POST /api/calculate/oc` `{plan, p_min, p_max, points}`：OC 曲线（points≤500；二次方案每点带 ASN）
- `POST /api/calculate/risks` `{plan, aql, ltpd}`：α=1−Pa(AQL)、β=Pa(LTPD)
- `POST /api/calculate/aoq`：单点 `{plan,p}`，或曲线 `{plan,p_min,p_max,points}`（需要 lot_size）
- `POST /api/calculate/aoql`：AOQL、对应 p 与该点 ATI
- `POST /api/plans/:id/{pa,oc,risks,aoq,aoql}`：对已存档计算

AOQ 采用「拒收批全检」口径：`AOQ=Pa·p·(N−ASN)/N`，`ATI=ASN+(1−Pa)(N−n_total)`。
AOQL 先密扫再用黄金分割法求精；测试以 2001 点扫描证明没有任一点 AOQ 超过 AOQL。

**手算例子 n=80,c=2（二项）**：
`Pa(0)=1`、`Pa(1)=0`；
`Pa(0.02)=Σ_{d=0}^2 C(80,d) 0.02^d 0.98^(80-d)=0.7844188871`；
`POST /api/calculate/pa {"plan":{"kind":"single","n":80,"c":2},"p":0.02}` 可直接核对。

### 反向设计

`POST /api/design/single` 与 `POST /api/design/double`，入参 `{aql,alpha,ltpd,beta,max_n?,max_n1?}`。

- 一次：在 `Pa(AQL)≥1−α` 且 `Pa(LTPD)≤β` 下取最小 n；n 相同取最小 c（对 c 二分）。
  测试额外证明 n−1 时任何 c 都不再满足。
- 二次：仅允许 `n2=n1` 或 `n2=2·n1`，并采用标准挂钩形式 `r1=c2+1`；
  对固定 (n1,n2,c2) 用前缀/加权后缀和把候选评估降为 O(n1)，c1 用二分定位，
  在全部可行方案中取 **p=AQL 处 ASN 最小** 者（平局按 n1、n2、c2、c1 确定性打破）。
- 搜索有上限（一次默认 n≤5000，二次默认 n1≤300，可由入参收紧）；找不到返回
  `422 {"code":"no_feasible_plan"}`。
- 设计按二项模型进行（设计阶段尚无具体批量）。

所有组合数与概率在**对数域**计算：`log C(n,k)` 用 log-gamma（取 k、n−k 较小者），
CDF 用按端点锚定的对数递推 + LogSumExp，并总是求和较短的一侧；n 到数千既不溢出也不会静默归零
（见 `internal/dist` 与 `TestLargeNNoOverflow`、`TestBinomialPMFSumsToOne`）。

## 4. 检验流与 GB/T 2828.1 转移规则

一条流绑定正常/加严/放宽三个档（创建时校验三个 id 存在）。
每批录入 `batch_no`、`inspected_at`（RFC3339）、`d1`；二次方案必须显式给 `d2`
（未抽第二样记 0，缺失会在 `d2` 字段上拒绝，避免被默默当成 0）。
每批记录包含：当时严格度 `severity`、所用方案快照 `plan`（id/名字/定义）、判定 `accepted`、
是否抽了第二样 `second_sample_taken`、转移得分 `switching_score`、本批后的 `transition`、批序号 `ordinal`。

已实现的规则（严格度切换总在触发批**之后**生效，触发批仍按当前严格度判定）：

- 正常→加严：连续不超过 5 批中有 2 批不接收（5 批滑动窗口，旧批滑出不再计数）。
- 加严→正常：加严下连续 5 批接收；一次拒收清零该连续计数。
- 正常→放宽：转移得分达到 30 **且**判定该批那一刻流上 `production_stable` 与
  `supervisor_approval` 都为真。
- 放宽→正常：出现一批不接收；或生产稳定标记被撤销（撤销对撤销边界**之后**录入的
  第一批立即生效，该批按正常判定；撤销之前已按放宽判过的批次记录永不改写）。
- 加严→暂停：本轮加严期间**累计**不接收达 5 批（不要求连续；连续 5 批接收则先回正常）。
- 暂停后：拒绝再录批；`POST /api/streams/:id/resume` 人工恢复，从加严重新开始、全部计数清零。

转移得分（标准加分/清零规则在任意方案上的可执行形式，已在代码注释与测试中固定）：
正常下接收的批，一次抽样 `d=0:+3、d=1:+2、2≤d≤c（c≥2）:+1`；
二次抽样在第一样接收按 (n1,c1) 同样加 3/2/1，经第二样才接收 +1；不接收清零。

**流级标记变更（`PATCH /api/streams/:id`）是时间线上的一个点事件，不再对整条历史生效。**
PATCH 到达时，以流上「检验时间最晚的那一批」为界：该批及更早批次的记录一字不动，
只有排在界之后的批次按新标记判定。实现上写一条 `stream_events.kind='flags'` 事件，
`occurred_at` 固定为这批界批的 `inspected_at`；同一时刻按「批次先于事件」排序，
所以与界批同一检验时刻的补录批仍按旧标记判（它不严格晚于界）。事件位置一经写定
**不随后续补录/删除/改时间移动**——这正是「按时间顺序从头重放整条历史」与流真实
经历完全一致的关键：

- 撤销同理：撤销前的批次不动，撤销后录入的第一批就按新标记判（放宽下撤销稳定即回正常）。
- 补录一个检验时间早于界的批次，按它所处时刻的标记状态判；晚于界的按新标记判。
- 建流时带的标记、以及流上**还没有批次时**打的标记，从头算：此时不写 flags 事件，
  而是改写 `streams` 上的初始标记（`initial_production_stable` /
  `initial_supervisor_approval`）。
- 两个标记可分别 PATCH；值没有实际变化时不落事件、不重算。
- `streams.production_stable/supervisor_approval` 只保存「当前」值（最近一次 flags
  事件之后），历史各时刻的值由 flags 事件序列重建。

时间线同时包含批次、「恢复」事件与「标记变更」事件，按
`(inspected_at, 批次先于事件, stream_events.id)` 确定唯一顺序，
因此补录、删除、改正、标记翻转交织时都有良定义的重放结果。

## 5. 历史会被改：重算策略（选择、理由、代价）

**选择一：标记变更如何与批次、恢复排进同一条时间线 —— 落成不可移动的点事件，而不是流上的恒定值。**

- 标记不是重放参数，而是时间线的一等元素：`stream_events` 在原有 `kind='resume'`
  之外增加 `kind='flags'`，携带变更后的两个标记值。折叠时遇到 flags 事件就切换
  `statemachine.StepWithFlags` 的入参；遇到 resume 事件则重置为加严。批次、两种
  事件共用同一套排序键 `(at, 批次先于事件, stream_events.id)`。
- PATCH 时刻的「界」取提交事务时流上 `max(inspected_at)`，flags 事件的
  `occurred_at` **冻结为该值**。为什么冻结而不是让它始终指向「当前最晚批」：
  若事件位置随后续补录/删除漂移，历史就不再是一条确定的时间线，并发下多个客户端
  对「当时标记是什么」会得到互相矛盾的答案；冻结后，标记事件与一条已录批次在
  语义上没有区别，重放规则只有一条——按 `at` 排序从头折叠。
- 同一时刻的决胜规则：批次先于事件。因此「界批同刻补录」的批次按旧标记判
  （它与界批同时检验，标记尚未翻），严格晚于界的批次才按新标记判；这也是
  GB/T 2828.1「撤销授权自撤销时点起生效」最保守的读法。两个管理事件同刻则按
  `stream_events` 的自增 id（即实际提交顺序）排。
- 流上没有批次时打标记没有「界」可锚：这类变更从头算，直接改写
  `streams.initial_*`（并丢弃此前同样在无批次期写入的 flags 事件——那些事件
  没有任何批次在其后，不可能影响过任何记录）。建流时带的标记就是初始标记。
- 代价：①标记变更多占一行 `stream_events`；②PATCH 只能影响界之后，若业务上
  需要「把这条流的历史标记也一起改」的管理操作，需要另设显式的全量订正接口，
  本实现刻意不提供（这正是本次事故要杜绝的行为）；③同一检验时刻、标记翻转
  之后才补录的批次按旧标记判，是选定的决胜规则，台账侧需要知道这条口径。

**选择二：补录/删批时已算好的中间状态怎么处置 —— 纯函数折叠 + 从检查点开始的后缀重算，标记状态不进检查点。**

- 转移逻辑只有一份，即无副作用的 `statemachine.StepWithFlags`：
  `(state, batch) -> (state', 该批记录)`。`replay.FullReplay` 与后缀重算调用的是同一个函数，
  因此「从检查点折叠后缀」与「从头折叠全部」在数学上是同一次计算，只是起点不同。
- 每次改动在**一个数据库事务**内完成：对 `streams` 行取 `FOR UPDATE` 行锁 → 写入增删改
  （flags 翻转即插入 flags 事件）→ 定位「最早受影响的时间线位置」→ 取该位置之前最近的
  检查点（每 50 个条目一个）→ 只折叠该后缀并回写每批记录、当前状态和检查点。
- 检查点只存状态机状态，**不存标记**：后缀起点的标记由「初始标记 + 起点之前的
  flags 事件」单独沿事件序列推一遍得到（O(事件数)，不折叠批次），因此检查点永远
  不可能携带过期标记，插入/删除 flags 事件也无需作废任何检查点的标记部分。
- 理由：纯增量式「在被改批周围加减计数」必然要复刻 5 批窗口滑动、得分清零、暂停/恢复、
  标记翻转等全部边界，极易在这些边界上算漏并与真正的转移逻辑逐渐漂移；而整条重放在流
  很长时每次都付 O(历史) 的代价。后缀重算保留单一事实来源，同时把常见的尾部追加降为
  O(1)（加一次周期性检查点写入）。
- 代价：
  1. 修改最早的一批时，后缀就是整条历史，最坏代价等同于全量重放；flags 翻转只重算界
     之后的后缀，常见情况下历史部分零重算、零回写；
  2. 每次改动要在事务内重新加载该流全部批次/事件以重建有序时间线（有索引，主要是读放大）；
  3. 检查点是冗余存储（稀疏、可随时删除后从初始状态重建），实现以「重放即权威」避免检查点本身成为第二套逻辑。
- 另一处实现修正：写操作返回前的批次列表改在**同一事务、同一连接**内读取（旧实现在
  事务提交后再向连接池借第二条连接）。高并发下每条流的写事务各持一条连接，再借第二条
  会把连接池借尽并自锁；新实现下一个写事务全程只占一条连接。
- 等价性由测试强制保证：
  - `TestFlagBoundaryLeavesHistoryUntouched` 逐字段复现报告中的 L1..L12 事故（打标记
    前后记录不变、当前仍正常/36，L13 正常判定后转放宽、L14 才用放宽档）；
  - `TestFlagRevokeOnlyAffectsLaterBatches`（撤销不动历史、之后第一批回正常）、
    `TestBackfillBeforeFlagBoundaryUsesOldFlags`（早于界的补录按旧标记）、
    `TestFlagOnNoBatchesAppliesFromStart`（建流带标记与无批次 PATCH 从头算）；
  - `TestRandomHistoryEdits` 对 4 个随机种子各做 400 步随机打乱的补录/改数/改时间/
    删除（含删到空流再补）/标记单边与双边翻转/随机恢复，**每一步之后**都独立从数据库
    读出全部行、用 `FullReplay` 从头折叠，逐字段（严格度、方案 id 与方案定义快照、
    判定、是否第二样、得分、转移、序号）比对当前状态与每批记录；
  - `internal/replay` 的 200 组含 flags 事件的随机时间线验证「前缀状态播种后缀 ≡ 全量折叠」；
  - `TestFlagBoundaryOverHTTP` 端到端复现 PATCH 语义。

并发：同一流的所有写入（录批、改删批、恢复、PATCH 标记）在该流行锁上串行化
（事务提交即结果与重放一致），因此不会丢批、重复计分或状态交错；
不同流行不同、互不阻塞。`TestConcurrentInsertSameStream`（16×60 并发、含补录与可能的拒收）、
`TestConcurrentFlagTogglesAndBatches`（12 路录批 + 4 路标记翻转并发，240 次 PATCH）
与 `TestConcurrentDifferentStreamsDoNotBlock`（8 条流并行、超时看护）在 `-race` 下通过。

## 6. HTTP 摘要

```
POST   /api/plans                         建方案档
GET    /api/plans                         列表
GET    /api/plans/:id                     按 id
GET    /api/plans/by-name?name=           按名查询
PUT    /api/plans/:id                     改定义（自动全量重放绑定它的流，返回 replayed_streams）
DELETE /api/plans/:id                     删除（仍被流绑定时 409 plan_in_use）

POST   /api/calculate/pa|oc|risks|aoq|aoql
POST   /api/design/single|double
POST   /api/plans/:id/pa|oc|risks|aoq|aoql

POST   /api/streams                       建流（绑定三个档 + 两个标记）
GET    /api/streams · GET /api/streams/:id（含状态与全部批次记录）
PATCH  /api/streams/:id                   改 production_stable / supervisor_approval（只对界之后的批次生效；可只给其中一个）
POST   /api/streams/:id/resume            暂停后人工恢复（可给 occurred_at）
POST   /api/streams/:id/batches           录批/补录（201 返回新状态与整条时间线）
PUT    /api/streams/:id/batches/:bid      改正（批号/时间/不合格数）
DELETE /api/streams/:id/batches/:bid      删除
GET    /healthz
```

错误统一为 `{"error":..., "fields":[{"field","message"}], "code":...}`：
结构/数值错误 422 且指出字段（n<1、c<0、c≥n、p∉[0,1]、lot_size<n、aql≥ltpd、
α/β∉(0,1)、d1/d2 超出样本量等），未找到 404，重名/绑定冲突/暂停 409，无可行方案 422(no_feasible_plan)。

## 7. 测试

```bash
DATABASE_URL=postgres://qc@127.0.0.1:5432/qcinspect?sslmode=disable \
  go test -race -count=1 ./...
```

没有可用 PostgreSQL 时，存储/HTTP 集成测试会 `t.Skip`，其余纯计算与状态机测试照常运行。

覆盖（对应需求逐条）：
p=0 时 Pa 恰为 1；p=1 且 c<n 时 Pa 恰为 0；n 不变 c 增大 OC 整条不下降；
c 固定 n 增大时 0<p<1 处 Pa 严格下降；大 N 超几何逼近二项；二次退化（n2=0,c1=c2,r1=c1+1）
与对应一次方案逐比特相等（多组 (n,c)、多个 p）；ASN∈[n1,n1+n2]；ATI∈[n,N]（一次/二次）；
AOQL 不小于密扫上任一点 AOQ；一次设计满足约束且 n−1 对任何 c 不可行、同 n 取最小 c；
二次设计 n2∈{n1,2n1}、挂钩约束成立、满足风险约束；
每一种转移与暂停各有构造序列；随机补录/删改下增量结果与从头重放逐字段一致；并发压测。
另含 n=80,c=2 的可手算例（0.7844188871）。

## 8. 关于判定口径的两点显式说明

- 二次抽样的计划判定沿用经典口径：首轮接收/拒收区域外（c1<d1<r1）抽第二样，按合计与 c2 判定。
- 「转移得分按标准加分清零」在用户自定义一次/二次方案上没有逐字条文，本服务采用
  d=0/+3、d=1/+2、2≤d≤c/+1、二次经第二样接收 +1、不接收清零的可执行形式，规则集中在
  `internal/statemachine/step.go` 的 `scoreAfter`，需要对齐贵司 SQE 的具体条文时只改这一处。
