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
- 正常→放宽：转移得分达到 30 **且**该批在时间线上所处时刻
  `production_stable` 与 `supervisor_approval` 都为真。
- 放宽→正常：出现一批不接收；或该批时刻「生产稳定」标记为假（见下，对下一批立即生效）。
- 加严→暂停：本轮加严期间**累计**不接收达 5 批（不要求连续；连续 5 批接收则先回正常）。
- 暂停后：拒绝再录批；`POST /api/streams/:id/resume` 人工恢复，从加严重新开始、全部计数清零。

转移得分（标准加分/清零规则在任意方案上的可执行形式，已在代码注释与测试中固定）：
正常下接收的批，一次抽样 `d=0:+3、d=1:+2、2≤d≤c（c≥2）:+1`；
二次抽样在第一样接收按 (n1,c1) 同样加 3/2/1，经第二样才接收 +1；不接收清零。

### 标记的时间语义（2026-10 修订）

两个流级标记**不再是整条时间线的恒定值**，而是时间线上的一种事件，与批次、恢复事件
统一排序：

- `PATCH /api/streams/:id` 只对**分界点之后**的批次负责。分界点取提交时流上
  「检验时间最晚的那一批」：在该分界时刻写入一条 `flags` 事件，携带变更后的整体标记值。
  因此之前已经判定的批次（严格度、所用方案、判定、得分、转移）一个字段都不会被改写，
  当前状态在打标记那一刻也保持不变；下一批按新标记判定。
- 同分界时刻的总序：**批次先于恢复事件先于 flags 事件**，同类型再按 id。
  所以正好补录在分界时刻的批次仍按旧标记判（它排在 flags 事件之前）；只有严格晚于
  分界点的批次才看到新标记。
- 多次只翻标记、不录新批：这些 `flags` 事件共享同一个分界时刻，按 id 升序排列，
  最近一次提交位于最后，即分界点之后生效的标记。
- **建流时带的标记**、以及**流上还没有批次时打的标记**：写为流的「初始标记」
  （`initial_production_stable/initial_supervisor_approval`），从头算起。
  空流打标记会清掉此前残留（因批次被删而失去位置）的 flags 事件。
- 撤销同理：撤销只在分界点之后生效；撤销时刻仍在放宽下的历史批次保持放宽，
  撤销之后录入的第一批按正常方案判（放宽→正常在该批判定前惰性发生，该批带
  `to_normal` 转移标记）。
- 删批/补录把分界点的相对位置改变时，不重写历史 flags 事件；它们是不可变的
  时间线事实，**当前标记以从头重放的结果为准**：例如「翻标记→删掉当时全部批次→
  在更早的时间补批→再翻标记」，后一次提交可能落在比旧事件更早的时刻，此时
  GET/PATCH 读到的当前标记是重放末尾的标记，而不是最后一次 PATCH 的入参。

时间线统一包含批次、「恢复」事件与「flags」事件，按
`(时间, 批次<恢复<flags, id)` 确定唯一顺序，补录、删除、改正、恢复与反复翻标记
都有良定义、且与从头重放逐字段相同的结果。

## 5. 历史会被改：重算策略（选择、理由、代价）

**选择：纯函数折叠 + 从检查点开始的后缀重算，而不是每次整条重放，也不做增量计数修补。**

- 转移逻辑只有一份，即无副作用的 `statemachine.StepWithFlags`：
  `(state, batch) -> (state', 该批记录)`。`replay.FullReplay` 与后缀重算调用的是同一个函数，
  因此「从检查点折叠后缀」与「从头折叠全部」在数学上是同一次计算，只是起点不同。
- 每次改动在**一个数据库事务**内完成：对 `streams` 行取 `FOR UPDATE` 行锁 → 写入增删改 →
  定位「最早受影响的时间线位置」→ 取该位置之前最近的检查点（每 50 个条目一个）→
  只折叠该后缀并回写每批记录、当前状态、当前标记和检查点。
- 理由：纯增量式「在被改批周围加减计数」必然要复刻 5 批窗口滑动、得分清零、暂停/恢复等全部边界，
  极易在这些边界上算漏并与真正的转移逻辑逐渐漂移；而整条重放在流很长时每次都付 O(历史) 的代价。
  后缀重算保留单一事实来源，同时把常见的尾部追加降为 O(1)（加一次周期性检查点写入）。
- 代价：
  1. 修改最早的一批时后缀就是整条历史，最坏代价等同于全量重放；
  2. 每次改动要在事务内重新加载该流全部批次/事件以重建有序时间线（有索引，主要是读放大）；
  3. 检查点是冗余存储（稀疏、可随时删除后从初始状态重建），实现以「重放即权威」避免检查点本身成为第二套逻辑。

### 标记事件的摆放、与补录/删批/恢复的排序、以及中间状态（检查点）处置

这三处都存在多种可行做法，本服务选定如下（**理由与代价**一并写明）：

1. **标记变化如何与批次、恢复排进同一条时间线**
   - 选择：PATCH 时以「流上检验时间最晚的批」为分界，写一条携带**整体新标记**的
     `flags` 事件，时间精确等于该批的 `inspected_at`；统一排序键为
     `(时间, 批次 < 恢复 < flags, 同类型 id 升序)`。
   - 理由：这正好实现「只对分界之后的批负责」——分界批因「批次先于事件」而保持原判，
     严格晚于分界的批才看到新标记；同时补录一条更早的批时，它天然落在该 flags 事件之前，
     按当时标记判，无需特判。携带整体值而非 diff，使任意位置的重放只需顺序赋值。
   - 代价：连续翻标记而不录新批会产生多条同一时刻的事件（稀疏冗余，可用压缩作业合并，
     但不能删除——它们在删批后可能重新变得有意义）；同刻多事件靠 id 定序，因此最近一次
     提交天然排在最后并成为分界之后生效的值。

2. **补录与删批时标记事件落在哪个位置**
   - 选择：flags 事件一旦写入就是**不可变的时间线事实**，补录/删批只移动批次，
     不移动、不重写、不删除 flags 事件；`streams.production_stable/supervisor_approval`
     （当前标记）**由重放末尾结果回写**，而不是 PATCH 入参。
   - 理由：与「整条历史按时间重放」的权威定义完全一致：删批使旧分界可能晚于剩余最晚批，
     此时旧标记事件在重放中仍然适用；反过来，在更早时间补批不会改写已提交的标记决定。
   - 代价（显式取舍）：在「翻标记→删掉当时的全部批→在更早处补批→再翻标记」这类
     极端操作后，GET 到的当前标记可能**不等于最后一次 PATCH 的值**（最后提交的事件
     可能排在更靠前的位置，末尾生效的是时间线最后的标记）。这是不可变事件模型的直接
     推论；若改为「最后提交永远赢」，就必须在删批时移动/删除历史标记事件，等于允许
     重写管理决策历史，与逐字段重放自洽相冲突。本服务选择时间线一致性。空流打标记是
     唯一例外（没有需要保护的历史）：直接改初始标记并清掉悬空的 flags 事件。

3. **已经算好存下来的中间状态（检查点）怎么处置**
   - 选择：检查点只存纯机器状态（严格度、得分、各计数、ordinal），**不存标记**；
     后缀重放从检查点播种时，用「初始标记 + 按序应用播种位置及之前的全部 flags 事件」
     现场重建该位置的有效标记（见 `replay.flagsAt`）。
   - 理由：插入批次/事件后，其后的 item_seq 全部平移，若检查点也存标记，就必须在每次
     插入时级联修正它们；而标记历史本来就在时间线上，重放前缀的 flags 事件即可恢复，
     代价只是播种时多扫一段（通常为 0 或极少条）。受影响后缀区间内的检查点一律删除
     并在折叠时按每 50 条重建；前缀检查点描述的前缀状态不随后缀编辑改变（被改项之前
     永远恰有 startIdx 个条目），无需重编号。
   - 代价：每个后缀多一次「从头数到播种位置的 flags 事件」的 O(flags事件数) 扫描；
     flags 事件通常远少于批次数，可接受。

- 等价性由测试强制保证：
  `TestRandomHistoryEdits`（400 步）与新增 `TestRandomFlagEdits`（500 步，反复翻转两个标记、
  补录、改数、改时间、删除、暂停/恢复交织）都在**每一步之后**独立从数据库读出全部行、
  用 `FullReplay` 从头折叠，逐字段比对当前状态与每批记录（严格度、判定、所用方案档
  id/名字/n/c、第二样、得分、转移、ordinal）及当前标记；
  `internal/replay` 的随机时间线与 flags 事件直接验证「前缀状态播种后缀 ≡ 全量折叠」，
  另含用户原始复现、撤销、早于分界补录、同刻补录、建流/空流打标记的定向用例。

并发：同一流的所有写入在该流行锁上串行化（事务提交即结果与重放一致），因此不会丢批、重复计分或状态交错；
不同流行不同、互不阻塞。`TestConcurrentInsertSameStream`（16×60 并发、含补录与可能的拒收）
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
PATCH  /api/streams/:id                   改 production_stable / supervisor_approval（分界点之后生效）
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
每一种转移与暂停各有构造序列；随机补录/删改下增量结果与从头重放逐字段一致；
标记事件的复现/撤销/早于分界补录/同刻补录/建流与空流打标记各有定向用例；
带标记反复翻转的随机补录删改序列每一步与从头重放逐字段一致；并发压测。
另含 n=80,c=2 的可手算例（0.7844188871）。

## 8. 关于判定口径的两点显式说明

- 二次抽样的计划判定沿用经典口径：首轮接收/拒收区域外（c1<d1<r1）抽第二样，按合计与 c2 判定。
- 「转移得分按标准加分清零」在用户自定义一次/二次方案上没有逐字条文，本服务采用
  d=0/+3、d=1/+2、2≤d≤c/+1、二次经第二样接收 +1、不接收清零的可执行形式，规则集中在
  `internal/statemachine/step.go` 的 `scoreAfter`，需要对齐贵司 SQE 的具体条文时只改这一处。
