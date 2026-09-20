# 卫星地面站窗口冲突处置服务（gscsvc）

本项目服务于卫星运营团队，帮助值班人员处理地面站可见弧段、天线能力和任务优先级之间的冲突。

系统应让排程依据、临时迁移和责任通知可追溯，保障测控任务在设备维护或轨道预报变化时仍能安全调整。

## 功能概览

- **候选排程生成**：接收轨道预报版本、天线能力、可见弧段、任务优先级、最小测控时长与设备维护锁定，按（优先级降序、开始时间升序）竞争生成带理由的候选排程版本。
- **冲突链与人工决策队列**：被挤出的任务记录冲突链（谁挤掉谁、重叠区间、理由），进入持久化的人工决策队列，并附替代站点建议；队列条目可 `retry`（重排）/ `migrate`（跨站迁移）/ `drop`（放弃）。
- **进行中测控段保护**：`start <= now` 的测控段在新版本中原样继承（`CARRIED` + `locked`），任何任务不得抢占，绝不静默改写。
- **预报/设备更新联动**：新轨道预报版本或维护锁定会把仍引用旧预报、或与锁定重叠的排程版本标记为 `AFFECTED`，并留审计事件。
- **幂等**：任务（`task_id`）、弧段（`window_id`）、预报（`satellite_id+version`）、维护锁定（`lock_id`）均幂等，重复提交返回既有记录。
- **权限**：值班长（`duty_officer`）只能处置自己负责的站点；跨站迁移必须由任务平台主管（`supervisor`）批准。
- **可追溯**：每个弧段可查询采用的预报版本、冲突链、替代站点、通知结果与审计事件；通知以 `duty_phone_log` 通道留痕，替代临时电话协调。

## 构建与运行

```bash
go build -o gscsvc ./cmd/gscsvc
./gscsvc -db gscsvc.db -addr :8080
```

启动时自动执行恢复：核对 SQLite 中待决的人工决策条目并写入 `SERVICE_RESTARTED` 审计事件，日志输出待决数量。

## API 一览（`/api/v1`）

除 `GET /healthz` 外均需请求头：

```
X-Operator-Id: zhangwei
X-Operator-Role: duty_officer | supervisor
X-Operator-Stations: ST1,ST2        # 值班长负责站点；主管无需
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/forecasts` | 登记轨道预报版本（幂等）；新版本标记受影响排程为 `AFFECTED` |
| POST | `/stations` | 登记/更新站点天线能力（频段、转速、可用状态） |
| POST | `/windows` | 批量提交可见弧段（按 `window_id` 幂等），携带预报版本 |
| POST | `/tasks` | 提交任务（按 `task_id` 幂等，重放返回 `idempotent_replay:true`） |
| POST | `/maintenance-locks` | 登记设备维护锁定（幂等），标记受影响排程 |
| POST | `/schedules/generate` | 为站点生成候选排程版本（站点级互斥锁 + IMMEDIATE 事务串行化） |
| GET | `/schedules?station_id=` | 当前生效排程（版本 + 条目 + 冲突链） |
| GET | `/schedules/versions?station_id=` | 历史版本列表（`ACTIVE/AFFECTED/SUPERSEDED`） |
| GET | `/decision-queue?status=PENDING` | 人工决策队列（值班长仅见自己站点） |
| POST | `/decision-queue/{item_id}/resolve` | 处置：`retry` / `migrate`（需主管）/ `drop` |
| GET | `/windows/{window_id}/trace` | 弧段追溯：预报版本、冲突链、替代站点、通知结果、审计 |
| GET | `/audit-events` | 审计事件检索（可按 `entity_type`/`entity_id` 过滤） |
| GET | `/notifications` | 通知结果记录 |

### 典型流程

```bash
# 1. 登记站点能力、轨道预报、任务、可见弧段
curl -X POST $B/stations  -d '{"id":"ST1","name":"喀什站","bands":["S","X"],"max_rate_dps":5}'
curl -X POST $B/forecasts -d '{"satellite_id":"YAOGAN-1","version":3,"generated_at":"2026-09-19T15:00:00Z"}'
curl -X POST $B/tasks     -d '{"task_id":"TASK-YG1-DL","satellite_id":"YAOGAN-1","kind":"DATA_DOWNLINK","priority":10,"min_tt_seconds":600,"required_band":"X"}'
curl -X POST $B/windows   -d '[{"window_id":"WIN-YG1-ST1","satellite_id":"YAOGAN-1","station_id":"ST1","start_utc":"2026-09-19T23:40:00Z","end_utc":"2026-09-20T00:15:00Z","forecast_version":3}]'

# 2. 值班长生成候选排程（跨午夜弧段自然支持，时间一律 UTC）
curl -X POST $B/schedules/generate -d '{"station_id":"ST1","from":"2026-09-19T23:00:00Z","to":"2026-09-20T01:00:00Z","note":"临时姿态调整后重排"}'

# 3. 查看决策队列并处置（migrate 需 supervisor）
curl -X POST $B/decision-queue/DISPLACED_BY_CONFLICT:TASK-YG2-TC:ST1:v1/resolve \
     -d '{"action":"migrate","target_station_id":"ST2"}'

# 4. 追溯任一弧段的排程依据
curl $B/windows/WIN-YG2-ST1/trace
```

## 排程条目状态与理由码

- `SCHEDULED`（`OK`）：无冲突排定，理由含采用的预报版本。
- `CARRIED`（`CARRIED`）：进行中测控段从上一版本原样继承。
- `DISPLACED`（`CONFLICT_LOST_PRIORITY` / `PREEMPT_STARTED_DENIED`）：冲突被挤出 / 不可抢占已开始段。
- `BLOCKED`（`MAINTENANCE_LOCK` / `MIN_TT_UNMET` / `CAPABILITY_MISMATCH`）：维护锁定、时长不足、能力不匹配。

## 测试

```bash
go vet ./... && go test -race ./...
```

覆盖：跨午夜窗口判冲突、并发排程串行化（版本号唯一、无重订）、重启恢复（队列与版本持久化）、
任务幂等、进行中测控段保护、预报/维护锁定影响标记、站点越权与迁移审批、端到端 API 流程。

## 结构

```
cmd/gscsvc/            服务入口（启动恢复 + 优雅退出）
internal/domain/       领域模型（操作员、弧段、任务、排程、队列、审计）
internal/store/        SQLite 持久化（WAL、单连接、BEGIN IMMEDIATE 事务）
internal/service/      排程引擎与业务操作（时钟可注入，便于测试）
internal/api/          Gin 路由、鉴权中间件、错误映射
```
