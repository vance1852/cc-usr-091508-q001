# 卫星地面站窗口协调

本项目服务于卫星运营团队，帮助值班人员处理地面站可见弧段、天线能力和任务优先级之间的冲突。

系统应让排程依据、临时迁移和责任通知可追溯，保障测控任务在设备维护或轨道预报变化时仍能安全调整。

## 功能概览

地面站窗口冲突处置服务（Go + Gin + SQLite）：

- **候选排程生成**：接收轨道预报版本、天线能力、可见弧段（支持跨午夜）、任务优先级、
  最小测控时长与设备维护锁定，按（优先级降序、幂等键升序）确定性贪心排定，
  每个条目都携带排定理由与采用的预报版本。
- **冲突挤出与人工决策队列**：被挤出的任务进入可恢复（SQLite 持久化）的人工决策队列，
  附冲突链（被谁抢占 / 进行中测控段 / 维护锁定）、替代站点建议与通知结果。
- **进行中测控段保护**：已开始未结束的测控段在重排时原样保留；激活候选排程时
  校验进行中段未被改写，否则返回 409。
- **失效标记**：新轨道预报发布或设备维护锁定变更时，引用旧预报弧段 / 被锁天线的
  候选与生效排程自动标记 `stale`，stale 版本禁止激活。
- **幂等提交**：任务以 `external_id` 为幂等键，重复提交返回既有任务（HTTP 200 +
  `deduplicated: true`），决策队列对同一任务只保留一条待决项。
- **权限模型**：值班长（`duty_officer`）只能处理自己负责的站点；
  跨站迁移只能由任务平台主管（`supervisor`）批准。
- **审计与通知**：所有状态变更写入审计事件（操作者、动作、实体、明细），
  通知结果持久化并可按关联实体查询。

## 构建与运行

```bash
go build ./cmd/gwsvc
./gwsvc -addr :8080 -db gwsvc.db
```

测试：

```bash
go test ./...          # 单元与接口测试
go test ./... -race    # 含并发竞态检测
```

测试覆盖：跨午夜窗口、并发排程生成（版本唯一）、并发重复提交（幂等）、
重启恢复（决策队列 / 排程 / 审计跨重启保留）、已开始测控段保护、
预报与设备失效标记、站点级 RBAC 与迁移审批。

## HTTP API

认证通过请求头完成（`/healthz` 除外）：

| 头 | 说明 |
|---|---|
| `X-Actor-ID` | 操作者标识（必填，缺失返回 401） |
| `X-Actor-Role` | `duty_officer` 或 `supervisor` |
| `X-Actor-Stations` | 值班长负责的站点，逗号分隔 |

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/stations` | 注册站点与天线能力 |
| GET | `/api/v1/stations` / `/api/v1/stations/:id/antennas` | 站点与天线查询 |
| POST | `/api/v1/forecasts` | 登记轨道预报版本（旧版作废并标记受影响排程 stale） |
| GET | `/api/v1/forecasts?satellite_id=` | 预报版本查询 |
| POST | `/api/v1/windows` | 批量登记可见弧段（跨午夜用绝对时间，LOS 须晚于 AOS） |
| GET | `/api/v1/windows/:id` | 窗口详情：预报版本、引用条目、冲突链、替代站点、通知结果 |
| POST | `/api/v1/tasks` | 提交任务（`external_id` 幂等；重复返回 200 + `deduplicated`） |
| GET | `/api/v1/tasks/:id` | 任务查询 |
| POST | `/api/v1/maintenance-locks` | 设备维护锁定（标记受影响排程 stale） |
| DELETE | `/api/v1/maintenance-locks/:id` | 解除锁定 |
| POST | `/api/v1/schedules/generate` | 生成候选排程 `{station_id, from, to}` |
| POST | `/api/v1/schedules/:id/activate` | 激活候选排程（stale / 改写进行中段返回 409） |
| GET | `/api/v1/schedules/:id` / `/api/v1/schedules?station_id=&status=` | 排程查询（条目含预报版本号与理由） |
| GET | `/api/v1/decision-queue?status=pending` | 人工决策队列（按操作者站点过滤） |
| POST | `/api/v1/decision-queue/:id/resolve` | 处置：`retry` / `migrate`（主管）/ `cancel` |
| GET | `/api/v1/audit-events?entity_type=&entity_id=` | 审计事件 |
| GET | `/api/v1/notifications?related_type=&related_id=` | 通知记录 |

## 代码结构

```
cmd/gwsvc/            服务入口
internal/domain/      领域模型、角色、错误
internal/store/       SQLite 持久化（immediate 事务、唯一约束、幂等建表）
internal/service/     排程生成、冲突挤出、决策队列、失效标记
internal/api/         Gin 路由、认证中间件、RBAC 错误映射
third_party/mimetype  validator 依赖的 API 兼容最小实现（见其中注释）
```

## 关键设计

- **时间表示**：所有时间为 UTC 绝对时间，持久化使用固定 9 位小数格式
  （`2006-01-02T15:04:05.000000000Z`），字典序即时间序，跨午夜弧段无需特判。
- **并发控制**：进程内按站点互斥锁 + SQLite `_txlock=immediate` 事务 +
  `(station_id, version)` 唯一约束，并发生成的排程版本号连续唯一；
  决策队列以部分唯一索引保证同一任务仅一条待决项。
- **可追溯性**：排程条目记录采用的预报版本与排定理由；决策项记录冲突链、
  替代站点与通知结果；审计事件贯穿任务提交、排程生成/激活/失效、
  锁定变更与队列处置全链路。
