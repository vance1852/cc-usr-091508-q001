package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gscsvc/internal/domain"
	"gscsvc/internal/store"
)

// Service 组合存储与排程引擎，提供全部业务操作。
type Service struct {
	st  *store.Store
	eng *Engine
	now func() time.Time
}

// New 创建服务。now 可注入以便测试。
func New(st *store.Store, now func() time.Time) *Service {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{st: st, eng: NewEngine(st, now), now: now}
}

// Engine 暴露排程引擎。
func (s *Service) Engine() *Engine { return s.eng }

// Store 暴露存储（测试用）。
func (s *Service) Store() *store.Store { return s.st }

func (s *Service) audit(ctx context.Context, op domain.Operator, action, entType, entID, detail string) {
	_ = s.st.Audit(ctx, domain.AuditEvent{
		Ts: s.now(), Actor: op.ID, Role: string(op.Role), Action: action,
		EntityType: entType, EntityID: entID, Detail: detail,
	})
}

// ---- 轨道预报 ----

// ForecastResult 预报登记结果。
type ForecastResult struct {
	Created          bool    `json:"created"`
	AffectedVersions []int64 `json:"affected_versions"`
}

// RegisterForecast 登记预报版本（幂等）；新版本会标记仍引用旧预报的排程版本为 AFFECTED。
func (s *Service) RegisterForecast(ctx context.Context, op domain.Operator, f domain.OrbitForecast) (*ForecastResult, error) {
	f.ReceivedAt = s.now()
	created, err := s.st.InsertForecast(ctx, f)
	if err != nil {
		return nil, err
	}
	res := &ForecastResult{Created: created}
	if !created {
		return res, nil // 幂等重放，不重复标记
	}
	affected, err := s.st.MarkSatelliteForecastStale(ctx, f.SatelliteID, f.Version)
	if err != nil {
		return nil, err
	}
	res.AffectedVersions = affected
	detail, _ := json.Marshal(map[string]any{
		"satellite_id": f.SatelliteID, "version": f.Version, "affected_versions": affected,
	})
	s.audit(ctx, op, "FORECAST_REGISTERED", "orbit_forecast",
		fmt.Sprintf("%s:v%d", f.SatelliteID, f.Version), string(detail))
	for _, vid := range affected {
		s.audit(ctx, op, "SCHEDULE_AFFECTED", "schedule_version",
			fmt.Sprintf("%d", vid), fmt.Sprintf(`{"cause":"FORECAST_UPDATED","satellite_id":%q,"new_version":%d}`, f.SatelliteID, f.Version))
	}
	return res, nil
}

// ---- 地面站 ----

// UpsertStation 登记/更新站点能力；若站点被置为不可用，标记受影响排程。
func (s *Service) UpsertStation(ctx context.Context, op domain.Operator, st domain.Station) error {
	if err := s.st.UpsertStation(ctx, st); err != nil {
		return err
	}
	detail, _ := json.Marshal(st)
	s.audit(ctx, op, "STATION_UPSERTED", "station", st.ID, string(detail))
	return nil
}

// ---- 可见弧段 ----

// SubmitWindows 批量提交弧段（按 window_id 幂等）。返回新建与重放数量。
func (s *Service) SubmitWindows(ctx context.Context, op domain.Operator, wins []domain.VisibilityWindow) (created, replayed int, err error) {
	for _, w := range wins {
		w.StartUTC, w.EndUTC = w.StartUTC.UTC(), w.EndUTC.UTC()
		ok, err := s.st.InsertWindow(ctx, w)
		if err != nil {
			return created, replayed, err
		}
		if ok {
			created++
		} else {
			replayed++
		}
	}
	detail, _ := json.Marshal(map[string]any{"created": created, "replayed": replayed})
	s.audit(ctx, op, "WINDOWS_SUBMITTED", "visibility_window", "", string(detail))
	return created, replayed, nil
}

// ---- 任务 ----

// TaskResult 任务提交结果。
type TaskResult struct {
	Task             domain.Task `json:"task"`
	IdempotentReplay bool        `json:"idempotent_replay"`
}

// SubmitTask 提交任务；同 task_id 重复提交返回既有记录（幂等）。
func (s *Service) SubmitTask(ctx context.Context, op domain.Operator, t domain.Task) (*TaskResult, error) {
	t.SubmittedBy = op.ID
	t.CreatedAt, t.UpdatedAt = s.now(), s.now()
	created, err := s.st.InsertTask(ctx, t)
	if err != nil {
		return nil, err
	}
	cur, err := s.st.GetTask(ctx, t.TaskID)
	if err != nil {
		return nil, err
	}
	if created {
		detail, _ := json.Marshal(t)
		s.audit(ctx, op, "TASK_SUBMITTED", "task", t.TaskID, string(detail))
	}
	return &TaskResult{Task: cur, IdempotentReplay: !created}, nil
}

// ---- 维护锁定 ----

// LockResult 维护锁定登记结果。
type LockResult struct {
	Created          bool    `json:"created"`
	AffectedVersions []int64 `json:"affected_versions"`
}

// CreateMaintenanceLock 登记维护锁定（幂等）；标记与其重叠的排程版本为 AFFECTED。
func (s *Service) CreateMaintenanceLock(ctx context.Context, op domain.Operator, l domain.MaintenanceLock) (*LockResult, error) {
	if !op.CanOperate(l.StationID) {
		return nil, ErrForbidden
	}
	l.CreatedBy = op.ID
	l.CreatedAt = s.now()
	l.StartUTC, l.EndUTC = l.StartUTC.UTC(), l.EndUTC.UTC()
	created, err := s.st.InsertLock(ctx, l)
	if err != nil {
		return nil, err
	}
	res := &LockResult{Created: created}
	if !created {
		return res, nil
	}
	affected, err := s.st.MarkLockOverlapAffected(ctx, l)
	if err != nil {
		return nil, err
	}
	res.AffectedVersions = affected
	detail, _ := json.Marshal(map[string]any{
		"lock_id": l.LockID, "station_id": l.StationID, "affected_versions": affected,
	})
	s.audit(ctx, op, "MAINTENANCE_LOCK_CREATED", "maintenance_lock", l.LockID, string(detail))
	for _, vid := range affected {
		s.audit(ctx, op, "SCHEDULE_AFFECTED", "schedule_version",
			fmt.Sprintf("%d", vid), fmt.Sprintf(`{"cause":"MAINTENANCE_LOCK","lock_id":%q}`, l.LockID))
	}
	return res, nil
}

// ---- 排程查询 ----

// ScheduleView 当前排程版本视图。
type ScheduleView struct {
	Version   domain.ScheduleVersion `json:"version"`
	Entries   []domain.ScheduleEntry `json:"entries"`
	Conflicts []domain.Conflict      `json:"conflicts"`
}

// CurrentSchedule 返回站点当前生效排程。
func (s *Service) CurrentSchedule(ctx context.Context, stationID string) (*ScheduleView, error) {
	ver, err := s.st.ActiveVersion(ctx, stationID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: station %s 尚无排程", ErrNotFound, stationID)
	}
	if err != nil {
		return nil, err
	}
	entries, err := s.st.EntriesOfVersion(ctx, ver.ID)
	if err != nil {
		return nil, err
	}
	conflicts, err := s.st.ConflictsOfVersion(ctx, ver.ID)
	if err != nil {
		return nil, err
	}
	if entries == nil {
		entries = []domain.ScheduleEntry{}
	}
	if conflicts == nil {
		conflicts = []domain.Conflict{}
	}
	return &ScheduleView{Version: ver, Entries: entries, Conflicts: conflicts}, nil
}

// ListVersions 列出站点排程版本。
func (s *Service) ListVersions(ctx context.Context, stationID string) ([]domain.ScheduleVersion, error) {
	return s.st.ListVersions(ctx, stationID)
}

// ---- 决策队列 ----

// ListDecisionQueue 列出队列；值班长仅见自己站点，主管见全部。
func (s *Service) ListDecisionQueue(ctx context.Context, op domain.Operator, status string) ([]domain.DecisionItem, error) {
	if op.Role == domain.RoleSupervisor {
		return s.st.ListDecisionItems(ctx, status, nil)
	}
	return s.st.ListDecisionItems(ctx, status, op.Stations)
}

// ResolveAction 队列处置动作。
const (
	ActionRetry   = "retry"   // 重新排程（本站点）
	ActionMigrate = "migrate" // 跨站迁移（需主管）
	ActionDrop    = "drop"    // 放弃任务
)

// ResolveDecisionItem 处置队列条目。
func (s *Service) ResolveDecisionItem(ctx context.Context, op domain.Operator, itemID, action, targetStation string) (*domain.DecisionItem, error) {
	it, err := s.st.GetDecisionItem(ctx, itemID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: decision item %s", ErrNotFound, itemID)
	}
	if err != nil {
		return nil, err
	}
	if it.Status != domain.QueuePending {
		return nil, ErrAlreadyResolved
	}

	switch action {
	case ActionRetry, ActionDrop:
		if !op.CanOperate(it.StationID) {
			return nil, ErrForbidden
		}
	case ActionMigrate:
		if op.Role != domain.RoleSupervisor {
			return nil, ErrNeedSupervisor
		}
		if targetStation == "" {
			return nil, fmt.Errorf("migrate 需要 target_station_id")
		}
		if _, err := s.st.GetStation(ctx, targetStation); err != nil {
			return nil, fmt.Errorf("%w: station %s", ErrNotFound, targetStation)
		}
	default:
		return nil, fmt.Errorf("未知处置动作 %q", action)
	}

	now := s.now()
	var resolution string
	switch action {
	case ActionRetry:
		// 以原排程区间重新生成，让任务重新竞争。
		if _, err := s.eng.Generate(ctx, op, it.StationID, it.Payload.RegenFrom, it.Payload.RegenTo,
			fmt.Sprintf("决策重试 %s", itemID)); err != nil {
			return nil, err
		}
		resolution = "RETRIED: 已按原区间重新排程"
	case ActionMigrate:
		if err := s.st.SetTaskMigratedTo(ctx, it.TaskID, targetStation, now); err != nil {
			return nil, err
		}
		if _, err := s.eng.Generate(ctx, op, targetStation, it.Payload.RegenFrom, it.Payload.RegenTo,
			fmt.Sprintf("跨站迁移 %s → %s", itemID, targetStation)); err != nil {
			return nil, err
		}
		resolution = fmt.Sprintf("MIGRATED: 经主管批准迁移至站点 %s", targetStation)
	case ActionDrop:
		if err := s.st.SetTaskStatus(ctx, it.TaskID, "DROPPED", now); err != nil {
			return nil, err
		}
		resolution = "DROPPED: 任务已放弃"
	}

	if err := s.st.ResolveDecisionItem(ctx, itemID, op.ID, resolution, now); err != nil {
		return nil, err
	}
	_ = s.st.Notify(ctx, domain.Notification{
		ItemID: itemID, Channel: "duty_phone_log", Target: it.StationID + "-oncall",
		Result: "SUCCESS", Detail: "队列条目已处置: " + resolution, CreatedAt: now,
	})
	detail, _ := json.Marshal(map[string]any{
		"item_id": itemID, "action": action, "target_station": targetStation, "resolution": resolution,
	})
	s.audit(ctx, op, "DECISION_RESOLVED", "decision_item", itemID, string(detail))

	out, err := s.st.GetDecisionItem(ctx, itemID)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ---- 窗口追溯 ----

// WindowTrace 单个弧段的完整追溯视图。
type WindowTrace struct {
	Window        domain.VisibilityWindow `json:"window"`
	Entries       []domain.ScheduleEntry  `json:"entries"`        // 各版本中的排程记录（含采用的预报版本）
	ConflictChain []domain.Conflict       `json:"conflict_chain"` // 冲突链
	DecisionItems []domain.DecisionItem   `json:"decision_items"` // 含替代站点
	Notifications []domain.Notification   `json:"notifications"`  // 通知结果
	AuditEvents   []domain.AuditEvent     `json:"audit_events"`
}

// TraceWindow 汇总弧段的预报版本、冲突链、替代站点与通知结果。
func (s *Service) TraceWindow(ctx context.Context, windowID string) (*WindowTrace, error) {
	w, err := s.st.GetWindow(ctx, windowID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: window %s", ErrNotFound, windowID)
	}
	if err != nil {
		return nil, err
	}
	tr := &WindowTrace{Window: w}
	entries, err := s.st.EntriesForWindow(ctx, windowID)
	if err != nil {
		return nil, err
	}
	tr.Entries = entries

	taskSet := map[string]bool{}
	verSet := map[int64]bool{}
	for _, e := range entries {
		taskSet[e.TaskID] = true
		verSet[e.VersionID] = true
	}
	for vid := range verSet {
		cs, err := s.st.ConflictsOfVersion(ctx, vid)
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			if taskSet[c.WinnerTaskID] || taskSet[c.LoserTaskID] {
				tr.ConflictChain = append(tr.ConflictChain, c)
			}
		}
	}
	items, err := s.st.ListDecisionItems(ctx, "", nil)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if taskSet[it.TaskID] {
			tr.DecisionItems = append(tr.DecisionItems, it)
			ns, err := s.st.NotificationsForItem(ctx, it.ItemID)
			if err != nil {
				return nil, err
			}
			tr.Notifications = append(tr.Notifications, ns...)
		}
	}
	tr.AuditEvents, err = s.st.ListAuditEvents(ctx, "visibility_window", windowID, 0)
	if err != nil {
		return nil, err
	}
	// 保证列表字段序列化为 [] 而非 null。
	if tr.Entries == nil {
		tr.Entries = []domain.ScheduleEntry{}
	}
	if tr.ConflictChain == nil {
		tr.ConflictChain = []domain.Conflict{}
	}
	if tr.DecisionItems == nil {
		tr.DecisionItems = []domain.DecisionItem{}
	}
	if tr.Notifications == nil {
		tr.Notifications = []domain.Notification{}
	}
	if tr.AuditEvents == nil {
		tr.AuditEvents = []domain.AuditEvent{}
	}
	return tr, nil
}

// ---- 审计 / 通知查询 ----

// ListAuditEvents 查询审计事件。
func (s *Service) ListAuditEvents(ctx context.Context, entityType, entityID string, limit int) ([]domain.AuditEvent, error) {
	return s.st.ListAuditEvents(ctx, entityType, entityID, limit)
}

// ListNotifications 查询通知结果。
func (s *Service) ListNotifications(ctx context.Context, limit int) ([]domain.Notification, error) {
	return s.st.ListNotifications(ctx, limit)
}

// ---- 重启恢复 ----

// RecoveryReport 启动恢复报告。
type RecoveryReport struct {
	PendingDecisions int `json:"pending_decisions"`
}

// StartupRecovery 服务重启后恢复：队列与排程均在 SQLite 中持久化，
// 此处核对待决条目并留审计痕迹，值班人员可据此继续处置。
func (s *Service) StartupRecovery(ctx context.Context) (*RecoveryReport, error) {
	pending, err := s.st.CountPendingItems(ctx)
	if err != nil {
		return nil, err
	}
	s.audit(ctx, domain.Operator{ID: "system", Role: domain.RoleSupervisor},
		"SERVICE_RESTARTED", "service", "gscsvc",
		fmt.Sprintf(`{"pending_decisions":%d}`, pending))
	return &RecoveryReport{PendingDecisions: pending}, nil
}
