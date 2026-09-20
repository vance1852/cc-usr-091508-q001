// Package service 实现地面站窗口冲突处置的核心业务逻辑：
// 候选排程生成、冲突挤出、人工决策队列、预报/设备变更的失效标记、
// 幂等提交与基于角色的站点权限控制。
package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"satops/groundstation/internal/domain"
	"satops/groundstation/internal/store"
)

// Service 业务服务。通过 WithClock 注入时钟以便测试复现跨午夜等场景。
type Service struct {
	st  *store.Store
	now func() time.Time

	mu           sync.Mutex
	stationLocks map[string]*sync.Mutex
}

// Option 服务可选项。
type Option func(*Service)

// WithClock 覆盖时钟（默认 time.Now）。
func WithClock(now func() time.Time) Option {
	return func(s *Service) { s.now = now }
}

// New 创建服务。
func New(st *store.Store, opts ...Option) *Service {
	s := &Service{
		st:           st,
		now:          func() time.Time { return time.Now().UTC() },
		stationLocks: map[string]*sync.Mutex{},
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Store 暴露底层存储（供 main 关闭等）。
func (s *Service) Store() *store.Store { return s.st }

// stationLock 返回站点级互斥锁：同一站点的排程生成/激活串行执行，
// 与 SQLite 唯一约束 (station_id, version) 共同保证并发安全。
func (s *Service) stationLock(stationID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.stationLocks[stationID]
	if !ok {
		l = &sync.Mutex{}
		s.stationLocks[stationID] = l
	}
	return l
}

// requireStation 校验操作者是否有权处置站点。
func requireStation(a domain.Actor, stationID string) error {
	if !a.CanOperate(stationID) {
		return fmt.Errorf("%w: 操作者 %s 无权处置站点 %s", domain.ErrForbidden, a.ID, stationID)
	}
	return nil
}

// audit 在事务内写审计事件。
func (s *Service) audit(ctx context.Context, tx *store.Tx, actor domain.Actor, action, entityType, entityID string, detail any) error {
	d := "{}"
	if detail != nil {
		if b, err := json.Marshal(detail); err == nil {
			d = string(b)
		}
	}
	return tx.InsertAudit(ctx, &domain.AuditEvent{
		TS:         s.now(),
		ActorID:    actor.ID,
		ActorRole:  string(actor.Role),
		Action:     action,
		EntityType: entityType,
		EntityID:   entityID,
		Detail:     d,
	})
}

// notFound 包装 sql.ErrNoRows 为领域错误。
func notFound(err error, what string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", domain.ErrNotFound, what)
	}
	return err
}

// ---- 站点与天线 ----

// AntennaInput 站点注册时随附的天线能力。
type AntennaInput struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Bands  []string `json:"bands"`
	Status string   `json:"status"`
}

// RegisterStation 注册站点及其天线。
func (s *Service) RegisterStation(ctx context.Context, actor domain.Actor, stationID, name string, antennas []AntennaInput) (*domain.Station, error) {
	if stationID == "" || name == "" {
		return nil, fmt.Errorf("%w: 站点 id 与名称必填", domain.ErrBadInput)
	}
	st := &domain.Station{ID: stationID, Name: name, CreatedAt: s.now()}
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		if err := tx.InsertStation(ctx, st); err != nil {
			return err
		}
		for _, in := range antennas {
			status := in.Status
			if status == "" {
				status = domain.AntennaStatusOperational
			}
			a := &domain.Antenna{
				ID:        in.ID,
				StationID: stationID,
				Name:      in.Name,
				Bands:     in.Bands,
				Status:    status,
				CreatedAt: s.now(),
			}
			if a.ID == "" {
				a.ID = domain.NewID("ant")
			}
			if err := tx.InsertAntenna(ctx, a); err != nil {
				return err
			}
		}
		return s.audit(ctx, tx, actor, "station.registered", "station", stationID, map[string]any{"antennas": len(antennas)})
	})
	if err != nil {
		return nil, err
	}
	return st, nil
}

// ListStations 列出站点。
func (s *Service) ListStations(ctx context.Context) ([]*domain.Station, error) {
	var out []*domain.Station
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListStations(ctx)
		return err
	})
	return out, err
}

// ListAntennas 列出站点天线。
func (s *Service) ListAntennas(ctx context.Context, stationID string) ([]*domain.Antenna, error) {
	var out []*domain.Antenna
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListAntennas(ctx, stationID)
		return err
	})
	return out, err
}

// ---- 轨道预报版本 ----

// RegisterForecast 登记新的轨道预报版本。同卫星旧版本被作废，
// 所有引用了旧版本弧段的候选/生效排程被标记为 stale（可追溯，不静默改写）。
func (s *Service) RegisterForecast(ctx context.Context, actor domain.Actor, satelliteID string, version int, issuedAt time.Time, note string) (*domain.ForecastVersion, error) {
	if satelliteID == "" || version <= 0 {
		return nil, fmt.Errorf("%w: satellite_id 与正整数 version 必填", domain.ErrBadInput)
	}
	if issuedAt.IsZero() {
		issuedAt = s.now()
	}
	f := &domain.ForecastVersion{
		ID:          domain.NewID("fv"),
		SatelliteID: satelliteID,
		Version:     version,
		IssuedAt:    issuedAt.UTC(),
		Note:        note,
		CreatedAt:   s.now(),
	}
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		if err := tx.InsertForecast(ctx, f); err != nil {
			return err
		}
		superseded, err := tx.SupersedeOtherForecasts(ctx, satelliteID, f.ID)
		if err != nil {
			return err
		}
		if err := s.audit(ctx, tx, actor, "forecast.registered", "forecast", f.ID,
			map[string]any{"satellite_id": satelliteID, "version": version, "superseded": superseded}); err != nil {
			return err
		}
		// 标记受影响的排程版本
		affected, err := tx.SchedulesUsingForecasts(ctx, superseded)
		if err != nil {
			return err
		}
		for _, schID := range affected {
			reason := fmt.Sprintf("轨道预报更新：卫星 %s 版本 v%d 发布，本排程引用的旧版弧段已作废", satelliteID, version)
			if err := tx.SetScheduleStatus(ctx, schID, domain.ScheduleStatusStale, reason); err != nil {
				return err
			}
			if err := s.releaseUnstartedTasks(ctx, tx, schID); err != nil {
				return err
			}
			if err := s.audit(ctx, tx, actor, "schedule.marked_stale", "schedule", schID,
				map[string]any{"reason": reason, "trigger_forecast": f.ID}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return f, nil
}

// ListForecasts 列出预报版本。
func (s *Service) ListForecasts(ctx context.Context, satelliteID string) ([]*domain.ForecastVersion, error) {
	var out []*domain.ForecastVersion
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListForecasts(ctx, satelliteID)
		return err
	})
	return out, err
}

// ---- 可见弧段 ----

// WindowInput 弧段登记入参。
type WindowInput struct {
	ID           string    `json:"id"`
	SatelliteID  string    `json:"satellite_id"`
	StationID    string    `json:"station_id"`
	ForecastID   string    `json:"forecast_id"`
	AOS          time.Time `json:"aos"`
	LOS          time.Time `json:"los"`
	MaxElevation float64   `json:"max_elevation"`
}

// RegisterWindows 批量登记可见弧段。跨午夜弧段以绝对时间表达（LOS 为次日），
// 服务端仅要求 LOS 严格晚于 AOS。
func (s *Service) RegisterWindows(ctx context.Context, actor domain.Actor, inputs []WindowInput) ([]*domain.VisibilityWindow, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("%w: 弧段列表为空", domain.ErrBadInput)
	}
	var out []*domain.VisibilityWindow
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		for _, in := range inputs {
			if in.SatelliteID == "" || in.StationID == "" || in.ForecastID == "" {
				return fmt.Errorf("%w: 弧段的 satellite_id/station_id/forecast_id 必填", domain.ErrBadInput)
			}
			if !in.LOS.After(in.AOS) {
				return fmt.Errorf("%w: 弧段 LOS 必须晚于 AOS（跨午夜请使用次日绝对时间）", domain.ErrBadInput)
			}
			if _, err := tx.GetStation(ctx, in.StationID); err != nil {
				return notFound(err, "站点 "+in.StationID)
			}
			f, err := tx.GetForecast(ctx, in.ForecastID)
			if err != nil {
				return notFound(err, "预报版本 "+in.ForecastID)
			}
			if f.SatelliteID != in.SatelliteID {
				return fmt.Errorf("%w: 弧段卫星 %s 与预报 %s 的卫星不一致", domain.ErrBadInput, in.SatelliteID, in.ForecastID)
			}
			w := &domain.VisibilityWindow{
				ID:           in.ID,
				SatelliteID:  in.SatelliteID,
				StationID:    in.StationID,
				ForecastID:   in.ForecastID,
				AOS:          in.AOS.UTC(),
				LOS:          in.LOS.UTC(),
				MaxElevation: in.MaxElevation,
				CreatedAt:    s.now(),
			}
			if w.ID == "" {
				w.ID = domain.NewID("win")
			}
			if err := tx.InsertWindow(ctx, w); err != nil {
				return err
			}
			out = append(out, w)
		}
		return s.audit(ctx, tx, actor, "window.registered", "station", inputs[0].StationID,
			map[string]any{"count": len(inputs)})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---- 任务 ----

// TaskInput 任务提交入参。
type TaskInput struct {
	ExternalID         string    `json:"external_id"` // 幂等键，必填
	Type               string    `json:"type"`
	SatelliteID        string    `json:"satellite_id"`
	Priority           int       `json:"priority"`
	MinDurationSeconds int64     `json:"min_duration_seconds"`
	RequiredBand       string    `json:"required_band"`
	PreferredStationID string    `json:"preferred_station_id"`
	NotBefore          time.Time `json:"not_before"`
	NotAfter           time.Time `json:"not_after"`
}

// SubmitTask 提交任务。同一 external_id 重复提交保持幂等：
// 返回既有任务且 created=false，不产生重复队列项或审计噪声之外的副作用。
func (s *Service) SubmitTask(ctx context.Context, actor domain.Actor, in TaskInput) (task *domain.Task, created bool, err error) {
	if in.ExternalID == "" {
		return nil, false, fmt.Errorf("%w: external_id（幂等键）必填", domain.ErrBadInput)
	}
	if in.Type != string(domain.TaskTypeDownlink) && in.Type != string(domain.TaskTypeTTNC) {
		return nil, false, fmt.Errorf("%w: type 须为 downlink 或 ttnc", domain.ErrBadInput)
	}
	if in.SatelliteID == "" {
		return nil, false, fmt.Errorf("%w: satellite_id 必填", domain.ErrBadInput)
	}
	if in.MinDurationSeconds <= 0 {
		return nil, false, fmt.Errorf("%w: min_duration_seconds 必须为正", domain.ErrBadInput)
	}
	if !in.NotBefore.IsZero() && !in.NotAfter.IsZero() && !in.NotAfter.After(in.NotBefore) {
		return nil, false, fmt.Errorf("%w: not_after 必须晚于 not_before", domain.ErrBadInput)
	}
	now := s.now()
	task = &domain.Task{
		ID:                 domain.NewID("tsk"),
		ExternalID:         in.ExternalID,
		Type:               domain.TaskType(in.Type),
		SatelliteID:        in.SatelliteID,
		Priority:           in.Priority,
		MinDuration:        time.Duration(in.MinDurationSeconds) * time.Second,
		RequiredBand:       in.RequiredBand,
		PreferredStationID: in.PreferredStationID,
		NotBefore:          in.NotBefore.UTC(),
		NotAfter:           in.NotAfter.UTC(),
		Status:             domain.TaskStatusPending,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	err = s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		created, err = tx.InsertTaskIfAbsent(ctx, task)
		if err != nil {
			return err
		}
		if !created {
			existing, err := tx.GetTaskByExternalID(ctx, in.ExternalID)
			if err != nil {
				return err
			}
			task = existing
			return s.audit(ctx, tx, actor, "task.deduplicated", "task", existing.ID,
				map[string]any{"external_id": in.ExternalID})
		}
		return s.audit(ctx, tx, actor, "task.submitted", "task", task.ID,
			map[string]any{"external_id": in.ExternalID, "priority": in.Priority, "type": in.Type})
	})
	if err != nil {
		return nil, false, err
	}
	return task, created, nil
}

// GetTask 查询任务。
func (s *Service) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	var task *domain.Task
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		task, err = tx.GetTask(ctx, id)
		return err
	})
	if err != nil {
		return nil, notFound(err, "任务 "+id)
	}
	return task, nil
}

// ---- 设备维护锁定 ----

// CreateMaintenanceLock 创建维护锁定，并把用到该天线且与锁定期重叠的
// 候选/生效排程标记为 stale。
func (s *Service) CreateMaintenanceLock(ctx context.Context, actor domain.Actor, antennaID string, start, end time.Time, reason string) (*domain.MaintenanceLock, error) {
	if antennaID == "" || !end.After(start) {
		return nil, fmt.Errorf("%w: antenna_id 必填且 end 须晚于 start", domain.ErrBadInput)
	}
	var lock *domain.MaintenanceLock
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		ant, err := tx.GetAntenna(ctx, antennaID)
		if err != nil {
			return notFound(err, "天线 "+antennaID)
		}
		if err := requireStation(actor, ant.StationID); err != nil {
			return err
		}
		lock = &domain.MaintenanceLock{
			ID:        domain.NewID("lck"),
			AntennaID: antennaID,
			StationID: ant.StationID,
			Start:     start.UTC(),
			End:       end.UTC(),
			Reason:    reason,
			Active:    true,
			CreatedAt: s.now(),
		}
		if err := tx.InsertLock(ctx, lock); err != nil {
			return err
		}
		if err := s.audit(ctx, tx, actor, "lock.created", "maintenance_lock", lock.ID,
			map[string]any{"antenna_id": antennaID, "start": lock.Start, "end": lock.End, "reason": reason}); err != nil {
			return err
		}
		affected, err := tx.SchedulesWithEntriesOnAntenna(ctx, antennaID, lock.Start, lock.End)
		if err != nil {
			return err
		}
		for _, schID := range affected {
			staleReason := fmt.Sprintf("设备维护锁定：天线 %s 在 %s~%s 不可用", antennaID,
				lock.Start.Format(time.RFC3339), lock.End.Format(time.RFC3339))
			if err := tx.SetScheduleStatus(ctx, schID, domain.ScheduleStatusStale, staleReason); err != nil {
				return err
			}
			if err := s.releaseUnstartedTasks(ctx, tx, schID); err != nil {
				return err
			}
			if err := s.audit(ctx, tx, actor, "schedule.marked_stale", "schedule", schID,
				map[string]any{"reason": staleReason, "trigger_lock": lock.ID}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// ReleaseMaintenanceLock 解除维护锁定（设备状态更新，写审计）。
func (s *Service) ReleaseMaintenanceLock(ctx context.Context, actor domain.Actor, lockID string) error {
	return s.st.WithTx(ctx, func(tx *store.Tx) error {
		lock, err := tx.GetLock(ctx, lockID)
		if err != nil {
			return notFound(err, "维护锁定 "+lockID)
		}
		if err := requireStation(actor, lock.StationID); err != nil {
			return err
		}
		if err := tx.SetLockActive(ctx, lockID, false); err != nil {
			return err
		}
		return s.audit(ctx, tx, actor, "lock.released", "maintenance_lock", lockID, nil)
	})
}

// releaseUnstartedTasks 排程失效时，把其中尚未开始的任务放回 pending 等待重排；
// 已开始的测控段任务保持 scheduled（进行中的段不受失效标记影响）。
func (s *Service) releaseUnstartedTasks(ctx context.Context, tx *store.Tx, scheduleID string) error {
	now := s.now()
	entries, err := tx.ListEntries(ctx, scheduleID)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.Start.After(now) {
			continue
		}
		task, err := tx.GetTask(ctx, e.TaskID)
		if err != nil {
			return err
		}
		if task.Status != domain.TaskStatusScheduled {
			continue
		}
		if err := tx.UpdateTaskStatus(ctx, task.ID, domain.TaskStatusPending, now); err != nil {
			return err
		}
	}
	return nil
}

// ---- 审计与通知查询 ----

// ListAuditEvents 查询审计事件。
func (s *Service) ListAuditEvents(ctx context.Context, entityType, entityID string, limit int) ([]*domain.AuditEvent, error) {
	var out []*domain.AuditEvent
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListAudit(ctx, entityType, entityID, limit)
		return err
	})
	return out, err
}

// ListNotifications 查询通知记录。
func (s *Service) ListNotifications(ctx context.Context, relatedType, relatedID string) ([]*domain.Notification, error) {
	var out []*domain.Notification
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListNotifications(ctx, relatedType, relatedID)
		return err
	})
	return out, err
}
