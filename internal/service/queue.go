package service

import (
	"context"
	"fmt"
	"time"

	"satops/groundstation/internal/domain"
	"satops/groundstation/internal/store"
)

// ---- 人工决策队列 ----

// ListDecisionQueue 列出决策队列。值班长只能看到自己负责站点的条目；
// 主管（stations 为空时）可见全部。
func (s *Service) ListDecisionQueue(ctx context.Context, actor domain.Actor, status string) ([]*domain.DecisionItem, error) {
	stationFilter := actor.Stations
	if actor.Role == domain.RoleSupervisor {
		stationFilter = nil
	}
	var out []*domain.DecisionItem
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListDecisionItems(ctx, stationFilter, status)
		return err
	})
	return out, err
}

// GetDecisionItem 查询单条决策项（含站点权限校验）。
func (s *Service) GetDecisionItem(ctx context.Context, actor domain.Actor, id string) (*domain.DecisionItem, error) {
	var item *domain.DecisionItem
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		item, err = tx.GetDecisionItem(ctx, id)
		return err
	})
	if err != nil {
		return nil, notFound(err, "决策项 "+id)
	}
	if err := requireStation(actor, item.StationID); err != nil {
		return nil, err
	}
	return item, nil
}

// ResolveDecision 处置决策项：
//   - retry：任务回到待排程状态，等待下次排程；
//   - migrate：跨站迁移，仅任务平台主管可批准；
//   - cancel：取消任务。
func (s *Service) ResolveDecision(ctx context.Context, actor domain.Actor, itemID, action, targetStationID string) (*domain.DecisionItem, error) {
	if action != domain.ResolutionRetry && action != domain.ResolutionMigrate && action != domain.ResolutionCancel {
		return nil, fmt.Errorf("%w: action 须为 retry/migrate/cancel", domain.ErrBadInput)
	}
	var item *domain.DecisionItem
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		item, err = tx.GetDecisionItem(ctx, itemID)
		if err != nil {
			return notFound(err, "决策项 "+itemID)
		}
		if item.Status != domain.DecisionStatusPending {
			return fmt.Errorf("%w: 决策项已处置（%s）", domain.ErrConflict, item.Resolution)
		}
		if err := requireStation(actor, item.StationID); err != nil {
			return err
		}
		now := s.now()
		task, err := tx.GetTask(ctx, item.TaskID)
		if err != nil {
			return err
		}
		switch action {
		case domain.ResolutionRetry:
			if err := tx.UpdateTaskStatus(ctx, task.ID, domain.TaskStatusPending, now); err != nil {
				return err
			}
			if err := s.audit(ctx, tx, actor, "task.requeued", "task", task.ID,
				map[string]any{"decision_item": itemID}); err != nil {
				return err
			}
		case domain.ResolutionMigrate:
			// 跨站迁移只能由任务平台主管批准
			if actor.Role != domain.RoleSupervisor {
				return fmt.Errorf("%w: 跨站迁移须由任务平台主管批准", domain.ErrForbidden)
			}
			if targetStationID == "" {
				return fmt.Errorf("%w: 迁移需指定 target_station_id", domain.ErrBadInput)
			}
			if targetStationID == item.StationID {
				return fmt.Errorf("%w: 目标站点与原站点相同，不构成迁移", domain.ErrBadInput)
			}
			if _, err := tx.GetStation(ctx, targetStationID); err != nil {
				return notFound(err, "目标站点 "+targetStationID)
			}
			if err := tx.UpdateTaskStation(ctx, task.ID, targetStationID, now); err != nil {
				return err
			}
			if err := tx.UpdateTaskStatus(ctx, task.ID, domain.TaskStatusPending, now); err != nil {
				return err
			}
			if err := s.audit(ctx, tx, actor, "task.migrated", "task", task.ID, map[string]any{
				"from_station": item.StationID, "to_station": targetStationID, "decision_item": itemID,
			}); err != nil {
				return err
			}
		case domain.ResolutionCancel:
			if err := tx.UpdateTaskStatus(ctx, task.ID, domain.TaskStatusCancelled, now); err != nil {
				return err
			}
			if err := s.audit(ctx, tx, actor, "task.cancelled", "task", task.ID,
				map[string]any{"decision_item": itemID}); err != nil {
				return err
			}
		}
		item.Status = domain.DecisionStatusResolved
		item.Resolution = action
		item.TargetStationID = targetStationID
		item.ResolvedBy = actor.ID
		item.ResolvedAt = &now
		item.UpdatedAt = now
		if err := tx.UpdateDecisionItem(ctx, item); err != nil {
			return err
		}
		return s.audit(ctx, tx, actor, "decision.resolved", "decision_item", itemID, map[string]any{
			"action": action, "target_station_id": targetStationID,
		})
	})
	if err != nil {
		return nil, err
	}
	return item, nil
}

// ---- 窗口详情聚合 ----

// WindowEntryRef 窗口详情中引用该弧段的排程条目。
type WindowEntryRef struct {
	EntryID        string    `json:"entry_id"`
	ScheduleID     string    `json:"schedule_id"`
	ScheduleVer    int       `json:"schedule_version"`
	ScheduleStatus string    `json:"schedule_status"`
	TaskExternalID string    `json:"task_external_id"`
	AntennaID      string    `json:"antenna_id"`
	Start          time.Time `json:"start"`
	End            time.Time `json:"end"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason"`
}

// WindowDetail 窗口详情：值班人员可看到采用的预报版本、引用条目、
// 关联冲突链、替代站点与通知结果。
type WindowDetail struct {
	Window         *domain.VisibilityWindow `json:"window"`
	Forecast       *domain.ForecastVersion  `json:"forecast"`
	Entries        []*WindowEntryRef        `json:"entries"`
	ConflictChains []*domain.DecisionItem   `json:"conflict_chains"`
}

// GetWindowDetail 聚合窗口详情。
func (s *Service) GetWindowDetail(ctx context.Context, actor domain.Actor, windowID string) (*WindowDetail, error) {
	detail := &WindowDetail{}
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		w, err := tx.GetWindow(ctx, windowID)
		if err != nil {
			return notFound(err, "弧段 "+windowID)
		}
		if err := requireStation(actor, w.StationID); err != nil {
			return err
		}
		detail.Window = w
		f, err := tx.GetForecast(ctx, w.ForecastID)
		if err != nil {
			return err
		}
		detail.Forecast = f
		entries, err := tx.EntriesByWindow(ctx, windowID)
		if err != nil {
			return err
		}
		for _, e := range entries {
			ref := &WindowEntryRef{
				EntryID:    e.ID,
				ScheduleID: e.ScheduleID,
				AntennaID:  e.AntennaID,
				Start:      e.Start,
				End:        e.End,
				Status:     e.Status,
				Reason:     e.Reason,
			}
			if sch, err := tx.GetSchedule(ctx, e.ScheduleID); err == nil {
				ref.ScheduleVer = sch.Version
				ref.ScheduleStatus = sch.Status
			}
			if t, err := tx.GetTask(ctx, e.TaskID); err == nil {
				ref.TaskExternalID = t.ExternalID
			}
			detail.Entries = append(detail.Entries, ref)
		}
		items, err := tx.DecisionItemsByWindow(ctx, windowID)
		if err != nil {
			return err
		}
		detail.ConflictChains = items
		return nil
	})
	if err != nil {
		return nil, err
	}
	return detail, nil
}
