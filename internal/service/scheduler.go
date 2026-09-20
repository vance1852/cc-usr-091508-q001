package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"satops/groundstation/internal/domain"
	"satops/groundstation/internal/store"
)

// ---- 排程生成 ----

// GenerateResult 一次排程生成的结果。
type GenerateResult struct {
	Schedule  *domain.Schedule        `json:"schedule"`
	Entries   []*domain.ScheduleEntry `json:"entries"`
	Displaced []*domain.DecisionItem  `json:"displaced"`
}

type interval struct {
	start, end time.Time
}

func overlaps(aStart, aEnd, bStart, bEnd time.Time) bool {
	return aStart.Before(bEnd) && bStart.Before(aEnd)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// freeIntervals 从 [s,e) 中扣除 busy 后剩余的空闲子区间（按起点升序）。
func freeIntervals(s, e time.Time, busy []interval) []interval {
	free := []interval{{s, e}}
	for _, b := range busy {
		var next []interval
		for _, f := range free {
			if !overlaps(f.start, f.end, b.start, b.end) {
				next = append(next, f)
				continue
			}
			if b.start.After(f.start) {
				next = append(next, interval{f.start, minTime(b.start, f.end)})
			}
			if b.end.Before(f.end) {
				next = append(next, interval{maxTime(b.end, f.start), f.end})
			}
		}
		free = next
	}
	return free
}

// occupant 天线占用者，用于冲突链归因。
type occupant struct {
	iv       interval
	kind     string // started_entry | planned_entry | maintenance_lock
	refID    string
	taskID   string
	extID    string
	priority int
	windowID string
}

// GenerateSchedule 为站点在 [from,to] 生成一版候选排程：
//  1. 已开始且未结束的测控段原样保留（不得静默改写），并作为占用参与冲突计算；
//  2. 其余任务按（优先级降序、external_id 升序）贪心排定，保证结果确定性；
//  3. 被挤出的任务进入可恢复的人工决策队列，附冲突链、替代站点与通知结果。
func (s *Service) GenerateSchedule(ctx context.Context, actor domain.Actor, stationID string, from, to time.Time) (*GenerateResult, error) {
	if stationID == "" || !to.After(from) {
		return nil, fmt.Errorf("%w: station_id 必填且 to 须晚于 from", domain.ErrBadInput)
	}
	if err := requireStation(actor, stationID); err != nil {
		return nil, err
	}
	lock := s.stationLock(stationID)
	lock.Lock()
	defer lock.Unlock()

	from, to = from.UTC(), to.UTC()
	now := s.now()
	result := &GenerateResult{}

	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		if _, err := tx.GetStation(ctx, stationID); err != nil {
			return notFound(err, "站点 "+stationID)
		}
		antennas, err := tx.ListAntennas(ctx, stationID)
		if err != nil {
			return err
		}
		var operational []*domain.Antenna
		for _, a := range antennas {
			if a.Status == domain.AntennaStatusOperational {
				operational = append(operational, a)
			}
		}
		locks, err := tx.ListActiveLocksOverlap(ctx, stationID, from, to)
		if err != nil {
			return err
		}
		windows, err := tx.ListWindowsOverlap(ctx, stationID, from, to)
		if err != nil {
			return err
		}
		// 过滤掉引用已作废预报的弧段，并记录预报版本号用于理由文案
		forecastVer := map[string]int{}
		var validWindows []*domain.VisibilityWindow
		for _, w := range windows {
			f, err := tx.GetForecast(ctx, w.ForecastID)
			if err != nil {
				return err
			}
			forecastVer[w.ForecastID] = f.Version
			if f.Superseded {
				continue
			}
			validWindows = append(validWindows, w)
		}

		// 每根天线的占用表：先放入维护锁定
		occupancy := map[string][]occupant{}
		for _, l := range locks {
			occupancy[l.AntennaID] = append(occupancy[l.AntennaID], occupant{
				iv:    interval{l.Start, l.End},
				kind:  "maintenance_lock",
				refID: l.ID,
			})
		}

		// 当前生效排程中已开始的测控段：原样保留并占用资源
		var frozen []*domain.ScheduleEntry
		active, err := tx.ActiveSchedule(ctx, stationID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if active != nil {
			frozen, err = tx.StartedEntries(ctx, active.ID, now)
			if err != nil {
				return err
			}
			for _, fe := range frozen {
				task, err := tx.GetTask(ctx, fe.TaskID)
				if err != nil {
					return err
				}
				occupancy[fe.AntennaID] = append(occupancy[fe.AntennaID], occupant{
					iv:       interval{fe.Start, fe.End},
					kind:     "started_entry",
					refID:    fe.ID,
					taskID:   task.ID,
					extID:    task.ExternalID,
					priority: task.Priority,
					windowID: fe.WindowID,
				})
			}
		}

		// 候选任务池：pending/displaced（时间约束与本区间有交集）
		// + 生效排程中尚未开始且落在本区间内的任务（允许被重排）。
		// 区间外的旧计划不重排，原样带入候选排程（见 carryOver）。
		var pool []*domain.Task
		inPool := map[string]bool{}
		rawPool, err := tx.ListSchedulableTasks(ctx, stationID)
		if err != nil {
			return err
		}
		for _, t := range rawPool {
			if !taskRangeIntersects(t, from, to) {
				continue
			}
			pool = append(pool, t)
			inPool[t.ID] = true
		}
		var carryOver []*domain.ScheduleEntry
		if active != nil {
			entries, err := tx.ListEntries(ctx, active.ID)
			if err != nil {
				return err
			}
			for _, e := range entries {
				if !e.Start.After(now) {
					continue // 已开始的由 frozen 保留
				}
				if !overlaps(e.Start, e.End, from, to) {
					carryOver = append(carryOver, e) // 区间外原样保留
					continue
				}
				if inPool[e.TaskID] {
					continue
				}
				task, err := tx.GetTask(ctx, e.TaskID)
				if err != nil {
					return err
				}
				if task.Status == domain.TaskStatusCancelled {
					continue
				}
				pool = append(pool, task)
				inPool[task.ID] = true
			}
		}
		// 确定性排序：优先级降序，再按幂等键升序
		sort.Slice(pool, func(i, j int) bool {
			if pool[i].Priority != pool[j].Priority {
				return pool[i].Priority > pool[j].Priority
			}
			return pool[i].ExternalID < pool[j].ExternalID
		})

		// 新候选排程
		version, err := tx.NextScheduleVersion(ctx, stationID)
		if err != nil {
			return err
		}
		sch := &domain.Schedule{
			ID:        domain.NewID("sch"),
			StationID: stationID,
			Version:   version,
			Status:    domain.ScheduleStatusCandidate,
			RangeFrom: from,
			RangeTo:   to,
			CreatedBy: actor.ID,
			CreatedAt: now,
		}
		if err := tx.InsertSchedule(ctx, sch); err != nil {
			return err
		}
		result.Schedule = sch

		// 保留已开始的测控段
		for _, fe := range frozen {
			kept := &domain.ScheduleEntry{
				ID:         domain.NewID("ent"),
				ScheduleID: sch.ID,
				TaskID:     fe.TaskID,
				WindowID:   fe.WindowID,
				AntennaID:  fe.AntennaID,
				StationID:  stationID,
				ForecastID: fe.ForecastID,
				Start:      fe.Start,
				End:        fe.End,
				Status:     domain.EntryStatusKeptStarted,
				Reason: fmt.Sprintf("测控段已于 %s 开始，按规约原样保留，不随重排改写",
					fe.Start.Format(time.RFC3339)),
				CreatedAt: now,
			}
			if err := tx.InsertEntry(ctx, kept); err != nil {
				return err
			}
			result.Entries = append(result.Entries, kept)
			if err := s.audit(ctx, tx, actor, "entry.kept_started", "schedule_entry", kept.ID,
				map[string]any{"task_id": fe.TaskID, "origin_entry": fe.ID}); err != nil {
				return err
			}
		}

		// 区间外旧计划原样带入候选，保证候选排程对站点是完整计划
		for _, ce := range carryOver {
			kept := &domain.ScheduleEntry{
				ID:         domain.NewID("ent"),
				ScheduleID: sch.ID,
				TaskID:     ce.TaskID,
				WindowID:   ce.WindowID,
				AntennaID:  ce.AntennaID,
				StationID:  stationID,
				ForecastID: ce.ForecastID,
				Start:      ce.Start,
				End:        ce.End,
				Status:     domain.EntryStatusPlanned,
				Reason:     "超出本次重排区间，自上一版排程原样保留",
				CreatedAt:  now,
			}
			if err := tx.InsertEntry(ctx, kept); err != nil {
				return err
			}
			result.Entries = append(result.Entries, kept)
		}

		// 贪心排定
		for _, task := range pool {
			entry, chain, contestedWindow, err := s.placeTask(task, validWindows, operational, occupancy, forecastVer, from, to, now)
			if err != nil {
				return err
			}
			if entry != nil {
				entry.ScheduleID = sch.ID
				entry.StationID = stationID
				if err := tx.InsertEntry(ctx, entry); err != nil {
					return err
				}
				result.Entries = append(result.Entries, entry)
				occupancy[entry.AntennaID] = append(occupancy[entry.AntennaID], occupant{
					iv:       interval{entry.Start, entry.End},
					kind:     "planned_entry",
					refID:    entry.ID,
					taskID:   task.ID,
					extID:    task.ExternalID,
					priority: task.Priority,
					windowID: entry.WindowID,
				})
				if err := tx.UpdateTaskStatus(ctx, task.ID, domain.TaskStatusScheduled, now); err != nil {
					return err
				}
				// 任务已被排定：自动关闭其在其他站点遗留的待决项
				if pending, err := tx.PendingDecisionForTask(ctx, task.ID); err != nil {
					return err
				} else if pending != nil {
					pending.Status = domain.DecisionStatusResolved
					pending.Resolution = domain.ResolutionAutoScheduled
					pending.ResolvedBy = actor.ID
					pending.ResolvedAt = &now
					pending.UpdatedAt = now
					if err := tx.UpdateDecisionItem(ctx, pending); err != nil {
						return err
					}
					if err := s.audit(ctx, tx, actor, "decision.auto_resolved", "decision_item", pending.ID,
						map[string]any{"task_id": task.ID, "scheduled_at": stationID}); err != nil {
						return err
					}
				}
				continue
			}
			// 被挤出：进入人工决策队列
			item, err := s.enqueueDecision(ctx, tx, actor, task, stationID, contestedWindow, chain, now)
			if err != nil {
				return err
			}
			result.Displaced = append(result.Displaced, item)
		}

		return s.audit(ctx, tx, actor, "schedule.generated", "schedule", sch.ID, map[string]any{
			"station_id": stationID, "version": version,
			"entries": len(result.Entries), "displaced": len(result.Displaced),
		})
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// taskRangeIntersects 判断任务的时间约束是否与排程区间有交集。
func taskRangeIntersects(t *domain.Task, from, to time.Time) bool {
	if !t.NotBefore.IsZero() && !t.NotBefore.Before(to) {
		return false
	}
	if !t.NotAfter.IsZero() && !t.NotAfter.After(from) {
		return false
	}
	return true
}

// placeTask 尝试为任务在本站找到可行位置；失败时返回冲突链与主要竞争窗口。
func (s *Service) placeTask(task *domain.Task, windows []*domain.VisibilityWindow, antennas []*domain.Antenna,
	occupancy map[string][]occupant, forecastVer map[string]int, from, to, now time.Time,
) (entry *domain.ScheduleEntry, chain []domain.ConflictNode, contestedWindow string, err error) {
	searchStart := maxTime(from, task.NotBefore)
	searchEnd := to
	if !task.NotAfter.IsZero() {
		searchEnd = minTime(searchEnd, task.NotAfter)
	}
	if !searchEnd.After(searchStart) {
		chain = append(chain, domain.ConflictNode{
			Kind:   "no_window",
			Detail: "任务时间约束与排程区间无交集",
		})
		return nil, chain, "", nil
	}

	var compatible []*domain.Antenna
	for _, a := range antennas {
		if a.SupportsBand(task.RequiredBand) {
			compatible = append(compatible, a)
		}
	}
	if len(compatible) == 0 {
		chain = append(chain, domain.ConflictNode{
			Kind:   "no_antenna",
			Detail: fmt.Sprintf("站点无支持频段 %q 的可用天线", task.RequiredBand),
		})
		return nil, chain, "", nil
	}

	hadWindow := false
	for _, w := range windows {
		if w.SatelliteID != task.SatelliteID {
			continue
		}
		slotStart := maxTime(w.AOS, searchStart)
		slotEnd := minTime(w.LOS, searchEnd)
		if !slotEnd.After(slotStart) || slotEnd.Sub(slotStart) < task.MinDuration {
			continue
		}
		hadWindow = true
		placed := false
		for _, ant := range compatible {
			var busy []interval
			for _, oc := range occupancy[ant.ID] {
				busy = append(busy, oc.iv)
			}
			for _, f := range freeIntervals(slotStart, slotEnd, busy) {
				if f.end.Sub(f.start) >= task.MinDuration {
					entry = &domain.ScheduleEntry{
						ID:         domain.NewID("ent"),
						TaskID:     task.ID,
						WindowID:   w.ID,
						AntennaID:  ant.ID,
						ForecastID: w.ForecastID,
						Start:      f.start,
						End:        f.start.Add(task.MinDuration),
						Status:     domain.EntryStatusPlanned,
						Reason: fmt.Sprintf("窗口 %s（预报 v%d）%s~%s 覆盖最小测控时长 %ds；天线 %s 频段匹配；按优先级 %d 排定",
							w.ID, forecastVer[w.ForecastID],
							w.AOS.Format(time.RFC3339), w.LOS.Format(time.RFC3339),
							int64(task.MinDuration/time.Second), ant.ID, task.Priority),
						CreatedAt: now,
					}
					placed = true
					break
				}
			}
			if placed {
				break
			}
		}
		if placed {
			return entry, nil, "", nil
		}
		// 该窗口失败：归因占用者
		if contestedWindow == "" {
			contestedWindow = w.ID
		}
		seen := map[string]bool{}
		for _, ant := range compatible {
			for _, oc := range occupancy[ant.ID] {
				if !overlaps(slotStart, slotEnd, oc.iv.start, oc.iv.end) || seen[oc.refID] {
					continue
				}
				seen[oc.refID] = true
				node := domain.ConflictNode{
					Kind:      oc.kind,
					TaskID:    oc.taskID,
					WindowID:  oc.windowID,
					AntennaID: ant.ID,
					Priority:  oc.priority,
					Start:     oc.iv.start.Format(time.RFC3339),
					End:       oc.iv.end.Format(time.RFC3339),
				}
				switch oc.kind {
				case "started_entry":
					node.Detail = fmt.Sprintf("测控段（任务 %s，优先级 %d）已开始，按规约不可改写", oc.extID, oc.priority)
				case "planned_entry":
					node.Detail = fmt.Sprintf("时段已被更高或同等优先级任务 %s（优先级 %d）排定", oc.extID, oc.priority)
				case "maintenance_lock":
					node.LockID = oc.refID
					node.Detail = "天线处于维护锁定期"
				}
				chain = append(chain, node)
			}
		}
	}
	if !hadWindow {
		chain = append(chain, domain.ConflictNode{
			Kind:   "no_window",
			Detail: "本站无满足最小测控时长与时间约束的可见弧段",
		})
	}
	return nil, chain, contestedWindow, nil
}

// enqueueDecision 把被挤出的任务写入人工决策队列（幂等：同任务已有待决项则更新），
// 计算替代站点并发送通知，全部记录可追溯。
func (s *Service) enqueueDecision(ctx context.Context, tx *store.Tx, actor domain.Actor, task *domain.Task,
	stationID, windowID string, chain []domain.ConflictNode, now time.Time,
) (*domain.DecisionItem, error) {
	if err := tx.UpdateTaskStatus(ctx, task.ID, domain.TaskStatusDisplaced, now); err != nil {
		return nil, err
	}
	reason := fmt.Sprintf("任务 %s（优先级 %d）在站点 %s 未能排定，被挤出等待人工决策",
		task.ExternalID, task.Priority, stationID)

	alts, err := s.findAlternatives(ctx, tx, task, stationID, now)
	if err != nil {
		return nil, err
	}

	item := &domain.DecisionItem{
		ID:            domain.NewID("dq"),
		TaskID:        task.ID,
		StationID:     stationID,
		WindowID:      windowID,
		Reason:        reason,
		ConflictChain: chain,
		Alternatives:  alts,
		Status:        domain.DecisionStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	// 通知本站值班通道，结果写入队列项
	notif := &domain.Notification{
		ID:          domain.NewID("ntf"),
		Target:      "duty:" + stationID,
		Channel:     "ops-log",
		Payload:     reason,
		Status:      "sent",
		RelatedType: "decision_item",
		RelatedID:   item.ID,
		CreatedAt:   now,
	}
	if err := tx.InsertNotification(ctx, notif); err != nil {
		return nil, err
	}
	item.Notifications = append(item.Notifications, domain.NotificationResult{
		ID: notif.ID, Target: notif.Target, Channel: notif.Channel,
		Status: notif.Status, At: notif.CreatedAt.Format(time.RFC3339),
	})

	// 幂等：同任务已有待决项则原地更新，避免重复队列项
	existing, err := tx.PendingDecisionForTask(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		item.ID = existing.ID
		item.CreatedAt = existing.CreatedAt
		if err := tx.UpdateDecisionItem(ctx, item); err != nil {
			return nil, err
		}
	} else if err := tx.InsertDecisionItem(ctx, item); err != nil {
		return nil, err
	}

	if err := s.audit(ctx, tx, actor, "task.displaced", "task", task.ID, map[string]any{
		"station_id": stationID, "decision_item": item.ID, "reason": reason,
	}); err != nil {
		return nil, err
	}
	if err := s.audit(ctx, tx, actor, "decision.enqueued", "decision_item", item.ID, map[string]any{
		"task_id": task.ID, "station_id": stationID, "alternatives": len(alts),
	}); err != nil {
		return nil, err
	}
	return item, nil
}

// findAlternatives 为被挤出任务寻找其他站点的可行窗口（供人工迁移参考）。
func (s *Service) findAlternatives(ctx context.Context, tx *store.Tx, task *domain.Task, excludeStation string, now time.Time) ([]domain.Alternative, error) {
	searchStart := now
	if !task.NotBefore.IsZero() && task.NotBefore.After(searchStart) {
		searchStart = task.NotBefore
	}
	searchEnd := searchStart.Add(72 * time.Hour)
	if !task.NotAfter.IsZero() && task.NotAfter.Before(searchEnd) {
		searchEnd = task.NotAfter
	}
	if !searchEnd.After(searchStart) {
		return nil, nil
	}
	windows, err := tx.ListWindowsForSatellite(ctx, task.SatelliteID, searchStart, searchEnd)
	if err != nil {
		return nil, err
	}
	var out []domain.Alternative
	for _, w := range windows {
		if w.StationID == excludeStation {
			continue
		}
		f, err := tx.GetForecast(ctx, w.ForecastID)
		if err != nil {
			return nil, err
		}
		if f.Superseded {
			continue
		}
		antennas, err := tx.ListAntennas(ctx, w.StationID)
		if err != nil {
			return nil, err
		}
		var usable []string
		for _, a := range antennas {
			if a.Status != domain.AntennaStatusOperational || !a.SupportsBand(task.RequiredBand) {
				continue
			}
			locks, err := tx.ListActiveLocksForAntenna(ctx, a.ID, w.AOS, w.LOS)
			if err != nil {
				return nil, err
			}
			if len(locks) == 0 {
				usable = append(usable, a.ID)
			}
		}
		if len(usable) == 0 {
			continue
		}
		out = append(out, domain.Alternative{
			StationID:  w.StationID,
			WindowID:   w.ID,
			AntennaIDs: usable,
			AOS:        w.AOS.Format(time.RFC3339),
			LOS:        w.LOS.Format(time.RFC3339),
			Detail: fmt.Sprintf("站点 %s 在 %s~%s 有兼容窗口（预报 v%d）", w.StationID,
				w.AOS.Format(time.RFC3339), w.LOS.Format(time.RFC3339), f.Version),
		})
		if len(out) >= 5 {
			break
		}
	}
	return out, nil
}

// ---- 排程激活 ----

// ActivateSchedule 激活候选排程：
//   - stale 版本拒绝激活（须重新生成）；
//   - 校验当前生效排程中已开始的测控段在候选中原样存在，否则 409；
//   - 被新排程丢弃且未开始的旧计划任务转入决策队列。
func (s *Service) ActivateSchedule(ctx context.Context, actor domain.Actor, scheduleID string) (*domain.Schedule, error) {
	var cand *domain.Schedule
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		cand, err = tx.GetSchedule(ctx, scheduleID)
		if err != nil {
			return notFound(err, "排程 "+scheduleID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := requireStation(actor, cand.StationID); err != nil {
		return nil, err
	}
	lock := s.stationLock(cand.StationID)
	lock.Lock()
	defer lock.Unlock()

	now := s.now()
	err = s.st.WithTx(ctx, func(tx *store.Tx) error {
		cand, err := tx.GetSchedule(ctx, scheduleID)
		if err != nil {
			return notFound(err, "排程 "+scheduleID)
		}
		switch cand.Status {
		case domain.ScheduleStatusStale:
			return fmt.Errorf("%w: %s", domain.ErrStaleSchedule, cand.StaleReason)
		case domain.ScheduleStatusCandidate:
		default:
			return fmt.Errorf("%w: 仅候选排程可激活，当前状态 %s", domain.ErrConflict, cand.Status)
		}

		active, err := tx.ActiveSchedule(ctx, cand.StationID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if active != nil && active.ID != cand.ID {
			frozen, err := tx.StartedEntries(ctx, active.ID, now)
			if err != nil {
				return err
			}
			candEntries, err := tx.ListEntries(ctx, cand.ID)
			if err != nil {
				return err
			}
			// 不得静默改写已开始的测控段
			for _, fe := range frozen {
				found := false
				for _, ce := range candEntries {
					if ce.TaskID == fe.TaskID && ce.WindowID == fe.WindowID &&
						ce.AntennaID == fe.AntennaID && ce.Start.Equal(fe.Start) && ce.End.Equal(fe.End) {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("%w: 任务 %s 的测控段 %s~%s 在候选排程 v%d 中被改写或移除",
						domain.ErrStartedSegment, fe.TaskID,
						fe.Start.Format(time.RFC3339), fe.End.Format(time.RFC3339), cand.Version)
				}
			}
			// 旧计划中未开始但被新排程丢弃的任务 → 挤出
			candTasks := map[string]bool{}
			for _, ce := range candEntries {
				candTasks[ce.TaskID] = true
			}
			oldEntries, err := tx.ListEntries(ctx, active.ID)
			if err != nil {
				return err
			}
			for _, oe := range oldEntries {
				if !oe.Start.After(now) || candTasks[oe.TaskID] {
					continue
				}
				task, err := tx.GetTask(ctx, oe.TaskID)
				if err != nil {
					return err
				}
				if task.Status != domain.TaskStatusScheduled {
					continue
				}
				chain := []domain.ConflictNode{{
					Kind:       "superseded_by_schedule",
					ScheduleID: cand.ID,
					WindowID:   oe.WindowID,
					Detail:     fmt.Sprintf("排程版本 v%d 激活，原计划条目被替换", cand.Version),
				}}
				if _, err := s.enqueueDecision(ctx, tx, actor, task, cand.StationID, oe.WindowID, chain, now); err != nil {
					return err
				}
			}
			if err := tx.SetScheduleStatus(ctx, active.ID, domain.ScheduleStatusSuperseded, ""); err != nil {
				return err
			}
		}
		if err := tx.SetScheduleStatus(ctx, cand.ID, domain.ScheduleStatusActive, ""); err != nil {
			return err
		}
		return s.audit(ctx, tx, actor, "schedule.activated", "schedule", cand.ID, map[string]any{
			"station_id": cand.StationID, "version": cand.Version,
		})
	})
	if err != nil {
		return nil, err
	}
	out, err := s.GetSchedule(ctx, scheduleID)
	if err != nil {
		return nil, err
	}
	return out.Schedule, nil
}

// ScheduleView 排程详情：条目携带预报版本号与任务信息，便于值班人员追溯。
type ScheduleView struct {
	Schedule *domain.Schedule `json:"schedule"`
	Entries  []*EntryView     `json:"entries"`
}

// EntryView 排程条目视图。
type EntryView struct {
	*domain.ScheduleEntry
	ForecastVersion int    `json:"forecast_version"`
	TaskExternalID  string `json:"task_external_id"`
	TaskPriority    int    `json:"task_priority"`
	TaskType        string `json:"task_type"`
}

// GetSchedule 查询排程详情。
func (s *Service) GetSchedule(ctx context.Context, id string) (*ScheduleView, error) {
	view := &ScheduleView{}
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		sch, err := tx.GetSchedule(ctx, id)
		if err != nil {
			return notFound(err, "排程 "+id)
		}
		view.Schedule = sch
		entries, err := tx.ListEntries(ctx, id)
		if err != nil {
			return err
		}
		for _, e := range entries {
			ev := &EntryView{ScheduleEntry: e}
			if f, err := tx.GetForecast(ctx, e.ForecastID); err == nil {
				ev.ForecastVersion = f.Version
			}
			if t, err := tx.GetTask(ctx, e.TaskID); err == nil {
				ev.TaskExternalID = t.ExternalID
				ev.TaskPriority = t.Priority
				ev.TaskType = string(t.Type)
			}
			view.Entries = append(view.Entries, ev)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return view, nil
}

// ListSchedules 列出站点排程版本。
func (s *Service) ListSchedules(ctx context.Context, stationID, status string) ([]*domain.Schedule, error) {
	var out []*domain.Schedule
	err := s.st.WithTx(ctx, func(tx *store.Tx) error {
		var err error
		out, err = tx.ListSchedules(ctx, stationID, status)
		return err
	})
	return out, err
}
