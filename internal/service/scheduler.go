// Package service 实现地面站窗口冲突处置业务逻辑。
package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"gscsvc/internal/domain"
	"gscsvc/internal/store"
)

// 业务错误。
var (
	ErrForbidden       = errors.New("forbidden: 无权处置该站点")
	ErrNotFound        = errors.New("not found")
	ErrStationInactive = errors.New("station inactive: 站点不可用")
	ErrAlreadyResolved = errors.New("decision item already resolved")
	ErrNeedSupervisor  = errors.New("forbidden: 跨站迁移需任务平台主管批准")
)

// GenerateResult 一次排程生成的结果摘要。
type GenerateResult struct {
	Version   domain.ScheduleVersion `json:"version"`
	Entries   []domain.ScheduleEntry `json:"entries"`
	Conflicts []domain.Conflict      `json:"conflicts"`
	Queued    []domain.DecisionItem  `json:"queued"`
}

// Engine 排程引擎：按站点串行生成候选排程，消解冲突并产出决策队列。
type Engine struct {
	st  *store.Store
	now func() time.Time

	mu     sync.Mutex
	stLock map[string]*sync.Mutex // 站点级互斥锁，防并发排程竞态
}

// NewEngine 创建引擎。now 可注入以便测试。
func NewEngine(st *store.Store, now func() time.Time) *Engine {
	return &Engine{st: st, now: now, stLock: map[string]*sync.Mutex{}}
}

func (e *Engine) stationLock(stationID string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	l, ok := e.stLock[stationID]
	if !ok {
		l = &sync.Mutex{}
		e.stLock[stationID] = l
	}
	return l
}

// interval 已占用时段（含进行中测控段与本次新排定段）。
type interval struct {
	start, end time.Time
	taskID     string
	locked     bool
}

func overlaps(aStart, aEnd, bStart, bEnd time.Time) bool {
	return aStart.Before(bEnd) && bStart.Before(aEnd)
}

// Generate 为站点在 [from,to] 区间生成候选排程版本。
// 规则：
//   - 进行中（start<=now）的测控段原样继承，任何任务不得抢占；
//   - 与维护锁定重叠、能力不匹配、时长不足的弧段被阻断并说明理由；
//   - 重叠弧段按（优先级降序，开始时间升序）竞争，败者进入人工决策队列并附替代站点。
func (e *Engine) Generate(ctx context.Context, op domain.Operator, stationID string, from, to time.Time, note string) (*GenerateResult, error) {
	if !op.CanOperate(stationID) {
		return nil, ErrForbidden
	}
	lock := e.stationLock(stationID)
	lock.Lock()
	defer lock.Unlock()

	now := e.now().UTC()
	from, to = from.UTC(), to.UTC()

	station, err := e.st.GetStation(ctx, stationID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: station %s", ErrNotFound, stationID)
	}
	if err != nil {
		return nil, err
	}
	if !station.Active {
		return nil, ErrStationInactive
	}

	tx, err := e.st.BeginImmediate(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 上一版本与进行中测控段（事务内读取，保证与写入一致）。
	prevVer, prevErr := tx.ActiveVersion(ctx, stationID)
	prevEntries := []domain.ScheduleEntry{}
	if prevErr == nil {
		prevEntries, err = tx.EntriesOfVersion(ctx, prevVer.ID)
		if err != nil {
			return nil, err
		}
	} else if prevErr != sql.ErrNoRows {
		return nil, prevErr
	}

	var occupied []interval
	var carried []domain.ScheduleEntry
	carriedKey := map[string]bool{}
	for _, en := range prevEntries {
		if (en.Status == domain.EntryScheduled || en.Status == domain.EntryCarried) && !en.StartUTC.After(now) {
			// 已开始测控段：不得静默改写，原样继承。
			en.Locked = true
			carried = append(carried, en)
			carriedKey[en.TaskID+"|"+en.WindowID] = true
			occupied = append(occupied, interval{start: en.StartUTC, end: en.EndUTC, taskID: en.TaskID, locked: true})
		}
	}

	// 候选弧段与约束（事务内读取）。
	windows, err := tx.WindowsForStation(ctx, stationID, from, to)
	if err != nil {
		return nil, err
	}
	tasks, err := tx.ActiveTasks(ctx)
	if err != nil {
		return nil, err
	}
	locks, err := tx.LocksForStation(ctx, stationID, from, to)
	if err != nil {
		return nil, err
	}

	taskBySat := map[string]domain.Task{}
	for _, t := range tasks {
		// 已迁移任务只在目标站参与排程。
		if t.MigratedTo != "" && t.MigratedTo != stationID {
			continue
		}
		if cur, ok := taskBySat[t.SatelliteID]; !ok || t.Priority > cur.Priority {
			taskBySat[t.SatelliteID] = t
		}
	}

	type candidate struct {
		task domain.Task
		win  domain.VisibilityWindow
	}
	var cands []candidate
	var blocked []domain.ScheduleEntry
	var blockedReason = map[string]string{} // taskID -> reason（用于队列）

	for _, w := range windows {
		t, ok := taskBySat[w.SatelliteID]
		if !ok {
			continue // 无活动任务使用该弧段
		}
		if carriedKey[t.TaskID+"|"+w.WindowID] {
			continue // 进行中测控段已继承，不再参与竞争
		}
		entry := domain.ScheduleEntry{
			TaskID: t.TaskID, WindowID: w.WindowID, StationID: stationID,
			SatelliteID: w.SatelliteID, StartUTC: w.StartUTC, EndUTC: w.EndUTC,
			ForecastVersion: w.ForecastVersion, Status: domain.EntryBlocked,
		}
		switch {
		case !station.HasBand(t.RequiredBand):
			entry.Reason = fmt.Sprintf("CAPABILITY_MISMATCH: 天线能力不含频段 %s", t.RequiredBand)
			blocked = append(blocked, entry)
			blockedReason[t.TaskID] = entry.Reason
		case w.DurationSeconds() < int64(t.MinTTSeconds):
			entry.Reason = fmt.Sprintf("MIN_TT_UNMET: 弧段时长 %ds 小于最小测控时长 %ds",
				w.DurationSeconds(), t.MinTTSeconds)
			blocked = append(blocked, entry)
			blockedReason[t.TaskID] = entry.Reason
		case lockOverlaps(w, locks) != nil:
			l := lockOverlaps(w, locks)
			entry.Reason = fmt.Sprintf("MAINTENANCE_LOCK: 与维护锁定 %s（%s）重叠", l.LockID, l.Reason)
			blocked = append(blocked, entry)
			blockedReason[t.TaskID] = entry.Reason
		default:
			cands = append(cands, candidate{task: t, win: w})
		}
	}

	// 竞争排序：优先级降序，同优先级按开始时间升序。
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].task.Priority != cands[j].task.Priority {
			return cands[i].task.Priority > cands[j].task.Priority
		}
		return cands[i].win.StartUTC.Before(cands[j].win.StartUTC)
	})

	var scheduled []domain.ScheduleEntry
	var displaced []domain.ScheduleEntry
	var conflicts []domain.Conflict
	conflictWith := map[string]string{} // loser taskID -> winner taskID

	for _, c := range cands {
		var hit *interval
		for i := range occupied {
			if overlaps(c.win.StartUTC, c.win.EndUTC, occupied[i].start, occupied[i].end) {
				hit = &occupied[i]
				break
			}
		}
		entry := domain.ScheduleEntry{
			TaskID: c.task.TaskID, WindowID: c.win.WindowID, StationID: stationID,
			SatelliteID: c.win.SatelliteID, StartUTC: c.win.StartUTC, EndUTC: c.win.EndUTC,
			ForecastVersion: c.win.ForecastVersion,
		}
		if hit == nil {
			entry.Status = domain.EntryScheduled
			entry.Reason = fmt.Sprintf("OK: 优先级 P%d，弧段无冲突，采用轨道预报 v%d",
				c.task.Priority, c.win.ForecastVersion)
			scheduled = append(scheduled, entry)
			occupied = append(occupied, interval{start: c.win.StartUTC, end: c.win.EndUTC, taskID: c.task.TaskID})
			continue
		}
		// 冲突：被挤出。
		entry.Status = domain.EntryDisplaced
		if hit.locked {
			entry.Reason = fmt.Sprintf("PREEMPT_STARTED_DENIED: 时段已被进行中测控段（任务 %s）占用，已开始段不可抢占", hit.taskID)
		} else {
			entry.Reason = fmt.Sprintf("CONFLICT_LOST_PRIORITY: 与任务 %s 弧段重叠，优先级较低被挤出", hit.taskID)
		}
		displaced = append(displaced, entry)
		os, oe := maxTime(c.win.StartUTC, hit.start), minTime(c.win.EndUTC, hit.end)
		conflicts = append(conflicts, domain.Conflict{
			StationID: stationID, WinnerTaskID: hit.taskID, LoserTaskID: c.task.TaskID,
			OverlapStart: os, OverlapEnd: oe, Reason: entry.Reason,
		})
		conflictWith[c.task.TaskID] = hit.taskID
	}

	// 新版本落库。
	verNo := 1
	if prevErr == nil {
		verNo = prevVer.VersionNo + 1
		if err := tx.SupersedeVersions(ctx, stationID); err != nil {
			return nil, err
		}
	}
	ver := domain.ScheduleVersion{
		StationID: stationID, VersionNo: verNo, Status: domain.VersionActive,
		Note: note, CreatedBy: op.ID, CreatedAt: now,
	}
	verID, err := tx.InsertVersion(ctx, ver)
	if err != nil {
		return nil, err
	}
	ver.ID = verID

	insertEntry := func(en domain.ScheduleEntry) error {
		en.VersionID = verID
		return tx.InsertEntry(ctx, en)
	}
	for i := range carried {
		carried[i].Status = domain.EntryCarried
		carried[i].Reason = fmt.Sprintf("CARRIED: 继承进行中测控段（原版本 v%d），不得改写", verNo-1)
		if err := insertEntry(carried[i]); err != nil {
			return nil, err
		}
	}
	for _, en := range scheduled {
		if err := insertEntry(en); err != nil {
			return nil, err
		}
	}
	for _, en := range blocked {
		if err := insertEntry(en); err != nil {
			return nil, err
		}
	}
	for _, en := range displaced {
		if err := insertEntry(en); err != nil {
			return nil, err
		}
	}
	for _, c := range conflicts {
		c.VersionID = verID
		if err := tx.InsertConflict(ctx, c); err != nil {
			return nil, err
		}
	}

	// 决策队列：被挤出/被阻断任务，附替代站点建议。
	var queued []domain.DecisionItem
	queueOne := func(t domain.Task, w domain.VisibilityWindow, kind, reason, conflictTask string) error {
		alts, err := e.findAlternatives(ctx, tx, t, w, from, to)
		if err != nil {
			return err
		}
		if alts == nil {
			alts = []domain.Alternative{}
		}
		itemID := fmt.Sprintf("%s:%s:%s:v%d", kind, t.TaskID, stationID, verNo)
		item := domain.DecisionItem{
			ItemID: itemID, Kind: kind, TaskID: t.TaskID, StationID: stationID,
			VersionID: verID, Status: domain.QueuePending, CreatedAt: now,
			Payload: domain.DecisionPayload{
				SatelliteID: t.SatelliteID, WindowID: w.WindowID,
				ForecastVersion: w.ForecastVersion, Reason: reason,
				ConflictWith: conflictTask, Alternatives: alts,
				RegenFrom: from, RegenTo: to,
			},
		}
		pj, err := json.Marshal(item.Payload)
		if err != nil {
			return err
		}
		created, err := tx.InsertDecisionItem(ctx, item, string(pj))
		if err != nil {
			return err
		}
		if created {
			queued = append(queued, item)
			if err := tx.InsertNotification(ctx, domain.Notification{
				ItemID: itemID, Channel: "duty_phone_log", Target: stationID + "-oncall",
				Result: "SUCCESS", Detail: "任务进入人工决策队列，已电话/值班通知留痕: " + reason,
				CreatedAt: now,
			}); err != nil {
				return err
			}
		}
		return nil
	}

	taskOf := map[string]domain.Task{}
	for _, t := range tasks {
		taskOf[t.TaskID] = t
	}
	winOf := map[string]domain.VisibilityWindow{}
	for _, w := range windows {
		winOf[w.WindowID] = w
	}
	for _, en := range displaced {
		if err := queueOne(taskOf[en.TaskID], winOf[en.WindowID],
			domain.KindDisplacedByConflict, en.Reason, conflictWith[en.TaskID]); err != nil {
			return nil, err
		}
	}
	for _, en := range blocked {
		kind := domain.KindRequirementUnmet
		if hasPrefix(en.Reason, "MAINTENANCE_LOCK") {
			kind = domain.KindBlockedByLock
		}
		if err := queueOne(taskOf[en.TaskID], winOf[en.WindowID], kind, en.Reason, ""); err != nil {
			return nil, err
		}
	}

	// 审计。
	audit := func(action, entType, entID, detail string) error {
		return tx.InsertAudit(ctx, domain.AuditEvent{
			Ts: now, Actor: op.ID, Role: string(op.Role), Action: action,
			EntityType: entType, EntityID: entID, Detail: detail,
		})
	}
	detail, _ := json.Marshal(map[string]any{
		"station_id": stationID, "version_no": verNo, "from": from, "to": to,
		"scheduled": len(scheduled), "carried": len(carried),
		"displaced": len(displaced), "blocked": len(blocked), "queued": len(queued),
	})
	if err := audit("SCHEDULE_GENERATED", "schedule_version", fmt.Sprintf("%d", verID), string(detail)); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	res := &GenerateResult{Version: ver, Conflicts: conflicts, Queued: queued}
	res.Entries = append(res.Entries, carried...)
	res.Entries = append(res.Entries, scheduled...)
	res.Entries = append(res.Entries, blocked...)
	res.Entries = append(res.Entries, displaced...)
	for i := range res.Entries {
		res.Entries[i].VersionID = verID
	}
	return res, nil
}

func lockOverlaps(w domain.VisibilityWindow, locks []domain.MaintenanceLock) *domain.MaintenanceLock {
	for i := range locks {
		if overlaps(w.StartUTC, w.EndUTC, locks[i].StartUTC, locks[i].EndUTC) {
			return &locks[i]
		}
	}
	return nil
}

// findAlternatives 为被挤出/阻断任务寻找替代站点：同卫星、能力匹配、无锁定、时长满足。
func (e *Engine) findAlternatives(ctx context.Context, tx *store.Tx, t domain.Task, w domain.VisibilityWindow, from, to time.Time) ([]domain.Alternative, error) {
	wins, err := tx.WindowsForSatellite(ctx, t.SatelliteID, from, to)
	if err != nil {
		return nil, err
	}
	var alts []domain.Alternative
	for _, ow := range wins {
		if ow.StationID == w.StationID {
			continue
		}
		st, err := tx.GetStation(ctx, ow.StationID)
		if err != nil || !st.Active || !st.HasBand(t.RequiredBand) {
			continue
		}
		if ow.DurationSeconds() < int64(t.MinTTSeconds) {
			continue
		}
		locks, err := tx.LocksForStation(ctx, ow.StationID, ow.StartUTC, ow.EndUTC)
		if err != nil {
			return nil, err
		}
		if lockOverlaps(ow, locks) != nil {
			continue
		}
		alts = append(alts, domain.Alternative{
			StationID: ow.StationID, WindowID: ow.WindowID,
			StartUTC: ow.StartUTC, EndUTC: ow.EndUTC,
			Reason: fmt.Sprintf("站点 %s 能力匹配且弧段空闲，采用预报 v%d", ow.StationID, ow.ForecastVersion),
		})
	}
	return alts, nil
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

func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
