package service_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"satops/groundstation/internal/domain"
	"satops/groundstation/internal/service"
	"satops/groundstation/internal/store"
)

// ---- 测试环境 ----

type env struct {
	t     *testing.T
	st    *store.Store
	svc   *service.Service
	now   time.Time
	actor domain.Actor
}

func newEnv(t *testing.T, dbPath string) *env {
	t.Helper()
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	e := &env{t: t, st: st}
	e.now = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	e.svc = service.New(st, service.WithClock(func() time.Time { return e.now }))
	e.actor = domain.Actor{ID: "duty-1", Role: domain.RoleDutyOfficer, Stations: []string{"ST-A", "ST-B"}}
	return e
}

func (e *env) close() { e.st.Close() }

func (e *env) ctx() context.Context { return context.Background() }

func (e *env) addStation(id string, antennas ...service.AntennaInput) {
	e.t.Helper()
	if _, err := e.svc.RegisterStation(e.ctx(), e.actor, id, "站点"+id, antennas); err != nil {
		e.t.Fatalf("注册站点失败: %v", err)
	}
}

func (e *env) addForecast(sat string, ver int) *domain.ForecastVersion {
	e.t.Helper()
	f, err := e.svc.RegisterForecast(e.ctx(), e.actor, sat, ver, e.now, "")
	if err != nil {
		e.t.Fatalf("登记预报失败: %v", err)
	}
	return f
}

func (e *env) addWindow(sat, station, forecastID string, aos, los time.Time) *domain.VisibilityWindow {
	e.t.Helper()
	ws, err := e.svc.RegisterWindows(e.ctx(), e.actor, []service.WindowInput{{
		SatelliteID: sat, StationID: station, ForecastID: forecastID, AOS: aos, LOS: los, MaxElevation: 60,
	}})
	if err != nil {
		e.t.Fatalf("登记弧段失败: %v", err)
	}
	return ws[0]
}

func (e *env) addTask(extID, typ, sat string, pri int, minDur time.Duration, band string) *domain.Task {
	e.t.Helper()
	task, created, err := e.svc.SubmitTask(e.ctx(), e.actor, service.TaskInput{
		ExternalID: extID, Type: typ, SatelliteID: sat, Priority: pri,
		MinDurationSeconds: int64(minDur / time.Second), RequiredBand: band,
	})
	if err != nil {
		e.t.Fatalf("提交任务失败: %v", err)
	}
	if !created {
		e.t.Fatalf("任务 %s 应为新创建", extID)
	}
	return task
}

func (e *env) generate(stationID string, from, to time.Time) *service.GenerateResult {
	e.t.Helper()
	res, err := e.svc.GenerateSchedule(e.ctx(), e.actor, stationID, from, to)
	if err != nil {
		e.t.Fatalf("生成排程失败: %v", err)
	}
	return res
}

func (e *env) activate(scheduleID string) *domain.Schedule {
	e.t.Helper()
	sch, err := e.svc.ActivateSchedule(e.ctx(), e.actor, scheduleID)
	if err != nil {
		e.t.Fatalf("激活排程失败: %v", err)
	}
	return sch
}

func at(day, hour, min int) time.Time {
	return time.Date(2026, 9, day, hour, min, 0, 0, time.UTC)
}

// ---- 跨午夜窗口 ----

func TestCrossMidnightWindow(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S", "X"}})
	f := e.addForecast("SAT-1", 1)

	// 跨午夜弧段：9月19日 23:40 ～ 9月20日 00:30
	w := e.addWindow("SAT-1", "ST-A", f.ID, at(19, 23, 40), at(20, 0, 30))
	if d := w.Duration(); d != 50*time.Minute {
		t.Fatalf("跨午夜弧段时长应为 50min，实际 %v", d)
	}

	e.addTask("T-TTNC-1", "ttnc", "SAT-1", 50, 45*time.Minute, "S")

	res := e.generate("ST-A", at(19, 0, 0), at(20, 6, 0))
	if len(res.Entries) != 1 {
		t.Fatalf("应排定 1 个条目，实际 %d", len(res.Entries))
	}
	entry := res.Entries[0]
	if !entry.Start.Equal(at(19, 23, 40)) {
		t.Fatalf("条目应始于 AOS 23:40，实际 %s", entry.Start)
	}
	if !entry.End.Equal(at(20, 0, 25)) {
		t.Fatalf("条目应跨午夜终于次日 00:25，实际 %s", entry.End)
	}
	if entry.End.Sub(entry.Start) != 45*time.Minute {
		t.Fatalf("条目时长应为 45min，实际 %v", entry.End.Sub(entry.Start))
	}
	if entry.ForecastID != f.ID {
		t.Fatalf("条目应记录采用的预报版本")
	}
	task, err := e.svc.GetTask(e.ctx(), entry.TaskID)
	if err != nil || task.Status != domain.TaskStatusScheduled {
		t.Fatalf("任务状态应为 scheduled，实际 %v (%v)", task.Status, err)
	}
}

// ---- 冲突挤出与人工决策队列 ----

func TestConflictDisplacementAndQueue(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
	e.addStation("ST-B", service.AntennaInput{ID: "ANT-2", Bands: []string{"S"}})
	f1 := e.addForecast("SAT-1", 1)
	f2 := e.addForecast("SAT-2", 1)

	// 两颗卫星在同一地面站的弧段完全重叠，且任务同为最高优先级；
	// 弧段 60min、任务各需 45min，同一根天线只能容纳其一
	e.addWindow("SAT-1", "ST-A", f1.ID, at(20, 10, 0), at(20, 11, 0))
	wB := e.addWindow("SAT-2", "ST-A", f2.ID, at(20, 10, 0), at(20, 11, 0))
	// SAT-2 在 ST-B 有可迁移窗口
	wC := e.addWindow("SAT-2", "ST-B", f2.ID, at(20, 10, 30), at(20, 11, 30))

	hi := e.addTask("A-数据回收", "downlink", "SAT-1", 90, 45*time.Minute, "S")
	lo := e.addTask("B-测控保活", "ttnc", "SAT-2", 90, 45*time.Minute, "S")

	res := e.generate("ST-A", at(20, 0, 0), at(20, 23, 59))
	if len(res.Entries) != 1 {
		t.Fatalf("同一时段只能排定 1 个任务，实际 %d", len(res.Entries))
	}
	if res.Entries[0].TaskID != hi.ID {
		t.Fatalf("同优先级按幂等键排序，A-数据回收 应先排定")
	}
	if len(res.Displaced) != 1 {
		t.Fatalf("应有 1 个任务被挤出，实际 %d", len(res.Displaced))
	}

	item := res.Displaced[0]
	if item.TaskID != lo.ID {
		t.Fatalf("被挤出的应是 B-测控保活")
	}
	if item.Status != domain.DecisionStatusPending {
		t.Fatalf("决策项应为 pending")
	}
	if item.WindowID != wB.ID {
		t.Fatalf("主要竞争窗口应为 SAT-2 在 ST-A 的弧段")
	}
	// 冲突链：指向占用时段的更高/同等优先级任务
	if len(item.ConflictChain) == 0 || item.ConflictChain[0].Kind != "planned_entry" {
		t.Fatalf("冲突链首节点应为 planned_entry，实际 %+v", item.ConflictChain)
	}
	if item.ConflictChain[0].TaskID != hi.ID {
		t.Fatalf("冲突链应归因到排定任务 A-数据回收")
	}
	// 替代站点
	if len(item.Alternatives) != 1 || item.Alternatives[0].StationID != "ST-B" || item.Alternatives[0].WindowID != wC.ID {
		t.Fatalf("替代站点应为 ST-B 的窗口，实际 %+v", item.Alternatives)
	}
	// 通知结果
	if len(item.Notifications) != 1 || item.Notifications[0].Status != "sent" || item.Notifications[0].Target != "duty:ST-A" {
		t.Fatalf("通知结果缺失: %+v", item.Notifications)
	}
	// 任务状态与审计
	task, _ := e.svc.GetTask(e.ctx(), lo.ID)
	if task.Status != domain.TaskStatusDisplaced {
		t.Fatalf("被挤出任务状态应为 displaced，实际 %s", task.Status)
	}
	events, err := e.svc.ListAuditEvents(e.ctx(), "decision_item", item.ID, 0)
	if err != nil || len(events) == 0 {
		t.Fatalf("决策项应有审计事件")
	}

	// 队列可恢复查询 + retry 处置
	queue, err := e.svc.ListDecisionQueue(e.ctx(), e.actor, domain.DecisionStatusPending)
	if err != nil || len(queue) != 1 {
		t.Fatalf("队列应有 1 条待决项，实际 %d (%v)", len(queue), err)
	}
	if _, err := e.svc.ResolveDecision(e.ctx(), e.actor, item.ID, domain.ResolutionRetry, ""); err != nil {
		t.Fatalf("retry 处置失败: %v", err)
	}
	task, _ = e.svc.GetTask(e.ctx(), lo.ID)
	if task.Status != domain.TaskStatusPending {
		t.Fatalf("retry 后任务应回到 pending，实际 %s", task.Status)
	}
}

// ---- 已开始的测控段不得被静默改写 ----

func TestStartedSegmentPreservedOnRegenerate(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
	f1 := e.addForecast("SAT-1", 1)
	f2 := e.addForecast("SAT-2", 1)

	// 进行中的测控段：11:00 开始，12:30 结束（当前 12:00）
	e.addWindow("SAT-1", "ST-A", f1.ID, at(19, 11, 0), at(19, 13, 0))
	running := e.addTask("RUN-进行中", "ttnc", "SAT-1", 50, 90*time.Minute, "S")

	first := e.generate("ST-A", at(19, 0, 0), at(19, 23, 59))
	e.activate(first.Schedule.ID)

	// 更高优先级任务争夺相同时段
	e.addWindow("SAT-2", "ST-A", f2.ID, at(19, 11, 30), at(19, 12, 30))
	hp := e.addTask("HP-抢占", "downlink", "SAT-2", 95, 30*time.Minute, "S")

	second := e.generate("ST-A", at(19, 0, 0), at(19, 23, 59))

	// 已开始测控段原样保留
	var kept *domain.ScheduleEntry
	for _, en := range second.Entries {
		if en.TaskID == running.ID {
			kept = en
		}
	}
	if kept == nil {
		t.Fatalf("进行中的测控段必须保留在新候选排程中")
	}
	if kept.Status != domain.EntryStatusKeptStarted {
		t.Fatalf("保留条目标记应为 kept_started，实际 %s", kept.Status)
	}
	if !kept.Start.Equal(at(19, 11, 0)) || !kept.End.Equal(at(19, 12, 30)) || kept.AntennaID != "ANT-1" {
		t.Fatalf("保留条目的时段/天线不得改变: %+v", kept)
	}
	// 高优任务被挤出，冲突链归因 started_entry
	if len(second.Displaced) != 1 || second.Displaced[0].TaskID != hp.ID {
		t.Fatalf("高优任务应被挤出（不能抢占进行中测控段）")
	}
	chain := second.Displaced[0].ConflictChain
	if len(chain) == 0 || chain[0].Kind != "started_entry" {
		t.Fatalf("冲突链应标记 started_entry，实际 %+v", chain)
	}
	// 激活保留了进行中测控段的候选排程应当成功
	e.activate(second.Schedule.ID)
}

func TestActivateRejectsRewrittenStartedSegment(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
	f1 := e.addForecast("SAT-1", 1)

	// C1 在任务提交前生成（不含任何条目）
	empty := e.generate("ST-A", at(19, 0, 0), at(19, 23, 59))

	e.addWindow("SAT-1", "ST-A", f1.ID, at(19, 12, 30), at(19, 13, 30))
	e.addTask("RUN", "ttnc", "SAT-1", 50, 30*time.Minute, "S")
	c2 := e.generate("ST-A", at(19, 0, 0), at(19, 23, 59))
	e.activate(c2.Schedule.ID)

	// 时间推进到测控段进行中
	e.now = at(19, 12, 45)

	// 旧的候选 C1 缺少进行中的测控段：激活必须被拒绝
	_, err := e.svc.ActivateSchedule(e.ctx(), e.actor, empty.Schedule.ID)
	if !errors.Is(err, domain.ErrStartedSegment) {
		t.Fatalf("应拒绝改写已开始测控段，实际错误 %v", err)
	}
	// C2 仍为生效排程
	schs, _ := e.svc.ListSchedules(e.ctx(), "ST-A", domain.ScheduleStatusActive)
	if len(schs) != 1 || schs[0].ID != c2.Schedule.ID {
		t.Fatalf("生效排程应仍为 C2")
	}
}

// ---- 预报/设备更新标记受影响排程 ----

func TestForecastUpdateMarksScheduleStale(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
	f1 := e.addForecast("SAT-1", 1)
	e.addWindow("SAT-1", "ST-A", f1.ID, at(20, 10, 0), at(20, 11, 0))
	e.addTask("T1", "downlink", "SAT-1", 80, 30*time.Minute, "S")

	res := e.generate("ST-A", at(20, 0, 0), at(20, 23, 59))
	e.activate(res.Schedule.ID)

	// 发布新版轨道预报 → 引用旧版弧段的生效排程被标记 stale
	f2 := e.addForecast("SAT-1", 2)

	schs, err := e.svc.ListSchedules(e.ctx(), "ST-A", domain.ScheduleStatusStale)
	if err != nil || len(schs) != 1 || schs[0].ID != res.Schedule.ID {
		t.Fatalf("生效排程应被标记为 stale，实际 %+v (%v)", schs, err)
	}
	events, _ := e.svc.ListAuditEvents(e.ctx(), "schedule", res.Schedule.ID, 0)
	found := false
	for _, ev := range events {
		if ev.Action == "schedule.marked_stale" {
			found = true
		}
	}
	if !found {
		t.Fatalf("应有 schedule.marked_stale 审计事件")
	}

	// 基于 v2 弧段生成新候选；v3 发布后该候选同样失效且不得激活
	e.addWindow("SAT-1", "ST-A", f2.ID, at(20, 10, 0), at(20, 11, 0))
	res2 := e.generate("ST-A", at(20, 0, 0), at(20, 23, 59))
	if len(res2.Entries) != 1 {
		t.Fatalf("基于 v2 弧段应排定 1 个条目")
	}
	e.addForecast("SAT-1", 3)
	view, err := e.svc.GetSchedule(e.ctx(), res2.Schedule.ID)
	if err != nil || view.Schedule.Status != domain.ScheduleStatusStale {
		t.Fatalf("候选排程应被标记 stale，实际 %v (%v)", view.Schedule.Status, err)
	}
	if _, err = e.svc.ActivateSchedule(e.ctx(), e.actor, res2.Schedule.ID); !errors.Is(err, domain.ErrStaleSchedule) {
		t.Fatalf("激活 stale 排程应返回 ErrStaleSchedule，实际 %v", err)
	}
}

func TestMaintenanceLockMarksStaleAndBlocksSlot(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
	f1 := e.addForecast("SAT-1", 1)
	e.addWindow("SAT-1", "ST-A", f1.ID, at(20, 10, 0), at(20, 11, 0))
	e.addTask("T1", "ttnc", "SAT-1", 80, 30*time.Minute, "S")

	res := e.generate("ST-A", at(20, 0, 0), at(20, 23, 59))
	e.activate(res.Schedule.ID)

	// 设备维护锁定覆盖整个弧段 → 生效排程失效
	if _, err := e.svc.CreateMaintenanceLock(e.ctx(), e.actor, "ANT-1", at(20, 9, 30), at(20, 11, 30), "功放更换"); err != nil {
		t.Fatalf("创建维护锁定失败: %v", err)
	}
	schs, _ := e.svc.ListSchedules(e.ctx(), "ST-A", domain.ScheduleStatusStale)
	if len(schs) != 1 {
		t.Fatalf("维护锁定应使生效排程失效")
	}

	// 重新生成：锁定期内唯一天线不可用 → 任务被挤出并归因维护锁定
	res2 := e.generate("ST-A", at(20, 0, 0), at(20, 23, 59))
	if len(res2.Displaced) != 1 {
		t.Fatalf("锁定期内任务应被挤出，实际 %+v", res2.Entries)
	}
	var hasLockNode bool
	for _, n := range res2.Displaced[0].ConflictChain {
		if n.Kind == "maintenance_lock" {
			hasLockNode = true
		}
	}
	if !hasLockNode {
		t.Fatalf("冲突链应包含 maintenance_lock 节点: %+v", res2.Displaced[0].ConflictChain)
	}
}

// ---- 幂等提交 ----

func TestIdempotentTaskSubmission(t *testing.T) {
	e := newEnv(t, ":memory:")
	defer e.close()

	in := service.TaskInput{
		ExternalID: "EXT-001", Type: "ttnc", SatelliteID: "SAT-1",
		Priority: 60, MinDurationSeconds: 600,
	}
	first, created1, err := e.svc.SubmitTask(e.ctx(), e.actor, in)
	if err != nil || !created1 {
		t.Fatalf("首次提交应创建任务: created=%v err=%v", created1, err)
	}
	second, created2, err := e.svc.SubmitTask(e.ctx(), e.actor, in)
	if err != nil {
		t.Fatalf("重复提交不应报错: %v", err)
	}
	if created2 {
		t.Fatalf("重复提交不应创建新任务")
	}
	if first.ID != second.ID {
		t.Fatalf("幂等提交应返回同一任务: %s vs %s", first.ID, second.ID)
	}
	events, _ := e.svc.ListAuditEvents(e.ctx(), "task", first.ID, 0)
	var submitted, dedup int
	for _, ev := range events {
		switch ev.Action {
		case "task.submitted":
			submitted++
		case "task.deduplicated":
			dedup++
		}
	}
	if submitted != 1 || dedup != 1 {
		t.Fatalf("审计应各有一次 submitted/deduplicated，实际 %d/%d", submitted, dedup)
	}
}

// ---- 并发锁定 ----

func TestConcurrentScheduleGeneration(t *testing.T) {
	e := newEnv(t, t.TempDir()+"/concurrent.db")
	defer e.close()

	e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
	f1 := e.addForecast("SAT-1", 1)
	e.addWindow("SAT-1", "ST-A", f1.ID, at(20, 10, 0), at(20, 11, 0))
	e.addTask("T1", "ttnc", "SAT-1", 80, 30*time.Minute, "S")

	const n = 16
	versions := make([]int, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := e.svc.GenerateSchedule(context.Background(), e.actor, "ST-A", at(20, 0, 0), at(20, 23, 59))
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			versions = append(versions, res.Schedule.Version)
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发生成失败: %v", err)
	}
	if len(versions) != n {
		t.Fatalf("应生成 %d 版排程，实际 %d", n, len(versions))
	}
	sort.Ints(versions)
	for i, v := range versions {
		if v != i+1 {
			t.Fatalf("版本号应连续唯一 1..%d，实际 %v", n, versions)
		}
	}
}

func TestConcurrentDuplicateTaskSubmission(t *testing.T) {
	e := newEnv(t, t.TempDir()+"/idem.db")
	defer e.close()

	const n = 16
	var wg sync.WaitGroup
	var createdCount int64
	var mu sync.Mutex
	ids := map[string]bool{}
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, created, err := e.svc.SubmitTask(context.Background(), e.actor, service.TaskInput{
				ExternalID: "EXT-DUP", Type: "ttnc", SatelliteID: "SAT-1",
				Priority: 10, MinDurationSeconds: 300,
			})
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			ids[task.ID] = true
			if created {
				createdCount++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("并发提交失败: %v", err)
	}
	if createdCount != 1 {
		t.Fatalf("并发重复提交只能创建 1 个任务，实际 %d", createdCount)
	}
	if len(ids) != 1 {
		t.Fatalf("所有提交应返回同一任务 ID，实际 %v", ids)
	}
}

// ---- 重启恢复 ----

func TestRestartRecovery(t *testing.T) {
	dbPath := t.TempDir() + "/recovery.db"

	var scheduleID, itemID string
	{
		e := newEnv(t, dbPath)
		e.addStation("ST-A", service.AntennaInput{ID: "ANT-1", Bands: []string{"S"}})
		f1 := e.addForecast("SAT-1", 1)
		f2 := e.addForecast("SAT-2", 1)
		e.addWindow("SAT-1", "ST-A", f1.ID, at(20, 10, 0), at(20, 11, 0))
		e.addWindow("SAT-2", "ST-A", f2.ID, at(20, 10, 0), at(20, 11, 0))
		e.addTask("A-高优", "downlink", "SAT-1", 90, 45*time.Minute, "S")
		e.addTask("B-低优", "ttnc", "SAT-2", 40, 45*time.Minute, "S")
		res := e.generate("ST-A", at(20, 0, 0), at(20, 23, 59))
		e.activate(res.Schedule.ID)
		if len(res.Displaced) != 1 {
			t.Fatalf("应有 1 个被挤出任务")
		}
		scheduleID = res.Schedule.ID
		itemID = res.Displaced[0].ID
		e.close() // 模拟进程退出
	}

	// 重启：新 Store + Service 实例加载同一数据库文件
	e := newEnv(t, dbPath)
	defer e.close()

	view, err := e.svc.GetSchedule(e.ctx(), scheduleID)
	if err != nil {
		t.Fatalf("重启后应能读取排程: %v", err)
	}
	if view.Schedule.Status != domain.ScheduleStatusActive || len(view.Entries) != 1 {
		t.Fatalf("重启后生效排程及条目应完整: %+v", view.Schedule)
	}
	if view.Entries[0].ForecastVersion != 1 {
		t.Fatalf("条目应携带预报版本号，实际 %d", view.Entries[0].ForecastVersion)
	}

	item, err := e.svc.GetDecisionItem(e.ctx(), e.actor, itemID)
	if err != nil {
		t.Fatalf("重启后决策项应可恢复: %v", err)
	}
	if item.Status != domain.DecisionStatusPending {
		t.Fatalf("决策项应仍为 pending，实际 %s", item.Status)
	}
	if len(item.ConflictChain) == 0 || len(item.Notifications) == 0 {
		t.Fatalf("冲突链与通知结果应在重启后保留")
	}

	// 重启后队列处置仍可执行
	if _, err := e.svc.ResolveDecision(e.ctx(), e.actor, itemID, domain.ResolutionRetry, ""); err != nil {
		t.Fatalf("重启后处置决策项失败: %v", err)
	}
	events, err := e.svc.ListAuditEvents(e.ctx(), "decision_item", itemID, 0)
	if err != nil || len(events) < 2 {
		t.Fatalf("审计事件应跨重启连续，实际 %d 条", len(events))
	}
}

// ---- 时间格式有序性（跨午夜范围的 SQL 比较依赖字典序） ----

func TestTimeLayoutLexicographicOrder(t *testing.T) {
	a := at(19, 23, 59)
	b := at(20, 0, 0)
	if domain.FormatTime(a) >= domain.FormatTime(b) {
		t.Fatalf("跨午夜时间格式化后应保持字典序: %s >= %s", domain.FormatTime(a), domain.FormatTime(b))
	}
	if _, err := domain.ParseTime(domain.FormatTime(b)); err != nil {
		t.Fatalf("时间应可往返解析: %v", err)
	}
}
