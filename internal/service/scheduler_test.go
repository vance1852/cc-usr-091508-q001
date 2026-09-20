package service

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gscsvc/internal/domain"
	"gscsvc/internal/store"
)

// testEnv 可注入时钟的测试环境。
type testEnv struct {
	svc *Service
	st  *store.Store
	now time.Time
	op  domain.Operator
}

func newTestEnv(t *testing.T, start time.Time) *testEnv {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	env := &testEnv{st: st, now: start.UTC(), op: domain.Operator{ID: "officer-1", Role: domain.RoleDutyOfficer, Stations: []string{"ST1", "ST2"}}}
	env.svc = New(st, func() time.Time { return env.now })
	return env
}

func (e *testEnv) setNow(t time.Time) { e.now = t.UTC() }

func (e *testEnv) addStation(t *testing.T, id string, bands ...string) {
	t.Helper()
	if err := e.svc.UpsertStation(context.Background(), e.op, domain.Station{
		ID: id, Name: id, Bands: bands, MaxRateDps: 5, Active: true,
	}); err != nil {
		t.Fatalf("add station: %v", err)
	}
}

func (e *testEnv) addTask(t *testing.T, taskID, sat string, prio, minTT int, band string) {
	t.Helper()
	if _, err := e.svc.SubmitTask(context.Background(), e.op, domain.Task{
		TaskID: taskID, SatelliteID: sat, Kind: "TT_C", Priority: prio,
		MinTTSeconds: minTT, RequiredBand: band,
	}); err != nil {
		t.Fatalf("add task: %v", err)
	}
}

func (e *testEnv) addWindow(t *testing.T, winID, sat, station string, start, end time.Time, fcVer int) {
	t.Helper()
	if _, _, err := e.svc.SubmitWindows(context.Background(), e.op, []domain.VisibilityWindow{{
		WindowID: winID, SatelliteID: sat, StationID: station,
		StartUTC: start, EndUTC: end, ForecastVersion: fcVer,
	}}); err != nil {
		t.Fatalf("add window: %v", err)
	}
}

func entryByTask(entries []domain.ScheduleEntry, taskID string) *domain.ScheduleEntry {
	for i := range entries {
		if entries[i].TaskID == taskID {
			return &entries[i]
		}
	}
	return nil
}

// TestCrossMidnightWindow 验证跨 UTC 午夜的弧段被正确排程与判冲突。
func TestCrossMidnightWindow(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 22, 0, 0, 0, time.UTC))
	env.addStation(t, "ST1", "S")
	// T1 弧段跨午夜：23:50 → 次日 00:20。
	env.addTask(t, "T1", "SAT1", 5, 600, "S")
	env.addWindow(t, "W1", "SAT1", "ST1",
		time.Date(2026, 9, 19, 23, 50, 0, 0, time.UTC),
		time.Date(2026, 9, 20, 0, 20, 0, 0, time.UTC), 1)
	// T2 优先级更高，弧段 00:10–00:40，与 W1 在跨午夜段重叠 10 分钟。
	env.addTask(t, "T2", "SAT2", 9, 600, "S")
	env.addWindow(t, "W2", "SAT2", "ST1",
		time.Date(2026, 9, 20, 0, 10, 0, 0, time.UTC),
		time.Date(2026, 9, 20, 0, 40, 0, 0, time.UTC), 1)

	res, err := env.svc.Engine().Generate(context.Background(), env.op, "ST1",
		time.Date(2026, 9, 19, 23, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 20, 1, 0, 0, 0, time.UTC), "跨午夜测试")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	e1 := entryByTask(res.Entries, "T1")
	e2 := entryByTask(res.Entries, "T2")
	if e1 == nil || e2 == nil {
		t.Fatalf("缺少排程条目: %+v", res.Entries)
	}
	if e2.Status != domain.EntryScheduled {
		t.Fatalf("高优先级 T2 应被排定，实际 %s", e2.Status)
	}
	if e1.Status != domain.EntryDisplaced {
		t.Fatalf("T1 应被挤出，实际 %s", e1.Status)
	}
	// 冲突重叠区间应为 00:10–00:20（跨午夜）。
	if len(res.Conflicts) != 1 {
		t.Fatalf("应记录 1 条冲突链，实际 %d", len(res.Conflicts))
	}
	c := res.Conflicts[0]
	wantOS := time.Date(2026, 9, 20, 0, 10, 0, 0, time.UTC)
	wantOE := time.Date(2026, 9, 20, 0, 20, 0, 0, time.UTC)
	if !c.OverlapStart.Equal(wantOS) || !c.OverlapEnd.Equal(wantOE) {
		t.Fatalf("跨午夜重叠区间错误: %s – %s", c.OverlapStart, c.OverlapEnd)
	}
	if c.WinnerTaskID != "T2" || c.LoserTaskID != "T1" {
		t.Fatalf("冲突链方向错误: %+v", c)
	}
	// T1 应进入人工决策队列。
	if len(res.Queued) != 1 || res.Queued[0].TaskID != "T1" || res.Queued[0].Kind != domain.KindDisplacedByConflict {
		t.Fatalf("T1 应进入决策队列: %+v", res.Queued)
	}

	// 单弧段场景：跨午夜弧段本身应完整排定（end 跨日）。
	env2 := newTestEnv(t, time.Date(2026, 9, 19, 22, 0, 0, 0, time.UTC))
	env2.addStation(t, "ST1", "S")
	env2.addTask(t, "T1", "SAT1", 5, 600, "S")
	env2.addWindow(t, "W1", "SAT1", "ST1",
		time.Date(2026, 9, 19, 23, 50, 0, 0, time.UTC),
		time.Date(2026, 9, 20, 0, 20, 0, 0, time.UTC), 1)
	res2, err := env2.svc.Engine().Generate(context.Background(), env2.op, "ST1",
		time.Date(2026, 9, 19, 23, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 20, 1, 0, 0, 0, time.UTC), "")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	e := entryByTask(res2.Entries, "T1")
	if e == nil || e.Status != domain.EntryScheduled {
		t.Fatalf("跨午夜弧段应被排定: %+v", res2.Entries)
	}
	if e.StartUTC.Day() == e.EndUTC.Day() {
		t.Fatalf("弧段应跨越午夜两日: %s – %s", e.StartUTC, e.EndUTC)
	}
}

// TestConcurrentGenerate 验证并发排程被站点锁串行化，版本号唯一且不重订。
func TestConcurrentGenerate(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	env.addStation(t, "ST1", "S")
	env.addTask(t, "T1", "SAT1", 8, 300, "S")
	env.addTask(t, "T2", "SAT2", 6, 300, "S")
	env.addWindow(t, "W1", "SAT1", "ST1",
		time.Date(2026, 9, 19, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 13, 30, 0, 0, time.UTC), 1)
	env.addWindow(t, "W2", "SAT2", "ST1",
		time.Date(2026, 9, 19, 13, 10, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 13, 40, 0, 0, time.UTC), 1)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	from := time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC)
	to := time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = env.svc.Engine().Generate(context.Background(), env.op, "ST1", from, to, "并发")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("并发生成 %d 失败: %v", i, err)
		}
	}

	versions, err := env.svc.ListVersions(context.Background(), "ST1")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != n {
		t.Fatalf("应有 %d 个版本，实际 %d", n, len(versions))
	}
	seen := map[int]bool{}
	for _, v := range versions {
		if seen[v.VersionNo] {
			t.Fatalf("版本号重复: %d", v.VersionNo)
		}
		seen[v.VersionNo] = true
	}
	for no := 1; no <= n; no++ {
		if !seen[no] {
			t.Fatalf("版本号 %d 缺失", no)
		}
	}
	// 每个版本的排定条目不得相互重叠。
	for _, v := range versions {
		entries, err := env.st.EntriesOfVersion(context.Background(), v.ID)
		if err != nil {
			t.Fatalf("entries: %v", err)
		}
		var placed []domain.ScheduleEntry
		for _, e := range entries {
			if e.Status == domain.EntryScheduled || e.Status == domain.EntryCarried {
				placed = append(placed, e)
			}
		}
		for i := 0; i < len(placed); i++ {
			for j := i + 1; j < len(placed); j++ {
				if placed[i].StartUTC.Before(placed[j].EndUTC) && placed[j].StartUTC.Before(placed[i].EndUTC) {
					t.Fatalf("版本 %d 存在重叠排定: %s 与 %s", v.VersionNo, placed[i].TaskID, placed[j].TaskID)
				}
			}
		}
	}
}

// TestRestartRecovery 验证重启后决策队列与排程状态可恢复。
func TestRestartRecovery(t *testing.T) {
	dbPath := t.TempDir() + "/gscsvc.db"
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	op := domain.Operator{ID: "officer-1", Role: domain.RoleDutyOfficer, Stations: []string{"ST1"}}

	// 第一次启动：造出冲突与待决队列。
	st1, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	svc1 := New(st1, func() time.Time { return now })
	ctx := context.Background()
	if err := svc1.UpsertStation(ctx, op, domain.Station{ID: "ST1", Name: "ST1", Bands: []string{"S"}, Active: true}); err != nil {
		t.Fatalf("station: %v", err)
	}
	for _, tc := range [][2]string{{"T1", "SAT1"}, {"T2", "SAT2"}} {
		if _, err := svc1.SubmitTask(ctx, op, domain.Task{TaskID: tc[0], SatelliteID: tc[1], Kind: "TT_C", Priority: 5, MinTTSeconds: 300}); err != nil {
			t.Fatalf("task: %v", err)
		}
	}
	if _, err := svc1.SubmitTask(ctx, op, domain.Task{TaskID: "T3", SatelliteID: "SAT3", Kind: "TT_C", Priority: 9, MinTTSeconds: 300}); err != nil {
		t.Fatalf("task: %v", err)
	}
	if _, _, err := svc1.SubmitWindows(ctx, op, []domain.VisibilityWindow{
		{WindowID: "W1", SatelliteID: "SAT1", StationID: "ST1", StartUTC: now.Add(time.Hour), EndUTC: now.Add(90 * time.Minute), ForecastVersion: 1},
		{WindowID: "W3", SatelliteID: "SAT3", StationID: "ST1", StartUTC: now.Add(80 * time.Minute), EndUTC: now.Add(2 * time.Hour), ForecastVersion: 1},
	}); err != nil {
		t.Fatalf("windows: %v", err)
	}
	res, err := svc1.Engine().Generate(ctx, op, "ST1", now, now.Add(3*time.Hour), "")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(res.Queued) != 1 {
		t.Fatalf("应产生 1 条待决队列，实际 %d", len(res.Queued))
	}
	itemID := res.Queued[0].ItemID
	if err := st1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 模拟重启：重新打开同一数据库文件。
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	svc2 := New(st2, func() time.Time { return now })
	rep, err := svc2.StartupRecovery(ctx)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if rep.PendingDecisions != 1 {
		t.Fatalf("重启后应恢复 1 条待决，实际 %d", rep.PendingDecisions)
	}
	// 队列条目仍可处置。
	items, err := svc2.ListDecisionQueue(ctx, op, domain.QueuePending)
	if err != nil {
		t.Fatalf("list queue: %v", err)
	}
	if len(items) != 1 || items[0].ItemID != itemID {
		t.Fatalf("重启后队列条目丢失: %+v", items)
	}
	if _, err := svc2.ResolveDecisionItem(ctx, op, itemID, ActionDrop, ""); err != nil {
		t.Fatalf("重启后处置失败: %v", err)
	}
	// 排程版本也应保留。
	versions, err := svc2.ListVersions(ctx, "ST1")
	if err != nil || len(versions) != 1 {
		t.Fatalf("重启后版本丢失: %v %+v", err, versions)
	}
}

// TestIdempotentTaskSubmit 验证同 task_id 重复提交幂等。
func TestIdempotentTaskSubmit(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	r1, err := env.svc.SubmitTask(ctx, env.op, domain.Task{TaskID: "T1", SatelliteID: "SAT1", Kind: "TT_C", Priority: 5, MinTTSeconds: 300})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if r1.IdempotentReplay {
		t.Fatalf("首次提交不应标记为幂等重放")
	}
	r2, err := env.svc.SubmitTask(ctx, env.op, domain.Task{TaskID: "T1", SatelliteID: "SAT1", Kind: "TT_C", Priority: 9, MinTTSeconds: 600})
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if !r2.IdempotentReplay {
		t.Fatalf("重复提交应标记为幂等重放")
	}
	// 重复提交不得覆盖原记录。
	if r2.Task.Priority != 5 || r2.Task.MinTTSeconds != 300 {
		t.Fatalf("幂等重放不应改写原任务: %+v", r2.Task)
	}
	// 审计中只应有一条 TASK_SUBMITTED。
	events, err := env.svc.ListAuditEvents(ctx, "task", "T1", 0)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	count := 0
	for _, e := range events {
		if e.Action == "TASK_SUBMITTED" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("应只有 1 条任务创建审计，实际 %d", count)
	}
}

// TestStartedSegmentNotRewritten 验证进行中测控段不被静默改写、不可被抢占。
func TestStartedSegmentNotRewritten(t *testing.T) {
	start := time.Date(2026, 9, 19, 9, 0, 0, 0, time.UTC)
	env := newTestEnv(t, start)
	env.addStation(t, "ST1", "S")
	env.addTask(t, "TA", "SATA", 5, 600, "S")
	env.addWindow(t, "WA", "SATA", "ST1",
		time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 10, 30, 0, 0, time.UTC), 1)
	res1, err := env.svc.Engine().Generate(context.Background(), env.op, "ST1",
		time.Date(2026, 9, 19, 9, 30, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 11, 0, 0, 0, time.UTC), "首版")
	if err != nil {
		t.Fatalf("generate v1: %v", err)
	}
	ea := entryByTask(res1.Entries, "TA")
	if ea == nil || ea.Status != domain.EntryScheduled {
		t.Fatalf("TA 应被排定: %+v", res1.Entries)
	}

	// 时间推进到 10:15，TA 已开始；更高优先级任务到来。
	env.setNow(time.Date(2026, 9, 19, 10, 15, 0, 0, time.UTC))
	env.addTask(t, "TB", "SATB", 9, 600, "S")
	env.addWindow(t, "WB", "SATB", "ST1",
		time.Date(2026, 9, 19, 10, 10, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 10, 40, 0, 0, time.UTC), 1)
	res2, err := env.svc.Engine().Generate(context.Background(), env.op, "ST1",
		time.Date(2026, 9, 19, 9, 30, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 11, 0, 0, 0, time.UTC), "重排")
	if err != nil {
		t.Fatalf("generate v2: %v", err)
	}
	carried := entryByTask(res2.Entries, "TA")
	if carried == nil || carried.Status != domain.EntryCarried || !carried.Locked {
		t.Fatalf("进行中 TA 应原样继承: %+v", carried)
	}
	if !carried.StartUTC.Equal(ea.StartUTC) || !carried.EndUTC.Equal(ea.EndUTC) || carried.ForecastVersion != ea.ForecastVersion {
		t.Fatalf("进行中测控段被改写: 原 %+v 新 %+v", ea, carried)
	}
	eb := entryByTask(res2.Entries, "TB")
	if eb == nil || eb.Status != domain.EntryDisplaced {
		t.Fatalf("TB 应被进行中测控段挡下: %+v", eb)
	}
	if got := eb.Reason; !contains(got, "PREEMPT_STARTED_DENIED") {
		t.Fatalf("理由应说明不可抢占已开始段: %s", got)
	}
	// 冲突链记录 TB 被 TA 挡下。
	found := false
	for _, c := range res2.Conflicts {
		if c.WinnerTaskID == "TA" && c.LoserTaskID == "TB" {
			found = true
		}
	}
	if !found {
		t.Fatalf("缺少 TA/TB 冲突链: %+v", res2.Conflicts)
	}
}

// TestForecastUpdateMarksAffected 验证新预报版本标记受影响排程。
func TestForecastUpdateMarksAffected(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	env.addStation(t, "ST1", "S")
	env.addTask(t, "T1", "SAT1", 5, 300, "S")
	env.addWindow(t, "W1", "SAT1", "ST1",
		time.Date(2026, 9, 19, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 13, 30, 0, 0, time.UTC), 1)
	if _, err := env.svc.Engine().Generate(context.Background(), env.op, "ST1",
		time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC), ""); err != nil {
		t.Fatalf("generate: %v", err)
	}

	res, err := env.svc.RegisterForecast(context.Background(), env.op, domain.OrbitForecast{
		SatelliteID: "SAT1", Version: 2, GeneratedAt: env.now,
	})
	if err != nil {
		t.Fatalf("forecast: %v", err)
	}
	if !res.Created || len(res.AffectedVersions) != 1 {
		t.Fatalf("应标记 1 个受影响版本: %+v", res)
	}
	view, err := env.svc.CurrentSchedule(context.Background(), "ST1")
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if view.Version.Status != domain.VersionAffected {
		t.Fatalf("版本应被标记 AFFECTED，实际 %s", view.Version.Status)
	}
	e := entryByTask(view.Entries, "T1")
	if e == nil || !e.Affected {
		t.Fatalf("条目应被标记受影响: %+v", e)
	}
	// 幂等：重复登记同版本预报不重复标记。
	res2, err := env.svc.RegisterForecast(context.Background(), env.op, domain.OrbitForecast{
		SatelliteID: "SAT1", Version: 2, GeneratedAt: env.now,
	})
	if err != nil {
		t.Fatalf("forecast replay: %v", err)
	}
	if res2.Created || len(res2.AffectedVersions) != 0 {
		t.Fatalf("重复预报应幂等: %+v", res2)
	}
}

// TestMaintenanceLockBlocks 验证维护锁定阻断弧段并给出替代站点。
func TestMaintenanceLockBlocks(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	env.addStation(t, "ST1", "S")
	env.addStation(t, "ST2", "S")
	env.addTask(t, "T1", "SAT1", 5, 300, "S")
	env.addWindow(t, "W1", "SAT1", "ST1",
		time.Date(2026, 9, 19, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 13, 30, 0, 0, time.UTC), 1)
	env.addWindow(t, "W2", "SAT1", "ST2",
		time.Date(2026, 9, 19, 13, 5, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 13, 35, 0, 0, time.UTC), 1)

	lres, err := env.svc.CreateMaintenanceLock(context.Background(), env.op, domain.MaintenanceLock{
		LockID: "L1", StationID: "ST1",
		StartUTC: time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC),
		EndUTC:   time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC),
		Reason:   "天线俯仰机构检修",
	})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if !lres.Created {
		t.Fatalf("锁定应创建成功")
	}

	res, err := env.svc.Engine().Generate(context.Background(), env.op, "ST1",
		time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC), "")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	e := entryByTask(res.Entries, "T1")
	if e == nil || e.Status != domain.EntryBlocked {
		t.Fatalf("T1 应被维护锁定阻断: %+v", e)
	}
	if len(res.Queued) != 1 || res.Queued[0].Kind != domain.KindBlockedByLock {
		t.Fatalf("应进入维护阻断队列: %+v", res.Queued)
	}
	alts := res.Queued[0].Payload.Alternatives
	if len(alts) != 1 || alts[0].StationID != "ST2" {
		t.Fatalf("应建议替代站点 ST2: %+v", alts)
	}
}

// TestMinTTDurationUnmet 验证弧段时长不足最小测控时长被阻断。
func TestMinTTDurationUnmet(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	env.addStation(t, "ST1", "S")
	env.addTask(t, "T1", "SAT1", 5, 1800, "S") // 需要 30 分钟
	env.addWindow(t, "W1", "SAT1", "ST1",
		time.Date(2026, 9, 19, 13, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 13, 10, 0, 0, time.UTC), 1) // 仅 10 分钟
	res, err := env.svc.Engine().Generate(context.Background(), env.op, "ST1",
		time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC), "")
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	e := entryByTask(res.Entries, "T1")
	if e == nil || e.Status != domain.EntryBlocked || !contains(e.Reason, "MIN_TT_UNMET") {
		t.Fatalf("时长不足应被阻断: %+v", e)
	}
}

// TestStationScopeForbidden 验证值班长无法处置非负责站点。
func TestStationScopeForbidden(t *testing.T) {
	env := newTestEnv(t, time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	env.addStation(t, "ST1", "S")
	env.addStation(t, "ST9", "S")
	_, err := env.svc.Engine().Generate(context.Background(), env.op, "ST9",
		time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC),
		time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC), "")
	if err != ErrForbidden {
		t.Fatalf("值班长处置非负责站点应被拒，实际 %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
