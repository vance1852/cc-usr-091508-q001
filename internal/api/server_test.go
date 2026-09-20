package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"satops/groundstation/internal/api"
	"satops/groundstation/internal/domain"
	"satops/groundstation/internal/service"
	"satops/groundstation/internal/store"
)

func init() { gin.SetMode(gin.TestMode) }

type apiEnv struct {
	t      *testing.T
	svc    *service.Service
	engine *gin.Engine
	now    time.Time
}

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("打开数据库失败: %v", err)
	}
	e := &apiEnv{t: t}
	e.now = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	e.svc = service.New(st, service.WithClock(func() time.Time { return e.now }))
	e.engine = api.NewServer(e.svc).Engine()
	t.Cleanup(func() { st.Close() })
	return e
}

func (e *apiEnv) do(method, path string, body any, headers map[string]string) *httptest.ResponseRecorder {
	e.t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.engine.ServeHTTP(rec, req)
	return rec
}

func dutyHeaders(stations ...string) map[string]string {
	h := map[string]string{"X-Actor-ID": "duty-1", "X-Actor-Role": "duty_officer"}
	s := ""
	for i, st := range stations {
		if i > 0 {
			s += ","
		}
		s += st
	}
	h["X-Actor-Stations"] = s
	return h
}

func supervisorHeaders() map[string]string {
	return map[string]string{"X-Actor-ID": "boss-1", "X-Actor-Role": "supervisor"}
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("响应非 JSON: %s", rec.Body.String())
	}
	return m
}

var seedActor = domain.Actor{ID: "seed", Role: domain.RoleSupervisor}

// seedConflict 构造两星争一站的冲突场景，返回被挤出任务的决策项与竞争窗口 ID。
func seedConflict(t *testing.T, e *apiEnv) (itemID, contestedWindowID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.svc.RegisterStation(ctx, seedActor, "ST-A", "站A",
		[]service.AntennaInput{{ID: "ANT-1", Bands: []string{"S"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.RegisterStation(ctx, seedActor, "ST-B", "站B",
		[]service.AntennaInput{{ID: "ANT-2", Bands: []string{"S"}}}); err != nil {
		t.Fatal(err)
	}
	f1, err := e.svc.RegisterForecast(ctx, seedActor, "SAT-1", 1, e.now, "")
	if err != nil {
		t.Fatal(err)
	}
	f2, err := e.svc.RegisterForecast(ctx, seedActor, "SAT-2", 1, e.now, "")
	if err != nil {
		t.Fatal(err)
	}
	at := func(day, h, m int) time.Time { return time.Date(2026, 9, day, h, m, 0, 0, time.UTC) }
	if _, err := e.svc.RegisterWindows(ctx, seedActor, []service.WindowInput{
		{SatelliteID: "SAT-1", StationID: "ST-A", ForecastID: f1.ID, AOS: at(20, 10, 0), LOS: at(20, 11, 0)},
	}); err != nil {
		t.Fatal(err)
	}
	w2, err := e.svc.RegisterWindows(ctx, seedActor, []service.WindowInput{
		{SatelliteID: "SAT-2", StationID: "ST-A", ForecastID: f2.ID, AOS: at(20, 10, 0), LOS: at(20, 11, 0)},
		{SatelliteID: "SAT-2", StationID: "ST-B", ForecastID: f2.ID, AOS: at(20, 10, 30), LOS: at(20, 11, 30)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []service.TaskInput{
		{ExternalID: "A-数传", Type: "downlink", SatelliteID: "SAT-1", Priority: 90, MinDurationSeconds: 2700, RequiredBand: "S"},
		{ExternalID: "B-保活", Type: "ttnc", SatelliteID: "SAT-2", Priority: 90, MinDurationSeconds: 2700, RequiredBand: "S"},
	} {
		if _, _, err := e.svc.SubmitTask(ctx, seedActor, in); err != nil {
			t.Fatal(err)
		}
	}
	res, err := e.svc.GenerateSchedule(ctx, seedActor, "ST-A", at(20, 0, 0), at(20, 23, 59))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Displaced) != 1 {
		t.Fatalf("应有 1 个被挤出任务，实际 %d", len(res.Displaced))
	}
	return res.Displaced[0].ID, w2[0].ID
}

// ---- 认证与授权 ----

func TestActorRequired(t *testing.T) {
	e := newAPIEnv(t)

	rec := e.do(http.MethodPost, "/api/v1/tasks", map[string]any{
		"external_id": "X", "type": "ttnc", "satellite_id": "SAT-1", "min_duration_seconds": 300,
	}, nil)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("缺少操作者应返回 401，实际 %d", rec.Code)
	}

	rec = e.do(http.MethodPost, "/api/v1/tasks", map[string]any{
		"external_id": "X", "type": "ttnc", "satellite_id": "SAT-1", "min_duration_seconds": 300,
	}, map[string]string{"X-Actor-ID": "x", "X-Actor-Role": "intern"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("非法角色应返回 403，实际 %d", rec.Code)
	}

	rec = e.do(http.MethodGet, "/healthz", nil, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("健康检查不应要求认证，实际 %d", rec.Code)
	}
}

func TestDutyOfficerStationScope(t *testing.T) {
	e := newAPIEnv(t)
	itemID, _ := seedConflict(t, e)

	// 值班长只负责 ST-A：队列里看不到 ST-B 的条目，也不能生成 ST-B 排程
	rec := e.do(http.MethodGet, "/api/v1/decision-queue?status=pending", nil, dutyHeaders("ST-A"))
	if rec.Code != http.StatusOK {
		t.Fatalf("查询队列失败: %d %s", rec.Code, rec.Body.String())
	}
	items := decode(t, rec)["items"].([]any)
	for _, it := range items {
		if it.(map[string]any)["station_id"] != "ST-A" {
			t.Fatalf("值班长不应看到其他站点的决策项: %v", it)
		}
	}

	rec = e.do(http.MethodPost, "/api/v1/schedules/generate", map[string]any{
		"station_id": "ST-B", "from": "2026-09-20T00:00:00Z", "to": "2026-09-20T23:59:00Z",
	}, dutyHeaders("ST-A"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("值班长操作非负责站点应 403，实际 %d", rec.Code)
	}

	rec = e.do(http.MethodPost, "/api/v1/schedules/generate", map[string]any{
		"station_id": "ST-A", "from": "2026-09-20T00:00:00Z", "to": "2026-09-20T23:59:00Z",
	}, dutyHeaders("ST-A"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("值班长操作负责站点应成功，实际 %d %s", rec.Code, rec.Body.String())
	}

	// 主管可见全部站点队列
	rec = e.do(http.MethodGet, "/api/v1/decision-queue?status=pending", nil, supervisorHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("主管查询队列失败: %d", rec.Code)
	}
	_ = itemID
}

func TestMigrationRequiresSupervisor(t *testing.T) {
	e := newAPIEnv(t)
	itemID, _ := seedConflict(t, e)

	// 值班长（即便负责两站）无权批准跨站迁移
	rec := e.do(http.MethodPost, "/api/v1/decision-queue/"+itemID+"/resolve", map[string]any{
		"action": "migrate", "target_station_id": "ST-B",
	}, dutyHeaders("ST-A", "ST-B"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("值班长批准跨站迁移应 403，实际 %d %s", rec.Code, rec.Body.String())
	}

	// 任务平台主管可批准
	rec = e.do(http.MethodPost, "/api/v1/decision-queue/"+itemID+"/resolve", map[string]any{
		"action": "migrate", "target_station_id": "ST-B",
	}, supervisorHeaders())
	if rec.Code != http.StatusOK {
		t.Fatalf("主管批准迁移应成功，实际 %d %s", rec.Code, rec.Body.String())
	}
	item := decode(t, rec)
	if item["resolution"] != "migrate" || item["target_station_id"] != "ST-B" {
		t.Fatalf("决策项应记录迁移结果: %v", item)
	}

	// 已处置的决策项不可重复处置
	rec = e.do(http.MethodPost, "/api/v1/decision-queue/"+itemID+"/resolve", map[string]any{
		"action": "retry",
	}, supervisorHeaders())
	if rec.Code != http.StatusConflict {
		t.Fatalf("重复处置应 409，实际 %d", rec.Code)
	}
}

func TestDutyOfficerCannotResolveOtherStation(t *testing.T) {
	e := newAPIEnv(t)
	itemID, _ := seedConflict(t, e)

	rec := e.do(http.MethodPost, "/api/v1/decision-queue/"+itemID+"/resolve", map[string]any{
		"action": "retry",
	}, dutyHeaders("ST-B")) // 决策项属于 ST-A
	if rec.Code != http.StatusForbidden {
		t.Fatalf("值班长处置非负责站点的决策项应 403，实际 %d", rec.Code)
	}

	rec = e.do(http.MethodPost, "/api/v1/decision-queue/"+itemID+"/resolve", map[string]any{
		"action": "retry",
	}, dutyHeaders("ST-A"))
	if rec.Code != http.StatusOK {
		t.Fatalf("值班长处置负责站点的决策项应成功，实际 %d %s", rec.Code, rec.Body.String())
	}
}

// ---- 窗口详情：预报版本、冲突链、替代站点、通知结果 ----

func TestWindowDetailAggregation(t *testing.T) {
	e := newAPIEnv(t)
	_, contestedWindowID := seedConflict(t, e)

	rec := e.do(http.MethodGet, "/api/v1/windows/"+contestedWindowID, nil, dutyHeaders("ST-A"))
	if rec.Code != http.StatusOK {
		t.Fatalf("查询窗口详情失败: %d %s", rec.Code, rec.Body.String())
	}
	detail := decode(t, rec)

	forecast := detail["forecast"].(map[string]any)
	if forecast["version"].(float64) != 1 || forecast["satellite_id"] != "SAT-2" {
		t.Fatalf("窗口应携带采用的预报版本: %v", forecast)
	}
	chains, ok := detail["conflict_chains"].([]any)
	if !ok || len(chains) != 1 {
		t.Fatalf("窗口详情应包含 1 条冲突链: %v", detail)
	}
	chain := chains[0].(map[string]any)
	if chain["reason"] == "" || len(chain["conflict_chain"].([]any)) == 0 {
		t.Fatalf("冲突链应有归因节点: %v", chain)
	}
	alts := chain["alternatives"].([]any)
	if len(alts) == 0 || alts[0].(map[string]any)["station_id"] != "ST-B" {
		t.Fatalf("冲突链应含替代站点 ST-B: %v", alts)
	}
	notifs := chain["notifications"].([]any)
	if len(notifs) == 0 || notifs[0].(map[string]any)["status"] != "sent" {
		t.Fatalf("冲突链应含通知结果: %v", notifs)
	}

	// 非负责站点值班长不可见
	rec = e.do(http.MethodGet, "/api/v1/windows/"+contestedWindowID, nil, dutyHeaders("ST-B"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("非负责站点窗口详情应 403，实际 %d", rec.Code)
	}
}

// ---- 任务幂等（HTTP 层） ----

func TestTaskIdempotencyViaAPI(t *testing.T) {
	e := newAPIEnv(t)

	body := map[string]any{
		"external_id": "EXT-HTTP-1", "type": "downlink", "satellite_id": "SAT-1",
		"priority": 70, "min_duration_seconds": 900, "required_band": "X",
	}
	rec := e.do(http.MethodPost, "/api/v1/tasks", body, dutyHeaders("ST-A"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("首次提交应 201，实际 %d %s", rec.Code, rec.Body.String())
	}
	first := decode(t, rec)
	if first["deduplicated"].(bool) {
		t.Fatalf("首次提交不应标记 deduplicated")
	}
	firstID := first["task"].(map[string]any)["id"]

	rec = e.do(http.MethodPost, "/api/v1/tasks", body, dutyHeaders("ST-A"))
	if rec.Code != http.StatusOK {
		t.Fatalf("重复提交应 200，实际 %d", rec.Code)
	}
	second := decode(t, rec)
	if !second["deduplicated"].(bool) {
		t.Fatalf("重复提交应标记 deduplicated")
	}
	if second["task"].(map[string]any)["id"] != firstID {
		t.Fatalf("幂等提交应返回同一任务")
	}
}

// ---- 审计事件可查 ----

func TestAuditEventsEndpoint(t *testing.T) {
	e := newAPIEnv(t)
	itemID, _ := seedConflict(t, e)

	rec := e.do(http.MethodGet, "/api/v1/audit-events?entity_type=decision_item&entity_id="+itemID, nil, dutyHeaders("ST-A"))
	if rec.Code != http.StatusOK {
		t.Fatalf("查询审计失败: %d", rec.Code)
	}
	events := decode(t, rec)["events"].([]any)
	if len(events) == 0 {
		t.Fatalf("决策项应有审计事件")
	}
	ev := events[0].(map[string]any)
	if ev["actor_id"] == "" || ev["action"] == "" || ev["ts"] == "" {
		t.Fatalf("审计事件应包含操作者、动作与时间: %v", ev)
	}
}
