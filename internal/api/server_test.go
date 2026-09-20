package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"gscsvc/internal/domain"
	"gscsvc/internal/service"
	"gscsvc/internal/store"
)

func init() { gin.SetMode(gin.TestMode) }

type apiEnv struct {
	router *gin.Engine
	now    time.Time
}

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	env := &apiEnv{now: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)}
	svc := service.New(st, func() time.Time { return env.now })
	env.router = NewRouter(svc)
	return env
}

func (e *apiEnv) do(t *testing.T, method, path string, body any, op domain.Operator) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if op.ID != "" {
		req.Header.Set("X-Operator-Id", op.ID)
		req.Header.Set("X-Operator-Role", string(op.Role))
	}
	if len(op.Stations) > 0 {
		joined := ""
		for i, s := range op.Stations {
			if i > 0 {
				joined += ","
			}
			joined += s
		}
		req.Header.Set("X-Operator-Stations", joined)
	}
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	var parsed map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &parsed)
	}
	return rec, parsed
}

var (
	officer    = domain.Operator{ID: "zhangwei", Role: domain.RoleDutyOfficer, Stations: []string{"ST1"}}
	officer2   = domain.Operator{ID: "liuyz", Role: domain.RoleDutyOfficer, Stations: []string{"ST2"}}
	supervisor = domain.Operator{ID: "boss", Role: domain.RoleSupervisor}
)

// seedConflict 构造 ST1 上 T1（P5）与 T3（P9）的冲突场景并生成排程，返回被挤出任务的队列条目 id。
func (e *apiEnv) seedConflict(t *testing.T) string {
	t.Helper()
	now := e.now
	e.do(t, "POST", "/api/v1/stations", map[string]any{
		"id": "ST1", "name": "喀什站", "bands": []string{"S", "X"}, "max_rate_dps": 5}, supervisor)
	e.do(t, "POST", "/api/v1/stations", map[string]any{
		"id": "ST2", "name": "三亚站", "bands": []string{"S", "X"}, "max_rate_dps": 5}, supervisor)
	e.do(t, "POST", "/api/v1/forecasts", map[string]any{
		"satellite_id": "SAT1", "version": 1, "generated_at": now.Format(time.RFC3339)}, supervisor)
	e.do(t, "POST", "/api/v1/forecasts", map[string]any{
		"satellite_id": "SAT2", "version": 1, "generated_at": now.Format(time.RFC3339)}, supervisor)
	e.do(t, "POST", "/api/v1/tasks", map[string]any{
		"task_id": "T1", "satellite_id": "SAT1", "kind": "DATA_DOWNLINK",
		"priority": 5, "min_tt_seconds": 300, "required_band": "X"}, officer)
	e.do(t, "POST", "/api/v1/tasks", map[string]any{
		"task_id": "T3", "satellite_id": "SAT2", "kind": "TT_C",
		"priority": 9, "min_tt_seconds": 300, "required_band": "S"}, officer)
	e.do(t, "POST", "/api/v1/windows", []map[string]any{
		{"window_id": "W1", "satellite_id": "SAT1", "station_id": "ST1",
			"start_utc": now.Add(time.Hour).Format(time.RFC3339),
			"end_utc":   now.Add(90 * time.Minute).Format(time.RFC3339), "forecast_version": 1},
		{"window_id": "W3", "satellite_id": "SAT2", "station_id": "ST1",
			"start_utc": now.Add(80 * time.Minute).Format(time.RFC3339),
			"end_utc":   now.Add(2 * time.Hour).Format(time.RFC3339), "forecast_version": 1},
		{"window_id": "W1-ST2", "satellite_id": "SAT1", "station_id": "ST2",
			"start_utc": now.Add(65 * time.Minute).Format(time.RFC3339),
			"end_utc":   now.Add(95 * time.Minute).Format(time.RFC3339), "forecast_version": 1},
	}, officer)
	rec, body := e.do(t, "POST", "/api/v1/schedules/generate", map[string]any{
		"station_id": "ST1", "from": now.Format(time.RFC3339),
		"to": now.Add(3 * time.Hour).Format(time.RFC3339), "note": "临时姿态调整后重排"}, officer)
	if rec.Code != http.StatusOK {
		t.Fatalf("generate 失败: %d %s", rec.Code, rec.Body.String())
	}
	queued, ok := body["queued"].([]any)
	if !ok || len(queued) != 1 {
		t.Fatalf("应有 1 条队列条目: %s", rec.Body.String())
	}
	return queued[0].(map[string]any)["item_id"].(string)
}

// TestAPIEndToEnd 端到端：登记→冲突→队列→追溯→处置。
func TestAPIEndToEnd(t *testing.T) {
	env := newAPIEnv(t)
	itemID := env.seedConflict(t)

	// 当前排程：T3 排定、T1 被挤出。
	rec, body := env.do(t, "GET", "/api/v1/schedules?station_id=ST1", nil, officer)
	if rec.Code != http.StatusOK {
		t.Fatalf("schedule: %d", rec.Code)
	}
	entries := body["entries"].([]any)
	status := map[string]string{}
	for _, e := range entries {
		m := e.(map[string]any)
		status[m["task_id"].(string)] = m["status"].(string)
	}
	if status["T3"] != "SCHEDULED" || status["T1"] != "DISPLACED" {
		t.Fatalf("排程状态不符: %v", status)
	}

	// 窗口追溯：预报版本、冲突链、替代站点、通知结果齐全。
	rec, body = env.do(t, "GET", "/api/v1/windows/W1/trace", nil, officer)
	if rec.Code != http.StatusOK {
		t.Fatalf("trace: %d", rec.Code)
	}
	trEntries := body["entries"].([]any)
	if len(trEntries) == 0 {
		t.Fatalf("追溯缺少条目")
	}
	fv := trEntries[0].(map[string]any)["forecast_version"].(float64)
	if fv != 1 {
		t.Fatalf("追溯应显示采用预报 v1，实际 v%v", fv)
	}
	chain := body["conflict_chain"].([]any)
	if len(chain) != 1 || chain[0].(map[string]any)["winner_task_id"] != "T3" {
		t.Fatalf("冲突链缺失或方向错误: %v", chain)
	}
	items := body["decision_items"].([]any)
	if len(items) != 1 {
		t.Fatalf("追溯缺少决策条目")
	}
	alts := items[0].(map[string]any)["payload"].(map[string]any)["alternatives"].([]any)
	if len(alts) != 1 || alts[0].(map[string]any)["station_id"] != "ST2" {
		t.Fatalf("应给出替代站点 ST2: %v", alts)
	}
	notifs := body["notifications"].([]any)
	if len(notifs) == 0 || notifs[0].(map[string]any)["result"] != "SUCCESS" {
		t.Fatalf("通知结果缺失: %v", notifs)
	}

	// 值班长处置自己站点：retry。
	rec, _ = env.do(t, "POST", "/api/v1/decision-queue/"+itemID+"/resolve",
		map[string]any{"action": "retry"}, officer)
	if rec.Code != http.StatusOK {
		t.Fatalf("retry: %d %s", rec.Code, rec.Body.String())
	}
	// 已处置条目不可重复处置。
	rec, _ = env.do(t, "POST", "/api/v1/decision-queue/"+itemID+"/resolve",
		map[string]any{"action": "drop"}, officer)
	if rec.Code != http.StatusConflict {
		t.Fatalf("重复处置应 409，实际 %d", rec.Code)
	}
}

// TestAPIRBAC 验证站点越权与迁移审批权限。
func TestAPIRBAC(t *testing.T) {
	env := newAPIEnv(t)
	itemID := env.seedConflict(t)

	// 未认证。
	rec, _ := env.do(t, "GET", "/api/v1/decision-queue", nil, domain.Operator{})
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("未认证应 401，实际 %d", rec.Code)
	}
	// 值班长对非负责站点生成排程。
	rec, _ = env.do(t, "POST", "/api/v1/schedules/generate", map[string]any{
		"station_id": "ST2", "from": env.now.Format(time.RFC3339),
		"to": env.now.Add(time.Hour).Format(time.RFC3339)}, officer)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("越权生成应 403，实际 %d", rec.Code)
	}
	// 值班长批准跨站迁移 → 拒绝。
	rec, body := env.do(t, "POST", "/api/v1/decision-queue/"+itemID+"/resolve",
		map[string]any{"action": "migrate", "target_station_id": "ST2"}, officer)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("值班长迁移应 403，实际 %d %s", rec.Code, rec.Body.String())
	}
	if body["error"] != "forbidden" {
		t.Fatalf("错误码不符: %v", body)
	}
	// 主管批准迁移 → 成功，任务迁移至 ST2 并排定。
	rec, body = env.do(t, "POST", "/api/v1/decision-queue/"+itemID+"/resolve",
		map[string]any{"action": "migrate", "target_station_id": "ST2"}, supervisor)
	if rec.Code != http.StatusOK {
		t.Fatalf("主管迁移应成功: %d %s", rec.Code, rec.Body.String())
	}
	if body["resolution"] == nil || body["status"] != "RESOLVED" {
		t.Fatalf("迁移结果异常: %v", body)
	}
	rec, body = env.do(t, "GET", "/api/v1/schedules?station_id=ST2", nil, officer2)
	if rec.Code != http.StatusOK {
		t.Fatalf("ST2 排程查询失败: %d", rec.Code)
	}
	found := false
	for _, e := range body["entries"].([]any) {
		m := e.(map[string]any)
		if m["task_id"] == "T1" && m["status"] == "SCHEDULED" {
			found = true
		}
	}
	if !found {
		t.Fatalf("迁移后 T1 应在 ST2 排定: %s", rec.Body.String())
	}
	// 值班长队列视图只含自己站点。
	_, body = env.do(t, "GET", "/api/v1/decision-queue", nil, officer2)
	for _, it := range body["items"].([]any) {
		if it.(map[string]any)["station_id"] != "ST2" {
			t.Fatalf("值班长不应看到非负责站点条目: %v", it)
		}
	}
}

// TestAPIIdempotentTask 验证 API 层任务幂等。
func TestAPIIdempotentTask(t *testing.T) {
	env := newAPIEnv(t)
	payload := map[string]any{
		"task_id": "T1", "satellite_id": "SAT1", "kind": "TT_C",
		"priority": 5, "min_tt_seconds": 300}
	rec, body := env.do(t, "POST", "/api/v1/tasks", payload, officer)
	if rec.Code != http.StatusOK || body["idempotent_replay"] != false {
		t.Fatalf("首次提交异常: %d %v", rec.Code, body)
	}
	rec, body = env.do(t, "POST", "/api/v1/tasks", payload, officer)
	if rec.Code != http.StatusOK || body["idempotent_replay"] != true {
		t.Fatalf("重复提交应幂等: %d %v", rec.Code, body)
	}
}

// TestAPIForecastInvalidation 验证预报更新后版本标记与审计可查。
func TestAPIForecastInvalidation(t *testing.T) {
	env := newAPIEnv(t)
	env.seedConflict(t)
	rec, body := env.do(t, "POST", "/api/v1/forecasts", map[string]any{
		"satellite_id": "SAT1", "version": 2,
		"generated_at": env.now.Format(time.RFC3339)}, supervisor)
	if rec.Code != http.StatusOK {
		t.Fatalf("forecast: %d", rec.Code)
	}
	if len(body["affected_versions"].([]any)) != 1 {
		t.Fatalf("应标记 1 个受影响版本: %v", body)
	}
	_, body = env.do(t, "GET", "/api/v1/schedules?station_id=ST1", nil, officer)
	if body["version"].(map[string]any)["status"] != "AFFECTED" {
		t.Fatalf("版本应被标记 AFFECTED: %v", body["version"])
	}
	// 审计事件可检索。
	_, body = env.do(t, "GET", "/api/v1/audit-events?entity_type=orbit_forecast&entity_id=SAT1:v2", nil, officer)
	events := body["events"].([]any)
	if len(events) == 0 || events[0].(map[string]any)["action"] != "FORECAST_REGISTERED" {
		t.Fatalf("缺少预报登记审计: %v", events)
	}
}
