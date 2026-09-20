// Package api 提供 Gin HTTP API。
package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"gscsvc/internal/domain"
	"gscsvc/internal/service"
)

// Server 持有业务服务。
type Server struct {
	svc *service.Service
}

// NewRouter 构建路由。鉴权通过请求头：
//
//	X-Operator-Id: 操作员标识
//	X-Operator-Role: duty_officer | supervisor
//	X-Operator-Stations: 值班长负责站点，逗号分隔
func NewRouter(svc *service.Service) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	s := &Server{svc: svc}

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	v1 := r.Group("/api/v1")
	v1.Use(authMiddleware())
	{
		v1.POST("/forecasts", s.createForecast)
		v1.POST("/stations", s.upsertStation)
		v1.POST("/windows", s.submitWindows)
		v1.POST("/tasks", s.submitTask)
		v1.POST("/maintenance-locks", s.createLock)
		v1.POST("/schedules/generate", s.generateSchedule)
		v1.GET("/schedules", s.currentSchedule)
		v1.GET("/schedules/versions", s.listVersions)
		v1.GET("/decision-queue", s.listDecisionQueue)
		v1.POST("/decision-queue/:item_id/resolve", s.resolveDecision)
		v1.GET("/windows/:window_id/trace", s.traceWindow)
		v1.GET("/audit-events", s.listAudit)
		v1.GET("/notifications", s.listNotifications)
	}
	return r
}

const ctxOperator = "operator"

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Operator-Id")
		role := domain.Role(c.GetHeader("X-Operator-Role"))
		if id == "" || (role != domain.RoleDutyOfficer && role != domain.RoleSupervisor) {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "unauthorized", "message": "缺少有效操作员身份头（X-Operator-Id / X-Operator-Role）",
			})
			return
		}
		var stations []string
		if h := c.GetHeader("X-Operator-Stations"); h != "" {
			for _, s := range strings.Split(h, ",") {
				if s = strings.TrimSpace(s); s != "" {
					stations = append(stations, s)
				}
			}
		}
		c.Set(ctxOperator, domain.Operator{ID: id, Role: role, Stations: stations})
		c.Next()
	}
}

func operatorOf(c *gin.Context) domain.Operator {
	return c.MustGet(ctxOperator).(domain.Operator)
}

// nonNil 保证列表字段序列化为 [] 而非 null。
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// respondError 将业务错误映射为 HTTP 状态码。
func respondError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrForbidden), errors.Is(err, service.ErrNeedSupervisor):
		c.JSON(http.StatusForbidden, gin.H{"error": "forbidden", "message": err.Error()})
	case errors.Is(err, service.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "not_found", "message": err.Error()})
	case errors.Is(err, service.ErrAlreadyResolved):
		c.JSON(http.StatusConflict, gin.H{"error": "conflict", "message": err.Error()})
	case errors.Is(err, service.ErrStationInactive):
		c.JSON(http.StatusConflict, gin.H{"error": "station_inactive", "message": err.Error()})
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad_request", "message": err.Error()})
	}
}

func parseTime(v string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// ---- 请求体 ----

type forecastReq struct {
	SatelliteID string `json:"satellite_id" binding:"required"`
	Version     int    `json:"version" binding:"required"`
	GeneratedAt string `json:"generated_at" binding:"required"`
}

func (s *Server) createForecast(c *gin.Context) {
	var req forecastReq
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, err)
		return
	}
	gen, err := parseTime(req.GeneratedAt)
	if err != nil {
		respondError(c, err)
		return
	}
	res, err := s.svc.RegisterForecast(c.Request.Context(), operatorOf(c), domain.OrbitForecast{
		SatelliteID: req.SatelliteID, Version: req.Version, GeneratedAt: gen,
	})
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, res)
}

type stationReq struct {
	ID         string   `json:"id" binding:"required"`
	Name       string   `json:"name" binding:"required"`
	Bands      []string `json:"bands"`
	MaxRateDps float64  `json:"max_rate_dps"`
	Active     *bool    `json:"active"`
}

func (s *Server) upsertStation(c *gin.Context) {
	var req stationReq
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, err)
		return
	}
	active := true
	if req.Active != nil {
		active = *req.Active
	}
	st := domain.Station{ID: req.ID, Name: req.Name, Bands: req.Bands, MaxRateDps: req.MaxRateDps, Active: active}
	if err := s.svc.UpsertStation(c.Request.Context(), operatorOf(c), st); err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, st)
}

type windowReq struct {
	WindowID        string  `json:"window_id" binding:"required"`
	SatelliteID     string  `json:"satellite_id" binding:"required"`
	StationID       string  `json:"station_id" binding:"required"`
	StartUTC        string  `json:"start_utc" binding:"required"`
	EndUTC          string  `json:"end_utc" binding:"required"`
	MaxElevDeg      float64 `json:"max_elev_deg"`
	ForecastVersion int     `json:"forecast_version" binding:"required"`
}

func (s *Server) submitWindows(c *gin.Context) {
	var reqs []windowReq
	if err := c.ShouldBindJSON(&reqs); err != nil {
		respondError(c, err)
		return
	}
	var wins []domain.VisibilityWindow
	for _, r := range reqs {
		st, err1 := parseTime(r.StartUTC)
		en, err2 := parseTime(r.EndUTC)
		if err1 != nil || err2 != nil {
			respondError(c, errors.New("start_utc/end_utc 需为 RFC3339 时间"))
			return
		}
		if !en.After(st) {
			respondError(c, errors.New("end_utc 必须晚于 start_utc"))
			return
		}
		wins = append(wins, domain.VisibilityWindow{
			WindowID: r.WindowID, SatelliteID: r.SatelliteID, StationID: r.StationID,
			StartUTC: st, EndUTC: en, MaxElevDeg: r.MaxElevDeg, ForecastVersion: r.ForecastVersion,
		})
	}
	created, replayed, err := s.svc.SubmitWindows(c.Request.Context(), operatorOf(c), wins)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"created": created, "replayed": replayed})
}

type taskReq struct {
	TaskID       string `json:"task_id" binding:"required"`
	SatelliteID  string `json:"satellite_id" binding:"required"`
	Kind         string `json:"kind" binding:"required"`
	Priority     int    `json:"priority" binding:"required"`
	MinTTSeconds int    `json:"min_tt_seconds" binding:"required"`
	RequiredBand string `json:"required_band"`
}

func (s *Server) submitTask(c *gin.Context) {
	var req taskReq
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, err)
		return
	}
	res, err := s.svc.SubmitTask(c.Request.Context(), operatorOf(c), domain.Task{
		TaskID: req.TaskID, SatelliteID: req.SatelliteID, Kind: req.Kind,
		Priority: req.Priority, MinTTSeconds: req.MinTTSeconds, RequiredBand: req.RequiredBand,
	})
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, res)
}

type lockReq struct {
	LockID    string `json:"lock_id" binding:"required"`
	StationID string `json:"station_id" binding:"required"`
	StartUTC  string `json:"start_utc" binding:"required"`
	EndUTC    string `json:"end_utc" binding:"required"`
	Reason    string `json:"reason"`
}

func (s *Server) createLock(c *gin.Context) {
	var req lockReq
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, err)
		return
	}
	st, err1 := parseTime(req.StartUTC)
	en, err2 := parseTime(req.EndUTC)
	if err1 != nil || err2 != nil {
		respondError(c, errors.New("start_utc/end_utc 需为 RFC3339 时间"))
		return
	}
	res, err := s.svc.CreateMaintenanceLock(c.Request.Context(), operatorOf(c), domain.MaintenanceLock{
		LockID: req.LockID, StationID: req.StationID, StartUTC: st, EndUTC: en, Reason: req.Reason,
	})
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, res)
}

type generateReq struct {
	StationID string `json:"station_id" binding:"required"`
	From      string `json:"from" binding:"required"`
	To        string `json:"to" binding:"required"`
	Note      string `json:"note"`
}

func (s *Server) generateSchedule(c *gin.Context) {
	var req generateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, err)
		return
	}
	from, err1 := parseTime(req.From)
	to, err2 := parseTime(req.To)
	if err1 != nil || err2 != nil || !to.After(from) {
		respondError(c, errors.New("from/to 需为 RFC3339 时间且 to 晚于 from"))
		return
	}
	res, err := s.svc.Engine().Generate(c.Request.Context(), operatorOf(c), req.StationID, from, to, req.Note)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, res)
}

func (s *Server) currentSchedule(c *gin.Context) {
	stationID := c.Query("station_id")
	if stationID == "" {
		respondError(c, errors.New("缺少 station_id 参数"))
		return
	}
	view, err := s.svc.CurrentSchedule(c.Request.Context(), stationID)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, view)
}

func (s *Server) listVersions(c *gin.Context) {
	stationID := c.Query("station_id")
	if stationID == "" {
		respondError(c, errors.New("缺少 station_id 参数"))
		return
	}
	vs, err := s.svc.ListVersions(c.Request.Context(), stationID)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"versions": nonNil(vs)})
}

func (s *Server) listDecisionQueue(c *gin.Context) {
	items, err := s.svc.ListDecisionQueue(c.Request.Context(), operatorOf(c), c.Query("status"))
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": nonNil(items)})
}

type resolveReq struct {
	Action          string `json:"action" binding:"required"` // retry | migrate | drop
	TargetStationID string `json:"target_station_id"`
}

func (s *Server) resolveDecision(c *gin.Context) {
	var req resolveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, err)
		return
	}
	item, err := s.svc.ResolveDecisionItem(c.Request.Context(), operatorOf(c),
		c.Param("item_id"), req.Action, req.TargetStationID)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, item)
}

func (s *Server) traceWindow(c *gin.Context) {
	tr, err := s.svc.TraceWindow(c.Request.Context(), c.Param("window_id"))
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, tr)
}

func (s *Server) listAudit(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	events, err := s.svc.ListAuditEvents(c.Request.Context(),
		c.Query("entity_type"), c.Query("entity_id"), limit)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": nonNil(events)})
}

func (s *Server) listNotifications(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	ns, err := s.svc.ListNotifications(c.Request.Context(), limit)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"notifications": nonNil(ns)})
}
