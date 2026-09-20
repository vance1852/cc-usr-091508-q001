// Package api 提供地面站窗口冲突处置服务的 Gin HTTP 接口。
// 认证通过请求头完成：X-Actor-ID / X-Actor-Role / X-Actor-Stations（逗号分隔）。
package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"satops/groundstation/internal/domain"
	"satops/groundstation/internal/service"
)

// Server HTTP 服务。
type Server struct {
	svc    *service.Service
	engine *gin.Engine
}

// NewServer 构建路由。
func NewServer(svc *service.Service) *Server {
	s := &Server{svc: svc}
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })

	v1 := r.Group("/api/v1", actorMiddleware())
	{
		v1.POST("/stations", s.createStation)
		v1.GET("/stations", s.listStations)
		v1.GET("/stations/:id/antennas", s.listAntennas)

		v1.POST("/forecasts", s.createForecast)
		v1.GET("/forecasts", s.listForecasts)

		v1.POST("/windows", s.createWindows)
		v1.GET("/windows/:id", s.getWindow)

		v1.POST("/tasks", s.createTask)
		v1.GET("/tasks/:id", s.getTask)

		v1.POST("/maintenance-locks", s.createLock)
		v1.DELETE("/maintenance-locks/:id", s.releaseLock)

		v1.POST("/schedules/generate", s.generateSchedule)
		v1.POST("/schedules/:id/activate", s.activateSchedule)
		v1.GET("/schedules/:id", s.getSchedule)
		v1.GET("/schedules", s.listSchedules)

		v1.GET("/decision-queue", s.listDecisionQueue)
		v1.GET("/decision-queue/:id", s.getDecisionItem)
		v1.POST("/decision-queue/:id/resolve", s.resolveDecision)

		v1.GET("/audit-events", s.listAudit)
		v1.GET("/notifications", s.listNotifications)
	}
	s.engine = r
	return s
}

// Engine 暴露 gin.Engine（测试用 httptest 直接驱动）。
func (s *Server) Engine() *gin.Engine { return s.engine }

// ---- 操作者中间件 ----

const actorKey = "actor"

func actorMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Actor-ID")
		if id == "" {
			abort(c, http.StatusUnauthorized, "unauthorized", "缺少 X-Actor-ID 请求头")
			return
		}
		role := domain.Role(c.GetHeader("X-Actor-Role"))
		if role != domain.RoleDutyOfficer && role != domain.RoleSupervisor {
			abort(c, http.StatusForbidden, "forbidden", "X-Actor-Role 须为 duty_officer 或 supervisor")
			return
		}
		var stations []string
		if h := c.GetHeader("X-Actor-Stations"); h != "" {
			for _, s := range strings.Split(h, ",") {
				if s = strings.TrimSpace(s); s != "" {
					stations = append(stations, s)
				}
			}
		}
		c.Set(actorKey, domain.Actor{ID: id, Role: role, Stations: stations})
		c.Next()
	}
}

func actorOf(c *gin.Context) domain.Actor {
	return c.MustGet(actorKey).(domain.Actor)
}

// ---- 错误处理 ----

func abort(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": msg}})
}

func respondErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, domain.ErrBadInput):
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		abort(c, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, domain.ErrForbidden):
		abort(c, http.StatusForbidden, "forbidden", err.Error())
	case errors.Is(err, domain.ErrStaleSchedule):
		abort(c, http.StatusConflict, "stale_schedule", err.Error())
	case errors.Is(err, domain.ErrStartedSegment):
		abort(c, http.StatusConflict, "started_segment_conflict", err.Error())
	case errors.Is(err, domain.ErrConflict):
		abort(c, http.StatusConflict, "conflict", err.Error())
	default:
		abort(c, http.StatusInternalServerError, "internal", err.Error())
	}
}

// ---- 站点 ----

type createStationReq struct {
	ID       string                 `json:"id" binding:"required"`
	Name     string                 `json:"name" binding:"required"`
	Antennas []service.AntennaInput `json:"antennas"`
}

func (s *Server) createStation(c *gin.Context) {
	var req createStationReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	st, err := s.svc.RegisterStation(c.Request.Context(), actorOf(c), req.ID, req.Name, req.Antennas)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, st)
}

func (s *Server) listStations(c *gin.Context) {
	out, err := s.svc.ListStations(c.Request.Context())
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"stations": out})
}

func (s *Server) listAntennas(c *gin.Context) {
	out, err := s.svc.ListAntennas(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"antennas": out})
}

// ---- 预报 ----

type createForecastReq struct {
	SatelliteID string    `json:"satellite_id" binding:"required"`
	Version     int       `json:"version" binding:"required"`
	IssuedAt    time.Time `json:"issued_at"`
	Note        string    `json:"note"`
}

func (s *Server) createForecast(c *gin.Context) {
	var req createForecastReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	f, err := s.svc.RegisterForecast(c.Request.Context(), actorOf(c), req.SatelliteID, req.Version, req.IssuedAt, req.Note)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, f)
}

func (s *Server) listForecasts(c *gin.Context) {
	out, err := s.svc.ListForecasts(c.Request.Context(), c.Query("satellite_id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"forecasts": out})
}

// ---- 弧段 ----

type createWindowsReq struct {
	Windows []service.WindowInput `json:"windows" binding:"required"`
}

func (s *Server) createWindows(c *gin.Context) {
	var req createWindowsReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	out, err := s.svc.RegisterWindows(c.Request.Context(), actorOf(c), req.Windows)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"windows": out})
}

func (s *Server) getWindow(c *gin.Context) {
	out, err := s.svc.GetWindowDetail(c.Request.Context(), actorOf(c), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, out)
}

// ---- 任务 ----

func (s *Server) createTask(c *gin.Context) {
	var req service.TaskInput
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	task, created, err := s.svc.SubmitTask(c.Request.Context(), actorOf(c), req)
	if err != nil {
		respondErr(c, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK // 幂等命中：返回既有任务
	}
	c.JSON(status, gin.H{"task": task, "deduplicated": !created})
}

func (s *Server) getTask(c *gin.Context) {
	task, err := s.svc.GetTask(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, task)
}

// ---- 维护锁定 ----

type createLockReq struct {
	AntennaID string    `json:"antenna_id" binding:"required"`
	Start     time.Time `json:"start" binding:"required"`
	End       time.Time `json:"end" binding:"required"`
	Reason    string    `json:"reason"`
}

func (s *Server) createLock(c *gin.Context) {
	var req createLockReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	lock, err := s.svc.CreateMaintenanceLock(c.Request.Context(), actorOf(c), req.AntennaID, req.Start, req.End, req.Reason)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, lock)
}

func (s *Server) releaseLock(c *gin.Context) {
	if err := s.svc.ReleaseMaintenanceLock(c.Request.Context(), actorOf(c), c.Param("id")); err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"released": c.Param("id")})
}

// ---- 排程 ----

type generateReq struct {
	StationID string    `json:"station_id" binding:"required"`
	From      time.Time `json:"from" binding:"required"`
	To        time.Time `json:"to" binding:"required"`
}

func (s *Server) generateSchedule(c *gin.Context) {
	var req generateReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	out, err := s.svc.GenerateSchedule(c.Request.Context(), actorOf(c), req.StationID, req.From, req.To)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, out)
}

func (s *Server) activateSchedule(c *gin.Context) {
	sch, err := s.svc.ActivateSchedule(c.Request.Context(), actorOf(c), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, sch)
}

func (s *Server) getSchedule(c *gin.Context) {
	out, err := s.svc.GetSchedule(c.Request.Context(), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, out)
}

func (s *Server) listSchedules(c *gin.Context) {
	out, err := s.svc.ListSchedules(c.Request.Context(), c.Query("station_id"), c.Query("status"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"schedules": out})
}

// ---- 决策队列 ----

func (s *Server) listDecisionQueue(c *gin.Context) {
	out, err := s.svc.ListDecisionQueue(c.Request.Context(), actorOf(c), c.Query("status"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": out})
}

func (s *Server) getDecisionItem(c *gin.Context) {
	out, err := s.svc.GetDecisionItem(c.Request.Context(), actorOf(c), c.Param("id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, out)
}

type resolveReq struct {
	Action          string `json:"action" binding:"required"`
	TargetStationID string `json:"target_station_id"`
}

func (s *Server) resolveDecision(c *gin.Context) {
	var req resolveReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, "bad_input", err.Error())
		return
	}
	out, err := s.svc.ResolveDecision(c.Request.Context(), actorOf(c), c.Param("id"), req.Action, req.TargetStationID)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, out)
}

// ---- 审计与通知 ----

func (s *Server) listAudit(c *gin.Context) {
	out, err := s.svc.ListAuditEvents(c.Request.Context(), c.Query("entity_type"), c.Query("entity_id"), 0)
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": out})
}

func (s *Server) listNotifications(c *gin.Context) {
	out, err := s.svc.ListNotifications(c.Request.Context(), c.Query("related_type"), c.Query("related_id"))
	if err != nil {
		respondErr(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"notifications": out})
}
