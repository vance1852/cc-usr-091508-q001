// Package domain 定义地面站窗口冲突处置服务的核心领域模型、角色与错误。
package domain

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// TimeLayout 是持久化到 SQLite 的时间格式：UTC、固定 9 位小数秒，
// 保证字符串字典序与时间序一致，可直接用于 SQL 范围比较。
const TimeLayout = "2006-01-02T15:04:05.000000000Z"

// FormatTime 将时间格式化为持久化布局（UTC）。
func FormatTime(t time.Time) string { return t.UTC().Format(TimeLayout) }

// ParseTime 解析持久化布局的时间字符串。
func ParseTime(s string) (time.Time, error) { return time.Parse(TimeLayout, s) }

// NewID 生成带实体前缀的随机 ID，便于审计时一眼识别实体类型。
func NewID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// ---- 角色与操作者 ----

// Role 操作者角色。
type Role string

const (
	RoleDutyOfficer Role = "duty_officer" // 值班长：只能处理自己负责的站点
	RoleSupervisor  Role = "supervisor"   // 任务平台主管：可批准跨站迁移
)

// Actor 一次操作的发起者，由 API 层从请求头解析。
type Actor struct {
	ID       string   `json:"id"`
	Role     Role     `json:"role"`
	Stations []string `json:"stations"` // 值班长负责的站点列表；主管为空表示全部
}

// CanOperate 判断操作者是否有权处置指定站点。
func (a Actor) CanOperate(stationID string) bool {
	if a.Role == RoleSupervisor {
		return true
	}
	for _, s := range a.Stations {
		if s == stationID {
			return true
		}
	}
	return false
}

// ---- 枚举 ----

// TaskType 任务类型。
type TaskType string

const (
	TaskTypeDownlink TaskType = "downlink" // 数据回收
	TaskTypeTTNC     TaskType = "ttnc"     // 测控保活
)

// 任务状态。
const (
	TaskStatusPending   = "pending"   // 待排程
	TaskStatusScheduled = "scheduled" // 已排入某版排程
	TaskStatusDisplaced = "displaced" // 被挤出，等待人工决策
	TaskStatusCancelled = "cancelled" // 已取消
)

// 排程版本状态。
const (
	ScheduleStatusCandidate  = "candidate"  // 候选排程
	ScheduleStatusActive     = "active"     // 当前生效
	ScheduleStatusSuperseded = "superseded" // 被新版本取代
	ScheduleStatusStale      = "stale"      // 因预报/设备更新失效
)

// 排程条目状态。
const (
	EntryStatusPlanned     = "planned"      // 计划条目
	EntryStatusKeptStarted = "kept_started" // 已开始测控段，按规约原样保留
)

// 决策队列状态与处置动作。
const (
	DecisionStatusPending  = "pending"
	DecisionStatusResolved = "resolved"

	ResolutionRetry   = "retry"   // 重新排队等待下次排程
	ResolutionMigrate = "migrate" // 跨站迁移（需主管批准）
	ResolutionCancel  = "cancel"  // 取消任务

	// ResolutionAutoScheduled 任务已被某版排程排定，待决项自动关闭。
	ResolutionAutoScheduled = "scheduled"
)

// 天线状态。
const (
	AntennaStatusOperational = "operational"
	AntennaStatusOffline     = "offline"
)

// ---- 实体 ----

// ForecastVersion 轨道预报版本。
type ForecastVersion struct {
	ID          string    `json:"id"`
	SatelliteID string    `json:"satellite_id"`
	Version     int       `json:"version"`
	IssuedAt    time.Time `json:"issued_at"`
	Note        string    `json:"note,omitempty"`
	Superseded  bool      `json:"superseded"`
	CreatedAt   time.Time `json:"created_at"`
}

// Station 地面站。
type Station struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// Antenna 天线能力。
type Antenna struct {
	ID        string    `json:"id"`
	StationID string    `json:"station_id"`
	Name      string    `json:"name"`
	Bands     []string  `json:"bands"` // 支持的频段列表
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// SupportsBand 判断天线是否支持指定频段；任务未指定频段时视为兼容。
func (a *Antenna) SupportsBand(band string) bool {
	if band == "" {
		return true
	}
	for _, b := range a.Bands {
		if b == band {
			return true
		}
	}
	return false
}

// VisibilityWindow 可见弧段。AOS/LOS 为绝对 UTC 时间，
// 跨午夜窗口天然表示为 LOS 落在次日（LOS 必须晚于 AOS）。
type VisibilityWindow struct {
	ID           string    `json:"id"`
	SatelliteID  string    `json:"satellite_id"`
	StationID    string    `json:"station_id"`
	ForecastID   string    `json:"forecast_id"` // 本弧段采用的轨道预报版本
	AOS          time.Time `json:"aos"`
	LOS          time.Time `json:"los"`
	MaxElevation float64   `json:"max_elevation"`
	CreatedAt    time.Time `json:"created_at"`
}

// Duration 弧段时长。
func (w *VisibilityWindow) Duration() time.Duration { return w.LOS.Sub(w.AOS) }

// Task 测控/数传任务。ExternalID 是幂等键：重复提交返回既有任务。
type Task struct {
	ID                 string        `json:"id"`
	ExternalID         string        `json:"external_id"`
	Type               TaskType      `json:"type"`
	SatelliteID        string        `json:"satellite_id"`
	Priority           int           `json:"priority"` // 数值越大优先级越高
	MinDuration        time.Duration `json:"-"`        // 最小测控时长
	RequiredBand       string        `json:"required_band,omitempty"`
	PreferredStationID string        `json:"preferred_station_id,omitempty"`
	NotBefore          time.Time     `json:"not_before,omitempty"`
	NotAfter           time.Time     `json:"not_after,omitempty"`
	Status             string        `json:"status"`
	CreatedAt          time.Time     `json:"created_at"`
	UpdatedAt          time.Time     `json:"updated_at"`
}

// MinDurationSeconds 以秒表示最小测控时长，供 JSON 使用。
func (t *Task) MinDurationSeconds() int64 { return int64(t.MinDuration / time.Second) }

// MaintenanceLock 设备维护锁定。
type MaintenanceLock struct {
	ID        string    `json:"id"`
	AntennaID string    `json:"antenna_id"`
	StationID string    `json:"station_id"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	Reason    string    `json:"reason"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

// Schedule 一版排程（按站点单调递增版本号）。
type Schedule struct {
	ID          string    `json:"id"`
	StationID   string    `json:"station_id"`
	Version     int       `json:"version"`
	Status      string    `json:"status"`
	StaleReason string    `json:"stale_reason,omitempty"`
	RangeFrom   time.Time `json:"range_from"`
	RangeTo     time.Time `json:"range_to"`
	CreatedBy   string    `json:"created_by"`
	CreatedAt   time.Time `json:"created_at"`
}

// ScheduleEntry 排程条目：一个任务在一个窗口上占用某天线的一段时间。
type ScheduleEntry struct {
	ID         string    `json:"id"`
	ScheduleID string    `json:"schedule_id"`
	TaskID     string    `json:"task_id"`
	WindowID   string    `json:"window_id"`
	AntennaID  string    `json:"antenna_id"`
	StationID  string    `json:"station_id"`
	ForecastID string    `json:"forecast_id"` // 本条目采用的预报版本（冗余自窗口）
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason"` // 排定/保留理由
	CreatedAt  time.Time `json:"created_at"`
}

// ConflictNode 冲突链节点：解释一个任务为何未能排定。
type ConflictNode struct {
	Kind       string `json:"kind"` // planned_entry | started_entry | maintenance_lock | no_window | no_antenna | superseded_by_schedule
	TaskID     string `json:"task_id,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	Priority   int    `json:"priority,omitempty"`
	WindowID   string `json:"window_id,omitempty"`
	AntennaID  string `json:"antenna_id,omitempty"`
	LockID     string `json:"lock_id,omitempty"`
	ScheduleID string `json:"schedule_id,omitempty"`
	Start      string `json:"start,omitempty"`
	End        string `json:"end,omitempty"`
	Detail     string `json:"detail"`
}

// Alternative 替代站点建议。
type Alternative struct {
	StationID  string   `json:"station_id"`
	WindowID   string   `json:"window_id"`
	AntennaIDs []string `json:"antenna_ids"`
	AOS        string   `json:"aos"`
	LOS        string   `json:"los"`
	Detail     string   `json:"detail"`
}

// NotificationResult 一次通知的结果记录。
type NotificationResult struct {
	ID      string `json:"id"`
	Target  string `json:"target"`
	Channel string `json:"channel"`
	Status  string `json:"status"` // sent | failed
	At      string `json:"at"`
}

// DecisionItem 人工决策队列条目（可恢复：持久化，重启后仍在）。
type DecisionItem struct {
	ID              string               `json:"id"`
	TaskID          string               `json:"task_id"`
	StationID       string               `json:"station_id"`
	WindowID        string               `json:"window_id,omitempty"` // 主要竞争窗口
	Reason          string               `json:"reason"`
	ConflictChain   []ConflictNode       `json:"conflict_chain"`
	Alternatives    []Alternative        `json:"alternatives"`
	Notifications   []NotificationResult `json:"notifications"`
	Status          string               `json:"status"`
	Resolution      string               `json:"resolution,omitempty"`
	TargetStationID string               `json:"target_station_id,omitempty"`
	ResolvedBy      string               `json:"resolved_by,omitempty"`
	ResolvedAt      *time.Time           `json:"resolved_at,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
}

// AuditEvent 审计事件。
type AuditEvent struct {
	ID         int64     `json:"id"`
	TS         time.Time `json:"ts"`
	ActorID    string    `json:"actor_id"`
	ActorRole  string    `json:"actor_role"`
	Action     string    `json:"action"`
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Detail     string    `json:"detail"` // JSON
}

// Notification 通知记录。
type Notification struct {
	ID          string    `json:"id"`
	Target      string    `json:"target"`
	Channel     string    `json:"channel"`
	Payload     string    `json:"payload"`
	Status      string    `json:"status"`
	RelatedType string    `json:"related_type"`
	RelatedID   string    `json:"related_id"`
	CreatedAt   time.Time `json:"created_at"`
}

// ---- 错误 ----

var (
	ErrNotFound       = errors.New("资源不存在")
	ErrForbidden      = errors.New("无权操作")
	ErrBadInput       = errors.New("输入不合法")
	ErrConflict       = errors.New("状态冲突")
	ErrStaleSchedule  = errors.New("排程版本已因预报或设备更新失效，需重新生成")
	ErrStartedSegment = errors.New("已开始的测控段不得被静默改写")
)
