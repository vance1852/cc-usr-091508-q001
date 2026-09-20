// Package domain 定义地面站窗口冲突处置服务的核心领域模型。
package domain

import "time"

// Role 操作员角色。
type Role string

const (
	// RoleDutyOfficer 值班长：只能处理自己负责的站点。
	RoleDutyOfficer Role = "duty_officer"
	// RoleSupervisor 任务平台主管：可批准跨站迁移，可操作任意站点。
	RoleSupervisor Role = "supervisor"
)

// Operator 当前操作员（由 API 层从请求头解析）。
type Operator struct {
	ID       string   `json:"operator_id"`
	Role     Role     `json:"role"`
	Stations []string `json:"stations"`
}

// CanOperate 判断操作员是否可处置指定站点。
func (o Operator) CanOperate(stationID string) bool {
	if o.Role == RoleSupervisor {
		return true
	}
	for _, s := range o.Stations {
		if s == stationID {
			return true
		}
	}
	return false
}

// OrbitForecast 轨道预报版本。
type OrbitForecast struct {
	SatelliteID string    `json:"satellite_id"`
	Version     int       `json:"version"`
	GeneratedAt time.Time `json:"generated_at"`
	ReceivedAt  time.Time `json:"received_at"`
}

// Station 地面站及天线能力。
type Station struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	Bands      []string `json:"bands"`        // 支持的频段，如 ["S","X"]
	MaxRateDps float64  `json:"max_rate_dps"` // 天线最大角速度（度/秒）
	Active     bool     `json:"active"`
}

// HasBand 判断天线能力是否覆盖所需频段；required 为空表示无要求。
func (s Station) HasBand(required string) bool {
	if required == "" {
		return true
	}
	for _, b := range s.Bands {
		if b == required {
			return true
		}
	}
	return false
}

// VisibilityWindow 可见弧段（时间一律为 UTC）。
type VisibilityWindow struct {
	WindowID        string    `json:"window_id"`
	SatelliteID     string    `json:"satellite_id"`
	StationID       string    `json:"station_id"`
	StartUTC        time.Time `json:"start_utc"`
	EndUTC          time.Time `json:"end_utc"`
	MaxElevDeg      float64   `json:"max_elev_deg"`
	ForecastVersion int       `json:"forecast_version"`
}

// DurationSeconds 弧段时长（秒），跨午夜由绝对时间差自然覆盖。
func (w VisibilityWindow) DurationSeconds() int64 {
	return int64(w.EndUTC.Sub(w.StartUTC).Seconds())
}

// Task 测控/数传任务。TaskID 为幂等键，重复提交返回同一记录。
type Task struct {
	TaskID       string    `json:"task_id"`
	SatelliteID  string    `json:"satellite_id"`
	Kind         string    `json:"kind"` // TT_C / DATA_DOWNLINK
	Priority     int       `json:"priority"`
	MinTTSeconds int       `json:"min_tt_seconds"` // 最小测控时长
	RequiredBand string    `json:"required_band"`
	MigratedTo   string    `json:"migrated_to"` // 非空表示经主管批准迁移至该站
	Status       string    `json:"status"`      // ACTIVE / DROPPED
	SubmittedBy  string    `json:"submitted_by"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// MaintenanceLock 设备维护锁定。
type MaintenanceLock struct {
	LockID    string    `json:"lock_id"`
	StationID string    `json:"station_id"`
	StartUTC  time.Time `json:"start_utc"`
	EndUTC    time.Time `json:"end_utc"`
	Reason    string    `json:"reason"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}

// 排程版本状态。
const (
	VersionActive     = "ACTIVE"     // 当前生效
	VersionAffected   = "AFFECTED"   // 被预报/设备更新波及，需复核
	VersionSuperseded = "SUPERSEDED" // 已被新版本取代
)

// ScheduleVersion 某站点的一次排程版本。
type ScheduleVersion struct {
	ID        int64     `json:"id"`
	StationID string    `json:"station_id"`
	VersionNo int       `json:"version_no"`
	Status    string    `json:"status"`
	Note      string    `json:"note"`
	CreatedBy string    `json:"created_by"`
	CreatedAt time.Time `json:"created_at"`
}

// 排程条目状态。
const (
	EntryScheduled = "SCHEDULED" // 已排定
	EntryCarried   = "CARRIED"   // 从上一版本原样继承（进行中测控段）
	EntryDisplaced = "DISPLACED" // 冲突中被挤出
	EntryBlocked   = "BLOCKED"   // 被维护锁定/能力/时长约束阻断
)

// ScheduleEntry 排程条目，记录采用的预报版本与理由。
type ScheduleEntry struct {
	ID              int64     `json:"id"`
	VersionID       int64     `json:"version_id"`
	TaskID          string    `json:"task_id"`
	WindowID        string    `json:"window_id"`
	StationID       string    `json:"station_id"`
	SatelliteID     string    `json:"satellite_id"`
	StartUTC        time.Time `json:"start_utc"`
	EndUTC          time.Time `json:"end_utc"`
	ForecastVersion int       `json:"forecast_version"`
	Status          string    `json:"status"`
	Reason          string    `json:"reason"`
	Locked          bool      `json:"locked"`   // 进行中测控段，禁止改写
	Affected        bool      `json:"affected"` // 被预报/设备更新波及
}

// Conflict 冲突链中的一环：winner 挤掉了 loser。
type Conflict struct {
	ID           int64     `json:"id"`
	VersionID    int64     `json:"version_id"`
	StationID    string    `json:"station_id"`
	WinnerTaskID string    `json:"winner_task_id"`
	LoserTaskID  string    `json:"loser_task_id"`
	OverlapStart time.Time `json:"overlap_start"`
	OverlapEnd   time.Time `json:"overlap_end"`
	Reason       string    `json:"reason"`
}

// 决策队列条目类型。
const (
	KindDisplacedByConflict = "DISPLACED_BY_CONFLICT"
	KindBlockedByLock       = "BLOCKED_BY_MAINTENANCE"
	KindRequirementUnmet    = "REQUIREMENT_UNMET"
	KindForecastInvalidated = "FORECAST_INVALIDATED"
)

// 决策队列条目状态。
const (
	QueuePending  = "PENDING"
	QueueResolved = "RESOLVED"
)

// Alternative 替代站点建议。
type Alternative struct {
	StationID string    `json:"station_id"`
	WindowID  string    `json:"window_id"`
	StartUTC  time.Time `json:"start_utc"`
	EndUTC    time.Time `json:"end_utc"`
	Reason    string    `json:"reason"`
}

// DecisionItem 人工决策队列条目（可恢复：持久化于 SQLite）。
type DecisionItem struct {
	ItemID     string          `json:"item_id"`
	Kind       string          `json:"kind"`
	TaskID     string          `json:"task_id"`
	StationID  string          `json:"station_id"`
	VersionID  int64           `json:"version_id"`
	Status     string          `json:"status"`
	Payload    DecisionPayload `json:"payload"`
	CreatedAt  time.Time       `json:"created_at"`
	ResolvedBy string          `json:"resolved_by,omitempty"`
	ResolvedAt *time.Time      `json:"resolved_at,omitempty"`
	Resolution string          `json:"resolution,omitempty"`
}

// DecisionPayload 决策上下文：冲突链、替代站点、再排程区间。
type DecisionPayload struct {
	SatelliteID     string        `json:"satellite_id"`
	WindowID        string        `json:"window_id"`
	ForecastVersion int           `json:"forecast_version"`
	Reason          string        `json:"reason"`
	ConflictWith    string        `json:"conflict_with,omitempty"`
	Alternatives    []Alternative `json:"alternatives"`
	RegenFrom       time.Time     `json:"regen_from"`
	RegenTo         time.Time     `json:"regen_to"`
}

// Notification 通知结果记录（电话/值班通知留痕）。
type Notification struct {
	ID        int64     `json:"id"`
	ItemID    string    `json:"item_id"`
	Channel   string    `json:"channel"`
	Target    string    `json:"target"`
	Result    string    `json:"result"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

// AuditEvent 审计事件。
type AuditEvent struct {
	ID         int64     `json:"id"`
	Ts         time.Time `json:"ts"`
	Actor      string    `json:"actor"`
	Role       string    `json:"role"`
	Action     string    `json:"action"`
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	Detail     string    `json:"detail"`
}
