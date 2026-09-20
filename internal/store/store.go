// Package store 提供基于 SQLite 的持久化。所有写路径通过
// _txlock=immediate 的事务串行化，配合唯一约束保证并发安全；
// 进程重启后由本包负责建表（幂等），实现重启恢复。
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"satops/groundstation/internal/domain"
)

// Store 包装 *sql.DB，提供领域级 CRUD。
type Store struct {
	db *sql.DB
}

// Open 打开（必要时创建）SQLite 数据库并执行幂等建表。
// path 传 ":memory:" 时使用共享缓存内存库（测试用）。
func Open(path string) (*Store, error) {
	var dsn string
	if path == ":memory:" {
		dsn = "file::memory:?cache=shared&_txlock=immediate&_busy_timeout=10000&_foreign_keys=on"
	} else {
		dsn = fmt.Sprintf("file:%s?_txlock=immediate&_busy_timeout=10000&_journal_mode=WAL&_foreign_keys=on", path)
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// SQLite 单写者：单连接即可彻底避免 SQLITE_BUSY 抖动，
	// 并发语义由上层事务与唯一约束保证。
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, fmt.Errorf("建表失败: %w", err)
	}
	return s, nil
}

// Close 关闭数据库。
func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层连接（仅用于测试与事务封装）。
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate() error {
	_, err := s.db.Exec(schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS forecasts (
    id           TEXT PRIMARY KEY,
    satellite_id TEXT NOT NULL,
    version      INTEGER NOT NULL,
    issued_at    TEXT NOT NULL,
    note         TEXT NOT NULL DEFAULT '',
    superseded   INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    UNIQUE (satellite_id, version)
);

CREATE TABLE IF NOT EXISTS stations (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS antennas (
    id         TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES stations(id),
    name       TEXT NOT NULL,
    bands      TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_antennas_station ON antennas(station_id);

CREATE TABLE IF NOT EXISTS windows (
    id            TEXT PRIMARY KEY,
    satellite_id  TEXT NOT NULL,
    station_id    TEXT NOT NULL REFERENCES stations(id),
    forecast_id   TEXT NOT NULL REFERENCES forecasts(id),
    aos           TEXT NOT NULL,
    los           TEXT NOT NULL,
    max_elevation REAL NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_windows_station ON windows(station_id, aos);
CREATE INDEX IF NOT EXISTS ix_windows_sat ON windows(satellite_id, aos);

CREATE TABLE IF NOT EXISTS tasks (
    id                   TEXT PRIMARY KEY,
    external_id          TEXT NOT NULL UNIQUE,   -- 幂等键
    type                 TEXT NOT NULL,
    satellite_id         TEXT NOT NULL,
    priority             INTEGER NOT NULL,
    min_duration_ns      INTEGER NOT NULL,
    required_band        TEXT NOT NULL DEFAULT '',
    preferred_station_id TEXT NOT NULL DEFAULT '',
    not_before           TEXT NOT NULL DEFAULT '',
    not_after            TEXT NOT NULL DEFAULT '',
    status               TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS maintenance_locks (
    id         TEXT PRIMARY KEY,
    antenna_id TEXT NOT NULL REFERENCES antennas(id),
    station_id TEXT NOT NULL,
    start      TEXT NOT NULL,
    end        TEXT NOT NULL,
    reason     TEXT NOT NULL DEFAULT '',
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_locks_antenna ON maintenance_locks(antenna_id, active);

CREATE TABLE IF NOT EXISTS schedules (
    id           TEXT PRIMARY KEY,
    station_id   TEXT NOT NULL REFERENCES stations(id),
    version      INTEGER NOT NULL,
    status       TEXT NOT NULL,
    stale_reason TEXT NOT NULL DEFAULT '',
    range_from   TEXT NOT NULL,
    range_to     TEXT NOT NULL,
    created_by   TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE (station_id, version)   -- 并发生成的版本号唯一
);
CREATE INDEX IF NOT EXISTS ix_schedules_station ON schedules(station_id, status);

CREATE TABLE IF NOT EXISTS schedule_entries (
    id          TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    task_id     TEXT NOT NULL REFERENCES tasks(id),
    window_id   TEXT NOT NULL,
    antenna_id  TEXT NOT NULL,
    station_id  TEXT NOT NULL,
    forecast_id TEXT NOT NULL,
    start       TEXT NOT NULL,
    [end]       TEXT NOT NULL,
    status      TEXT NOT NULL,
    reason      TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_entries_schedule ON schedule_entries(schedule_id);
CREATE INDEX IF NOT EXISTS ix_entries_task ON schedule_entries(task_id);
CREATE INDEX IF NOT EXISTS ix_entries_window ON schedule_entries(window_id);

CREATE TABLE IF NOT EXISTS decision_items (
    id                TEXT PRIMARY KEY,
    task_id           TEXT NOT NULL REFERENCES tasks(id),
    station_id        TEXT NOT NULL,
    window_id         TEXT NOT NULL DEFAULT '',
    reason            TEXT NOT NULL DEFAULT '',
    conflict_chain    TEXT NOT NULL DEFAULT '[]',
    alternatives      TEXT NOT NULL DEFAULT '[]',
    notifications     TEXT NOT NULL DEFAULT '[]',
    status            TEXT NOT NULL,
    resolution        TEXT NOT NULL DEFAULT '',
    target_station_id TEXT NOT NULL DEFAULT '',
    resolved_by       TEXT NOT NULL DEFAULT '',
    resolved_at       TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL
);
-- 同一任务同一时间只允许一条待决策项（可恢复的幂等队列）
CREATE UNIQUE INDEX IF NOT EXISTS ux_decision_pending
    ON decision_items(task_id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS ix_decision_station ON decision_items(station_id, status);

CREATE TABLE IF NOT EXISTS audit_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    actor_id    TEXT NOT NULL,
    actor_role  TEXT NOT NULL,
    action      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    detail      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_audit_entity ON audit_events(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS notifications (
    id           TEXT PRIMARY KEY,
    target       TEXT NOT NULL,
    channel      TEXT NOT NULL,
    payload      TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL,
    related_type TEXT NOT NULL DEFAULT '',
    related_id   TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_notifications_related ON notifications(related_type, related_id);
`

// ---- 事务 ----

// Tx 事务句柄，所有写操作在事务内执行。
type Tx struct{ tx *sql.Tx }

// WithTx 在 immediate 事务中执行 fn；fn 返回错误则回滚。
func (s *Store) WithTx(ctx context.Context, fn func(*Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	t := &Tx{tx: tx}
	if err := fn(t); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// ---- 时间辅助 ----

func tstr(t time.Time) string { return domain.FormatTime(t) }

func tparse(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := domain.ParseTime(s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ---- 预报版本 ----

func (t *Tx) InsertForecast(ctx context.Context, f *domain.ForecastVersion) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO forecasts (id, satellite_id, version, issued_at, note, superseded, created_at)
		 VALUES (?,?,?,?,?,?,?)`,
		f.ID, f.SatelliteID, f.Version, tstr(f.IssuedAt), f.Note, boolInt(f.Superseded), tstr(f.CreatedAt))
	return err
}

func scanForecast(row interface{ Scan(...any) error }) (*domain.ForecastVersion, error) {
	var f domain.ForecastVersion
	var issued, created string
	var sup int
	if err := row.Scan(&f.ID, &f.SatelliteID, &f.Version, &issued, &f.Note, &sup, &created); err != nil {
		return nil, err
	}
	f.IssuedAt, f.CreatedAt = tparse(issued), tparse(created)
	f.Superseded = sup != 0
	return &f, nil
}

const forecastCols = `id, satellite_id, version, issued_at, note, superseded, created_at`

func (t *Tx) GetForecast(ctx context.Context, id string) (*domain.ForecastVersion, error) {
	return scanForecast(t.tx.QueryRowContext(ctx,
		`SELECT `+forecastCols+` FROM forecasts WHERE id = ?`, id))
}

// SupersedeOtherForecasts 将同卫星其他预报标记为作废，返回被作废的 ID 列表。
func (t *Tx) SupersedeOtherForecasts(ctx context.Context, satelliteID, keepID string) ([]string, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT id FROM forecasts WHERE satellite_id = ? AND id != ? AND superseded = 0`, satelliteID, keepID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, nil
	}
	if _, err := t.tx.ExecContext(ctx,
		`UPDATE forecasts SET superseded = 1 WHERE satellite_id = ? AND id != ?`, satelliteID, keepID); err != nil {
		return nil, err
	}
	return ids, nil
}

func (t *Tx) ListForecasts(ctx context.Context, satelliteID string) ([]*domain.ForecastVersion, error) {
	q := `SELECT ` + forecastCols + ` FROM forecasts`
	args := []any{}
	if satelliteID != "" {
		q += ` WHERE satellite_id = ?`
		args = append(args, satelliteID)
	}
	q += ` ORDER BY satellite_id, version DESC`
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.ForecastVersion
	for rows.Next() {
		f, err := scanForecast(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// ---- 站点与天线 ----

func (t *Tx) InsertStation(ctx context.Context, st *domain.Station) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO stations (id, name, created_at) VALUES (?,?,?)`,
		st.ID, st.Name, tstr(st.CreatedAt))
	return err
}

func (t *Tx) GetStation(ctx context.Context, id string) (*domain.Station, error) {
	var st domain.Station
	var created string
	err := t.tx.QueryRowContext(ctx, `SELECT id, name, created_at FROM stations WHERE id = ?`, id).
		Scan(&st.ID, &st.Name, &created)
	if err != nil {
		return nil, err
	}
	st.CreatedAt = tparse(created)
	return &st, nil
}

func (t *Tx) ListStations(ctx context.Context) ([]*domain.Station, error) {
	rows, err := t.tx.QueryContext(ctx, `SELECT id, name, created_at FROM stations ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Station
	for rows.Next() {
		var st domain.Station
		var created string
		if err := rows.Scan(&st.ID, &st.Name, &created); err != nil {
			return nil, err
		}
		st.CreatedAt = tparse(created)
		out = append(out, &st)
	}
	return out, rows.Err()
}

func (t *Tx) InsertAntenna(ctx context.Context, a *domain.Antenna) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO antennas (id, station_id, name, bands, status, created_at) VALUES (?,?,?,?,?,?)`,
		a.ID, a.StationID, a.Name, strings.Join(a.Bands, ","), a.Status, tstr(a.CreatedAt))
	return err
}

func scanAntenna(row interface{ Scan(...any) error }) (*domain.Antenna, error) {
	var a domain.Antenna
	var bands, created string
	if err := row.Scan(&a.ID, &a.StationID, &a.Name, &bands, &a.Status, &created); err != nil {
		return nil, err
	}
	if bands != "" {
		a.Bands = strings.Split(bands, ",")
	}
	a.CreatedAt = tparse(created)
	return &a, nil
}

func (t *Tx) GetAntenna(ctx context.Context, id string) (*domain.Antenna, error) {
	return scanAntenna(t.tx.QueryRowContext(ctx,
		`SELECT id, station_id, name, bands, status, created_at FROM antennas WHERE id = ?`, id))
}

func (t *Tx) ListAntennas(ctx context.Context, stationID string) ([]*domain.Antenna, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT id, station_id, name, bands, status, created_at FROM antennas WHERE station_id = ? ORDER BY id`, stationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Antenna
	for rows.Next() {
		a, err := scanAntenna(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---- 可见弧段 ----

func (t *Tx) InsertWindow(ctx context.Context, w *domain.VisibilityWindow) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO windows (id, satellite_id, station_id, forecast_id, aos, los, max_elevation, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		w.ID, w.SatelliteID, w.StationID, w.ForecastID, tstr(w.AOS), tstr(w.LOS), w.MaxElevation, tstr(w.CreatedAt))
	return err
}

func scanWindow(row interface{ Scan(...any) error }) (*domain.VisibilityWindow, error) {
	var w domain.VisibilityWindow
	var aos, los, created string
	if err := row.Scan(&w.ID, &w.SatelliteID, &w.StationID, &w.ForecastID, &aos, &los, &w.MaxElevation, &created); err != nil {
		return nil, err
	}
	w.AOS, w.LOS, w.CreatedAt = tparse(aos), tparse(los), tparse(created)
	return &w, nil
}

const windowCols = `id, satellite_id, station_id, forecast_id, aos, los, max_elevation, created_at`

func (t *Tx) GetWindow(ctx context.Context, id string) (*domain.VisibilityWindow, error) {
	return scanWindow(t.tx.QueryRowContext(ctx,
		`SELECT `+windowCols+` FROM windows WHERE id = ?`, id))
}

// ListWindowsOverlap 列出站点在 [from,to] 内有重叠的弧段（含跨午夜：纯区间重叠判断）。
func (t *Tx) ListWindowsOverlap(ctx context.Context, stationID string, from, to time.Time) ([]*domain.VisibilityWindow, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+windowCols+` FROM windows
		 WHERE station_id = ? AND aos < ? AND los > ? ORDER BY aos, id`, stationID, tstr(to), tstr(from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWindows(rows)
}

// ListWindowsForSatellite 列出某卫星在 [from,to] 内、可含其他站点的弧段（替代站点计算用）。
func (t *Tx) ListWindowsForSatellite(ctx context.Context, satelliteID string, from, to time.Time) ([]*domain.VisibilityWindow, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+windowCols+` FROM windows
		 WHERE satellite_id = ? AND aos < ? AND los > ? ORDER BY aos, id`, satelliteID, tstr(to), tstr(from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWindows(rows)
}

func scanWindows(rows *sql.Rows) ([]*domain.VisibilityWindow, error) {
	var out []*domain.VisibilityWindow
	for rows.Next() {
		w, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ---- 任务 ----

// InsertTaskIfAbsent 幂等插入：external_id 已存在时不写入，返回 created=false。
func (t *Tx) InsertTaskIfAbsent(ctx context.Context, task *domain.Task) (created bool, err error) {
	res, err := t.tx.ExecContext(ctx,
		`INSERT INTO tasks (id, external_id, type, satellite_id, priority, min_duration_ns,
		                    required_band, preferred_station_id, not_before, not_after, status, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(external_id) DO NOTHING`,
		task.ID, task.ExternalID, string(task.Type), task.SatelliteID, task.Priority,
		int64(task.MinDuration), task.RequiredBand, task.PreferredStationID,
		tstr(task.NotBefore), tstr(task.NotAfter), task.Status, tstr(task.CreatedAt), tstr(task.UpdatedAt))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func scanTask(row interface{ Scan(...any) error }) (*domain.Task, error) {
	var task domain.Task
	var nb, na, created, updated string
	if err := row.Scan(&task.ID, &task.ExternalID, &task.Type, &task.SatelliteID, &task.Priority,
		&task.MinDuration, &task.RequiredBand, &task.PreferredStationID, &nb, &na,
		&task.Status, &created, &updated); err != nil {
		return nil, err
	}
	task.NotBefore, task.NotAfter = tparse(nb), tparse(na)
	task.CreatedAt, task.UpdatedAt = tparse(created), tparse(updated)
	return &task, nil
}

const taskCols = `id, external_id, type, satellite_id, priority, min_duration_ns,
                  required_band, preferred_station_id, not_before, not_after, status, created_at, updated_at`

func (t *Tx) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	return scanTask(t.tx.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE id = ?`, id))
}

func (t *Tx) GetTaskByExternalID(ctx context.Context, externalID string) (*domain.Task, error) {
	return scanTask(t.tx.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE external_id = ?`, externalID))
}

func (t *Tx) UpdateTaskStatus(ctx context.Context, id, status string, at time.Time) error {
	_, err := t.tx.ExecContext(ctx,
		`UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?`, status, tstr(at), id)
	return err
}

func (t *Tx) UpdateTaskStation(ctx context.Context, id, stationID string, at time.Time) error {
	_, err := t.tx.ExecContext(ctx,
		`UPDATE tasks SET preferred_station_id = ?, updated_at = ? WHERE id = ?`, stationID, tstr(at), id)
	return err
}

// ListSchedulableTasks 列出可在指定站点重排的任务（pending/displaced，且未指定站点或指定本站）。
func (t *Tx) ListSchedulableTasks(ctx context.Context, stationID string) ([]*domain.Task, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+taskCols+` FROM tasks
		 WHERE status IN ('pending','displaced')
		   AND (preferred_station_id = '' OR preferred_station_id = ?)
		 ORDER BY priority DESC, external_id`, stationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

// ---- 维护锁定 ----

func (t *Tx) InsertLock(ctx context.Context, l *domain.MaintenanceLock) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO maintenance_locks (id, antenna_id, station_id, start, [end], reason, active, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		l.ID, l.AntennaID, l.StationID, tstr(l.Start), tstr(l.End), l.Reason, boolInt(l.Active), tstr(l.CreatedAt))
	return err
}

func scanLock(row interface{ Scan(...any) error }) (*domain.MaintenanceLock, error) {
	var l domain.MaintenanceLock
	var start, end, created string
	var active int
	if err := row.Scan(&l.ID, &l.AntennaID, &l.StationID, &start, &end, &l.Reason, &active, &created); err != nil {
		return nil, err
	}
	l.Start, l.End, l.CreatedAt = tparse(start), tparse(end), tparse(created)
	l.Active = active != 0
	return &l, nil
}

const lockCols = `id, antenna_id, station_id, start, [end], reason, active, created_at`

func (t *Tx) GetLock(ctx context.Context, id string) (*domain.MaintenanceLock, error) {
	return scanLock(t.tx.QueryRowContext(ctx, `SELECT `+lockCols+` FROM maintenance_locks WHERE id = ?`, id))
}

// ListActiveLocksOverlap 列出站点内在 [from,to] 有重叠的有效锁定。
func (t *Tx) ListActiveLocksOverlap(ctx context.Context, stationID string, from, to time.Time) ([]*domain.MaintenanceLock, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+lockCols+` FROM maintenance_locks
		 WHERE station_id = ? AND active = 1 AND start < ? AND [end] > ?`,
		stationID, tstr(to), tstr(from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.MaintenanceLock
	for rows.Next() {
		l, err := scanLock(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ListActiveLocksForAntenna 列出天线在 [from,to] 有重叠的有效锁定。
func (t *Tx) ListActiveLocksForAntenna(ctx context.Context, antennaID string, from, to time.Time) ([]*domain.MaintenanceLock, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+lockCols+` FROM maintenance_locks
		 WHERE antenna_id = ? AND active = 1 AND start < ? AND [end] > ?`,
		antennaID, tstr(to), tstr(from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.MaintenanceLock
	for rows.Next() {
		l, err := scanLock(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

func (t *Tx) SetLockActive(ctx context.Context, id string, active bool) error {
	_, err := t.tx.ExecContext(ctx, `UPDATE maintenance_locks SET active = ? WHERE id = ?`, boolInt(active), id)
	return err
}

// ---- 排程 ----

func (t *Tx) InsertSchedule(ctx context.Context, sch *domain.Schedule) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO schedules (id, station_id, version, status, stale_reason, range_from, range_to, created_by, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		sch.ID, sch.StationID, sch.Version, sch.Status, sch.StaleReason,
		tstr(sch.RangeFrom), tstr(sch.RangeTo), sch.CreatedBy, tstr(sch.CreatedAt))
	return err
}

// NextScheduleVersion 返回站点下一个排程版本号（须在事务内调用）。
func (t *Tx) NextScheduleVersion(ctx context.Context, stationID string) (int, error) {
	var v int
	err := t.tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) + 1 FROM schedules WHERE station_id = ?`, stationID).Scan(&v)
	return v, err
}

func scanSchedule(row interface{ Scan(...any) error }) (*domain.Schedule, error) {
	var sch domain.Schedule
	var from, to, created string
	if err := row.Scan(&sch.ID, &sch.StationID, &sch.Version, &sch.Status, &sch.StaleReason,
		&from, &to, &sch.CreatedBy, &created); err != nil {
		return nil, err
	}
	sch.RangeFrom, sch.RangeTo, sch.CreatedAt = tparse(from), tparse(to), tparse(created)
	return &sch, nil
}

const scheduleCols = `id, station_id, version, status, stale_reason, range_from, range_to, created_by, created_at`

func (t *Tx) GetSchedule(ctx context.Context, id string) (*domain.Schedule, error) {
	return scanSchedule(t.tx.QueryRowContext(ctx, `SELECT `+scheduleCols+` FROM schedules WHERE id = ?`, id))
}

// ActiveSchedule 返回站点当前生效排程；无则返回 sql.ErrNoRows 包装错误。
func (t *Tx) ActiveSchedule(ctx context.Context, stationID string) (*domain.Schedule, error) {
	return scanSchedule(t.tx.QueryRowContext(ctx,
		`SELECT `+scheduleCols+` FROM schedules WHERE station_id = ? AND status = 'active'
		 ORDER BY version DESC LIMIT 1`, stationID))
}

func (t *Tx) ListSchedules(ctx context.Context, stationID, status string) ([]*domain.Schedule, error) {
	q := `SELECT ` + scheduleCols + ` FROM schedules WHERE station_id = ?`
	args := []any{stationID}
	if status != "" {
		q += ` AND status = ?`
		args = append(args, status)
	}
	q += ` ORDER BY version DESC`
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Schedule
	for rows.Next() {
		sch, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sch)
	}
	return out, rows.Err()
}

func (t *Tx) SetScheduleStatus(ctx context.Context, id, status, staleReason string) error {
	_, err := t.tx.ExecContext(ctx,
		`UPDATE schedules SET status = ?, stale_reason = ? WHERE id = ?`, status, staleReason, id)
	return err
}

// ---- 排程条目 ----

func (t *Tx) InsertEntry(ctx context.Context, e *domain.ScheduleEntry) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO schedule_entries (id, schedule_id, task_id, window_id, antenna_id, station_id,
		                               forecast_id, start, [end], status, reason, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.ID, e.ScheduleID, e.TaskID, e.WindowID, e.AntennaID, e.StationID,
		e.ForecastID, tstr(e.Start), tstr(e.End), e.Status, e.Reason, tstr(e.CreatedAt))
	return err
}

func scanEntry(row interface{ Scan(...any) error }) (*domain.ScheduleEntry, error) {
	var e domain.ScheduleEntry
	var start, end, created string
	if err := row.Scan(&e.ID, &e.ScheduleID, &e.TaskID, &e.WindowID, &e.AntennaID, &e.StationID,
		&e.ForecastID, &start, &end, &e.Status, &e.Reason, &created); err != nil {
		return nil, err
	}
	e.Start, e.End, e.CreatedAt = tparse(start), tparse(end), tparse(created)
	return &e, nil
}

const entryCols = `id, schedule_id, task_id, window_id, antenna_id, station_id, forecast_id, start, [end], status, reason, created_at`

func (t *Tx) ListEntries(ctx context.Context, scheduleID string) ([]*domain.ScheduleEntry, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+entryCols+` FROM schedule_entries WHERE schedule_id = ? ORDER BY start`, scheduleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

func scanEntries(rows *sql.Rows) ([]*domain.ScheduleEntry, error) {
	var out []*domain.ScheduleEntry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// StartedEntries 列出某版排程中已开始且未结束的条目（start <= now < end）。
func (t *Tx) StartedEntries(ctx context.Context, scheduleID string, now time.Time) ([]*domain.ScheduleEntry, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+entryCols+` FROM schedule_entries
		 WHERE schedule_id = ? AND start <= ? AND [end] > ? ORDER BY start`, scheduleID, tstr(now), tstr(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

// EntriesByWindow 列出引用了指定弧段的条目（窗口详情聚合用）。
func (t *Tx) EntriesByWindow(ctx context.Context, windowID string) ([]*domain.ScheduleEntry, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+entryCols+` FROM schedule_entries WHERE window_id = ? ORDER BY start`, windowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEntries(rows)
}

// SchedulesUsingForecasts 返回引用了给定预报版本、且状态为 candidate/active 的排程 ID。
func (t *Tx) SchedulesUsingForecasts(ctx context.Context, forecastIDs []string) ([]string, error) {
	if len(forecastIDs) == 0 {
		return nil, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(forecastIDs)), ",")
	args := make([]any, 0, len(forecastIDs))
	for _, id := range forecastIDs {
		args = append(args, id)
	}
	rows, err := t.tx.QueryContext(ctx,
		`SELECT DISTINCT s.id FROM schedules s
		 JOIN schedule_entries e ON e.schedule_id = s.id
		 JOIN windows w ON w.id = e.window_id
		 WHERE w.forecast_id IN (`+ph+`) AND s.status IN ('candidate','active')`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SchedulesWithEntriesOnAntenna 返回在 [from,to] 内使用了指定天线、状态为 candidate/active 的排程 ID。
func (t *Tx) SchedulesWithEntriesOnAntenna(ctx context.Context, antennaID string, from, to time.Time) ([]string, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT DISTINCT s.id FROM schedules s
		 JOIN schedule_entries e ON e.schedule_id = s.id
		 WHERE e.antenna_id = ? AND e.start < ? AND e.[end] > ?
		   AND s.status IN ('candidate','active')`, antennaID, tstr(to), tstr(from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---- 决策队列 ----

func (t *Tx) InsertDecisionItem(ctx context.Context, it *domain.DecisionItem) error {
	chain, _ := json.Marshal(it.ConflictChain)
	alts, _ := json.Marshal(it.Alternatives)
	notifs, _ := json.Marshal(it.Notifications)
	var resolvedAt string
	if it.ResolvedAt != nil {
		resolvedAt = tstr(*it.ResolvedAt)
	}
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO decision_items (id, task_id, station_id, window_id, reason, conflict_chain, alternatives,
		                             notifications, status, resolution, target_station_id, resolved_by, resolved_at,
		                             created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		it.ID, it.TaskID, it.StationID, it.WindowID, it.Reason, string(chain), string(alts), string(notifs),
		it.Status, it.Resolution, it.TargetStationID, it.ResolvedBy, resolvedAt,
		tstr(it.CreatedAt), tstr(it.UpdatedAt))
	return err
}

func (t *Tx) UpdateDecisionItem(ctx context.Context, it *domain.DecisionItem) error {
	chain, _ := json.Marshal(it.ConflictChain)
	alts, _ := json.Marshal(it.Alternatives)
	notifs, _ := json.Marshal(it.Notifications)
	var resolvedAt string
	if it.ResolvedAt != nil {
		resolvedAt = tstr(*it.ResolvedAt)
	}
	_, err := t.tx.ExecContext(ctx,
		`UPDATE decision_items SET station_id=?, window_id=?, reason=?, conflict_chain=?, alternatives=?,
		 notifications=?, status=?, resolution=?, target_station_id=?, resolved_by=?, resolved_at=?, updated_at=?
		 WHERE id = ?`,
		it.StationID, it.WindowID, it.Reason, string(chain), string(alts), string(notifs),
		it.Status, it.Resolution, it.TargetStationID, it.ResolvedBy, resolvedAt,
		tstr(it.UpdatedAt), it.ID)
	return err
}

func scanDecisionItem(row interface{ Scan(...any) error }) (*domain.DecisionItem, error) {
	var it domain.DecisionItem
	var chain, alts, notifs, resolvedAt, created, updated string
	if err := row.Scan(&it.ID, &it.TaskID, &it.StationID, &it.WindowID, &it.Reason, &chain, &alts, &notifs,
		&it.Status, &it.Resolution, &it.TargetStationID, &it.ResolvedBy, &resolvedAt, &created, &updated); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(chain), &it.ConflictChain)
	_ = json.Unmarshal([]byte(alts), &it.Alternatives)
	_ = json.Unmarshal([]byte(notifs), &it.Notifications)
	if resolvedAt != "" {
		t := tparse(resolvedAt)
		it.ResolvedAt = &t
	}
	it.CreatedAt, it.UpdatedAt = tparse(created), tparse(updated)
	return &it, nil
}

const decisionCols = `id, task_id, station_id, window_id, reason, conflict_chain, alternatives, notifications,
                      status, resolution, target_station_id, resolved_by, resolved_at, created_at, updated_at`

func (t *Tx) GetDecisionItem(ctx context.Context, id string) (*domain.DecisionItem, error) {
	return scanDecisionItem(t.tx.QueryRowContext(ctx,
		`SELECT `+decisionCols+` FROM decision_items WHERE id = ?`, id))
}

// PendingDecisionForTask 返回任务的待决策项；无则返回 nil, nil。
func (t *Tx) PendingDecisionForTask(ctx context.Context, taskID string) (*domain.DecisionItem, error) {
	it, err := scanDecisionItem(t.tx.QueryRowContext(ctx,
		`SELECT `+decisionCols+` FROM decision_items WHERE task_id = ? AND status = 'pending'`, taskID))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return it, err
}

func (t *Tx) ListDecisionItems(ctx context.Context, stationIDs []string, status string) ([]*domain.DecisionItem, error) {
	q := `SELECT ` + decisionCols + ` FROM decision_items`
	var conds []string
	var args []any
	if len(stationIDs) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(stationIDs)), ",")
		conds = append(conds, `station_id IN (`+ph+`)`)
		for _, id := range stationIDs {
			args = append(args, id)
		}
	}
	if status != "" {
		conds = append(conds, `status = ?`)
		args = append(args, status)
	}
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	q += ` ORDER BY created_at`
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.DecisionItem
	for rows.Next() {
		it, err := scanDecisionItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// DecisionItemsByWindow 列出与指定窗口关联的决策项。
func (t *Tx) DecisionItemsByWindow(ctx context.Context, windowID string) ([]*domain.DecisionItem, error) {
	rows, err := t.tx.QueryContext(ctx,
		`SELECT `+decisionCols+` FROM decision_items WHERE window_id = ? ORDER BY created_at`, windowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.DecisionItem
	for rows.Next() {
		it, err := scanDecisionItem(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// ---- 审计与通知 ----

func (t *Tx) InsertAudit(ctx context.Context, ev *domain.AuditEvent) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO audit_events (ts, actor_id, actor_role, action, entity_type, entity_id, detail)
		 VALUES (?,?,?,?,?,?,?)`,
		tstr(ev.TS), ev.ActorID, ev.ActorRole, ev.Action, ev.EntityType, ev.EntityID, ev.Detail)
	return err
}

func (t *Tx) ListAudit(ctx context.Context, entityType, entityID string, limit int) ([]*domain.AuditEvent, error) {
	q := `SELECT id, ts, actor_id, actor_role, action, entity_type, entity_id, detail FROM audit_events`
	var conds []string
	var args []any
	if entityType != "" {
		conds = append(conds, `entity_type = ?`)
		args = append(args, entityType)
	}
	if entityID != "" {
		conds = append(conds, `entity_id = ?`)
		args = append(args, entityID)
	}
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	q += ` ORDER BY id`
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, limit)
	}
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.AuditEvent
	for rows.Next() {
		var ev domain.AuditEvent
		var ts string
		if err := rows.Scan(&ev.ID, &ts, &ev.ActorID, &ev.ActorRole, &ev.Action, &ev.EntityType, &ev.EntityID, &ev.Detail); err != nil {
			return nil, err
		}
		ev.TS = tparse(ts)
		out = append(out, &ev)
	}
	return out, rows.Err()
}

func (t *Tx) InsertNotification(ctx context.Context, n *domain.Notification) error {
	_, err := t.tx.ExecContext(ctx,
		`INSERT INTO notifications (id, target, channel, payload, status, related_type, related_id, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		n.ID, n.Target, n.Channel, n.Payload, n.Status, n.RelatedType, n.RelatedID, tstr(n.CreatedAt))
	return err
}

func (t *Tx) ListNotifications(ctx context.Context, relatedType, relatedID string) ([]*domain.Notification, error) {
	q := `SELECT id, target, channel, payload, status, related_type, related_id, created_at FROM notifications`
	var conds []string
	var args []any
	if relatedType != "" {
		conds = append(conds, `related_type = ?`)
		args = append(args, relatedType)
	}
	if relatedID != "" {
		conds = append(conds, `related_id = ?`)
		args = append(args, relatedID)
	}
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, ` AND `)
	}
	q += ` ORDER BY created_at`
	rows, err := t.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Notification
	for rows.Next() {
		var n domain.Notification
		var created string
		if err := rows.Scan(&n.ID, &n.Target, &n.Channel, &n.Payload, &n.Status, &n.RelatedType, &n.RelatedID, &created); err != nil {
			return nil, err
		}
		n.CreatedAt = tparse(created)
		out = append(out, &n)
	}
	return out, rows.Err()
}
