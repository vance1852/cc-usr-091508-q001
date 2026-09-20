// Package store 提供 SQLite 持久化。所有时间以 UTC unix 秒存储，
// 跨午夜弧段由绝对时间比较自然覆盖。
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"gscsvc/internal/domain"
)

// execer 抽象 *sql.DB / *sql.Conn，使读写方法可在事务内外复用。
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store 封装 *sql.DB。
type Store struct {
	db *sql.DB
}

// Open 打开（必要时创建）数据库并执行迁移。path 为文件路径，":memory:" 亦可。
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=1", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	// 单连接避免 SQLITE_BUSY 抖动；写事务经 BEGIN IMMEDIATE 在该连接上串行执行。
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close 关闭数据库。
func (s *Store) Close() error { return s.db.Close() }

func migrate(db *sql.DB) error {
	_, err := db.Exec(schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS orbit_forecasts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  satellite_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  generated_at INTEGER NOT NULL,
  received_at INTEGER NOT NULL,
  UNIQUE(satellite_id, version)
);
CREATE TABLE IF NOT EXISTS stations (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  bands TEXT NOT NULL DEFAULT '',
  max_rate_dps REAL NOT NULL DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL UNIQUE,
  satellite_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  priority INTEGER NOT NULL,
  min_tt_seconds INTEGER NOT NULL,
  required_band TEXT NOT NULL DEFAULT '',
  migrated_to TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  submitted_by TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS visibility_windows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id TEXT NOT NULL UNIQUE,
  satellite_id TEXT NOT NULL,
  station_id TEXT NOT NULL,
  start_utc INTEGER NOT NULL,
  end_utc INTEGER NOT NULL,
  max_elev_deg REAL NOT NULL DEFAULT 0,
  forecast_version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_windows_station ON visibility_windows(station_id, start_utc);
CREATE INDEX IF NOT EXISTS idx_windows_sat ON visibility_windows(satellite_id, start_utc);
CREATE TABLE IF NOT EXISTS maintenance_locks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lock_id TEXT NOT NULL UNIQUE,
  station_id TEXT NOT NULL,
  start_utc INTEGER NOT NULL,
  end_utc INTEGER NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_by TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  station_id TEXT NOT NULL,
  version_no INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  note TEXT NOT NULL DEFAULT '',
  created_by TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  UNIQUE(station_id, version_no)
);
CREATE TABLE IF NOT EXISTS schedule_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  version_id INTEGER NOT NULL REFERENCES schedule_versions(id),
  task_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  station_id TEXT NOT NULL,
  satellite_id TEXT NOT NULL,
  start_utc INTEGER NOT NULL,
  end_utc INTEGER NOT NULL,
  forecast_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  locked INTEGER NOT NULL DEFAULT 0,
  affected INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_entries_version ON schedule_entries(version_id);
CREATE INDEX IF NOT EXISTS idx_entries_window ON schedule_entries(window_id);
CREATE INDEX IF NOT EXISTS idx_entries_task ON schedule_entries(task_id);
CREATE TABLE IF NOT EXISTS conflicts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  version_id INTEGER NOT NULL REFERENCES schedule_versions(id),
  station_id TEXT NOT NULL,
  winner_task_id TEXT NOT NULL,
  loser_task_id TEXT NOT NULL,
  overlap_start INTEGER NOT NULL,
  overlap_end INTEGER NOT NULL,
  reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS decision_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL,
  task_id TEXT NOT NULL,
  station_id TEXT NOT NULL,
  version_id INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'PENDING',
  payload TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  resolved_by TEXT NOT NULL DEFAULT '',
  resolved_at INTEGER,
  resolution TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_queue_status ON decision_queue(status);
CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id TEXT NOT NULL DEFAULT '',
  channel TEXT NOT NULL,
  target TEXT NOT NULL,
  result TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notifications_item ON notifications(item_id);
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL,
  actor TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT '',
  action TEXT NOT NULL,
  entity_type TEXT NOT NULL DEFAULT '',
  entity_id TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_events(entity_type, entity_id);
`

func unix(t time.Time) int64       { return t.UTC().Unix() }
func fromUnix(sec int64) time.Time { return time.Unix(sec, 0).UTC() }

// Tx 独占单连接的手动事务（BEGIN IMMEDIATE）。
type Tx struct {
	conn *sql.Conn
	done bool
}

// BeginImmediate 在独占连接上开启 IMMEDIATE 事务：写操作串行化，
// 配合服务层站点互斥锁防止并发排程竞态。
func (s *Store) BeginImmediate(ctx context.Context) (*Tx, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		conn.Close()
		return nil, err
	}
	return &Tx{conn: conn}, nil
}

// Commit 提交事务并释放连接。
func (t *Tx) Commit() error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.conn.Close()
	_, err := t.conn.ExecContext(context.Background(), "COMMIT")
	return err
}

// Rollback 回滚事务并释放连接（幂等，便于 defer）。
func (t *Tx) Rollback() error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.conn.Close()
	_, err := t.conn.ExecContext(context.Background(), "ROLLBACK")
	return err
}

// ---- 内部读写实现（execer 复用） ----

func insertForecast(ctx context.Context, ex execer, f domain.OrbitForecast) (bool, error) {
	res, err := ex.ExecContext(ctx,
		`INSERT OR IGNORE INTO orbit_forecasts(satellite_id,version,generated_at,received_at) VALUES(?,?,?,?)`,
		f.SatelliteID, f.Version, unix(f.GeneratedAt), unix(f.ReceivedAt))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func getStation(ctx context.Context, ex execer, id string) (domain.Station, error) {
	var st domain.Station
	var bands string
	var active int
	err := ex.QueryRowContext(ctx,
		`SELECT id,name,bands,max_rate_dps,active FROM stations WHERE id=?`, id).
		Scan(&st.ID, &st.Name, &bands, &st.MaxRateDps, &active)
	if err != nil {
		return st, err
	}
	if bands != "" {
		st.Bands = strings.Split(bands, ",")
	}
	st.Active = active == 1
	return st, nil
}

func activeTasks(ctx context.Context, ex execer) ([]domain.Task, error) {
	rows, err := ex.QueryContext(ctx,
		`SELECT task_id,satellite_id,kind,priority,min_tt_seconds,required_band,migrated_to,status,submitted_by,created_at,updated_at
		 FROM tasks WHERE status='ACTIVE'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Task
	for rows.Next() {
		var t domain.Task
		var created, updated int64
		if err := rows.Scan(&t.TaskID, &t.SatelliteID, &t.Kind, &t.Priority, &t.MinTTSeconds,
			&t.RequiredBand, &t.MigratedTo, &t.Status, &t.SubmittedBy, &created, &updated); err != nil {
			return nil, err
		}
		t.CreatedAt, t.UpdatedAt = fromUnix(created), fromUnix(updated)
		out = append(out, t)
	}
	return out, rows.Err()
}

func queryWindows(ctx context.Context, ex execer, q string, args ...any) ([]domain.VisibilityWindow, error) {
	rows, err := ex.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.VisibilityWindow
	for rows.Next() {
		var w domain.VisibilityWindow
		var start, end int64
		if err := rows.Scan(&w.WindowID, &w.SatelliteID, &w.StationID, &start, &end,
			&w.MaxElevDeg, &w.ForecastVersion); err != nil {
			return nil, err
		}
		w.StartUTC, w.EndUTC = fromUnix(start), fromUnix(end)
		out = append(out, w)
	}
	return out, rows.Err()
}

func windowsForStation(ctx context.Context, ex execer, stationID string, from, to time.Time) ([]domain.VisibilityWindow, error) {
	return queryWindows(ctx, ex,
		`SELECT window_id,satellite_id,station_id,start_utc,end_utc,max_elev_deg,forecast_version
		 FROM visibility_windows WHERE station_id=? AND start_utc<? AND end_utc>? ORDER BY start_utc`,
		stationID, unix(to), unix(from))
}

func windowsForSatellite(ctx context.Context, ex execer, satelliteID string, from, to time.Time) ([]domain.VisibilityWindow, error) {
	return queryWindows(ctx, ex,
		`SELECT window_id,satellite_id,station_id,start_utc,end_utc,max_elev_deg,forecast_version
		 FROM visibility_windows WHERE satellite_id=? AND start_utc<? AND end_utc>? ORDER BY start_utc`,
		satelliteID, unix(to), unix(from))
}

func locksForStation(ctx context.Context, ex execer, stationID string, from, to time.Time) ([]domain.MaintenanceLock, error) {
	rows, err := ex.QueryContext(ctx,
		`SELECT lock_id,station_id,start_utc,end_utc,reason,created_by,created_at
		 FROM maintenance_locks WHERE station_id=? AND start_utc<? AND end_utc>?`,
		stationID, unix(to), unix(from))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MaintenanceLock
	for rows.Next() {
		var l domain.MaintenanceLock
		var start, end, created int64
		if err := rows.Scan(&l.LockID, &l.StationID, &start, &end, &l.Reason, &l.CreatedBy, &created); err != nil {
			return nil, err
		}
		l.StartUTC, l.EndUTC, l.CreatedAt = fromUnix(start), fromUnix(end), fromUnix(created)
		out = append(out, l)
	}
	return out, rows.Err()
}

func scanVersionRow(row *sql.Row) (domain.ScheduleVersion, error) {
	var v domain.ScheduleVersion
	var created int64
	err := row.Scan(&v.ID, &v.StationID, &v.VersionNo, &v.Status, &v.Note, &v.CreatedBy, &created)
	if err != nil {
		return v, err
	}
	v.CreatedAt = fromUnix(created)
	return v, nil
}

func activeVersion(ctx context.Context, ex execer, stationID string) (domain.ScheduleVersion, error) {
	return scanVersionRow(ex.QueryRowContext(ctx,
		`SELECT id,station_id,version_no,status,note,created_by,created_at
		 FROM schedule_versions WHERE station_id=? AND status IN ('ACTIVE','AFFECTED')
		 ORDER BY version_no DESC LIMIT 1`, stationID))
}

func queryEntries(ctx context.Context, ex execer, q string, args ...any) ([]domain.ScheduleEntry, error) {
	rows, err := ex.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ScheduleEntry
	for rows.Next() {
		var e domain.ScheduleEntry
		var start, end int64
		var locked, affected int
		if err := rows.Scan(&e.ID, &e.VersionID, &e.TaskID, &e.WindowID, &e.StationID, &e.SatelliteID,
			&start, &end, &e.ForecastVersion, &e.Status, &e.Reason, &locked, &affected); err != nil {
			return nil, err
		}
		e.StartUTC, e.EndUTC = fromUnix(start), fromUnix(end)
		e.Locked, e.Affected = locked == 1, affected == 1
		out = append(out, e)
	}
	return out, rows.Err()
}

func entriesOfVersion(ctx context.Context, ex execer, versionID int64) ([]domain.ScheduleEntry, error) {
	return queryEntries(ctx, ex,
		`SELECT id,version_id,task_id,window_id,station_id,satellite_id,start_utc,end_utc,forecast_version,status,reason,locked,affected
		 FROM schedule_entries WHERE version_id=? ORDER BY start_utc`, versionID)
}

func insertVersion(ctx context.Context, ex execer, v domain.ScheduleVersion) (int64, error) {
	res, err := ex.ExecContext(ctx,
		`INSERT INTO schedule_versions(station_id,version_no,status,note,created_by,created_at) VALUES(?,?,?,?,?,?)`,
		v.StationID, v.VersionNo, v.Status, v.Note, v.CreatedBy, unix(v.CreatedAt))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertEntry(ctx context.Context, ex execer, e domain.ScheduleEntry) error {
	locked, affected := 0, 0
	if e.Locked {
		locked = 1
	}
	if e.Affected {
		affected = 1
	}
	_, err := ex.ExecContext(ctx,
		`INSERT INTO schedule_entries(version_id,task_id,window_id,station_id,satellite_id,start_utc,end_utc,forecast_version,status,reason,locked,affected)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.VersionID, e.TaskID, e.WindowID, e.StationID, e.SatelliteID,
		unix(e.StartUTC), unix(e.EndUTC), e.ForecastVersion, e.Status, e.Reason, locked, affected)
	return err
}

func insertConflict(ctx context.Context, ex execer, c domain.Conflict) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO conflicts(version_id,station_id,winner_task_id,loser_task_id,overlap_start,overlap_end,reason)
		 VALUES(?,?,?,?,?,?,?)`,
		c.VersionID, c.StationID, c.WinnerTaskID, c.LoserTaskID, unix(c.OverlapStart), unix(c.OverlapEnd), c.Reason)
	return err
}

func insertDecisionItem(ctx context.Context, ex execer, it domain.DecisionItem, payloadJSON string) (bool, error) {
	res, err := ex.ExecContext(ctx,
		`INSERT OR IGNORE INTO decision_queue(item_id,kind,task_id,station_id,version_id,status,payload,created_at)
		 VALUES(?,?,?,?,?,'PENDING',?,?)`,
		it.ItemID, it.Kind, it.TaskID, it.StationID, it.VersionID, payloadJSON, unix(it.CreatedAt))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func insertNotification(ctx context.Context, ex execer, n domain.Notification) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO notifications(item_id,channel,target,result,detail,created_at) VALUES(?,?,?,?,?,?)`,
		n.ItemID, n.Channel, n.Target, n.Result, n.Detail, unix(n.CreatedAt))
	return err
}

func insertAudit(ctx context.Context, ex execer, a domain.AuditEvent) error {
	_, err := ex.ExecContext(ctx,
		`INSERT INTO audit_events(ts,actor,role,action,entity_type,entity_id,detail) VALUES(?,?,?,?,?,?,?)`,
		unix(a.Ts), a.Actor, a.Role, a.Action, a.EntityType, a.EntityID, a.Detail)
	return err
}

// ---- Store 公开方法（事务外） ----

// InsertForecast 写入预报版本；已存在同版本时返回 created=false（幂等）。
func (s *Store) InsertForecast(ctx context.Context, f domain.OrbitForecast) (bool, error) {
	return insertForecast(ctx, s.db, f)
}

// LatestForecastVersion 返回卫星最新预报版本；无记录返回 0。
func (s *Store) LatestForecastVersion(ctx context.Context, satelliteID string) (int, error) {
	var v int
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version),0) FROM orbit_forecasts WHERE satellite_id=?`, satelliteID).Scan(&v)
	return v, err
}

// UpsertStation 写入或更新站点能力。
func (s *Store) UpsertStation(ctx context.Context, st domain.Station) error {
	active := 0
	if st.Active {
		active = 1
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO stations(id,name,bands,max_rate_dps,active) VALUES(?,?,?,?,?)
		 ON CONFLICT(id) DO UPDATE SET name=excluded.name,bands=excluded.bands,
		 max_rate_dps=excluded.max_rate_dps,active=excluded.active`,
		st.ID, st.Name, strings.Join(st.Bands, ","), st.MaxRateDps, active)
	return err
}

// GetStation 读取站点；不存在返回 sql.ErrNoRows。
func (s *Store) GetStation(ctx context.Context, id string) (domain.Station, error) {
	return getStation(ctx, s.db, id)
}

// InsertTask 幂等插入任务；已存在返回 created=false。
func (s *Store) InsertTask(ctx context.Context, t domain.Task) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO tasks(task_id,satellite_id,kind,priority,min_tt_seconds,required_band,status,submitted_by,created_at,updated_at)
		 VALUES(?,?,?,?,?,?,'ACTIVE',?,?,?)`,
		t.TaskID, t.SatelliteID, t.Kind, t.Priority, t.MinTTSeconds, t.RequiredBand,
		t.SubmittedBy, unix(t.CreatedAt), unix(t.UpdatedAt))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// GetTask 按 task_id 读取；不存在返回 sql.ErrNoRows。
func (s *Store) GetTask(ctx context.Context, taskID string) (domain.Task, error) {
	var t domain.Task
	var created, updated int64
	err := s.db.QueryRowContext(ctx,
		`SELECT task_id,satellite_id,kind,priority,min_tt_seconds,required_band,migrated_to,status,submitted_by,created_at,updated_at
		 FROM tasks WHERE task_id=?`, taskID).
		Scan(&t.TaskID, &t.SatelliteID, &t.Kind, &t.Priority, &t.MinTTSeconds,
			&t.RequiredBand, &t.MigratedTo, &t.Status, &t.SubmittedBy, &created, &updated)
	if err != nil {
		return t, err
	}
	t.CreatedAt, t.UpdatedAt = fromUnix(created), fromUnix(updated)
	return t, nil
}

// ActiveTasks 返回所有 ACTIVE 任务。
func (s *Store) ActiveTasks(ctx context.Context) ([]domain.Task, error) {
	return activeTasks(ctx, s.db)
}

// SetTaskMigratedTo 记录任务跨站迁移目标。
func (s *Store) SetTaskMigratedTo(ctx context.Context, taskID, stationID string, now time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET migrated_to=?, updated_at=? WHERE task_id=?`, stationID, unix(now), taskID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// SetTaskStatus 更新任务状态（如 DROPPED）。
func (s *Store) SetTaskStatus(ctx context.Context, taskID, status string, now time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET status=?, updated_at=? WHERE task_id=?`, status, unix(now), taskID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// InsertWindow 幂等插入弧段；已存在返回 created=false。
func (s *Store) InsertWindow(ctx context.Context, w domain.VisibilityWindow) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO visibility_windows(window_id,satellite_id,station_id,start_utc,end_utc,max_elev_deg,forecast_version)
		 VALUES(?,?,?,?,?,?,?)`,
		w.WindowID, w.SatelliteID, w.StationID, unix(w.StartUTC), unix(w.EndUTC), w.MaxElevDeg, w.ForecastVersion)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// GetWindow 按 window_id 读取。
func (s *Store) GetWindow(ctx context.Context, windowID string) (domain.VisibilityWindow, error) {
	var w domain.VisibilityWindow
	var start, end int64
	err := s.db.QueryRowContext(ctx,
		`SELECT window_id,satellite_id,station_id,start_utc,end_utc,max_elev_deg,forecast_version
		 FROM visibility_windows WHERE window_id=?`, windowID).
		Scan(&w.WindowID, &w.SatelliteID, &w.StationID, &start, &end, &w.MaxElevDeg, &w.ForecastVersion)
	if err != nil {
		return w, err
	}
	w.StartUTC, w.EndUTC = fromUnix(start), fromUnix(end)
	return w, nil
}

// WindowsForStation 返回站点在 [from,to] 内重叠的弧段。
func (s *Store) WindowsForStation(ctx context.Context, stationID string, from, to time.Time) ([]domain.VisibilityWindow, error) {
	return windowsForStation(ctx, s.db, stationID, from, to)
}

// WindowsForSatellite 返回卫星在 [from,to] 内重叠的所有站点弧段。
func (s *Store) WindowsForSatellite(ctx context.Context, satelliteID string, from, to time.Time) ([]domain.VisibilityWindow, error) {
	return windowsForSatellite(ctx, s.db, satelliteID, from, to)
}

// InsertLock 幂等插入维护锁定。
func (s *Store) InsertLock(ctx context.Context, l domain.MaintenanceLock) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO maintenance_locks(lock_id,station_id,start_utc,end_utc,reason,created_by,created_at)
		 VALUES(?,?,?,?,?,?,?)`,
		l.LockID, l.StationID, unix(l.StartUTC), unix(l.EndUTC), l.Reason, l.CreatedBy, unix(l.CreatedAt))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// LocksForStation 返回站点在 [from,to] 内重叠的维护锁定。
func (s *Store) LocksForStation(ctx context.Context, stationID string, from, to time.Time) ([]domain.MaintenanceLock, error) {
	return locksForStation(ctx, s.db, stationID, from, to)
}

// ActiveVersion 返回站点当前 ACTIVE/AFFECTED 版本；无则返回 sql.ErrNoRows。
func (s *Store) ActiveVersion(ctx context.Context, stationID string) (domain.ScheduleVersion, error) {
	return activeVersion(ctx, s.db, stationID)
}

// ListVersions 列出站点全部排程版本（新→旧）。
func (s *Store) ListVersions(ctx context.Context, stationID string) ([]domain.ScheduleVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,station_id,version_no,status,note,created_by,created_at
		 FROM schedule_versions WHERE station_id=? ORDER BY version_no DESC`, stationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ScheduleVersion
	for rows.Next() {
		var v domain.ScheduleVersion
		var created int64
		if err := rows.Scan(&v.ID, &v.StationID, &v.VersionNo, &v.Status, &v.Note, &v.CreatedBy, &created); err != nil {
			return nil, err
		}
		v.CreatedAt = fromUnix(created)
		out = append(out, v)
	}
	return out, rows.Err()
}

// EntriesOfVersion 返回版本全部条目。
func (s *Store) EntriesOfVersion(ctx context.Context, versionID int64) ([]domain.ScheduleEntry, error) {
	return entriesOfVersion(ctx, s.db, versionID)
}

// EntriesForWindow 返回某弧段在所有版本中的条目（新→旧），用于追溯。
func (s *Store) EntriesForWindow(ctx context.Context, windowID string) ([]domain.ScheduleEntry, error) {
	return queryEntries(ctx, s.db,
		`SELECT id,version_id,task_id,window_id,station_id,satellite_id,start_utc,end_utc,forecast_version,status,reason,locked,affected
		 FROM schedule_entries WHERE window_id=? ORDER BY id DESC`, windowID)
}

// ---- Tx 事务内方法 ----

// GetStation 事务内读取站点。
func (t *Tx) GetStation(ctx context.Context, id string) (domain.Station, error) {
	return getStation(ctx, t.conn, id)
}

// ActiveTasks 事务内读取活动任务。
func (t *Tx) ActiveTasks(ctx context.Context) ([]domain.Task, error) {
	return activeTasks(ctx, t.conn)
}

// WindowsForStation 事务内读取站点弧段。
func (t *Tx) WindowsForStation(ctx context.Context, stationID string, from, to time.Time) ([]domain.VisibilityWindow, error) {
	return windowsForStation(ctx, t.conn, stationID, from, to)
}

// WindowsForSatellite 事务内读取卫星弧段。
func (t *Tx) WindowsForSatellite(ctx context.Context, satelliteID string, from, to time.Time) ([]domain.VisibilityWindow, error) {
	return windowsForSatellite(ctx, t.conn, satelliteID, from, to)
}

// LocksForStation 事务内读取维护锁定。
func (t *Tx) LocksForStation(ctx context.Context, stationID string, from, to time.Time) ([]domain.MaintenanceLock, error) {
	return locksForStation(ctx, t.conn, stationID, from, to)
}

// ActiveVersion 事务内读取当前版本。
func (t *Tx) ActiveVersion(ctx context.Context, stationID string) (domain.ScheduleVersion, error) {
	return activeVersion(ctx, t.conn, stationID)
}

// EntriesOfVersion 事务内读取版本条目。
func (t *Tx) EntriesOfVersion(ctx context.Context, versionID int64) ([]domain.ScheduleEntry, error) {
	return entriesOfVersion(ctx, t.conn, versionID)
}

// InsertVersion 在事务内插入新版本并返回 id。
func (t *Tx) InsertVersion(ctx context.Context, v domain.ScheduleVersion) (int64, error) {
	return insertVersion(ctx, t.conn, v)
}

// SupersedeVersions 将站点现有 ACTIVE/AFFECTED 版本置为 SUPERSEDED。
func (t *Tx) SupersedeVersions(ctx context.Context, stationID string) error {
	_, err := t.conn.ExecContext(ctx,
		`UPDATE schedule_versions SET status='SUPERSEDED' WHERE station_id=? AND status IN ('ACTIVE','AFFECTED')`,
		stationID)
	return err
}

// InsertEntry 事务内插入排程条目。
func (t *Tx) InsertEntry(ctx context.Context, e domain.ScheduleEntry) error {
	return insertEntry(ctx, t.conn, e)
}

// InsertConflict 事务内插入冲突记录。
func (t *Tx) InsertConflict(ctx context.Context, c domain.Conflict) error {
	return insertConflict(ctx, t.conn, c)
}

// InsertDecisionItem 事务内幂等插入决策队列条目；重复 item_id 返回 created=false。
func (t *Tx) InsertDecisionItem(ctx context.Context, it domain.DecisionItem, payloadJSON string) (bool, error) {
	return insertDecisionItem(ctx, t.conn, it, payloadJSON)
}

// InsertNotification 事务内写入通知结果。
func (t *Tx) InsertNotification(ctx context.Context, n domain.Notification) error {
	return insertNotification(ctx, t.conn, n)
}

// InsertAudit 事务内写入审计事件。
func (t *Tx) InsertAudit(ctx context.Context, a domain.AuditEvent) error {
	return insertAudit(ctx, t.conn, a)
}

// ---- 非事务便捷写入 ----

// Audit 非事务写入审计事件。
func (s *Store) Audit(ctx context.Context, a domain.AuditEvent) error {
	return insertAudit(ctx, s.db, a)
}

// Notify 非事务写入通知结果。
func (s *Store) Notify(ctx context.Context, n domain.Notification) error {
	return insertNotification(ctx, s.db, n)
}

// ---- 预报/锁定影响标记 ----

// MarkSatelliteForecastStale 将使用指定卫星旧版预报的生效版本条目标记为 affected，
// 并将所属版本置为 AFFECTED。返回受影响版本 id 列表。
func (s *Store) MarkSatelliteForecastStale(ctx context.Context, satelliteID string, newVersion int) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT e.version_id FROM schedule_entries e
		 JOIN schedule_versions v ON v.id=e.version_id
		 WHERE e.satellite_id=? AND e.forecast_version<? AND v.status IN ('ACTIVE','AFFECTED')`,
		satelliteID, newVersion)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := s.db.ExecContext(ctx,
			`UPDATE schedule_entries SET affected=1 WHERE version_id=? AND satellite_id=? AND forecast_version<?`,
			id, satelliteID, newVersion); err != nil {
			return nil, err
		}
		if _, err := s.db.ExecContext(ctx,
			`UPDATE schedule_versions SET status='AFFECTED' WHERE id=? AND status='ACTIVE'`, id); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// MarkLockOverlapAffected 标记与维护锁定重叠的生效版本条目，返回受影响版本 id。
func (s *Store) MarkLockOverlapAffected(ctx context.Context, l domain.MaintenanceLock) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT e.version_id FROM schedule_entries e
		 JOIN schedule_versions v ON v.id=e.version_id
		 WHERE e.station_id=? AND e.status IN ('SCHEDULED','CARRIED')
		 AND e.start_utc<? AND e.end_utc>? AND v.status IN ('ACTIVE','AFFECTED')`,
		l.StationID, unix(l.EndUTC), unix(l.StartUTC))
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := s.db.ExecContext(ctx,
			`UPDATE schedule_entries SET affected=1 WHERE version_id=? AND station_id=?
			 AND start_utc<? AND end_utc>?`,
			id, l.StationID, unix(l.EndUTC), unix(l.StartUTC)); err != nil {
			return nil, err
		}
		if _, err := s.db.ExecContext(ctx,
			`UPDATE schedule_versions SET status='AFFECTED' WHERE id=? AND status='ACTIVE'`, id); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// ---- 决策队列 ----

// GetDecisionItem 读取队列条目。
func (s *Store) GetDecisionItem(ctx context.Context, itemID string) (domain.DecisionItem, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT item_id,kind,task_id,station_id,version_id,status,payload,created_at,resolved_by,resolved_at,resolution
		 FROM decision_queue WHERE item_id=?`, itemID)
	return scanItem(row)
}

func scanItem(row *sql.Row) (domain.DecisionItem, error) {
	var it domain.DecisionItem
	var payload string
	var created int64
	var resolvedAt sql.NullInt64
	err := row.Scan(&it.ItemID, &it.Kind, &it.TaskID, &it.StationID, &it.VersionID,
		&it.Status, &payload, &created, &it.ResolvedBy, &resolvedAt, &it.Resolution)
	if err != nil {
		return it, err
	}
	it.CreatedAt = fromUnix(created)
	if resolvedAt.Valid {
		t := fromUnix(resolvedAt.Int64)
		it.ResolvedAt = &t
	}
	if err := json.Unmarshal([]byte(payload), &it.Payload); err != nil {
		return it, fmt.Errorf("decision payload decode: %w", err)
	}
	return it, nil
}

// ListDecisionItems 按状态（空为全部）与站点过滤列出队列条目。
func (s *Store) ListDecisionItems(ctx context.Context, status string, stations []string) ([]domain.DecisionItem, error) {
	q := `SELECT item_id,kind,task_id,station_id,version_id,status,payload,created_at,resolved_by,resolved_at,resolution
	      FROM decision_queue`
	var conds []string
	var args []any
	if status != "" {
		conds = append(conds, "status=?")
		args = append(args, status)
	}
	if len(stations) > 0 {
		ph := make([]string, len(stations))
		for i, st := range stations {
			ph[i] = "?"
			args = append(args, st)
		}
		conds = append(conds, "station_id IN ("+strings.Join(ph, ",")+")")
	}
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	q += " ORDER BY id"
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.DecisionItem
	for rows.Next() {
		var it domain.DecisionItem
		var payload string
		var created int64
		var resolvedAt sql.NullInt64
		if err := rows.Scan(&it.ItemID, &it.Kind, &it.TaskID, &it.StationID, &it.VersionID,
			&it.Status, &payload, &created, &it.ResolvedBy, &resolvedAt, &it.Resolution); err != nil {
			return nil, err
		}
		it.CreatedAt = fromUnix(created)
		if resolvedAt.Valid {
			t := fromUnix(resolvedAt.Int64)
			it.ResolvedAt = &t
		}
		if err := json.Unmarshal([]byte(payload), &it.Payload); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// ResolveDecisionItem 将条目置为 RESOLVED。
func (s *Store) ResolveDecisionItem(ctx context.Context, itemID, by, resolution string, now time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE decision_queue SET status='RESOLVED', resolved_by=?, resolved_at=?, resolution=? WHERE item_id=? AND status='PENDING'`,
		by, unix(now), resolution, itemID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// CountPendingItems 统计待决条目数（重启恢复报告用）。
func (s *Store) CountPendingItems(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM decision_queue WHERE status='PENDING'`).Scan(&n)
	return n, err
}

// ---- 冲突 / 通知 / 审计查询 ----

// ConflictsOfVersion 返回版本冲突链。
func (s *Store) ConflictsOfVersion(ctx context.Context, versionID int64) ([]domain.Conflict, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,version_id,station_id,winner_task_id,loser_task_id,overlap_start,overlap_end,reason
		 FROM conflicts WHERE version_id=? ORDER BY id`, versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Conflict
	for rows.Next() {
		var c domain.Conflict
		var os, oe int64
		if err := rows.Scan(&c.ID, &c.VersionID, &c.StationID, &c.WinnerTaskID, &c.LoserTaskID, &os, &oe, &c.Reason); err != nil {
			return nil, err
		}
		c.OverlapStart, c.OverlapEnd = fromUnix(os), fromUnix(oe)
		out = append(out, c)
	}
	return out, rows.Err()
}

// NotificationsForItem 返回条目相关通知结果。
func (s *Store) NotificationsForItem(ctx context.Context, itemID string) ([]domain.Notification, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,item_id,channel,target,result,detail,created_at FROM notifications WHERE item_id=? ORDER BY id`, itemID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		var n domain.Notification
		var created int64
		if err := rows.Scan(&n.ID, &n.ItemID, &n.Channel, &n.Target, &n.Result, &n.Detail, &created); err != nil {
			return nil, err
		}
		n.CreatedAt = fromUnix(created)
		out = append(out, n)
	}
	return out, rows.Err()
}

// ListNotifications 返回最近通知（limit 上限）。
func (s *Store) ListNotifications(ctx context.Context, limit int) ([]domain.Notification, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,item_id,channel,target,result,detail,created_at FROM notifications ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		var n domain.Notification
		var created int64
		if err := rows.Scan(&n.ID, &n.ItemID, &n.Channel, &n.Target, &n.Result, &n.Detail, &created); err != nil {
			return nil, err
		}
		n.CreatedAt = fromUnix(created)
		out = append(out, n)
	}
	return out, rows.Err()
}

// ListAuditEvents 按实体过滤审计事件（空实体则返回最近 limit 条）。
func (s *Store) ListAuditEvents(ctx context.Context, entityType, entityID string, limit int) ([]domain.AuditEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	var rows *sql.Rows
	var err error
	if entityType != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id,ts,actor,role,action,entity_type,entity_id,detail FROM audit_events
			 WHERE entity_type=? AND entity_id=? ORDER BY id`, entityType, entityID)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id,ts,actor,role,action,entity_type,entity_id,detail FROM audit_events
			 ORDER BY id DESC LIMIT ?`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AuditEvent
	for rows.Next() {
		var a domain.AuditEvent
		var ts int64
		if err := rows.Scan(&a.ID, &ts, &a.Actor, &a.Role, &a.Action, &a.EntityType, &a.EntityID, &a.Detail); err != nil {
			return nil, err
		}
		a.Ts = fromUnix(ts)
		out = append(out, a)
	}
	return out, rows.Err()
}
