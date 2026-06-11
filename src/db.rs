//! Local SQLite database — primary store for raw observations.
//!
//! The hybrid writes here first. The ws_relay reads from here and ships
//! rows to mantis-archive (TimescaleDB) via the encrypted WS pipeline.
//! If the WS drops, the local DB keeps accumulating. Nothing is lost.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use crate::feed::FeedRow;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// SQLite schema. Matches the beacon-v1 schema exactly so existing tests
/// (venue upserts, book parsing) continue to work.
pub const PAPER_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS paper_orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       INTEGER NOT NULL,
    ticker          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    oracle          TEXT NOT NULL,
    side            TEXT NOT NULL,
    limit_price     REAL NOT NULL,
    size            REAL NOT NULL,
    strike          REAL NOT NULL,
    status          TEXT NOT NULL DEFAULT 'resting',
    placed_ts       REAL NOT NULL,
    fill_ts         REAL,
    fill_price      REAL,
    -- kernel outputs at placement
    p_true          REAL NOT NULL,
    p_market        REAL NOT NULL,
    gap             REAL NOT NULL,
    omega           REAL NOT NULL,
    d1              REAL NOT NULL,
    sigma_1s        REAL NOT NULL,
    t_secs          REAL NOT NULL,
    regime          TEXT NOT NULL,
    -- terrain features (physics, knob-free)
    net_edge        REAL NOT NULL DEFAULT 0.0,
    gate_d1         REAL NOT NULL DEFAULT 0.0,
    gate_trend      REAL NOT NULL DEFAULT 0.0,
    displacement    REAL NOT NULL DEFAULT 0.0,
    spread          REAL NOT NULL DEFAULT 0.0,
    spread_pct      REAL NOT NULL DEFAULT 0.0,
    fee_rate        REAL NOT NULL DEFAULT 0.0,
    -- perp context at placement (terrain, not kernel)
    hl_funding      REAL,
    hl_oi           REAL,
    hl_premium      REAL,
    hl_bid_depth    REAL,
    hl_ask_depth    REAL
);
CREATE INDEX IF NOT EXISTS idx_paper_orders_market  ON paper_orders(market_id);
CREATE INDEX IF NOT EXISTS idx_paper_orders_status  ON paper_orders(status);

CREATE TABLE IF NOT EXISTS paper_positions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id          INTEGER NOT NULL,
    ticker             TEXT NOT NULL,
    venue              TEXT NOT NULL,
    oracle             TEXT NOT NULL,
    side               TEXT NOT NULL,
    size               REAL NOT NULL,
    strike             REAL NOT NULL,
    entry_price        REAL NOT NULL,
    entry_ts           REAL NOT NULL,
    entry_gap          REAL NOT NULL,
    entry_fee          REAL NOT NULL,
    committed_capital  REAL NOT NULL,
    spread_at_fill     REAL NOT NULL DEFAULT 0.0,
    -- entry terrain
    entry_net_edge     REAL NOT NULL DEFAULT 0.0,
    entry_gate_d1      REAL NOT NULL DEFAULT 0.0,
    entry_displacement REAL NOT NULL DEFAULT 0.0,
    -- perp context at entry
    hl_funding         REAL,
    hl_oi              REAL,
    hl_premium         REAL,
    hl_bid_depth       REAL,
    hl_ask_depth       REAL,
    -- exit fields (populated on close)
    exit_price         REAL,
    exit_ts            REAL,
    exit_reason        TEXT,
    exit_fee           REAL,
    gross_pnl          REAL,
    net_pnl            REAL,
    exit_p_true        REAL,
    exit_p_market      REAL,
    exit_gap           REAL,
    exit_gate_d1       REAL,
    exit_gate_trend    REAL,
    exit_net_edge      REAL,
    spread_at_exit     REAL,
    peak_unrealized    REAL,
    hold_secs          REAL
);
CREATE INDEX IF NOT EXISTS idx_paper_pos_market ON paper_positions(market_id);
CREATE INDEX IF NOT EXISTS idx_paper_pos_open   ON paper_positions(exit_ts) WHERE exit_ts IS NULL;

CREATE TABLE IF NOT EXISTS paper_pnl (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              REAL NOT NULL,
    capital         REAL NOT NULL,
    realized_pnl    REAL NOT NULL,
    unrealized_pnl  REAL NOT NULL,
    total_value     REAL NOT NULL,
    peak_equity     REAL NOT NULL,
    open_positions  INTEGER NOT NULL,
    resting_orders  INTEGER NOT NULL,
    total_trades    INTEGER NOT NULL,
    wins            INTEGER NOT NULL,
    losses          INTEGER NOT NULL,
    drawdown_pct    REAL NOT NULL
);
"#;

pub const SCHEMA_SQL: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS feeds (
    id      INTEGER PRIMARY KEY,
    ts      REAL    NOT NULL,
    source  TEXT    NOT NULL,
    value   REAL    NOT NULL,
    meta    TEXT,
    ticker  TEXT
);
CREATE INDEX IF NOT EXISTS idx_feeds_ts_source ON feeds(ts, source);

CREATE TABLE IF NOT EXISTS markets (
    id               INTEGER PRIMARY KEY,
    venue            TEXT    NOT NULL,
    ticker           TEXT    NOT NULL,
    series           TEXT,
    market_type      TEXT,
    oracle           TEXT,
    strike           REAL,
    open_time        TEXT,
    close_time       TEXT,
    resolution_time  TEXT,
    outcome          TEXT,
    rules            TEXT,
    token_id         TEXT,
    discovered_at    REAL    NOT NULL,
    UNIQUE(venue, ticker)
);
CREATE INDEX IF NOT EXISTS idx_markets_venue_ser ON markets(venue, series);

CREATE TABLE IF NOT EXISTS book_snapshots (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    market_id  INTEGER NOT NULL,
    venue      TEXT NOT NULL,
    bid_depth  REAL NOT NULL,
    ask_depth  REAL NOT NULL,
    spread     REAL NOT NULL,
    best_bid   REAL,
    best_ask   REAL,
    levels     TEXT
);
CREATE INDEX IF NOT EXISTS idx_book_snaps_mkt_ts ON book_snapshots(market_id, ts DESC);

CREATE TABLE IF NOT EXISTS resolutions (
    id                     INTEGER PRIMARY KEY,
    market_id              INTEGER NOT NULL UNIQUE,
    close_time             REAL,
    resolution_time        REAL,
    resolution_lag_s       REAL,
    outcome                TEXT,
    oracle_value_at_close  REAL,
    binance_at_close       REAL,
    displacement_at_close  REAL,
    meta                   TEXT
);
"#;

// ---------------------------------------------------------------------------
// Open / init
// ---------------------------------------------------------------------------

/// Open (or create) the local SQLite database and apply the schema.
pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path).context("db::open")?;
    conn.execute_batch(SCHEMA_SQL).context("db::init_schema")?;
    // Forward-only migration: add ticker column to feeds.
    // On fresh DBs the column already exists from SCHEMA_SQL — ALTER TABLE
    // returns "duplicate column name", which we swallow with let _.
    // On existing DBs the column is absent and the ALTER succeeds.
    let _ = conn.execute_batch("ALTER TABLE feeds ADD COLUMN ticker TEXT;");
    // Create partial index after the column is guaranteed to exist.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_feeds_ticker_ts \
         ON feeds(ticker, ts) WHERE ticker IS NOT NULL;",
    )
    .context("db::ticker_index")?;
    Ok(conn)
}

// ---------------------------------------------------------------------------
// Feed inserts
// ---------------------------------------------------------------------------

/// Bulk-insert feed rows into the `feeds` table.
/// Returns the count of rows inserted.
pub fn insert_feeds(conn: &Connection, rows: &[FeedRow]) -> Result<usize> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO feeds (ts, source, value, meta, ticker) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .context("insert_feeds prepare")?;
    let mut count = 0usize;
    for row in rows {
        stmt.execute(params![row.ts, row.source, row.value, row.meta, row.ticker])?;
        count += 1;
    }
    Ok(count)
}

/// Bulk-insert L2 book snapshots. Fails-open per row (logs, does not abort).
pub fn insert_book_snapshots(conn: &Connection, rows: &[BookSnapshotRow]) -> Result<usize> {
    let mut stmt = conn
        .prepare_cached(
            "INSERT INTO book_snapshots \
             (ts, market_id, venue, bid_depth, ask_depth, spread, \
              best_bid, best_ask, levels) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        )
        .context("insert_book_snapshots prepare")?;
    let mut count = 0usize;
    for r in rows {
        match stmt.execute(params![
            r.ts,
            r.market_id,
            r.venue,
            r.bid_depth,
            r.ask_depth,
            r.spread,
            r.best_bid,
            r.best_ask,
            r.levels,
        ]) {
            Ok(_) => count += 1,
            Err(e) => eprintln!("[db] book_snapshot insert error: {e}"),
        }
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// Market queries
// ---------------------------------------------------------------------------

/// Load all active (unresolved) markets.
pub fn active_markets(conn: &Connection) -> Result<Vec<MarketRow>> {
    let mut stmt = conn.prepare_cached(
        "SELECT id, venue, ticker, series, market_type, oracle, strike, \
         open_time, close_time, token_id \
         FROM markets WHERE outcome IS NULL ORDER BY id",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(MarketRow {
            id: r.get(0)?,
            venue: r.get(1)?,
            ticker: r.get(2)?,
            series: r.get(3)?,
            market_type: r.get(4)?,
            oracle: r.get(5)?,
            strike: r.get(6)?,
            open_time: r.get(7)?,
            close_time: r.get(8)?,
            token_id: r.get(9)?,
            resolution_time: None,
            outcome: None,
            rules: None,
            discovered_at: None,
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Nearest Binance tick within 300s before `ts`.
pub fn binance_at_close(conn: &Connection, ts: f64) -> Result<Option<f64>> {
    use rusqlite::OptionalExtension;
    conn.query_row(
        "SELECT value FROM feeds
         WHERE source = 'binance' AND ts <= ?1 AND ts >= ?1 - 300
         ORDER BY ts DESC LIMIT 1",
        params![ts],
        |row| row.get(0),
    )
    .optional()
    .context("binance_at_close")
}

/// Markets past close_time with no outcome and no resolution record. Max 50.
pub fn resolution_candidates(conn: &Connection) -> Result<Vec<MarketRow>> {
    let mut stmt = conn.prepare(
        "SELECT m.id, m.venue, m.ticker, m.series, m.market_type, m.oracle,
                m.strike, m.open_time, m.close_time, m.token_id
         FROM markets m
         WHERE m.outcome IS NULL
           AND m.close_time IS NOT NULL
           AND datetime(m.close_time) < datetime('now')
           AND NOT EXISTS (SELECT 1 FROM resolutions r WHERE r.market_id = m.id)
         LIMIT 50",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(MarketRow {
            id: r.get(0)?,
            venue: r.get(1)?,
            ticker: r.get(2)?,
            series: r.get(3)?,
            market_type: r.get(4)?,
            oracle: r.get(5)?,
            strike: r.get(6)?,
            open_time: r.get(7)?,
            close_time: r.get(8)?,
            token_id: r.get(9)?,
            resolution_time: None,
            outcome: None,
            rules: None,
            discovered_at: None,
        })
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Insert a resolution record and update the market's outcome.
pub fn insert_resolution(conn: &Connection, r: &ResolutionRow) -> Result<bool> {
    let changed = conn.execute(
        "INSERT OR IGNORE INTO resolutions
            (market_id, close_time, resolution_time, resolution_lag_s,
             outcome, oracle_value_at_close, binance_at_close,
             displacement_at_close, meta)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![
            r.market_id,
            r.close_time,
            r.resolution_time,
            r.resolution_lag_s,
            r.outcome,
            r.oracle_value_at_close,
            r.binance_at_close,
            r.displacement_at_close,
            r.meta,
        ],
    )?;
    if changed > 0 {
        conn.execute(
            "UPDATE markets SET outcome=?1, resolution_time=?2 WHERE id=?3",
            params![
                r.outcome,
                r.resolution_time.map(|t| format!("{t:.3}")),
                r.market_id,
            ],
        )?;
    }
    Ok(changed > 0)
}

// ---------------------------------------------------------------------------
// Watermark — tracks what has been shipped to mantis-archive
// ---------------------------------------------------------------------------

/// Read un-shipped feed rows (id > watermark), up to `limit`.
pub fn unshipped_feeds(
    conn: &Connection,
    watermark: i64,
    limit: usize,
) -> Result<Vec<(i64, FeedRow)>> {
    let mut stmt = conn.prepare_cached(
        "SELECT id, ts, source, value, meta, ticker FROM feeds WHERE id > ?1 ORDER BY id LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![watermark, limit as i64], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            FeedRow {
                ts: r.get(1)?,
                source: Box::leak(r.get::<_, String>(2)?.into_boxed_str()),
                value: r.get(3)?,
                meta: r.get(4)?,
                ticker: r.get(5)?,
            },
        ))
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Read un-shipped book snapshots (id > watermark), up to `limit`.
///
/// JOINs against markets to resolve `market_id` → `ticker`. Rows with
/// no matching market get a fallback ticker of `"market-{market_id}"`.
/// Returns `(row_id, ticker, BookSnapshotRow)` tuples.
pub fn unshipped_books(
    conn: &Connection,
    watermark: i64,
    limit: usize,
) -> Result<Vec<(i64, String, BookSnapshotRow)>> {
    let mut stmt = conn.prepare_cached(
        "SELECT b.id, b.ts, b.market_id, b.venue, b.bid_depth, b.ask_depth, \
                b.spread, b.best_bid, b.best_ask, b.levels, \
                COALESCE(m.ticker, 'market-' || b.market_id) \
         FROM book_snapshots b \
         LEFT JOIN markets m ON m.id = b.market_id \
         WHERE b.id > ?1 ORDER BY b.id LIMIT ?2",
    )?;
    let rows = stmt.query_map(params![watermark, limit as i64], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(10)?, // ticker from COALESCE
            BookSnapshotRow {
                ts: r.get(1)?,
                market_id: r.get(2)?,
                venue: r.get(3)?,
                bid_depth: r.get(4)?,
                ask_depth: r.get(5)?,
                spread: r.get(6)?,
                best_bid: r.get(7)?,
                best_ask: r.get(8)?,
                levels: r.get(9)?,
            },
        ))
    })?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

// ---------------------------------------------------------------------------
// ISO timestamp parsing
// ---------------------------------------------------------------------------

/// Parse an ISO-8601 / RFC-3339 timestamp to Unix seconds.
/// Handles `Z`, `+00:00`, `+HH:MM`, `-HH:MM` suffixes.
pub fn parse_iso_to_unix(s: &str) -> Option<f64> {
    // Strip timezone suffix and extract offset
    let (datetime_part, offset_secs) = if let Some(stripped) = s.strip_suffix('Z') {
        (stripped, 0i64)
    } else if s.len() >= 6 {
        let sign_pos = s.len() - 6;
        let sign = s.as_bytes()[sign_pos];
        if sign == b'+' || sign == b'-' {
            let hh: i64 = s[sign_pos + 1..sign_pos + 3].parse().ok()?;
            let mm: i64 = s[sign_pos + 4..sign_pos + 6].parse().ok()?;
            let total = hh * 3600 + mm * 60;
            let off = if sign == b'+' { -total } else { total };
            (&s[..sign_pos], off)
        } else {
            (s, 0)
        }
    } else {
        (s, 0)
    };

    // Parse "YYYY-MM-DDTHH:MM:SS" or "YYYY-MM-DDTHH:MM:SS.frac"
    let (date_str, time_str) = datetime_part.split_once('T')?;
    let mut date_parts = date_str.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;

    let mut time_parts = time_str.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let min: i64 = time_parts.next()?.parse().ok()?;
    let sec_str = time_parts.next()?;
    let (sec, frac) = if let Some((s, f)) = sec_str.split_once('.') {
        let sec: i64 = s.parse().ok()?;
        let frac_str = &f[..f.len().min(6)];
        let frac: f64 = format!("0.{frac_str}").parse().ok()?;
        (sec, frac)
    } else {
        (sec_str.parse().ok()?, 0.0)
    };

    // Gregorian to days since epoch
    let m = if month <= 2 { month + 9 } else { month - 3 };
    let y = if month <= 2 { year - 1 } else { year };
    let days = 365 * y + y / 4 - y / 100 + y / 400 + (m * 306 + 5) / 10 + day - 1 - 719468;

    let unix = days * 86400 + hour * 3600 + min * 60 + sec + offset_secs;
    Some(unix as f64 + frac)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A prediction market discovered by a venue client.
#[derive(Debug, Clone)]
pub struct MarketRow {
    pub id: i64,
    pub venue: String,
    pub ticker: String,
    pub series: Option<String>,
    pub market_type: Option<String>,
    pub oracle: Option<String>,
    pub strike: Option<f64>,
    pub open_time: Option<String>,
    pub close_time: Option<String>,
    pub resolution_time: Option<String>,
    pub outcome: Option<String>,
    pub rules: Option<String>,
    pub token_id: Option<String>,
    pub discovered_at: Option<f64>,
}

/// A resolved market outcome.
#[derive(Debug, Clone)]
pub struct ResolutionRow {
    pub market_id: i64,
    pub close_time: Option<f64>,
    pub resolution_time: Option<f64>,
    pub resolution_lag_s: Option<f64>,
    pub outcome: Option<String>,
    pub oracle_value_at_close: Option<f64>,
    pub binance_at_close: Option<f64>,
    pub displacement_at_close: Option<f64>,
    pub meta: Option<String>,
}

/// L2 orderbook depth snapshot from a venue.
#[derive(Debug, Clone)]
pub struct BookSnapshotRow {
    pub ts: f64,
    pub market_id: i64,
    pub venue: String,
    pub bid_depth: f64,
    pub ask_depth: f64,
    pub spread: f64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub levels: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        conn
    }

    #[test]
    fn open_in_memory() {
        let conn = test_db();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM feeds", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_and_count_feeds() {
        let conn = test_db();
        let rows = vec![
            FeedRow {
                ts: 1710000000.0,
                source: "binance",
                value: 95123.45,
                meta: None,
                ticker: None,
            },
            FeedRow {
                ts: 1710000001.0,
                source: "brti",
                value: 95110.0,
                meta: Some(r#"{"lag":3.2}"#.to_string()),
                ticker: None,
            },
        ];
        let count = insert_feeds(&conn, &rows).unwrap();
        assert_eq!(count, 2);

        let db_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM feeds", [], |r| r.get(0))
            .unwrap();
        assert_eq!(db_count, 2);
    }

    #[test]
    fn insert_book_snapshot() {
        let conn = test_db();
        conn.execute(
            "INSERT INTO markets (venue, ticker, discovered_at) VALUES ('kalshi', 'TEST-1', 0.0)",
            [],
        )
        .unwrap();

        let rows = vec![BookSnapshotRow {
            ts: 1710000000.0,
            market_id: 1,
            venue: "kalshi".to_string(),
            bid_depth: 500.0,
            ask_depth: 600.0,
            spread: 0.02,
            best_bid: Some(0.48),
            best_ask: Some(0.50),
            levels: Some(r#"{"yes":[[0.48,500]],"no":[[0.50,600]]}"#.to_string()),
        }];
        let count = insert_book_snapshots(&conn, &rows).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn parse_iso_z() {
        let ts = parse_iso_to_unix("2024-03-10T08:00:00Z").unwrap();
        assert!((ts - 1710057600.0).abs() < 1.0);
    }

    #[test]
    fn parse_iso_offset() {
        let z = parse_iso_to_unix("2024-01-01T00:00:00Z").unwrap();
        let plus5 = parse_iso_to_unix("2024-01-01T05:00:00+05:00").unwrap();
        assert!((z - plus5).abs() < 0.01);
    }

    #[test]
    fn parse_iso_fractional() {
        let ts = parse_iso_to_unix("2024-01-01T00:00:00.500Z").unwrap();
        assert!((ts - 1704067200.5).abs() < 0.01);
    }

    #[test]
    fn unshipped_feeds_respects_watermark() {
        let conn = test_db();
        let rows = vec![
            FeedRow {
                ts: 1.0,
                source: "a",
                value: 1.0,
                meta: None,
                ticker: None,
            },
            FeedRow {
                ts: 2.0,
                source: "b",
                value: 2.0,
                meta: None,
                ticker: None,
            },
            FeedRow {
                ts: 3.0,
                source: "c",
                value: 3.0,
                meta: None,
                ticker: None,
            },
        ];
        insert_feeds(&conn, &rows).unwrap();

        // Watermark 0 → all 3
        let batch = unshipped_feeds(&conn, 0, 100).unwrap();
        assert_eq!(batch.len(), 3);

        // Watermark at last id → 0
        let max_id = batch.last().unwrap().0;
        let batch2 = unshipped_feeds(&conn, max_id, 100).unwrap();
        assert!(batch2.is_empty());

        // Watermark at first id → 2 remaining
        let first_id = batch[0].0;
        let batch3 = unshipped_feeds(&conn, first_id, 100).unwrap();
        assert_eq!(batch3.len(), 2);
    }

    #[test]
    fn resolution_insert_and_update_market() {
        let conn = test_db();
        conn.execute(
            "INSERT INTO markets (venue, ticker, discovered_at, close_time) \
             VALUES ('kalshi', 'TEST-1', 0.0, '2024-01-01T00:00:00Z')",
            [],
        )
        .unwrap();

        let row = ResolutionRow {
            market_id: 1,
            close_time: Some(1704067200.0),
            resolution_time: Some(1704067260.0),
            resolution_lag_s: Some(60.0),
            outcome: Some("Yes".to_string()),
            oracle_value_at_close: Some(95000.0),
            binance_at_close: Some(95010.0),
            displacement_at_close: Some(10.0),
            meta: None,
        };
        assert!(insert_resolution(&conn, &row).unwrap());

        // Market outcome should be updated
        let outcome: Option<String> = conn
            .query_row("SELECT outcome FROM markets WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(outcome, Some("Yes".to_string()));

        // Duplicate insert should be ignored
        assert!(!insert_resolution(&conn, &row).unwrap());
    }

    #[test]
    fn active_markets_excludes_resolved() {
        let conn = test_db();
        conn.execute(
            "INSERT INTO markets (venue, ticker, discovered_at) VALUES ('kalshi', 'OPEN-1', 0.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO markets (venue, ticker, discovered_at, outcome) VALUES ('kalshi', 'DONE-1', 0.0, 'Yes')",
            [],
        )
        .unwrap();

        let markets = active_markets(&conn).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].ticker, "OPEN-1");
    }
}
