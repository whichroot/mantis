//! Cold-path L2 book depth — side channel from venue WS feeds to SQLite.
//!
//! The ring holds BBO scalars (hot path, 160 bytes, kernel reads this).
//! Full L2 book depth flows through a bounded mpsc channel to the book
//! flusher, which bulk-inserts into the `book_snapshots` table. The relay
//! ships those rows to mantis-archive.
//!
//! ```text
//! Venue WS feed receives full book
//!   ↓ extract BBO → ring (hot path, kernel reads this)
//!   ↓ full book JSON → bounded mpsc channel (cold path)
//!                         ↓
//!                     book_flusher → book_snapshots table → relay ships it
//! ```
//!
//! One String allocation per book event (the full levels JSON). The rest
//! is scalars. If the channel fills (burst, flusher slow), new books drop
//! via `try_send`. Acceptable — the ring still has BBO, the kernel still
//! evaluates, the next book event replaces what was lost.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::db;
use crate::feed::LiveState;

// ---------------------------------------------------------------------------
// BookEvent — the cold-path payload
// ---------------------------------------------------------------------------

/// A full L2 orderbook snapshot from a venue WS feed.
///
/// Produced by venue WS handlers (kalshi_ws, polymarket_ws) alongside
/// the BBO ring write. Consumed by [`book_flusher`].
#[derive(Debug, Clone)]
pub struct BookEvent {
    pub ts: f64,
    pub market_id: i64,
    pub ticker: String,
    pub venue: &'static str,
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread: f64,
    pub bid_depth: f64,
    pub ask_depth: f64,
    /// Full L2 levels as JSON string. One heap allocation.
    pub levels_json: String,
}

/// Channel capacity for the book side channel.
/// At ~10 books/sec across both venues, 256 entries ≈ 25s of buffer.
pub const BOOK_CHANNEL_CAP: usize = 256;

// ---------------------------------------------------------------------------
// Book flusher — drains channel to book_snapshots table
// ---------------------------------------------------------------------------

/// Drains `BookEvent`s from the bounded channel and bulk-inserts into
/// the `book_snapshots` table every 5 seconds.
///
/// Separate task from the feed flusher. Two flushers, two data paths,
/// two tables. They do not know about each other.
pub async fn book_flusher(
    db_path: String,
    mut rx: mpsc::Receiver<BookEvent>,
    state: Arc<LiveState>,
    stop: CancellationToken,
) {
    let conn = match db::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[book_flusher] DB open error: {e}");
            return;
        }
    };

    loop {
        let timed_out = tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(5)) => true,
            () = stop.cancelled() => false,
        };

        // Drain all pending book events
        let mut batch: Vec<BookEvent> = Vec::new();
        while let Ok(evt) = rx.try_recv() {
            batch.push(evt);
        }

        if !batch.is_empty() {
            let rows: Vec<db::BookSnapshotRow> = batch
                .iter()
                .map(|e| db::BookSnapshotRow {
                    ts: e.ts,
                    market_id: e.market_id,
                    venue: e.venue.to_string(),
                    bid_depth: e.bid_depth,
                    ask_depth: e.ask_depth,
                    spread: e.spread,
                    best_bid: if e.best_bid > 0.0 { Some(e.best_bid) } else { None },
                    best_ask: if e.best_ask > 0.0 { Some(e.best_ask) } else { None },
                    levels: Some(e.levels_json.clone()),
                })
                .collect();

            let n = rows.len();
            match db::insert_book_snapshots(&conn, &rows) {
                Ok(inserted) => {
                    if inserted > 0 {
                        state
                            .book_inserts
                            .fetch_add(inserted as u64, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    eprintln!("[book_flusher] insert error ({n} rows): {e}");
                    state.inc_errors();
                }
            }
        }

        if !timed_out {
            // Final drain on shutdown
            let mut final_batch: Vec<BookEvent> = Vec::new();
            while let Ok(evt) = rx.try_recv() {
                final_batch.push(evt);
            }
            if !final_batch.is_empty() {
                let rows: Vec<db::BookSnapshotRow> = final_batch
                    .iter()
                    .map(|e| db::BookSnapshotRow {
                        ts: e.ts,
                        market_id: e.market_id,
                        venue: e.venue.to_string(),
                        bid_depth: e.bid_depth,
                        ask_depth: e.ask_depth,
                        spread: e.spread,
                        best_bid: if e.best_bid > 0.0 { Some(e.best_bid) } else { None },
                        best_ask: if e.best_ask > 0.0 { Some(e.best_ask) } else { None },
                        levels: Some(e.levels_json.clone()),
                    })
                    .collect();
                let _ = db::insert_book_snapshots(&conn, &rows);
            }
            break;
        }
    }

    eprintln!("[book_flusher] shutdown complete");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn book_event_struct_fields() {
        let evt = BookEvent {
            ts: 1_700_000_000.0,
            market_id: 42,
            ticker: "KXBTCD-T70000".to_string(),
            venue: "kalshi",
            best_bid: 0.48,
            best_ask: 0.52,
            spread: 0.04,
            bid_depth: 500.0,
            ask_depth: 600.0,
            levels_json: r#"{"yes":[[0.48,500]],"no":[[0.52,600]]}"#.to_string(),
        };
        assert_eq!(evt.venue, "kalshi");
        assert_eq!(evt.ticker, "KXBTCD-T70000");
        assert!((evt.spread - 0.04).abs() < 1e-10);
    }

    #[test]
    fn book_event_to_snapshot_row() {
        let evt = BookEvent {
            ts: 1_700_000_000.0,
            market_id: 1,
            ticker: "TEST".to_string(),
            venue: "polymarket",
            best_bid: 0.45,
            best_ask: 0.55,
            spread: 0.10,
            bid_depth: 1000.0,
            ask_depth: 800.0,
            levels_json: "{}".to_string(),
        };
        let row = db::BookSnapshotRow {
            ts: evt.ts,
            market_id: evt.market_id,
            venue: evt.venue.to_string(),
            bid_depth: evt.bid_depth,
            ask_depth: evt.ask_depth,
            spread: evt.spread,
            best_bid: Some(evt.best_bid),
            best_ask: Some(evt.best_ask),
            levels: Some(evt.levels_json.clone()),
        };
        assert_eq!(row.venue, "polymarket");
        assert_eq!(row.best_bid, Some(0.45));
        assert_eq!(row.levels, Some("{}".to_string()));
    }

    #[test]
    fn zero_bid_ask_maps_to_none() {
        let evt = BookEvent {
            ts: 1.0,
            market_id: 1,
            ticker: "X".to_string(),
            venue: "kalshi",
            best_bid: 0.0,
            best_ask: 0.0,
            spread: 0.0,
            bid_depth: 0.0,
            ask_depth: 0.0,
            levels_json: "[]".to_string(),
        };
        let best_bid = if evt.best_bid > 0.0 { Some(evt.best_bid) } else { None };
        let best_ask = if evt.best_ask > 0.0 { Some(evt.best_ask) } else { None };
        assert_eq!(best_bid, None);
        assert_eq!(best_ask, None);
    }

    #[tokio::test]
    async fn channel_try_send_drops_on_full() {
        let (tx, _rx) = mpsc::channel::<BookEvent>(2);
        let evt = BookEvent {
            ts: 1.0, market_id: 1, ticker: "X".to_string(), venue: "kalshi",
            best_bid: 0.5, best_ask: 0.5, spread: 0.0, bid_depth: 0.0,
            ask_depth: 0.0, levels_json: "{}".to_string(),
        };
        // Fill the channel
        assert!(tx.try_send(evt.clone()).is_ok());
        assert!(tx.try_send(evt.clone()).is_ok());
        // Third should fail (channel full, cap=2)
        assert!(tx.try_send(evt).is_err());
    }
}
