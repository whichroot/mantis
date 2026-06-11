//! PolymarketWsFeed — WebSocket-first CLOB feed for Polymarket BTC markets.
//!
//! Streams real-time orderbook data from the Polymarket CLOB WebSocket API.
//! Replaces REST polling of the CLOB book endpoint.
//!
//! Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
//! No authentication required.
//!
//! Subscribe message (first text frame after connect):
//! ```json
//! {"assets_ids":["<id1>","<id2>"],"type":"market","custom_feature_enabled":true}
//! ```
//!
//! Message types handled:
//! - `book`              — full L2 snapshot → FeedRow source="poly_book"
//! - `price_change`      — incremental L1 updates → FeedRow source="poly_price"
//! - `best_bid_ask`      — BBO update → FeedRow source="poly_bbo"
//! - `last_trade_price`  — trade tick → FeedRow source="poly_trade"
//! - `market_resolved`   — resolution event → FeedRow source="poly_resolved"

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use futures_util::{SinkExt, StreamExt};
use rusqlite;

use super::{Backoff, Feed, FeedRow, LiveState, finite, wall_clock};

const URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

// ---------------------------------------------------------------------------
// Feed struct
// ---------------------------------------------------------------------------

/// WebSocket feed for Polymarket CLOB real-time data.
///
/// Accepts token_ids from the caller (main.rs queries the DB and passes them in).
/// Each token_id is a Yes-outcome CLOB token for one Polymarket binary market.
///
/// Every 300s the feed re-reads active token_ids from SQLite. If the set has
/// changed (new 5m/15m markets discovered), it drops the connection and
/// reconnects with the full updated subscription.
pub struct PolymarketWsFeed {
    token_ids: Vec<String>,
    db_path: String,
    /// Cold-path channel for full L2 book depth. None = books not persisted.
    book_tx: Option<tokio::sync::mpsc::Sender<crate::book::BookEvent>>,
}

impl PolymarketWsFeed {
    /// Construct with a list of CLOB token_ids to subscribe to.
    /// If empty, the feed will log a warning and idle rather than crash.
    pub fn new(token_ids: Vec<String>, db_path: String) -> Self {
        Self { token_ids, db_path, book_tx: None }
    }

    /// Set the book side channel for full L2 depth persistence.
    pub fn with_book_tx(mut self, tx: tokio::sync::mpsc::Sender<crate::book::BookEvent>) -> Self {
        self.book_tx = Some(tx);
        self
    }
}

impl Default for PolymarketWsFeed {
    fn default() -> Self {
        Self::new(Vec::new(), String::new())
    }
}

// ---------------------------------------------------------------------------
// Feed impl
// ---------------------------------------------------------------------------

impl Feed for PolymarketWsFeed {
    fn name(&self) -> &'static str {
        "polymarket_ws"
    }

    async fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) {
        // Mutable locals so we can update the subscription set on refresh.
        let mut token_ids = self.token_ids;
        let db_path = self.db_path;
        let book_tx = self.book_tx;

        if token_ids.is_empty() {
            eprintln!("[polymarket_ws] no token_ids — feed idle");
            stop.cancelled().await;
            return;
        }

        let mut sub_msg = build_subscribe_msg(&token_ids);
        let mut backoff = Backoff::new(2.0, 30.0);
        // Refresh interval: re-read token_ids from DB every 300s.
        let mut refresh = tokio::time::interval(std::time::Duration::from_secs(300));
        refresh.tick().await; // consume the immediate first tick

        loop {
            if stop.is_cancelled() {
                eprintln!("[polymarket_ws] shutting down");
                break;
            }

            let ws = match tokio_tungstenite::connect_async(URL).await {
                Ok((stream, _)) => {
                    backoff.reset();
                    eprintln!("[polymarket_ws] connected ({} token_ids)", token_ids.len());
                    stream
                }
                Err(e) => {
                    eprintln!("[polymarket_ws] connect error: {e}");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // Send subscription as first text frame
            let sub = tokio_tungstenite::tungstenite::Message::Text(sub_msg.clone().into());
            if let Err(e) = write.send(sub).await {
                eprintln!("[polymarket_ws] subscribe error: {e}");
                state.inc_errors();
                backoff.wait(&stop).await;
                continue;
            }

            loop {
                let msg = tokio::select! {
                    m = tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        read.next()
                    ) => m,
                    _ = refresh.tick() => {
                        // Re-read active token_ids from DB. If the set has grown
                        // (new 5m/15m markets), break to reconnect with full set.
                        let new_ids = load_poly_token_ids(&db_path);
                        if new_ids != token_ids {
                            let added = new_ids.len().saturating_sub(token_ids.len());
                            eprintln!(
                                "[polymarket_ws] token_id refresh: {} → {} (+{added} new), reconnecting",
                                token_ids.len(), new_ids.len()
                            );
                            token_ids = new_ids;
                            sub_msg = build_subscribe_msg(&token_ids);
                            break; // drop connection, reconnect with updated sub
                        }
                        continue;
                    },
                    () = stop.cancelled() => break,
                };

                let msg = match msg {
                    Err(_) => continue, // recv timeout — silent continue
                    Ok(None) => break,  // stream ended
                    Ok(Some(Err(e))) => {
                        eprintln!("[polymarket_ws] ws error: {e}");
                        state.inc_errors();
                        break;
                    }
                    Ok(Some(Ok(m))) => m,
                };

                let text = match msg {
                    tokio_tungstenite::tungstenite::Message::Text(t) => t,
                    tokio_tungstenite::tungstenite::Message::Ping(_)
                    | tokio_tungstenite::tungstenite::Message::Pong(_) => continue,
                    tokio_tungstenite::tungstenite::Message::Close(_) => break,
                    _ => continue,
                };

                // The API sends arrays of events — wrap single objects too
                let events: Vec<serde_json::Value> = match serde_json::from_str::<serde_json::Value>(&text) {
                    Ok(serde_json::Value::Array(arr)) => arr,
                    Ok(obj @ serde_json::Value::Object(_)) => vec![obj],
                    _ => continue,
                };

                for event in &events {
                    let event_type = match event.get("event_type").and_then(|v| v.as_str()) {
                        Some(t) => t,
                        None => continue,
                    };

                    match event_type {
                        "book" => {
                            write_book(event, &rings, &book_tx);
                        }
                        "price_change" => {
                            write_price_change(event, &rings);
                        }
                        "best_bid_ask" => {
                            write_best_bid_ask(event, &rings);
                        }
                        "last_trade_price" => {
                            write_last_trade_price(event, &rings);
                        }
                        "market_resolved" => {
                            write_market_resolved(event, &rings);
                        }
                        _ => {} // unknown event_type, skip
                    }
                }
            }

            if !stop.is_cancelled() {
                eprintln!(
                    "[polymarket_ws] disconnected, reconnecting in {}s",
                    backoff.current as u64
                );
                backoff.wait(&stop).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DB reader for token_id refresh
// ---------------------------------------------------------------------------

/// Read active Polymarket token_ids from SQLite (for the 300s refresh).
/// Returns a sorted Vec so equality comparison is stable.
fn load_poly_token_ids(db_path: &str) -> Vec<String> {
    let conn = match rusqlite::Connection::open(db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[polymarket_ws] token_id refresh DB error: {e}");
            return Vec::new();
        }
    };
    let mut stmt = match conn.prepare(
        "SELECT token_id FROM markets \
         WHERE venue='polymarket' AND outcome IS NULL AND token_id IS NOT NULL \
         ORDER BY token_id",
    ) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[polymarket_ws] token_id refresh query error: {e}");
            return Vec::new();
        }
    };
    match stmt.query_map([], |r| r.get::<_, String>(0)) {
        Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
        Err(e) => {
            eprintln!("[polymarket_ws] token_id refresh query_map error: {e}");
            Vec::new()
        }
    }
}

// ---------------------------------------------------------------------------
// Subscribe message builder
// ---------------------------------------------------------------------------

/// Build the JSON subscription message for the given token_ids.
///
/// Format:
/// ```json
/// {"assets_ids":["<id1>","<id2>"],"type":"market","custom_feature_enabled":true}
/// ```
pub fn build_subscribe_msg(token_ids: &[String]) -> String {
    let msg = serde_json::json!({
        "assets_ids": token_ids,
        "type": "market",
        "custom_feature_enabled": true,
    });
    msg.to_string()
}

// ---------------------------------------------------------------------------
// Event handlers — pure functions, no I/O, testable in isolation
// ---------------------------------------------------------------------------

/// Parse a `book` event into a FeedRow.
///
/// Event shape:
/// ```json
/// {"event_type":"book","asset_id":"...","market":"...","bids":[{"price":"0.45","size":"100"}],
///  "asks":[{"price":"0.55","size":"80"}],"timestamp":"1710000000000"}
/// ```
///
/// Value: midpoint of best_bid and best_ask. 0.0 if book is empty.
/// Meta: full bids/asks arrays, asset_id, n_bids, n_asks, spread, best_bid, best_ask.
pub fn handle_book(event: &serde_json::Value) -> Option<FeedRow> {
    let asset_id = event.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");

    let bids = parse_price_levels(event.get("bids"));
    let asks = parse_price_levels(event.get("asks"));

    // Bids descending: best = first (highest price).
    // Asks ascending: best = first (lowest price).
    let best_bid = bids.first().map(|&(p, _)| p);
    let best_ask = asks.first().map(|&(p, _)| p);

    let mid = match (best_bid, best_ask) {
        (Some(b), Some(a)) if b > 0.0 && a > 0.0 => (b + a) / 2.0,
        _ => 0.0,
    };

    let spread = match (best_bid, best_ask) {
        (Some(b), Some(a)) => a - b,
        _ => 0.0,
    };

    let n_bids = bids.len();
    let n_asks = asks.len();

    // Serialize bids/asks as [[price, size], ...] arrays
    let bids_json: Vec<[f64; 2]> = bids.iter().map(|&(p, q)| [p, q]).collect();
    let asks_json: Vec<[f64; 2]> = asks.iter().map(|&(p, q)| [p, q]).collect();

    let ts = wall_clock();

    let meta = serde_json::json!({
        "asset_id": asset_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "n_bids": n_bids,
        "n_asks": n_asks,
        "bids": bids_json,
        "asks": asks_json,
    });

    Some(FeedRow {
        ts,
        source: "poly_book",
        value: mid,
        meta: Some(meta.to_string()),
        ticker: None,
    })
}

/// Parse a `price_change` event into one FeedRow per price_change entry.
///
/// Event shape:
/// ```json
/// {"event_type":"price_change","market":"...","price_changes":[
///   {"asset_id":"...","price":"0.52","size":"50","side":"BUY","best_bid":"0.51","best_ask":"0.53"}
/// ],"timestamp":"1710000000000"}
/// ```
///
/// Value: best_bid as f64 (0.0 if absent/invalid).
/// Meta: asset_id, price, size, side, best_bid, best_ask.
pub fn handle_price_change(event: &serde_json::Value) -> Vec<FeedRow> {
    let changes = match event.get("price_changes").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return Vec::new(),
    };

    let ts = wall_clock();
    let mut rows = Vec::with_capacity(changes.len());

    for change in changes {
        let asset_id = change.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
        let price_str = change.get("price").and_then(|v| v.as_str()).unwrap_or("0");
        let size_str = change.get("size").and_then(|v| v.as_str()).unwrap_or("0");
        let side_str = change.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let best_bid_str = change.get("best_bid").and_then(|v| v.as_str()).unwrap_or("0");
        let best_ask_str = change.get("best_ask").and_then(|v| v.as_str()).unwrap_or("0");

        // value = best_bid as f64; NaN firewall via finite()
        let value = finite(&serde_json::Value::String(best_bid_str.to_owned())).unwrap_or(0.0);

        let meta = serde_json::json!({
            "asset_id": asset_id,
            "price": price_str,
            "size": size_str,
            "side": side_str,
            "best_bid": best_bid_str,
            "best_ask": best_ask_str,
        });

        rows.push(FeedRow {
            ts,
            source: "poly_price",
            value,
            meta: Some(meta.to_string()),
            ticker: None,
        });
    }

    rows
}

/// Parse a `best_bid_ask` event into a FeedRow.
///
/// Event shape:
/// ```json
/// {"event_type":"best_bid_ask","asset_id":"...","best_bid":"0.51","best_ask":"0.53",
///  "spread":"0.02","timestamp":"1710000000000"}
/// ```
///
/// Value: midpoint of best_bid and best_ask.
/// Meta: asset_id, best_bid, best_ask, spread.
pub fn handle_best_bid_ask(event: &serde_json::Value) -> Option<FeedRow> {
    let asset_id = event.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");

    let best_bid = finite(event.get("best_bid").unwrap_or(&serde_json::Value::Null));
    let best_ask = finite(event.get("best_ask").unwrap_or(&serde_json::Value::Null));
    let spread_raw = finite(event.get("spread").unwrap_or(&serde_json::Value::Null));

    let mid = match (best_bid, best_ask) {
        (Some(b), Some(a)) if b > 0.0 && a > 0.0 => (b + a) / 2.0,
        _ => 0.0,
    };

    let ts = wall_clock();

    let meta = serde_json::json!({
        "asset_id": asset_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread_raw,
    });

    Some(FeedRow {
        ts,
        source: "poly_bbo",
        value: mid,
        meta: Some(meta.to_string()),
        ticker: None,
    })
}

/// Parse a `last_trade_price` event into a FeedRow.
///
/// Event shape:
/// ```json
/// {"event_type":"last_trade_price","asset_id":"...","price":"0.52","size":"30",
///  "side":"BUY","timestamp":"1710000000000"}
/// ```
///
/// Value: price as f64.
/// Meta: asset_id, size, side.
pub fn handle_last_trade_price(event: &serde_json::Value) -> Option<FeedRow> {
    let asset_id = event.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");

    let price = finite(event.get("price").unwrap_or(&serde_json::Value::Null))?;
    let size_str = event.get("size").and_then(|v| v.as_str()).unwrap_or("0");
    let side_str = event.get("side").and_then(|v| v.as_str()).unwrap_or("");

    let ts = wall_clock();

    let meta = serde_json::json!({
        "asset_id": asset_id,
        "size": size_str,
        "side": side_str,
    });

    Some(FeedRow {
        ts,
        source: "poly_trade",
        value: price,
        meta: Some(meta.to_string()),
        ticker: None,
    })
}

/// Parse a `market_resolved` event into a FeedRow.
///
/// Event shape:
/// ```json
/// {"event_type":"market_resolved","winning_asset_id":"...","winning_outcome":"Yes",...}
/// ```
///
/// Value: 1.0 if winning_outcome == "Yes", 0.0 if "No".
/// Meta: all resolution fields.
pub fn handle_market_resolved(event: &serde_json::Value) -> Option<FeedRow> {
    let winning_outcome = event
        .get("winning_outcome")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let value = match winning_outcome {
        "Yes" => 1.0,
        "No" => 0.0,
        // Unknown outcome — still emit the row with 0.0 so it's logged
        _ => 0.0,
    };

    let winning_asset_id = event
        .get("winning_asset_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let ts = wall_clock();

    let meta = serde_json::json!({
        "winning_asset_id": winning_asset_id,
        "winning_outcome": winning_outcome,
    });

    Some(FeedRow {
        ts,
        source: "poly_resolved",
        value,
        meta: Some(meta.to_string()),
        ticker: None,
    })
}

// ---------------------------------------------------------------------------
// Ring-writing dispatch helpers (called from run loop)
// ---------------------------------------------------------------------------

fn write_book(
    event: &serde_json::Value,
    rings: &crate::ring::RingSet,
    book_tx: &Option<tokio::sync::mpsc::Sender<crate::book::BookEvent>>,
) {
    if let Some(row) = handle_book(event) {
        let asset_id = event.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");

        // BBO to ring (hot path)
        let bbo_meta = serde_json::json!({
            "asset_id": asset_id,
            "best_bid": finite(&event["bids"].as_array().and_then(|a| a.first()).map(|v| v["price"].clone()).unwrap_or_default()),
            "best_ask": finite(&event["asks"].as_array().and_then(|a| a.first()).map(|v| v["price"].clone()).unwrap_or_default()),
        });
        let bbo_s = bbo_meta.to_string();
        rings.poly_book.write(row.ts, row.value, bbo_s.as_bytes(), Some(asset_id));

        // Full book to cold-path channel (if wired)
        if let Some(tx) = book_tx {
            let meta_s = row.meta.unwrap_or_default();
            // Parse back to extract bid/ask depth from the full meta
            if let Ok(meta_v) = serde_json::from_str::<serde_json::Value>(&meta_s) {
                let best_bid = meta_v["best_bid"].as_f64().unwrap_or(0.0);
                let best_ask = meta_v["best_ask"].as_f64().unwrap_or(0.0);
                let spread = meta_v["spread"].as_f64().unwrap_or(0.0);
                let bid_depth: f64 = meta_v["bids"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_array().and_then(|arr| arr.get(1)).and_then(|v| v.as_f64())).sum())
                    .unwrap_or(0.0);
                let ask_depth: f64 = meta_v["asks"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_array().and_then(|arr| arr.get(1)).and_then(|v| v.as_f64())).sum())
                    .unwrap_or(0.0);

                let _ = tx.try_send(crate::book::BookEvent {
                    ts: row.ts,
                    market_id: 0,
                    ticker: asset_id.to_string(),
                    venue: "polymarket",
                    best_bid,
                    best_ask,
                    spread,
                    bid_depth,
                    ask_depth,
                    levels_json: meta_s,
                });
            }
        }
    }
}

fn write_price_change(event: &serde_json::Value, rings: &crate::ring::RingSet) {
    for row in handle_price_change(event) {
        // asset_id is embedded in meta; extract from event's price_changes array isn't trivial.
        // The row's meta contains "asset_id" — use the source from handle_price_change which
        // reads asset_id from each change entry. We re-read asset_id from meta JSON.
        let asset_id = if let Some(meta_str) = &row.meta {
            serde_json::from_str::<serde_json::Value>(meta_str)
                .ok()
                .and_then(|v| v["asset_id"].as_str().map(|s| s.to_owned()))
        } else {
            None
        };
        let meta_s = row.meta.unwrap_or_default();
        rings.poly_price.write(row.ts, row.value, meta_s.as_bytes(), asset_id.as_deref());
    }
}

fn write_best_bid_ask(event: &serde_json::Value, rings: &crate::ring::RingSet) {
    if let Some(row) = handle_best_bid_ask(event) {
        let asset_id = event.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
        // Compact BBO meta for ring hot-path. Asset ID is already in the ticker
        // index; spread is derivable. Full meta (asset_id + spread + both prices)
        // can exceed META_CAP (128 bytes) for long Polymarket token IDs, producing
        // truncated JSON that parse_f64_from_meta cannot read.
        let best_bid = finite(event.get("best_bid").unwrap_or(&serde_json::Value::Null));
        let best_ask = finite(event.get("best_ask").unwrap_or(&serde_json::Value::Null));
        let compact = serde_json::json!({ "best_bid": best_bid, "best_ask": best_ask });
        rings.poly_bbo.write(row.ts, row.value, compact.to_string().as_bytes(), Some(asset_id));
    }
}

fn write_last_trade_price(event: &serde_json::Value, rings: &crate::ring::RingSet) {
    if let Some(row) = handle_last_trade_price(event) {
        let asset_id = event.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
        let meta_s = row.meta.unwrap_or_default();
        rings.poly_trade.write(row.ts, row.value, meta_s.as_bytes(), Some(asset_id));
    }
}

fn write_market_resolved(event: &serde_json::Value, rings: &crate::ring::RingSet) {
    if let Some(row) = handle_market_resolved(event) {
        let winning_asset_id = event.get("winning_asset_id").and_then(|v| v.as_str()).unwrap_or("");
        let meta_s = row.meta.unwrap_or_default();
        rings.poly_resolved.write(row.ts, row.value, meta_s.as_bytes(), Some(winning_asset_id));
    }
}

// ---------------------------------------------------------------------------
// Shared level parser
// ---------------------------------------------------------------------------

/// Parse a Polymarket price-level array: `[{"price":"0.45","size":"100"}, ...]` → `Vec<(f64, f64)>`.
///
/// Invalid entries (non-finite, zero, unparseable) are silently skipped.
fn parse_price_levels(arr: Option<&serde_json::Value>) -> Vec<(f64, f64)> {
    let arr = match arr.and_then(|v| v.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };

    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let price: f64 = match item
            .get("price")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
            Some(v) if v.is_finite() && v > 0.0 => v,
            _ => continue,
        };
        let qty: f64 = match item
            .get("size")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
            Some(v) if v.is_finite() && v >= 0.0 => v,
            _ => continue,
        };
        levels.push((price, qty));
    }
    levels
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── subscribe message ─────────────────────────────────────────────────────

    #[test]
    fn subscribe_msg_format_single() {
        let ids = vec!["token_abc123".to_owned()];
        let msg = build_subscribe_msg(&ids);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["type"], "market");
        assert_eq!(v["custom_feature_enabled"], true);
        let assets = v["assets_ids"].as_array().unwrap();
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0], "token_abc123");
    }

    #[test]
    fn subscribe_msg_format_multiple() {
        let ids = vec!["id_1".to_owned(), "id_2".to_owned(), "id_3".to_owned()];
        let msg = build_subscribe_msg(&ids);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let assets = v["assets_ids"].as_array().unwrap();
        assert_eq!(assets.len(), 3);
        assert_eq!(assets[0], "id_1");
        assert_eq!(assets[1], "id_2");
        assert_eq!(assets[2], "id_3");
    }

    #[test]
    fn subscribe_msg_format_empty() {
        let ids: Vec<String> = vec![];
        let msg = build_subscribe_msg(&ids);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let assets = v["assets_ids"].as_array().unwrap();
        assert!(assets.is_empty());
    }

    // ── book event ────────────────────────────────────────────────────────────

    #[test]
    fn book_event_midpoint() {
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "token_abc",
            "market": "0xdeadbeef",
            "bids": [
                {"price": "0.48", "size": "200"},
                {"price": "0.45", "size": "500"},
            ],
            "asks": [
                {"price": "0.52", "size": "150"},
                {"price": "0.55", "size": "300"},
            ],
            "timestamp": "1710000000000"
        });

        let row = handle_book(&event).unwrap();
        assert_eq!(row.source, "poly_book");
        // midpoint = (0.48 + 0.52) / 2 = 0.50
        assert!((row.value - 0.50).abs() < 1e-9);
        assert!(row.ts > 0.0);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["asset_id"], "token_abc");
        assert!((meta["best_bid"].as_f64().unwrap() - 0.48).abs() < 1e-9);
        assert!((meta["best_ask"].as_f64().unwrap() - 0.52).abs() < 1e-9);
        assert!((meta["spread"].as_f64().unwrap() - 0.04).abs() < 1e-9);
        assert_eq!(meta["n_bids"].as_u64().unwrap(), 2);
        assert_eq!(meta["n_asks"].as_u64().unwrap(), 2);
    }

    #[test]
    fn book_event_single_level_each_side() {
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "tok1",
            "bids": [{"price": "0.60", "size": "100"}],
            "asks": [{"price": "0.62", "size": "80"}],
        });

        let row = handle_book(&event).unwrap();
        // mid = (0.60 + 0.62) / 2 = 0.61
        assert!((row.value - 0.61).abs() < 1e-9);
    }

    #[test]
    fn book_event_empty_bids_and_asks_value_zero() {
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "tok_empty",
            "bids": [],
            "asks": [],
        });

        let row = handle_book(&event).unwrap();
        assert_eq!(row.value, 0.0);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["n_bids"].as_u64().unwrap(), 0);
        assert_eq!(meta["n_asks"].as_u64().unwrap(), 0);
    }

    #[test]
    fn book_event_missing_asks_value_zero() {
        // Only bids — can't compute midpoint
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "tok2",
            "bids": [{"price": "0.50", "size": "100"}],
        });

        let row = handle_book(&event).unwrap();
        assert_eq!(row.value, 0.0);
    }

    #[test]
    fn book_event_invalid_price_levels_skipped() {
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "tok3",
            "bids": [
                {"price": "nan", "size": "100"},
                {"price": "0", "size": "50"},
                {"price": "0.45", "size": "200"},
            ],
            "asks": [
                {"price": "inf", "size": "100"},
                {"price": "0.55", "size": "150"},
            ],
        });

        let row = handle_book(&event).unwrap();
        // Only valid bid: 0.45, only valid ask: 0.55
        // mid = (0.45 + 0.55) / 2 = 0.50
        assert!((row.value - 0.50).abs() < 1e-9);
    }

    #[test]
    fn book_event_bids_asks_in_meta() {
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "tok4",
            "bids": [{"price": "0.40", "size": "300"}],
            "asks": [{"price": "0.60", "size": "200"}],
        });

        let row = handle_book(&event).unwrap();
        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();

        let bids = meta["bids"].as_array().unwrap();
        assert_eq!(bids.len(), 1);
        assert!((bids[0][0].as_f64().unwrap() - 0.40).abs() < 1e-9);
        assert!((bids[0][1].as_f64().unwrap() - 300.0).abs() < 1e-9);

        let asks = meta["asks"].as_array().unwrap();
        assert_eq!(asks.len(), 1);
        assert!((asks[0][0].as_f64().unwrap() - 0.60).abs() < 1e-9);
    }

    // ── price_change event ────────────────────────────────────────────────────

    #[test]
    fn price_change_single_entry() {
        let event = serde_json::json!({
            "event_type": "price_change",
            "market": "0xdeadbeef",
            "price_changes": [
                {
                    "asset_id": "tok5",
                    "price": "0.52",
                    "size": "50",
                    "side": "BUY",
                    "best_bid": "0.51",
                    "best_ask": "0.53"
                }
            ],
            "timestamp": "1710000000000"
        });

        let rows = handle_price_change(&event);
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.source, "poly_price");
        // value = best_bid = 0.51
        assert!((row.value - 0.51).abs() < 1e-9);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["asset_id"], "tok5");
        assert_eq!(meta["price"], "0.52");
        assert_eq!(meta["size"], "50");
        assert_eq!(meta["side"], "BUY");
        assert_eq!(meta["best_bid"], "0.51");
        assert_eq!(meta["best_ask"], "0.53");
    }

    #[test]
    fn price_change_multiple_entries() {
        let event = serde_json::json!({
            "event_type": "price_change",
            "market": "0xdeadbeef",
            "price_changes": [
                {
                    "asset_id": "tok_a",
                    "price": "0.60",
                    "size": "100",
                    "side": "BUY",
                    "best_bid": "0.59",
                    "best_ask": "0.61"
                },
                {
                    "asset_id": "tok_b",
                    "price": "0.40",
                    "size": "200",
                    "side": "SELL",
                    "best_bid": "0.39",
                    "best_ask": "0.41"
                }
            ],
        });

        let rows = handle_price_change(&event);
        assert_eq!(rows.len(), 2);
        assert!((rows[0].value - 0.59).abs() < 1e-9); // best_bid of first
        assert!((rows[1].value - 0.39).abs() < 1e-9); // best_bid of second
    }

    #[test]
    fn price_change_missing_best_bid_value_zero() {
        let event = serde_json::json!({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "tok6", "price": "0.50", "size": "10", "side": "BUY"}
            ],
        });

        let rows = handle_price_change(&event);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 0.0);
    }

    #[test]
    fn price_change_no_price_changes_array() {
        let event = serde_json::json!({
            "event_type": "price_change",
            "market": "0xdeadbeef",
        });

        let rows = handle_price_change(&event);
        assert!(rows.is_empty());
    }

    #[test]
    fn price_change_invalid_best_bid_value_zero() {
        let event = serde_json::json!({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "tok7", "price": "0.50", "size": "10", "side": "BUY",
                 "best_bid": "not_a_number", "best_ask": "0.55"}
            ],
        });

        let rows = handle_price_change(&event);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 0.0);
    }

    // ── best_bid_ask event ────────────────────────────────────────────────────

    #[test]
    fn best_bid_ask_midpoint() {
        let event = serde_json::json!({
            "event_type": "best_bid_ask",
            "asset_id": "tok8",
            "best_bid": "0.48",
            "best_ask": "0.52",
            "spread": "0.04",
            "timestamp": "1710000000000"
        });

        let row = handle_best_bid_ask(&event).unwrap();
        assert_eq!(row.source, "poly_bbo");
        // mid = (0.48 + 0.52) / 2 = 0.50
        assert!((row.value - 0.50).abs() < 1e-9);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["asset_id"], "tok8");
        assert!((meta["best_bid"].as_f64().unwrap() - 0.48).abs() < 1e-9);
        assert!((meta["best_ask"].as_f64().unwrap() - 0.52).abs() < 1e-9);
        assert!((meta["spread"].as_f64().unwrap() - 0.04).abs() < 1e-9);
    }

    #[test]
    fn best_bid_ask_extreme_prices() {
        // Near 0 (long-shot Yes)
        let event = serde_json::json!({
            "event_type": "best_bid_ask",
            "asset_id": "tok9",
            "best_bid": "0.02",
            "best_ask": "0.04",
            "spread": "0.02",
        });

        let row = handle_best_bid_ask(&event).unwrap();
        assert!((row.value - 0.03).abs() < 1e-9);
    }

    #[test]
    fn best_bid_ask_missing_bid_value_zero() {
        let event = serde_json::json!({
            "event_type": "best_bid_ask",
            "asset_id": "tok10",
            "best_ask": "0.52",
        });

        let row = handle_best_bid_ask(&event).unwrap();
        assert_eq!(row.value, 0.0);
    }

    #[test]
    fn best_bid_ask_string_prices_parsed() {
        // Verify finite() handles string prices correctly
        let event = serde_json::json!({
            "event_type": "best_bid_ask",
            "asset_id": "tok11",
            "best_bid": "0.55",
            "best_ask": "0.57",
            "spread": "0.02",
        });

        let row = handle_best_bid_ask(&event).unwrap();
        // mid = (0.55 + 0.57) / 2 = 0.56
        assert!((row.value - 0.56).abs() < 1e-9);
    }

    // ── last_trade_price event ────────────────────────────────────────────────

    #[test]
    fn last_trade_price_buy() {
        let event = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "tok12",
            "price": "0.52",
            "size": "30",
            "side": "BUY",
            "timestamp": "1710000000000"
        });

        let row = handle_last_trade_price(&event).unwrap();
        assert_eq!(row.source, "poly_trade");
        assert!((row.value - 0.52).abs() < 1e-9);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["asset_id"], "tok12");
        assert_eq!(meta["size"], "30");
        assert_eq!(meta["side"], "BUY");
    }

    #[test]
    fn last_trade_price_sell() {
        let event = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "tok13",
            "price": "0.48",
            "size": "75",
            "side": "SELL",
        });

        let row = handle_last_trade_price(&event).unwrap();
        assert!((row.value - 0.48).abs() < 1e-9);
        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["side"], "SELL");
    }

    #[test]
    fn last_trade_price_missing_price_returns_none() {
        let event = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "tok14",
            "size": "10",
            "side": "BUY",
        });

        assert!(handle_last_trade_price(&event).is_none());
    }

    #[test]
    fn last_trade_price_invalid_price_returns_none() {
        let event = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "tok15",
            "price": "not_a_number",
            "size": "10",
            "side": "BUY",
        });

        assert!(handle_last_trade_price(&event).is_none());
    }

    #[test]
    fn last_trade_price_numeric_price_field() {
        // finite() handles both string and numeric JSON values
        let event = serde_json::json!({
            "event_type": "last_trade_price",
            "asset_id": "tok16",
            "price": 0.65,
            "size": "20",
            "side": "BUY",
        });

        let row = handle_last_trade_price(&event).unwrap();
        assert!((row.value - 0.65).abs() < 1e-9);
    }

    // ── market_resolved event ─────────────────────────────────────────────────

    #[test]
    fn market_resolved_yes_is_1() {
        let event = serde_json::json!({
            "event_type": "market_resolved",
            "winning_asset_id": "tok_yes",
            "winning_outcome": "Yes",
        });

        let row = handle_market_resolved(&event).unwrap();
        assert_eq!(row.source, "poly_resolved");
        assert!((row.value - 1.0).abs() < 1e-12);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["winning_outcome"], "Yes");
        assert_eq!(meta["winning_asset_id"], "tok_yes");
    }

    #[test]
    fn market_resolved_no_is_0() {
        let event = serde_json::json!({
            "event_type": "market_resolved",
            "winning_asset_id": "tok_no",
            "winning_outcome": "No",
        });

        let row = handle_market_resolved(&event).unwrap();
        assert_eq!(row.source, "poly_resolved");
        assert!((row.value - 0.0).abs() < 1e-12);

        let meta: serde_json::Value = serde_json::from_str(row.meta.as_ref().unwrap()).unwrap();
        assert_eq!(meta["winning_outcome"], "No");
    }

    #[test]
    fn market_resolved_unknown_outcome_is_0() {
        let event = serde_json::json!({
            "event_type": "market_resolved",
            "winning_asset_id": "tok_x",
            "winning_outcome": "INVALID",
        });

        let row = handle_market_resolved(&event).unwrap();
        assert_eq!(row.value, 0.0);
    }

    #[test]
    fn market_resolved_missing_outcome_is_0() {
        let event = serde_json::json!({
            "event_type": "market_resolved",
            "winning_asset_id": "tok_y",
        });

        let row = handle_market_resolved(&event).unwrap();
        assert_eq!(row.value, 0.0);
    }

    #[test]
    fn market_resolved_case_sensitive_yes() {
        // "yes" (lowercase) is NOT the same as "Yes" — field must match exactly
        let event = serde_json::json!({
            "event_type": "market_resolved",
            "winning_asset_id": "tok_z",
            "winning_outcome": "yes",
        });

        // "yes" falls through to the wildcard arm → 0.0
        let row = handle_market_resolved(&event).unwrap();
        assert_eq!(row.value, 0.0);
    }

    // ── parse_price_levels ────────────────────────────────────────────────────

    #[test]
    fn parse_price_levels_valid() {
        let arr = serde_json::json!([
            {"price": "0.48", "size": "200"},
            {"price": "0.45", "size": "500"},
        ]);

        let levels = parse_price_levels(Some(&arr));
        assert_eq!(levels.len(), 2);
        assert!((levels[0].0 - 0.48).abs() < 1e-9);
        assert!((levels[0].1 - 200.0).abs() < 1e-9);
        assert!((levels[1].0 - 0.45).abs() < 1e-9);
        assert!((levels[1].1 - 500.0).abs() < 1e-9);
    }

    #[test]
    fn parse_price_levels_zero_price_skipped() {
        let arr = serde_json::json!([
            {"price": "0", "size": "100"},
            {"price": "0.50", "size": "50"},
        ]);

        let levels = parse_price_levels(Some(&arr));
        assert_eq!(levels.len(), 1);
        assert!((levels[0].0 - 0.50).abs() < 1e-9);
    }

    #[test]
    fn parse_price_levels_invalid_price_skipped() {
        let arr = serde_json::json!([
            {"price": "bad", "size": "100"},
            {"price": "0.50", "size": "50"},
        ]);

        let levels = parse_price_levels(Some(&arr));
        assert_eq!(levels.len(), 1);
    }

    #[test]
    fn parse_price_levels_none_returns_empty() {
        let levels = parse_price_levels(None);
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_price_levels_zero_size_allowed() {
        // zero size entries pass through (resting order removed; size=0 is valid update)
        let arr = serde_json::json!([
            {"price": "0.50", "size": "0"},
        ]);

        let levels = parse_price_levels(Some(&arr));
        assert_eq!(levels.len(), 1);
        assert!((levels[0].1 - 0.0).abs() < 1e-12);
    }

    // ── FeedRow field invariants ──────────────────────────────────────────────

    #[test]
    fn feed_row_source_is_static_str() {
        // Compile-time check: &'static str is assignable; this just confirms
        // the constants round-trip through a FeedRow correctly.
        let sources = ["poly_book", "poly_price", "poly_bbo", "poly_trade", "poly_resolved"];
        for &s in &sources {
            let row = FeedRow { ts: 1.0, source: s, value: 0.5, meta: None, ticker: None };
            assert_eq!(row.source, s);
        }
    }

    #[test]
    fn feed_row_ts_positive() {
        // Every handler uses wall_clock() which is always > 0 in production.
        // We verify the value field is finite in the generated row.
        let event = serde_json::json!({
            "event_type": "book",
            "asset_id": "ts_test",
            "bids": [{"price": "0.50", "size": "100"}],
            "asks": [{"price": "0.52", "size": "80"}],
        });
        let row = handle_book(&event).unwrap();
        assert!(row.value.is_finite());
    }
}
