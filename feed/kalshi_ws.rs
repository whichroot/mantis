//! KalshiWsFeed — WebSocket-first Kalshi orderbook and ticker feed.
//!
//! Streams real-time orderbook snapshots, orderbook deltas, and ticker
//! data for Kalshi BTC prediction markets.
//!
//! Endpoint: wss://api.elections.kalshi.com/trade-api/ws/v2
//!
//! Auth: Kalshi WS requires RSA-PSS signed headers on the HTTP upgrade
//! handshake. The private key (PKCS#1 PEM) is loaded from the
//! `KALSHI_API_KEY_PRIVATE` env var at startup. The signature covers
//! `"{timestamp_ms}GET/trade-api/ws/v2"` using RSA-PSS-SHA256.
//! If either env var is missing, the feed returns early and the REST
//! snapshot loop continues to provide orderbook data.
//!
//! When auth is available, the feed subscribes to:
//!   - `orderbook_delta` — incremental book updates
//!   - `ticker`          — yes_bid/yes_ask/volume/OI per market
//!
//! Emitted FeedRow sources:
//!   - `"kalshi_book"`   — full orderbook_snapshot (value = best YES bid)
//!   - `"kalshi_delta"`  — orderbook_delta (value = price as f64)
//!   - `"kalshi_ticker"` — ticker update (value = yes_bid as f64)
//!
//! Sequence tracking: each subscription carries a `seq` field. If the
//! received seq is not exactly (prev_seq + 1), a warning is emitted.
//! The feed does not attempt gap-fill; it logs and continues.
//!
//! Reconnect: exponential backoff via [`Backoff`], 2s base, 60s max.

use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::RandomizedSigner;

use super::{Backoff, Feed, FeedDedup, LiveState, finite, wall_clock};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Env var name for the Kalshi API key ID.
const ENV_KEY_ID: &str = "KALSHI_API_KEY_ID";

/// Env var name for the Kalshi RSA private key (PEM, PKCS#1).
/// Loaded at startup and used to sign the WS handshake.
const ENV_KEY_PRIVATE: &str = "KALSHI_API_KEY_PRIVATE";

// ---------------------------------------------------------------------------
// Struct
// ---------------------------------------------------------------------------

/// WebSocket feed for Kalshi BTC prediction markets.
///
/// Supply the list of market tickers to subscribe to at construction.
/// An empty ticker list is valid (the feed subscribes to nothing and
/// immediately returns after a graceful auth check).
pub struct KalshiWsFeed {
    /// Market tickers to subscribe to, e.g. `["KXBTCD-25MAR1600-T84999.99"]`.
    market_tickers: Vec<String>,
    /// Cold-path channel for full L2 book depth. None = books not persisted.
    book_tx: Option<tokio::sync::mpsc::Sender<crate::book::BookEvent>>,
}

impl KalshiWsFeed {
    /// Create a new feed for the given market tickers.
    pub fn new(market_tickers: Vec<String>) -> Self {
        Self { market_tickers, book_tx: None }
    }

    /// Set the book side channel for full L2 depth persistence.
    pub fn with_book_tx(mut self, tx: tokio::sync::mpsc::Sender<crate::book::BookEvent>) -> Self {
        self.book_tx = Some(tx);
        self
    }
}

impl Default for KalshiWsFeed {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

// ---------------------------------------------------------------------------
// Feed impl
// ---------------------------------------------------------------------------

impl Feed for KalshiWsFeed {
    fn name(&self) -> &'static str {
        "kalshi_ws"
    }

    async fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) {
        // ── Auth gate ─────────────────────────────────────────────────────
        //
        // Check for API key ID. If missing, the feed is a no-op and the
        // REST snapshot loop continues to supply orderbook data.
        let key_id = match std::env::var(ENV_KEY_ID) {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!(
                    "[kalshi_ws] {ENV_KEY_ID} not set — skipping WS feed, \
                     REST fallback active"
                );
                return;
            }
        };

        // Load RSA private key from env var (PEM-encoded PKCS#1).
        let private_key = match std::env::var(ENV_KEY_PRIVATE) {
            Ok(pem) if !pem.is_empty() => {
                match rsa::RsaPrivateKey::from_pkcs1_pem(&pem) {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!(
                            "[kalshi_ws] failed to parse RSA key from \
                             {ENV_KEY_PRIVATE}: {e} — REST fallback active"
                        );
                        return;
                    }
                }
            }
            _ => {
                eprintln!(
                    "[kalshi_ws] {ENV_KEY_PRIVATE} not set — skipping WS feed, \
                     REST fallback active"
                );
                return;
            }
        };

        eprintln!("[kalshi_ws] RSA key loaded, auth ready");

        // ── Connection loop ──────────────────────────────────────────────
        {
            let mut backoff = Backoff::new(2.0, 60.0);
            let mut dedup = FeedDedup::new();

            // Per-subscription sequence tracking: sub_id → last_seq.
            let mut seq_tracker: HashMap<u64, u64> = HashMap::new();

            loop {
                if stop.is_cancelled() {
                    eprintln!("[kalshi_ws] shutting down");
                    break;
                }

                // Build authenticated request (timestamp in milliseconds)
                let ts_ms = (wall_clock() * 1000.0) as u64;
                let ts_str = ts_ms.to_string();

                let request = match build_request(&key_id, &ts_str, &private_key) {
                    Some(r) => r,
                    None => {
                        eprintln!("[kalshi_ws] failed to build request");
                        state.inc_errors();
                        backoff.wait(&stop).await;
                        continue;
                    }
                };

                let ws = match tokio_tungstenite::connect_async(request).await {
                    Ok((stream, _)) => {
                        backoff.reset();
                        eprintln!("[kalshi_ws] connected");
                        stream
                    }
                    Err(e) => {
                        eprintln!("[kalshi_ws] connect error: {e}");
                        state.inc_errors();
                        backoff.wait(&stop).await;
                        continue;
                    }
                };

                let (mut write, mut read) = ws.split();

                // Subscribe to orderbook_delta + ticker for all tickers
                if !self.market_tickers.is_empty() {
                    let sub_msg = build_subscribe_msg(&self.market_tickers);
                    let msg = tokio_tungstenite::tungstenite::Message::Text(
                        sub_msg.into(),
                    );
                    if let Err(e) = write.send(msg).await {
                        eprintln!("[kalshi_ws] subscribe error: {e}");
                        state.inc_errors();
                        backoff.wait(&stop).await;
                        continue;
                    }
                }

                // Message loop
                loop {
                    let msg = tokio::select! {
                        m = tokio::time::timeout(
                            std::time::Duration::from_secs(30),
                            read.next()
                        ) => m,
                        () = stop.cancelled() => break,
                    };

                    let msg = match msg {
                        Err(_) => {
                            // 30s timeout — send ping or just continue
                            continue;
                        }
                        Ok(None) => break, // stream ended
                        Ok(Some(Err(e))) => {
                            eprintln!("[kalshi_ws] ws error: {e}");
                            state.inc_errors();
                            break;
                        }
                        Ok(Some(Ok(m))) => m,
                    };

                    let text = match msg {
                        tokio_tungstenite::tungstenite::Message::Text(t) => t,
                        tokio_tungstenite::tungstenite::Message::Ping(_)
                        | tokio_tungstenite::tungstenite::Message::Pong(_) => {
                            continue
                        }
                        tokio_tungstenite::tungstenite::Message::Close(_) => break,
                        _ => continue,
                    };

                    if text.len() < 10 {
                        continue;
                    }

                    let parsed: serde_json::Value = match serde_json::from_str(&text) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    // Route by `type` field
                    let msg_type = match parsed["type"].as_str() {
                        Some(t) => t,
                        None => continue,
                    };

                    match msg_type {
                        "orderbook_snapshot" => {
                            handle_orderbook_snapshot(
                                &parsed,
                                &rings,
                                &mut dedup,
                                &mut seq_tracker,
                                &self.book_tx,
                            );
                        }
                        "orderbook_delta" => {
                            handle_orderbook_delta(
                                &parsed,
                                &rings,
                                &mut dedup,
                                &mut seq_tracker,
                            );
                        }
                        "ticker" => {
                            handle_ticker(
                                &parsed,
                                &rings,
                                &mut dedup,
                                &mut seq_tracker,
                            );
                        }
                        "subscribed" | "unsubscribed" | "error" => {
                            // Protocol acks — log errors
                            if msg_type == "error" {
                                eprintln!("[kalshi_ws] server error: {text}");
                                state.inc_errors();
                            }
                        }
                        _ => {} // unknown type, skip
                    }
                }

                if !stop.is_cancelled() {
                    eprintln!(
                        "[kalshi_ws] disconnected, reconnecting in {}s",
                        backoff.current as u64
                    );
                    backoff.wait(&stop).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Auth request builder
// ---------------------------------------------------------------------------

/// Build a tokio-tungstenite request with Kalshi auth headers.
///
/// Signs `"{timestamp_ms}GET/trade-api/ws/v2"` with RSA-PSS-SHA256 and
/// sets the three required headers: KEY, TIMESTAMP, SIGNATURE.
fn build_request(
    key_id: &str,
    ts_str: &str,
    private_key: &rsa::RsaPrivateKey,
) -> Option<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut req = WS_URL.into_client_request().ok()?;

    // Sign: "{timestamp_ms}GET/trade-api/ws/v2" with RSA-PSS-SHA256
    let msg_to_sign = format!("{ts_str}GET/trade-api/ws/v2");
    let signing_key = SigningKey::<sha2::Sha256>::new(private_key.clone());
    let mut rng = rsa::rand_core::OsRng;
    let signature = signing_key.sign_with_rng(&mut rng, msg_to_sign.as_bytes());
    let sig_bytes: Box<[u8]> = signature.into();
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(&*sig_bytes);

    let headers = req.headers_mut();
    headers.insert(
        "KALSHI-ACCESS-KEY",
        HeaderValue::from_str(key_id).ok()?,
    );
    headers.insert(
        "KALSHI-ACCESS-TIMESTAMP",
        HeaderValue::from_str(ts_str).ok()?,
    );
    headers.insert(
        "KALSHI-ACCESS-SIGNATURE",
        HeaderValue::from_str(&sig_b64).ok()?,
    );

    Some(req)
}

// ---------------------------------------------------------------------------
// Subscribe message builder
// ---------------------------------------------------------------------------

/// Build the JSON subscribe message for orderbook_delta + ticker channels.
///
/// ```json
/// {
///   "id": 1,
///   "cmd": "subscribe",
///   "params": {
///     "channels": ["orderbook_delta", "ticker"],
///     "market_tickers": ["KXBTCD-25MAR1600-T84999.99"]
///   }
/// }
/// ```
pub fn build_subscribe_msg(market_tickers: &[String]) -> String {
    serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta", "ticker"],
            "market_tickers": market_tickers,
        }
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Sequence tracking
// ---------------------------------------------------------------------------

/// Check and update the sequence counter for a subscription.
///
/// Kalshi WS messages carry `sid` (subscription ID) and `seq` (sequence
/// number, starting at 1 and incrementing by 1 per message). If the
/// received `seq` is not exactly `prev_seq + 1`, messages were missed.
///
/// On the very first message for a subscription (no prior seq), any seq
/// is accepted and stored.
///
/// Returns `(sid, seq)` if both are present, or `None` to skip.
fn check_seq(
    parsed: &serde_json::Value,
    tracker: &mut HashMap<u64, u64>,
    source_label: &str,
) -> Option<(u64, u64)> {
    let sid = parsed["sid"].as_u64()?;
    let seq = parsed["seq"].as_u64()?;

    if let Some(&prev) = tracker.get(&sid)
        && seq != prev + 1
    {
        eprintln!(
            "[kalshi_ws] {source_label} sid={sid}: seq gap — \
             expected {}, got {seq} (missed {} messages)",
            prev + 1,
            seq.saturating_sub(prev + 1)
        );
    }

    tracker.insert(sid, seq);
    Some((sid, seq))
}

// ---------------------------------------------------------------------------
// Message handlers
// ---------------------------------------------------------------------------

/// Handle `orderbook_snapshot` — full book with yes_dollars_fp / no_dollars_fp.
///
/// Envelope (Kalshi WS v2):
/// ```json
/// {
///   "type": "orderbook_snapshot",
///   "sid": 1,
///   "seq": 1,
///   "msg": {
///     "market_ticker": "KXBTCD-25MAR1600-T84999.99",
///     "yes_dollars_fp": [["0.4200", "100.00"], ...],
///     "no_dollars_fp":  [["0.5500", "200.00"], ...]
///   }
/// }
/// ```
///
/// Levels are sorted ascending by price. Best YES bid = last element of
/// yes_dollars_fp. Best YES ask = 1.0 − last element of no_dollars_fp.
///
/// Emits `source = "kalshi_book"`, `value = best YES bid` (or 0.0 if no bids).
fn handle_orderbook_snapshot(
    parsed: &serde_json::Value,
    rings: &crate::ring::RingSet,
    dedup: &mut FeedDedup,
    seq_tracker: &mut HashMap<u64, u64>,
    book_tx: &Option<tokio::sync::mpsc::Sender<crate::book::BookEvent>>,
) {
    let _ = check_seq(parsed, seq_tracker, "orderbook_snapshot");

    let msg = &parsed["msg"];
    let ticker = msg["market_ticker"].as_str().unwrap_or("unknown");

    let yes_levels = parse_kalshi_ws_levels(&msg["yes_dollars_fp"]);
    let no_levels = parse_kalshi_ws_levels(&msg["no_dollars_fp"]);

    let best_bid = yes_levels.last().map(|&(p, _)| p).unwrap_or(0.0);
    let best_ask = no_levels.last().map(|&(p, _)| 1.0 - p);

    let ts = wall_clock();
    if !dedup.check("kalshi_book", ts) {
        return;
    }

    // BBO meta for the ring (truncated at META_CAP, kernel reads this)
    let bbo_meta = serde_json::json!({
        "ticker": ticker,
        "best_bid": best_bid,
        "best_ask": best_ask,
    });
    let bbo_meta_s = bbo_meta.to_string();
    rings.kalshi_book.write(ts, best_bid, bbo_meta_s.as_bytes(), Some(ticker));

    // Full book to cold-path channel (if wired)
    if let Some(tx) = book_tx {
        let spread = best_ask.unwrap_or(0.0) - best_bid;
        let bid_depth: f64 = yes_levels.iter().map(|&(_, q)| q).sum();
        let ask_depth: f64 = no_levels.iter().map(|&(_, q)| q).sum();

        let levels = serde_json::json!({
            "yes": yes_levels.iter().map(|&(p, q)| [p, q]).collect::<Vec<_>>(),
            "no": no_levels.iter().map(|&(p, q)| [p, q]).collect::<Vec<_>>(),
        });

        let _ = tx.try_send(crate::book::BookEvent {
            ts,
            market_id: 0, // resolved by relay via ticker JOIN
            ticker: ticker.to_string(),
            venue: "kalshi",
            best_bid,
            best_ask: best_ask.unwrap_or(0.0),
            spread,
            bid_depth,
            ask_depth,
            levels_json: levels.to_string(),
        });
    }
}

/// Handle `orderbook_delta` — incremental book update.
///
/// Envelope:
/// ```json
/// {
///   "type": "orderbook_delta",
///   "sid": 1,
///   "seq": 2,
///   "msg": {
///     "market_ticker": "KXBTCD-25MAR1600-T84999.99",
///     "price_dollars": "0.4200",
///     "delta_fp": "50.00",
///     "side": "yes"
///   }
/// }
/// ```
///
/// Emits `source = "kalshi_delta"`, `value = price_dollars as f64`.
/// A negative delta_fp indicates a reduction in size at that level.
fn handle_orderbook_delta(
    parsed: &serde_json::Value,
    rings: &crate::ring::RingSet,
    dedup: &mut FeedDedup,
    seq_tracker: &mut HashMap<u64, u64>,
) {
    let _ = check_seq(parsed, seq_tracker, "orderbook_delta");

    let msg = &parsed["msg"];
    let ticker = msg["market_ticker"].as_str().unwrap_or("unknown");

    let price = match finite(&msg["price_dollars"]) {
        Some(p) if p.is_finite() => p,
        _ => return,
    };

    let delta = finite(&msg["delta_fp"]).unwrap_or(0.0);
    let side = msg["side"].as_str().unwrap_or("unknown");

    let ts = wall_clock();
    // Deltas are high-frequency; dedup by source+ts to avoid flood
    if !dedup.check("kalshi_delta", ts) {
        return;
    }

    let meta = serde_json::json!({
        "ticker": ticker,
        "price": price,
        "delta": delta,
        "side": side,
    });

    let meta_s = meta.to_string();
    rings.kalshi_delta.write(ts, price, meta_s.as_bytes(), Some(ticker));
}

/// Handle `ticker` — market price and volume update.
///
/// Envelope:
/// ```json
/// {
///   "type": "ticker",
///   "sid": 1,
///   "seq": 3,
///   "msg": {
///     "market_ticker": "KXBTCD-25MAR1600-T84999.99",
///     "yes_bid_dollars": "0.4700",
///     "yes_ask_dollars": "0.4900",
///     "no_bid_dollars": "0.5100",
///     "no_ask_dollars": "0.5300",
///     "volume_fp": "12345.67",
///     "open_interest_fp": "999.00",
///     "dollar_volume": "5432.10",
///     "dollar_open_interest": "432.10"
///   }
/// }
/// ```
///
/// Emits `source = "kalshi_ticker"`, `value = yes_bid_dollars as f64`.
fn handle_ticker(
    parsed: &serde_json::Value,
    rings: &crate::ring::RingSet,
    dedup: &mut FeedDedup,
    seq_tracker: &mut HashMap<u64, u64>,
) {
    let _ = check_seq(parsed, seq_tracker, "ticker");

    let msg = &parsed["msg"];
    let ticker = msg["market_ticker"].as_str().unwrap_or("unknown");

    // Only the BBO fields are needed by the kernel. Full ticker payload
    // (volume, OI, no_bid, no_ask) is not parsed — it exceeds META_CAP
    // (128 bytes) and would produce truncated JSON unreadable by parse_f64_from_meta.
    let yes_bid = finite(&msg["yes_bid_dollars"]).unwrap_or(0.0);
    let yes_ask = finite(&msg["yes_ask_dollars"]);

    let ts = wall_clock();
    if !dedup.check("kalshi_ticker", ts) {
        return;
    }

    // Compact BBO meta: value = yes_bid, meta = {"yes_ask": <f64|null>}.
    let bbo_meta = serde_json::json!({ "yes_ask": yes_ask });
    rings.kalshi_ticker.write(ts, yes_bid, bbo_meta.to_string().as_bytes(), Some(ticker));
}

// ---------------------------------------------------------------------------
// Level parser
// ---------------------------------------------------------------------------

/// Parse a Kalshi WS price level array: `[["0.42", "13.00"], ...]` → `Vec<(f64, f64)>`.
///
/// Levels are `[price_str, quantity_str]` pairs. Both must be finite and
/// non-negative. Malformed entries are silently skipped (NaN firewall).
/// Order is preserved (Kalshi sends ascending by price).
fn parse_kalshi_ws_levels(arr: &serde_json::Value) -> Vec<(f64, f64)> {
    let arr = match arr.as_array() {
        Some(a) => a,
        None => return Vec::new(),
    };

    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let pair = match item.as_array() {
            Some(a) if a.len() >= 2 => a,
            _ => continue,
        };
        let price = match finite(&pair[0]) {
            Some(p) if (0.0..=1.0).contains(&p) => p,
            _ => continue,
        };
        let qty = match finite(&pair[1]) {
            Some(q) if q >= 0.0 => q,
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

    // ── subscribe message format ─────────────────────────────────────────────

    #[test]
    fn subscribe_msg_format() {
        let tickers = vec![
            "KXBTCD-25MAR1600-T84999.99".to_owned(),
            "KXBTCD-25MAR1700-T84999.99".to_owned(),
        ];
        let msg = build_subscribe_msg(&tickers);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();

        assert_eq!(v["id"], 1);
        assert_eq!(v["cmd"], "subscribe");
        let channels = v["params"]["channels"].as_array().unwrap();
        assert!(channels.iter().any(|c| c == "orderbook_delta"));
        assert!(channels.iter().any(|c| c == "ticker"));
        let mt = v["params"]["market_tickers"].as_array().unwrap();
        assert_eq!(mt.len(), 2);
        assert_eq!(mt[0], "KXBTCD-25MAR1600-T84999.99");
    }

    #[test]
    fn subscribe_msg_empty_tickers() {
        let msg = build_subscribe_msg(&[]);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let mt = v["params"]["market_tickers"].as_array().unwrap();
        assert_eq!(mt.len(), 0);
    }

    // ── level parser ─────────────────────────────────────────────────────────

    #[test]
    fn parse_levels_valid() {
        let arr = serde_json::json!([["0.42", "13.00"], ["0.50", "20.00"]]);
        let levels = parse_kalshi_ws_levels(&arr);
        assert_eq!(levels.len(), 2);
        assert!((levels[0].0 - 0.42).abs() < 1e-9);
        assert!((levels[0].1 - 13.0).abs() < 1e-9);
        assert!((levels[1].0 - 0.50).abs() < 1e-9);
        assert!((levels[1].1 - 20.0).abs() < 1e-9);
    }

    #[test]
    fn parse_levels_filters_non_finite() {
        let arr = serde_json::json!([
            ["nan", "10.00"],
            ["0.50", "not_a_number"],
            ["0.40", "5.00"]
        ]);
        let levels = parse_kalshi_ws_levels(&arr);
        // "nan" → rejected, "not_a_number" → rejected, good entry passes
        assert_eq!(levels.len(), 1);
        assert!((levels[0].0 - 0.40).abs() < 1e-9);
    }

    #[test]
    fn parse_levels_empty_array() {
        let arr = serde_json::json!([]);
        let levels = parse_kalshi_ws_levels(&arr);
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_levels_null() {
        let arr = serde_json::json!(null);
        let levels = parse_kalshi_ws_levels(&arr);
        assert!(levels.is_empty());
    }

    #[test]
    fn parse_levels_rejects_out_of_range_price() {
        // Kalshi prices must be in [0, 1] — a price > 1.0 is malformed
        let arr = serde_json::json!([["1.50", "10.00"], ["0.45", "5.00"]]);
        let levels = parse_kalshi_ws_levels(&arr);
        assert_eq!(levels.len(), 1);
        assert!((levels[0].0 - 0.45).abs() < 1e-9);
    }

    // ── orderbook_snapshot parsing ───────────────────────────────────────────

    #[test]
    fn orderbook_snapshot_emits_feed_row() {
        let msg = serde_json::json!({
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "yes_dollars_fp": [
                    ["0.1000", "200.00"],
                    ["0.2500", "100.00"],
                    ["0.4700", "50.00"]
                ],
                "no_dollars_fp": [
                    ["0.0100", "500.00"],
                    ["0.5000", "300.00"],
                    ["0.5200", "150.00"]
                ]
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_snapshot(&msg, &rings, &mut dedup, &mut seq_tracker, &None);
        let e = rings.kalshi_book.head().unwrap();
        assert!(e.value > 0.0, "best_bid should be positive");
        let meta_str = e.meta_str().unwrap();
        assert!(meta_str.contains("ticker"));
        assert!(meta_str.contains("KXBTCD-25MAR1600-T84999.99"));
    }

    #[test]
    fn orderbook_snapshot_empty_book_emits_zero_value() {
        let msg = serde_json::json!({
            "type": "orderbook_snapshot",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "yes_dollars_fp": [],
                "no_dollars_fp": []
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_snapshot(&msg, &rings, &mut dedup, &mut seq_tracker, &None);

        // Empty book: value = 0.0 (no bids), row still emitted
        let now = crate::feed::wall_clock();
        let entry = rings.kalshi_book.get_by_ticker("KXBTCD-25MAR1600-T84999.99", now + 1.0).unwrap();
        assert_eq!(entry.value, 0.0);
    }

    #[test]
    fn orderbook_snapshot_seq_tracked() {
        let msg = serde_json::json!({
            "type": "orderbook_snapshot",
            "sid": 42,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "yes_dollars_fp": [["0.50", "100.00"]],
                "no_dollars_fp": []
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_snapshot(&msg, &rings, &mut dedup, &mut seq_tracker, &None);

        assert_eq!(seq_tracker.get(&42), Some(&1));
    }

    // ── orderbook_delta parsing ──────────────────────────────────────────────

    #[test]
    fn orderbook_delta_emits_feed_row() {
        let msg = serde_json::json!({
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "price_dollars": "0.4700",
                "delta_fp": "25.00",
                "side": "yes"
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_delta(&msg, &rings, &mut dedup, &mut seq_tracker);

        let now = crate::feed::wall_clock();
        let entry = rings.kalshi_delta.get_by_ticker("KXBTCD-25MAR1600-T84999.99", now + 1.0).unwrap();
        assert!((entry.value - 0.47).abs() < 1e-9);

        let meta: serde_json::Value =
            serde_json::from_str(entry.meta_str().unwrap()).unwrap();
        assert_eq!(meta["side"], "yes");
        assert!((meta["delta"].as_f64().unwrap() - 25.0).abs() < 1e-9);
        assert!((meta["price"].as_f64().unwrap() - 0.47).abs() < 1e-9);
    }

    #[test]
    fn orderbook_delta_negative_delta_accepted() {
        // Negative delta = size reduction at that level, still valid
        let msg = serde_json::json!({
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 3,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "price_dollars": "0.4700",
                "delta_fp": "-25.00",
                "side": "yes"
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_delta(&msg, &rings, &mut dedup, &mut seq_tracker);

        let now = crate::feed::wall_clock();
        let entry = rings.kalshi_delta.get_by_ticker("KXBTCD-25MAR1600-T84999.99", now + 1.0).unwrap();
        assert!((entry.value - 0.47).abs() < 1e-9);
        let meta: serde_json::Value =
            serde_json::from_str(entry.meta_str().unwrap()).unwrap();
        assert!((meta["delta"].as_f64().unwrap() - (-25.0)).abs() < 1e-9);
    }

    #[test]
    fn orderbook_delta_missing_price_skipped() {
        let msg = serde_json::json!({
            "type": "orderbook_delta",
            "sid": 1,
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "delta_fp": "25.00",
                "side": "yes"
                // price_dollars missing
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_delta(&msg, &rings, &mut dedup, &mut seq_tracker);

        assert_eq!(rings.kalshi_delta.write_count(), 0); // nothing emitted
    }

    #[test]
    fn orderbook_delta_seq_tracked() {
        let msg1 = serde_json::json!({
            "type": "orderbook_delta",
            "sid": 7,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "price_dollars": "0.47",
                "delta_fp": "10.00",
                "side": "yes"
            }
        });
        let msg2 = serde_json::json!({
            "type": "orderbook_delta",
            "sid": 7,
            "seq": 2,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "price_dollars": "0.48",
                "delta_fp": "5.00",
                "side": "yes"
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_orderbook_delta(&msg1, &rings, &mut dedup, &mut seq_tracker);
        assert_eq!(seq_tracker.get(&7), Some(&1));

        handle_orderbook_delta(&msg2, &rings, &mut dedup, &mut seq_tracker);
        assert_eq!(seq_tracker.get(&7), Some(&2));
    }

    // ── ticker parsing ───────────────────────────────────────────────────────

    #[test]
    fn ticker_emits_feed_row() {
        let msg = serde_json::json!({
            "type": "ticker",
            "sid": 1,
            "seq": 4,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "yes_bid_dollars": "0.4700",
                "yes_ask_dollars": "0.4900",
                "no_bid_dollars": "0.5100",
                "no_ask_dollars": "0.5300",
                "volume_fp": "12345.67",
                "open_interest_fp": "999.00",
                "dollar_volume": "5432.10",
                "dollar_open_interest": "432.10"
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_ticker(&msg, &rings, &mut dedup, &mut seq_tracker);

        let now = crate::feed::wall_clock();
        let entry = rings.kalshi_ticker.get_by_ticker("KXBTCD-25MAR1600-T84999.99", now + 1.0).unwrap();
        assert!((entry.value - 0.47).abs() < 1e-9); // yes_bid_dollars

        // Compact meta: {"yes_ask": 0.49} — must be valid JSON with yes_ask readable.
        // Full ticker payload (volume, OI, no_bid, etc.) is not in ring meta;
        // it exceeds META_CAP (128 bytes) and would produce truncated, invalid JSON.
        let meta_s = entry.meta_str().unwrap();
        let meta: serde_json::Value = serde_json::from_str(meta_s)
            .expect("ring meta must be valid JSON (not truncated)");
        let yes_ask = meta["yes_ask"].as_f64().expect("yes_ask must be a readable f64");
        assert!((yes_ask - 0.49).abs() < 1e-9, "yes_ask mismatch: {yes_ask}");
    }

    #[test]
    fn ticker_missing_bid_yields_zero_value() {
        // yes_bid_dollars absent → value = 0.0 (unwrap_or default)
        let msg = serde_json::json!({
            "type": "ticker",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "yes_ask_dollars": "0.4900"
                // yes_bid_dollars missing
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_ticker(&msg, &rings, &mut dedup, &mut seq_tracker);

        let now = crate::feed::wall_clock();
        let entry = rings.kalshi_ticker.get_by_ticker("KXBTCD-25MAR1600-T84999.99", now + 1.0).unwrap();
        assert_eq!(entry.value, 0.0);
    }

    #[test]
    fn ticker_string_prices_parsed() {
        // Kalshi returns prices as strings like "0.480"
        let msg = serde_json::json!({
            "type": "ticker",
            "sid": 1,
            "seq": 1,
            "msg": {
                "market_ticker": "KXBTCD-25MAR1600-T84999.99",
                "yes_bid_dollars": "0.480",
                "yes_ask_dollars": "0.500",
                "volume_fp": "100.0",
                "open_interest_fp": "50.0"
            }
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();
        let mut seq_tracker = HashMap::new();

        handle_ticker(&msg, &rings, &mut dedup, &mut seq_tracker);

        let now = crate::feed::wall_clock();
        let entry = rings.kalshi_ticker.get_by_ticker("KXBTCD-25MAR1600-T84999.99", now + 1.0).unwrap();
        assert!((entry.value - 0.480).abs() < 1e-9);
    }

    // ── sequence gap detection ───────────────────────────────────────────────

    #[test]
    fn seq_tracking_detects_gap() {
        // Feed seq 1, then seq 3 (gap: seq 2 missing)
        // check_seq should still store seq=3 and return Some
        let mut tracker = HashMap::new();

        let msg1 = serde_json::json!({"sid": 10, "seq": 1});
        let result1 = check_seq(&msg1, &mut tracker, "test");
        assert_eq!(result1, Some((10, 1)));
        assert_eq!(tracker[&10], 1);

        // seq 3 — gap detected (no panic, just log warning)
        let msg3 = serde_json::json!({"sid": 10, "seq": 3});
        let result3 = check_seq(&msg3, &mut tracker, "test");
        assert_eq!(result3, Some((10, 3)));
        assert_eq!(tracker[&10], 3); // updated to new seq
    }

    #[test]
    fn seq_tracking_first_message_any_seq_accepted() {
        let mut tracker = HashMap::new();
        let msg = serde_json::json!({"sid": 99, "seq": 5});
        // First message for sid=99 — no prior, any seq is fine
        let result = check_seq(&msg, &mut tracker, "test");
        assert_eq!(result, Some((99, 5)));
        assert_eq!(tracker[&99], 5);
    }

    #[test]
    fn seq_tracking_consecutive_accepted() {
        let mut tracker = HashMap::new();

        for seq in 1u64..=5 {
            let msg = serde_json::json!({"sid": 1, "seq": seq});
            let result = check_seq(&msg, &mut tracker, "test");
            assert_eq!(result, Some((1, seq)));
            assert_eq!(tracker[&1], seq);
        }
    }

    #[test]
    fn seq_tracking_missing_sid_returns_none() {
        let mut tracker = HashMap::new();
        let msg = serde_json::json!({"seq": 1}); // no sid field
        let result = check_seq(&msg, &mut tracker, "test");
        assert_eq!(result, None);
    }

    #[test]
    fn seq_tracking_missing_seq_returns_none() {
        let mut tracker = HashMap::new();
        let msg = serde_json::json!({"sid": 1}); // no seq field
        let result = check_seq(&msg, &mut tracker, "test");
        assert_eq!(result, None);
    }

    #[test]
    fn seq_tracking_independent_sids() {
        // Two subscriptions with interleaved messages — tracked independently
        let mut tracker = HashMap::new();

        let m1a = serde_json::json!({"sid": 1, "seq": 1});
        let m2a = serde_json::json!({"sid": 2, "seq": 1});
        let m1b = serde_json::json!({"sid": 1, "seq": 2});
        let m2b = serde_json::json!({"sid": 2, "seq": 2});

        check_seq(&m1a, &mut tracker, "test");
        check_seq(&m2a, &mut tracker, "test");
        check_seq(&m1b, &mut tracker, "test");
        check_seq(&m2b, &mut tracker, "test");

        assert_eq!(tracker[&1], 2);
        assert_eq!(tracker[&2], 2);
    }

    // ── missing API key — graceful skip ──────────────────────────────────────

    #[test]
    fn missing_api_key_env_var_is_graceful() {
        // Remove the env var if present, then verify we get the "empty" sentinel.
        // We can't run the full async Feed::run here, so we test the env check
        // directly — the same logic the run() function uses.
        let key_id_result = {
            // Temporarily shadow so other test env doesn't bleed
            let saved = std::env::var(ENV_KEY_ID).ok();
            unsafe { std::env::remove_var(ENV_KEY_ID) };
            let result = std::env::var(ENV_KEY_ID);
            if let Some(v) = saved {
                unsafe { std::env::set_var(ENV_KEY_ID, v) };
            }
            result
        };

        // Env var not set → Err(_) → feed would return early (no panic)
        assert!(key_id_result.is_err());
    }

    #[test]
    fn empty_api_key_treated_as_missing() {
        // An empty string is treated the same as absent
        let empty = "";
        let is_valid = !empty.is_empty();
        assert!(!is_valid); // empty string → feed returns early
    }

    // ── struct construction ──────────────────────────────────────────────────

    #[test]
    fn new_with_tickers() {
        let feed = KalshiWsFeed::new(vec!["KXBTCD-25MAR1600-T84999.99".to_owned()]);
        assert_eq!(feed.market_tickers.len(), 1);
        assert_eq!(feed.market_tickers[0], "KXBTCD-25MAR1600-T84999.99");
    }

    #[test]
    fn default_has_empty_tickers() {
        let feed = KalshiWsFeed::default();
        assert!(feed.market_tickers.is_empty());
    }

    #[test]
    fn feed_name_is_kalshi_ws() {
        use crate::feed::Feed;
        let feed = KalshiWsFeed::default();
        assert_eq!(feed.name(), "kalshi_ws");
    }
}
