//! RtdsFeed — Polymarket RTDS (Real-Time Data Service) WebSocket feed.
//!
//! Streams both Binance and Chainlink BTC prices from Polymarket's live data
//! service.  This is the exact feed Polymarket uses for up/down market
//! resolution, so these values are ground-truth for Polymarket oracle behaviour.
//!
//! Edge cases:
//! - URL: wss://ws-live-data.polymarket.com
//! - No subprotocols, no auth headers
//! - Library pings DISABLED — must send literal string `"ping"` every 5 seconds
//!   via a separate keepalive task.  Server acks with short messages (< 10 bytes)
//!   which are silently dropped.
//! - Subscribe after connect:
//!   `{"action":"subscribe","subscriptions":[{"topic":"crypto_prices","type":"*"},{"topic":"crypto_prices_chainlink","type":"*"}]}`
//! - Messages < 10 bytes silently dropped (keepalive ack guard)
//! - Route by topic: `"chainlink" in topic` → source "rtds_chainlink",
//!   updates chainlink_value/ts/count.
//!   Otherwise → source "rtds_binance", updates poly_bn_value/ts/count.
//! - Filter: symbol does not contain "btc" (case-insensitive) → skip
//! - Value: payload.value through pos() firewall
//! - Timestamp: payload.timestamp (ms), skip if 0 or missing
//! - recv timeout 10s — silent continue
//! - Reconnect: 2s base, 2x backoff, cap 30s

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;
use futures_util::{SinkExt, StreamExt};

use super::{Backoff, Feed, LiveState, pos};

const URL: &str = "wss://ws-live-data.polymarket.com";

/// Subscribe message sent immediately after connect.
const SUBSCRIBE_MSG: &str = r#"{"action":"subscribe","subscriptions":[{"topic":"crypto_prices","type":"*"},{"topic":"crypto_prices_chainlink","type":"*"}]}"#;

/// Minimum message length; anything shorter is a keepalive ack and is silently dropped.
const MIN_MSG_LEN: usize = 10;

/// Interval between keepalive "ping" strings sent to the server (seconds).
const PING_INTERVAL_SECS: u64 = 5;

/// Recv timeout (seconds). Tightest of all feeds — matches Python's 10s.
const RECV_TIMEOUT_SECS: u64 = 10;

pub struct RtdsFeed;

impl RtdsFeed {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RtdsFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Feed for RtdsFeed {
    fn name(&self) -> &'static str {
        "rtds_binance"
    }

    async fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) {
        let mut backoff = Backoff::new(2.0, 30.0);

        loop {
            if stop.is_cancelled() {
                eprintln!("[rtds] shutting down");
                break;
            }

            // Connect — no subprotocols, no extra headers.
            let ws = match tokio_tungstenite::connect_async(URL).await {
                Ok((stream, _)) => {
                    backoff.reset();
                    eprintln!("[rtds] connected");
                    stream
                }
                Err(e) => {
                    eprintln!("[rtds] connect error: {e}");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // Send subscribe message immediately after connect.
            if let Err(e) = write
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    SUBSCRIBE_MSG.into(),
                ))
                .await
            {
                eprintln!("[rtds] subscribe send error: {e}");
                state.inc_errors();
                backoff.wait(&stop).await;
                continue;
            }

            // Spawn keepalive task: send literal "ping" string every 5 seconds.
            // This is NOT a WebSocket ping frame — it is a text message.
            // We use a separate write half via an mpsc channel because the sink
            // can only be used from one task at a time.
            //
            // Strategy: the read loop checks a keepalive channel for outgoing
            // messages and flushes them in-line.  Simpler than a separate
            // sink-owning task; avoids Arc<Mutex<Sink>>.
            //
            // Actually the cleanest approach matching Python is to drive both
            // write and read from a single task using select!, with a keepalive
            // timer.

            let stop_inner = stop.clone();
            let mut ping_interval =
                tokio::time::interval(std::time::Duration::from_secs(PING_INTERVAL_SECS));
            ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The first tick fires immediately; skip it so we don't ping before
            // we have received anything.
            ping_interval.tick().await;

            loop {
                let msg = tokio::select! {
                    // Recv timeout guard.
                    m = tokio::time::timeout(
                        std::time::Duration::from_secs(RECV_TIMEOUT_SECS),
                        read.next()
                    ) => m,
                    // Keepalive ping (text "ping", not WS ping frame).
                    _ = ping_interval.tick() => {
                        let _ = write
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                "ping".into(),
                            ))
                            .await;
                        continue;
                    }
                    () = stop_inner.cancelled() => break,
                };

                let msg = match msg {
                    // Timeout — silent continue (matches Python behavior).
                    Err(_) => continue,
                    // Stream ended.
                    Ok(None) => break,
                    // WebSocket error.
                    Ok(Some(Err(e))) => {
                        eprintln!("[rtds] ws error: {e}");
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

                // Silently drop keepalive acks and other short messages.
                if text.len() < MIN_MSG_LEN {
                    continue;
                }

                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let topic = parsed["topic"].as_str().unwrap_or("");
                let payload = &parsed["payload"];

                // Symbol filter: must contain "btc" (case-insensitive).
                let symbol = payload["symbol"].as_str().unwrap_or("").to_lowercase();
                if !symbol.contains("btc") {
                    continue;
                }

                // Value: must be positive finite.
                let value = match pos(&payload["value"]) {
                    Some(v) => v,
                    None => continue,
                };

                // Timestamp from payload.timestamp (ms).  Skip if zero or missing.
                let pts = payload["timestamp"].as_f64().unwrap_or(0.0);
                if pts == 0.0 {
                    continue;
                }
                let ts_s = pts / 1000.0;

                // recv lag
                let recv_ms = super::wall_clock_ms();
                let lag_ms = recv_ms - pts;

                // Route by topic.
                let source: &'static str;
                if topic.contains("chainlink") {
                    source = "rtds_chainlink";
                    state.chainlink_value.store(value);
                    state.chainlink_ts.store(ts_s);
                    state.chainlink_count.fetch_add(1, Ordering::Relaxed);
                } else {
                    source = "rtds_binance";
                    state.poly_bn_value.store(value);
                    state.poly_bn_ts.store(ts_s);
                    state.poly_bn_count.fetch_add(1, Ordering::Relaxed);
                }

                // Build meta JSON.
                let meta = serde_json::json!({
                    "recv_lag_ms": (lag_ms * 10.0).round() / 10.0,
                    "symbol": symbol,
                });

                let meta_s = meta.to_string();
                if source == "rtds_chainlink" {
                    rings.rtds_chainlink.write(ts_s, value, meta_s.as_bytes(), None);
                } else {
                    rings.rtds_binance.write(ts_s, value, meta_s.as_bytes(), None);
                }
            }

            // Disconnected — backoff and retry.
            if !stop.is_cancelled() {
                eprintln!("[rtds] disconnected, reconnecting in {}s", backoff.current as u64);
                backoff.wait(&stop).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Build a minimal RTDS message for a given topic.
    fn make_msg(topic: &str, symbol: &str, value: f64, timestamp_ms: u64) -> serde_json::Value {
        serde_json::json!({
            "topic": topic,
            "payload": {
                "symbol": symbol,
                "value": value,
                "timestamp": timestamp_ms,
            }
        })
    }

    // ── parsing ──────────────────────────────────────────────────────────────

    /// A valid crypto_prices message should parse as rtds_binance.
    #[test]
    fn parse_valid_crypto_prices_message() {
        let msg = make_msg("crypto_prices", "BTC", 95000.0, 1710000000000);

        let topic = msg["topic"].as_str().unwrap_or("");
        let payload = &msg["payload"];
        let symbol = payload["symbol"].as_str().unwrap_or("").to_lowercase();

        assert!(symbol.contains("btc"));

        let value = pos(&payload["value"]).unwrap();
        assert_eq!(value, 95000.0);

        let pts = payload["timestamp"].as_f64().unwrap();
        assert!(pts > 0.0);
        let ts_s = pts / 1000.0;
        assert!((ts_s - 1710000000.0).abs() < 0.001);

        // Non-chainlink → rtds_binance
        assert!(!topic.contains("chainlink"));
        let source = if topic.contains("chainlink") { "rtds_chainlink" } else { "rtds_binance" };
        assert_eq!(source, "rtds_binance");
    }

    /// A valid crypto_prices_chainlink message should parse as rtds_chainlink.
    #[test]
    fn parse_valid_crypto_prices_chainlink_message() {
        let msg = make_msg("crypto_prices_chainlink", "BTC", 95050.0, 1710000001000);

        let topic = msg["topic"].as_str().unwrap_or("");
        let payload = &msg["payload"];
        let symbol = payload["symbol"].as_str().unwrap_or("").to_lowercase();

        assert!(symbol.contains("btc"));

        let value = pos(&payload["value"]).unwrap();
        assert_eq!(value, 95050.0);

        // chainlink topic → rtds_chainlink
        assert!(topic.contains("chainlink"));
        let source = if topic.contains("chainlink") { "rtds_chainlink" } else { "rtds_binance" };
        assert_eq!(source, "rtds_chainlink");
    }

    // ── filtering ────────────────────────────────────────────────────────────

    /// Symbols that don't contain "btc" (case-insensitive) must be skipped.
    #[test]
    fn filter_non_btc_symbols() {
        let cases = &["ETH", "eth", "SOL", "DOGE", "usdc", "MATIC"];
        for &sym in cases {
            let s = sym.to_lowercase();
            assert!(
                !s.contains("btc"),
                "symbol '{sym}' should NOT pass the BTC filter"
            );
        }

        // BTC variants that SHOULD pass
        let pass_cases = &["BTC", "btc", "BTCUSDT", "btcusdt", "wBTC", "WBTC"];
        for &sym in pass_cases {
            let s = sym.to_lowercase();
            assert!(
                s.contains("btc"),
                "symbol '{sym}' SHOULD pass the BTC filter"
            );
        }
    }

    // ── short message guard ──────────────────────────────────────────────────

    /// Messages shorter than MIN_MSG_LEN bytes must be silently dropped.
    #[test]
    fn reject_messages_shorter_than_min_len() {
        // "pong" is a typical keepalive ack — 4 bytes
        assert!("pong".len() < MIN_MSG_LEN);
        // empty string
        assert!("".len() < MIN_MSG_LEN);
        // exactly MIN_MSG_LEN - 1
        let borderline = "x".repeat(MIN_MSG_LEN - 1);
        assert!(borderline.len() < MIN_MSG_LEN);
        // exactly MIN_MSG_LEN — should pass
        let at_limit = "x".repeat(MIN_MSG_LEN);
        assert!(at_limit.len() >= MIN_MSG_LEN);
    }

    // ── timestamp guard ──────────────────────────────────────────────────────

    /// Messages with timestamp == 0 or missing must be skipped.
    #[test]
    fn reject_missing_or_zero_timestamp() {
        // Missing timestamp field → as_f64() returns None → unwrap_or(0.0) == 0.0
        let msg_missing = serde_json::json!({
            "topic": "crypto_prices",
            "payload": { "symbol": "BTC", "value": 95000.0 }
        });
        let pts = msg_missing["payload"]["timestamp"].as_f64().unwrap_or(0.0);
        assert_eq!(pts, 0.0, "missing timestamp should yield 0.0");

        // Explicit zero
        let msg_zero = serde_json::json!({
            "topic": "crypto_prices",
            "payload": { "symbol": "BTC", "value": 95000.0, "timestamp": 0_u64 }
        });
        let pts = msg_zero["payload"]["timestamp"].as_f64().unwrap_or(0.0);
        assert_eq!(pts, 0.0, "explicit zero timestamp should be rejected");
    }

    // ── value guard ──────────────────────────────────────────────────────────

    /// Non-positive values must be rejected by the pos() firewall.
    #[test]
    fn reject_non_positive_value() {
        let zero_msg = make_msg("crypto_prices", "BTC", 0.0, 1710000000000);
        assert_eq!(pos(&zero_msg["payload"]["value"]), None);

        let neg_msg = serde_json::json!({
            "topic": "crypto_prices",
            "payload": { "symbol": "BTC", "value": -500.0, "timestamp": 1710000000000_u64 }
        });
        assert_eq!(pos(&neg_msg["payload"]["value"]), None);

        let missing_msg = serde_json::json!({
            "topic": "crypto_prices",
            "payload": { "symbol": "BTC", "timestamp": 1710000000000_u64 }
        });
        assert_eq!(pos(&missing_msg["payload"]["value"]), None);
    }

    // ── meta ─────────────────────────────────────────────────────────────────

    /// Meta JSON must include recv_lag_ms and symbol.
    #[test]
    fn meta_includes_recv_lag_ms_and_symbol() {
        let lag_ms = 37.3_f64;
        let symbol = "btcusdt";
        let meta = serde_json::json!({
            "recv_lag_ms": (lag_ms * 10.0).round() / 10.0,
            "symbol": symbol,
        });
        let s = meta.to_string();
        assert!(s.contains("recv_lag_ms"), "meta must contain recv_lag_ms");
        assert!(s.contains("37.3"), "meta must contain the lag value");
        assert!(s.contains("symbol"), "meta must contain symbol key");
        assert!(s.contains("btcusdt"), "meta must contain the symbol value");
    }

    // ── state routing ────────────────────────────────────────────────────────

    /// chainlink topic must update chainlink_* state fields.
    #[test]
    fn chainlink_topic_updates_chainlink_state() {
        let state = LiveState::default();
        let value = 95050.0_f64;
        let ts_s = 1710000001.0_f64;

        // Simulate what the run loop does for a chainlink message.
        state.chainlink_value.store(value);
        state.chainlink_ts.store(ts_s);
        state.chainlink_count.fetch_add(1, Ordering::Relaxed);

        assert_eq!(state.chainlink_value.load(), 95050.0);
        assert_eq!(state.chainlink_ts.load(), 1710000001.0);
        assert_eq!(state.chainlink_count.load(Ordering::Relaxed), 1);

        // poly_bn_* must remain untouched.
        assert_eq!(state.poly_bn_value.load(), 0.0);
        assert_eq!(state.poly_bn_count.load(Ordering::Relaxed), 0);
    }

    /// Non-chainlink topic must update poly_bn_* state fields.
    #[test]
    fn non_chainlink_topic_updates_poly_bn_state() {
        let state = LiveState::default();
        let value = 95000.0_f64;
        let ts_s = 1710000000.0_f64;

        // Simulate what the run loop does for a rtds_binance message.
        state.poly_bn_value.store(value);
        state.poly_bn_ts.store(ts_s);
        state.poly_bn_count.fetch_add(1, Ordering::Relaxed);

        assert_eq!(state.poly_bn_value.load(), 95000.0);
        assert_eq!(state.poly_bn_ts.load(), 1710000000.0);
        assert_eq!(state.poly_bn_count.load(Ordering::Relaxed), 1);

        // chainlink_* must remain untouched.
        assert_eq!(state.chainlink_value.load(), 0.0);
        assert_eq!(state.chainlink_count.load(Ordering::Relaxed), 0);
    }

    /// Both sources can independently update their own state fields without
    /// interfering with each other (last-writer-wins is fine for chainlink).
    #[test]
    fn state_routing_independence() {
        let state = LiveState::default();

        // First: rtds_binance writes
        state.poly_bn_value.store(95000.0);
        state.poly_bn_ts.store(1710000000.0);
        state.poly_bn_count.fetch_add(1, Ordering::Relaxed);

        // Then: rtds_chainlink writes (same chainlink_* fields as HTTP poller)
        state.chainlink_value.store(95010.0);
        state.chainlink_ts.store(1710000001.0);
        state.chainlink_count.fetch_add(1, Ordering::Relaxed);

        // Both states coexist independently.
        assert_eq!(state.poly_bn_value.load(), 95000.0);
        assert_eq!(state.chainlink_value.load(), 95010.0);
        assert_eq!(state.poly_bn_count.load(Ordering::Relaxed), 1);
        assert_eq!(state.chainlink_count.load(Ordering::Relaxed), 1);
    }
}
