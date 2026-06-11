//! Binance aggTrade WebSocket — authoritative BTC/USDT spot price.
//!
//! Edge cases from collect.py:
//! - No subscribe needed — data flows on connect
//! - Price field `p` is a STRING, not a number
//! - Timestamp from `T` (trade time ms), NOT `E` (event time)
//! - recv timeout 30s — silent continue, not an error
//! - Reconnect: 2s base, 2x backoff, cap 30s

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;
use futures_util::StreamExt;

use super::{Backoff, Feed, LiveState, pos};

const URLS: &[&str] = &[
    "wss://stream.binance.com:9443/ws/btcusdt@aggTrade",
    "wss://data-stream.binance.vision/ws/btcusdt@aggTrade",
    "wss://stream.binance.us:9443/ws/btcusdt@aggTrade",
];

pub struct BinanceFeed;

impl BinanceFeed {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BinanceFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Feed for BinanceFeed {
    fn name(&self) -> &'static str {
        "binance"
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
                eprintln!("[binance] shutting down");
                break;
            }

            let mut connected = None;
            for url in URLS {
                match tokio_tungstenite::connect_async(*url).await {
                    Ok((stream, _)) => {
                        backoff.reset();
                        eprintln!("[binance] connected to {url}");
                        connected = Some(stream);
                        break;
                    }
                    Err(e) => {
                        eprintln!("[binance] {url} failed: {e}");
                    }
                }
            }
            let ws = match connected {
                Some(s) => s,
                None => {
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let (_, mut read) = ws.split();

            loop {
                let msg = tokio::select! {
                    m = tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        read.next()
                    ) => m,
                    () = stop.cancelled() => break,
                };

                let msg = match msg {
                    // Timeout — silent continue (matches Python behavior)
                    Err(_) => continue,
                    // Stream ended
                    Ok(None) => break,
                    // WebSocket error
                    Ok(Some(Err(e))) => {
                        eprintln!("[binance] ws error: {e}");
                        state.inc_errors();
                        break;
                    }
                    Ok(Some(Ok(m))) => m,
                };

                let text = match msg {
                    tokio_tungstenite::tungstenite::Message::Text(t) => t,
                    tokio_tungstenite::tungstenite::Message::Ping(_) |
                    tokio_tungstenite::tungstenite::Message::Pong(_) => continue,
                    tokio_tungstenite::tungstenite::Message::Close(_) => break,
                    _ => continue,
                };

                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Price is a STRING field "p", not a number
                let price = match pos(&parsed["p"]) {
                    Some(p) => p,
                    None => continue,
                };

                // Timestamp from "T" (trade time ms), fallback to wall clock
                let ts_ms = parsed["T"]
                    .as_f64()
                    .unwrap_or_else(super::wall_clock_ms);
                let ts_s = ts_ms / 1000.0;

                // Update shared state
                state.binance_price.store(price);
                state.binance_ts.store(ts_s);
                state.binance_count.fetch_add(1, Ordering::Relaxed);

                // Build meta JSON
                let meta = serde_json::json!({
                    "trade_id": parsed["a"],
                    "qty": parsed["q"],
                });

                let meta_s = meta.to_string();
                rings.binance.write(ts_s, price, meta_s.as_bytes(), None);
            }

            // Disconnected — backoff and retry
            if !stop.is_cancelled() {
                eprintln!("[binance] disconnected, reconnecting in {}s", backoff.current as u64);
                backoff.wait(&stop).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_agg_trade_message() {
        let msg = serde_json::json!({
            "e": "aggTrade",
            "E": 1710000000123_u64,
            "s": "BTCUSDT",
            "a": 123456789,
            "p": "95123.45",
            "q": "0.001",
            "T": 1710000000120_u64,
            "m": true,
        });

        // Price from string "p"
        let price = pos(&msg["p"]).unwrap();
        assert_eq!(price, 95123.45);

        // Timestamp from "T", not "E"
        let ts_ms = msg["T"].as_f64().unwrap();
        assert_eq!(ts_ms, 1710000000120.0);
        let ts_s = ts_ms / 1000.0;
        assert!((ts_s - 1710000000.12).abs() < 0.001);
    }

    #[test]
    fn parse_missing_price_returns_none() {
        let msg = serde_json::json!({"e": "aggTrade", "T": 1710000000000_u64});
        assert_eq!(pos(&msg["p"]), None);
    }

    #[test]
    fn parse_zero_price_returns_none() {
        let msg = serde_json::json!({"p": "0.0"});
        assert_eq!(pos(&msg["p"]), None);
    }

    #[test]
    fn parse_negative_price_returns_none() {
        let msg = serde_json::json!({"p": "-100.0"});
        assert_eq!(pos(&msg["p"]), None);
    }

    #[test]
    fn state_updated_on_valid_message() {
        let state = LiveState::default();
        let price = 95123.45_f64;
        let ts = 1710000000.12_f64;

        state.binance_price.store(price);
        state.binance_ts.store(ts);
        state.binance_count.fetch_add(1, Ordering::Relaxed);

        assert_eq!(state.binance_price.load(), 95123.45);
        assert_eq!(state.binance_ts.load(), 1710000000.12);
        assert_eq!(state.binance_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn meta_json_structure() {
        let msg = serde_json::json!({
            "a": 123456789,
            "q": "0.001",
        });
        let meta = serde_json::json!({
            "trade_id": msg["a"],
            "qty": msg["q"],
        });
        let s = meta.to_string();
        assert!(s.contains("trade_id"));
        assert!(s.contains("123456789"));
        assert!(s.contains("0.001"));
    }
}
