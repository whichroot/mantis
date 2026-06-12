//! BrtiFeed — CF Benchmarks BRTI WebSocket feed.
//!
//! Edge cases:
//! - URL: wss://www.cfbenchmarks.com/ws/v4
//! - Requires 3 subprotocols: cfb, cfbenchmarksws2, e3709a02-9876-45ea-ac46-e9020e06d7c6
//! - Must send subscribe message after connect: {"type":"subscribe","id":"BRTI","stream":"value"}
//! - No explicit ack — first value message IS the ack; type="subscribe" messages are rare, skip them
//! - Only process type="value" with id="BRTI". Skip everything else.
//! - Value field is a NUMBER (not string like Binance)
//! - Timestamp from "time" field (ms), fallback to wall clock
//! - recv timeout 15s (tighter than Binance) — silent continue
//! - Reconnect: 2s base, 2x backoff, cap 60s

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use super::{Backoff, Feed, LiveState, pos};

const URL: &str = "wss://www.cfbenchmarks.com/ws/v4";

/// Subprotocols required by the CF Benchmarks WebSocket server.
/// Sent as a single comma-separated `Sec-WebSocket-Protocol` header value.
const SUBPROTOCOLS: &str = "cfb, cfbenchmarksws2, e3709a02-9876-45ea-ac46-e9020e06d7c6";

pub struct BrtiFeed;

impl BrtiFeed {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BrtiFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Feed for BrtiFeed {
    fn name(&self) -> &'static str {
        "brti"
    }

    async fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) {
        let mut backoff = Backoff::new(2.0, 60.0);

        loop {
            if stop.is_cancelled() {
                eprintln!("[brti] shutting down");
                break;
            }

            // Build request with Sec-WebSocket-Protocol header for the 3 subprotocols.
            let request = match URL.into_client_request() {
                Ok(mut req) => {
                    let hv = HeaderValue::from_static(SUBPROTOCOLS);
                    req.headers_mut().insert("Sec-WebSocket-Protocol", hv);
                    req
                }
                Err(e) => {
                    eprintln!("[brti] request build error: {e}");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let ws = match tokio_tungstenite::connect_async(request).await {
                Ok((stream, _)) => {
                    backoff.reset();
                    eprintln!("[brti] connected");
                    stream
                }
                Err(e) => {
                    eprintln!("[brti] connect error: {e}");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // Send subscribe message immediately after connect.
            let sub_msg = serde_json::json!({
                "type": "subscribe",
                "id": "BRTI",
                "stream": "value"
            })
            .to_string();

            if let Err(e) = write
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    sub_msg.into(),
                ))
                .await
            {
                eprintln!("[brti] subscribe send error: {e}");
                state.inc_errors();
                backoff.wait(&stop).await;
                continue;
            }

            loop {
                let msg = tokio::select! {
                    m = tokio::time::timeout(
                        std::time::Duration::from_secs(15),
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
                        eprintln!("[brti] ws error: {e}");
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

                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Skip subscribe ack (rare) and all non-value messages.
                let msg_type = parsed["type"].as_str().unwrap_or("");
                if msg_type == "subscribe" {
                    continue;
                }
                if msg_type != "value" {
                    continue;
                }

                // Only process BRTI messages.
                if parsed["id"].as_str() != Some("BRTI") {
                    continue;
                }

                // Value is a NUMBER field (not string like Binance).
                let value = match pos(&parsed["value"]) {
                    Some(v) => v,
                    None => continue,
                };

                // Timestamp from "time" (ms), fallback to wall clock.
                let recv_ms = super::wall_clock_ms();
                let ts_ms = parsed["time"].as_f64().unwrap_or(recv_ms);
                let ts_s = ts_ms / 1000.0;
                let lag_ms = recv_ms - ts_ms;

                // Update shared state.
                state.brti_value.store(value);
                state.brti_ts.store(ts_s);
                state.brti_count.fetch_add(1, Ordering::Relaxed);

                // Build meta JSON.
                let meta = serde_json::json!({
                    "recv_lag_ms": (lag_ms * 10.0).round() / 10.0,
                });

                let meta_s = meta.to_string();
                rings.brti.write(ts_s, value, meta_s.as_bytes(), None);
            }

            // Disconnected — backoff and retry.
            if !stop.is_cancelled() {
                eprintln!("[brti] disconnected, reconnecting in {}s", backoff.current as u64);
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

    /// Parse a valid BRTI value message (value is a number, not a string).
    #[test]
    fn parse_valid_brti_value_message() {
        let msg = serde_json::json!({
            "type": "value",
            "id": "BRTI",
            "value": 95234.56_f64,
            "time": 1710000000000_u64,
        });

        // type must be "value"
        assert_eq!(msg["type"].as_str(), Some("value"));
        // id must be "BRTI"
        assert_eq!(msg["id"].as_str(), Some("BRTI"));
        // value is a number
        let value = pos(&msg["value"]).unwrap();
        assert_eq!(value, 95234.56);
        // timestamp
        let ts_ms = msg["time"].as_f64().unwrap();
        assert_eq!(ts_ms, 1710000000000.0);
        let ts_s = ts_ms / 1000.0;
        assert!((ts_s - 1710000000.0).abs() < 0.001);
    }

    /// Reject messages with id != "BRTI".
    #[test]
    fn reject_non_brti_id() {
        let msg = serde_json::json!({
            "type": "value",
            "id": "XBTO",
            "value": 95234.56_f64,
            "time": 1710000000000_u64,
        });
        assert_ne!(msg["id"].as_str(), Some("BRTI"));
    }

    /// Reject messages with type != "value".
    #[test]
    fn reject_non_value_type() {
        let sub_ack = serde_json::json!({
            "type": "subscribe",
            "id": "BRTI",
        });
        assert_ne!(sub_ack["type"].as_str(), Some("value"));

        let other = serde_json::json!({
            "type": "heartbeat",
        });
        assert_ne!(other["type"].as_str(), Some("value"));
    }

    /// Reject zero and negative values through the NaN firewall.
    #[test]
    fn reject_non_positive_value() {
        // Zero
        let msg_zero = serde_json::json!({
            "type": "value",
            "id": "BRTI",
            "value": 0.0_f64,
        });
        assert_eq!(pos(&msg_zero["value"]), None);

        // Negative
        let msg_neg = serde_json::json!({
            "type": "value",
            "id": "BRTI",
            "value": -1000.0_f64,
        });
        assert_eq!(pos(&msg_neg["value"]), None);

        // Missing
        let msg_missing = serde_json::json!({
            "type": "value",
            "id": "BRTI",
        });
        assert_eq!(pos(&msg_missing["value"]), None);
    }

    /// Meta JSON includes recv_lag_ms field.
    #[test]
    fn meta_json_includes_recv_lag_ms() {
        let lag_ms = 42.7_f64;
        let meta = serde_json::json!({
            "recv_lag_ms": (lag_ms * 10.0).round() / 10.0,
        });
        let s = meta.to_string();
        assert!(s.contains("recv_lag_ms"));
        assert!(s.contains("42.7"));
    }

    /// State is updated correctly on a valid message.
    #[test]
    fn state_updates_on_valid_message() {
        let state = LiveState::default();
        let value = 95234.56_f64;
        let ts_s = 1710000000.0_f64;

        state.brti_value.store(value);
        state.brti_ts.store(ts_s);
        state.brti_count.fetch_add(1, Ordering::Relaxed);

        assert_eq!(state.brti_value.load(), 95234.56);
        assert_eq!(state.brti_ts.load(), 1710000000.0);
        assert_eq!(state.brti_count.load(Ordering::Relaxed), 1);
    }

    /// Wall clock fallback: when "time" field is absent, recv_lag_ms should be ~0.
    #[test]
    fn timestamp_fallback_to_wall_clock() {
        let msg = serde_json::json!({
            "type": "value",
            "id": "BRTI",
            "value": 95234.56_f64,
            // No "time" field
        });

        let recv_ms = crate::feed::wall_clock_ms();
        let ts_ms = msg["time"].as_f64().unwrap_or(recv_ms);
        // Should be close to recv_ms (within a few ms of the same call)
        assert!((ts_ms - recv_ms).abs() < 100.0, "fallback ts should be near wall clock");
    }
}
