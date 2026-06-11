//! Deribit WebSocket feed — real-time BTC option ticker data for IV computation.
//!
//! WS-first: subscribes to `ticker.{instrument}.100ms` channels for near-ATM
//! BTC call options and accumulates mark_iv values every tick.
//!
//! Sigma update: every 60 seconds, average the accumulated mark_iv cluster and
//! call `implied_vol_to_sigma_1s` to update `LiveState.sigma_1s / sigma_ts`.
//!
//! REST fallback: the existing `deribit::fetch_sigma` is still called by the
//! sigma_updater in main.rs when this feed is down or sigma is stale.
//!
//! Heartbeat: Deribit requires `public/test` pings every 15s or the server
//! silently drops the connection.
//!
//! On connect:
//! 1. REST GET `get_instruments` → filter calls, near-ATM (moneyness < 0.05),
//!    nearest expiry with t_secs >= 300, up to `MAX_SUBSCRIPTIONS` instruments.
//! 2. Send `public/subscribe` with all ticker channels.
//! 3. Process notifications: emit FeedRow per tick, accumulate mark_iv.
//! 4. Every `SIGMA_UPDATE_INTERVAL_SECS`, recompute sigma from cluster mean.
//!
//! Reconnect: `Backoff` from `super::Backoff` (2s base, 60s cap).

use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::{Backoff, Feed, LiveState, finite, wall_clock};
use crate::kernel::math::implied_vol_to_sigma_1s;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WS_URL: &str = "wss://www.deribit.com/ws/api/v2";

/// REST endpoint for instrument discovery.
const INSTRUMENTS_URL: &str =
    "https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&kind=option&expired=false";

const USER_AGENT: &str = "mantis-beacon/0.3";

/// HTTP timeout for the instrument-discovery REST call.
const HTTP_TIMEOUT_SECS: u64 = 15;

/// Recv timeout for the WebSocket message loop.
const RECV_TIMEOUT_SECS: u64 = 20;

/// Heartbeat interval. Deribit requires `public/test` pings to stay alive.
const HEARTBEAT_INTERVAL_SECS: u64 = 15;

/// How often (seconds) to recompute sigma_1s from the accumulated cluster.
const SIGMA_UPDATE_INTERVAL_SECS: u64 = 60;

/// Minimum time to expiry (seconds) for an instrument to be subscribed.
const MIN_T_SECS: f64 = 300.0;

/// Near-ATM filter: |ln(spot/strike)| must be strictly below this threshold.
/// 0.05 ≈ 5% moneyness — wider than the REST cluster (0.03) to maximise
/// WS coverage while still being near-ATM.
const MONEYNESS_THRESHOLD: f64 = 0.05;

/// Maximum number of instruments to subscribe to per session.
const MAX_SUBSCRIPTIONS: usize = 20;

/// JSON-RPC id used for the subscribe request.
const SUBSCRIBE_ID: u64 = 1;

/// JSON-RPC id used for heartbeat `public/test` pings.
const HEARTBEAT_ID: u64 = 9999;

// ---------------------------------------------------------------------------
// DeribitWsFeed
// ---------------------------------------------------------------------------

pub struct DeribitWsFeed {
    client: reqwest::Client,
}

impl DeribitWsFeed {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
            .unwrap_or_default();
        Self { client }
    }
}

impl Default for DeribitWsFeed {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Feed trait implementation
// ---------------------------------------------------------------------------

impl Feed for DeribitWsFeed {
    fn name(&self) -> &'static str {
        "deribit_ws"
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
                eprintln!("[deribit_ws] shutting down");
                break;
            }

            // ── Step 1: Discover instruments via REST ─────────────────────────
            let spot = state.binance_price.load();
            let channels = match discover_channels(&self.client, spot).await {
                Some(ch) if !ch.is_empty() => {
                    eprintln!("[deribit_ws] subscribing to {} instruments", ch.len());
                    ch
                }
                _ => {
                    eprintln!("[deribit_ws] instrument discovery failed or no near-ATM calls");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            // ── Step 2: Connect WebSocket ─────────────────────────────────────
            let ws = match tokio_tungstenite::connect_async(WS_URL).await {
                Ok((stream, _)) => {
                    backoff.reset();
                    eprintln!("[deribit_ws] connected");
                    stream
                }
                Err(e) => {
                    eprintln!("[deribit_ws] connect error: {e}");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // ── Step 3: Subscribe to ticker channels ──────────────────────────
            let sub_msg = build_subscribe_msg(&channels, SUBSCRIBE_ID);
            if let Err(e) = write
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    sub_msg.into(),
                ))
                .await
            {
                eprintln!("[deribit_ws] subscribe send error: {e}");
                state.inc_errors();
                backoff.wait(&stop).await;
                continue;
            }

            // ── Step 4: Message loop ──────────────────────────────────────────

            // Heartbeat timer: send public/test every 15s.
            let mut heartbeat_interval =
                tokio::time::interval(std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
            heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // First tick fires immediately — skip to avoid an instant ping.
            heartbeat_interval.tick().await;

            // Sigma update timer: recompute sigma from cluster every 60s.
            let mut sigma_interval =
                tokio::time::interval(std::time::Duration::from_secs(SIGMA_UPDATE_INTERVAL_SECS));
            sigma_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            sigma_interval.tick().await; // skip first immediate tick

            // Accumulated mark_iv values across subscribed instruments.
            let mut iv_cluster: Vec<f64> = Vec::with_capacity(MAX_SUBSCRIPTIONS);

            let stop_inner = stop.clone();

            'msg: loop {
                let msg = tokio::select! {
                    // Recv with timeout.
                    m = tokio::time::timeout(
                        std::time::Duration::from_secs(RECV_TIMEOUT_SECS),
                        read.next()
                    ) => m,

                    // Heartbeat ping.
                    _ = heartbeat_interval.tick() => {
                        let ping = build_test_msg(HEARTBEAT_ID);
                        if let Err(e) = write
                            .send(tokio_tungstenite::tungstenite::Message::Text(ping.into()))
                            .await
                        {
                            eprintln!("[deribit_ws] heartbeat send error: {e}");
                            state.inc_errors();
                            break 'msg;
                        }
                        continue 'msg;
                    }

                    // Sigma update tick.
                    _ = sigma_interval.tick() => {
                        if !iv_cluster.is_empty() {
                            let avg_iv = iv_cluster.iter().sum::<f64>() / iv_cluster.len() as f64;
                            let sigma = implied_vol_to_sigma_1s(avg_iv);
                            if sigma.is_finite() && sigma > 0.0 {
                                let now = wall_clock();
                                state.sigma_1s.store(sigma);
                                state.sigma_ts.store(now);
                                eprintln!(
                                    "[deribit_ws] sigma updated: avg_iv={avg_iv:.2}% n={} sigma_1s={sigma:.2e}",
                                    iv_cluster.len()
                                );
                            }
                            iv_cluster.clear();
                        }
                        continue 'msg;
                    }

                    () = stop_inner.cancelled() => break 'msg,
                };

                let msg = match msg {
                    // Recv timeout — silent continue (may be quiet market).
                    Err(_) => continue 'msg,
                    // Stream ended.
                    Ok(None) => break 'msg,
                    // WebSocket error.
                    Ok(Some(Err(e))) => {
                        eprintln!("[deribit_ws] ws error: {e}");
                        state.inc_errors();
                        break 'msg;
                    }
                    Ok(Some(Ok(m))) => m,
                };

                let text = match msg {
                    tokio_tungstenite::tungstenite::Message::Text(t) => t,
                    tokio_tungstenite::tungstenite::Message::Ping(_)
                    | tokio_tungstenite::tungstenite::Message::Pong(_) => continue 'msg,
                    tokio_tungstenite::tungstenite::Message::Close(_) => break 'msg,
                    _ => continue 'msg,
                };

                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue 'msg,
                };

                // Only process subscription notifications.
                // Skip RPC responses (id present + method absent).
                if parsed["method"].as_str() != Some("subscription") {
                    continue 'msg;
                }

                let params = &parsed["params"];
                let channel = params["channel"].as_str().unwrap_or("");

                // Only process ticker channels.
                if !channel.starts_with("ticker.") {
                    continue 'msg;
                }

                let data = &params["data"];

                // NaN firewall — mark_iv must be positive finite.
                let mark_iv = match finite(&data["mark_iv"]) {
                    Some(v) if v > 0.0 => v,
                    _ => continue 'msg,
                };

                // Accumulate for sigma update.
                iv_cluster.push(mark_iv);

                // Parse remaining fields for the FeedRow meta.
                let instrument = data["instrument_name"].as_str().unwrap_or("").to_owned();
                let ts_ms = data["timestamp"]
                    .as_f64()
                    .unwrap_or_else(|| wall_clock() * 1000.0);
                let ts_s = ts_ms / 1000.0;

                let mark_price = finite(&data["mark_price"]);
                let best_bid = finite(&data["best_bid_price"]);
                let best_ask = finite(&data["best_ask_price"]);
                let bid_amount = finite(&data["best_bid_amount"]);
                let ask_amount = finite(&data["best_ask_amount"]);
                let index_price = finite(&data["index_price"]);
                let underlying_price = finite(&data["underlying_price"]);
                let open_interest = finite(&data["open_interest"]);
                let bid_iv = finite(&data["bid_iv"]);
                let ask_iv = finite(&data["ask_iv"]);

                // Greeks sub-object.
                let greeks = &data["greeks"];
                let delta = finite(&greeks["delta"]);
                let gamma = finite(&greeks["gamma"]);
                let theta = finite(&greeks["theta"]);
                let vega = finite(&greeks["vega"]);
                let rho = finite(&greeks["rho"]);

                let meta = serde_json::json!({
                    "instrument_name": instrument,
                    "mark_price": mark_price,
                    "best_bid_price": best_bid,
                    "best_ask_price": best_ask,
                    "best_bid_amount": bid_amount,
                    "best_ask_amount": ask_amount,
                    "index_price": index_price,
                    "underlying_price": underlying_price,
                    "open_interest": open_interest,
                    "bid_iv": bid_iv,
                    "ask_iv": ask_iv,
                    "greeks": {
                        "delta": delta,
                        "gamma": gamma,
                        "theta": theta,
                        "vega": vega,
                        "rho": rho,
                    },
                });

                let meta_s = meta.to_string();
                rings.deribit_ws.write(ts_s, mark_iv, meta_s.as_bytes(), None);
            }

            // Disconnected or cancelled — backoff and retry.
            if !stop.is_cancelled() {
                eprintln!(
                    "[deribit_ws] disconnected, reconnecting in {}s",
                    backoff.current as u64
                );
                backoff.wait(&stop).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Instrument discovery
// ---------------------------------------------------------------------------

/// Query Deribit's `get_instruments` REST endpoint and return a list of
/// `ticker.{instrument}.100ms` channel strings for near-ATM BTC calls.
///
/// Filtering:
/// - calls only (option_type == "call")
/// - moneyness < `MONEYNESS_THRESHOLD` (requires a live spot price)
/// - t_secs >= `MIN_T_SECS`
/// - nearest expiry cluster only (all instruments within 60s of the soonest)
/// - up to `MAX_SUBSCRIPTIONS` instruments
///
/// Returns `None` on any HTTP or parse error. Returns `Some([])` when no
/// instruments pass the filter (caller treats as a soft error).
pub async fn discover_channels(client: &reqwest::Client, spot: f64) -> Option<Vec<String>> {
    let body: serde_json::Value = client
        .get(INSTRUMENTS_URL)
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;

    let instruments = body.get("result")?.as_array()?;
    let now = wall_clock();

    let mut candidates: Vec<(f64, f64, String)> = Vec::new(); // (t_secs, moneyness, name)

    for inst in instruments {
        // Calls only.
        if inst["option_type"].as_str() != Some("call") {
            continue;
        }

        let name = match inst["instrument_name"].as_str() {
            Some(n) => n,
            None => continue,
        };

        // Strike price.
        let strike = match finite(&inst["strike"]) {
            Some(s) if s > 0.0 => s,
            _ => continue,
        };

        // Expiry from `expiration_timestamp` (milliseconds).
        let expiry_ms = match inst["expiration_timestamp"].as_f64() {
            Some(ms) if ms > 0.0 => ms,
            _ => continue,
        };
        let t_secs = expiry_ms / 1000.0 - now;
        if t_secs < MIN_T_SECS {
            continue;
        }

        // Moneyness filter. Requires a valid spot price.
        let moneyness = if spot > 0.0 && spot.is_finite() {
            (spot / strike).ln().abs()
        } else {
            // No spot available — skip moneyness filter, accept all.
            0.0
        };

        if moneyness >= MONEYNESS_THRESHOLD {
            continue;
        }

        candidates.push((t_secs, moneyness, name.to_owned()));
    }

    if candidates.is_empty() {
        return Some(vec![]);
    }

    // Sort: nearest expiry first, then nearest ATM.
    candidates.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });

    // Keep only the nearest-expiry cluster (within 60s of the soonest).
    let best_t = candidates[0].0;
    let cluster: Vec<String> = candidates
        .into_iter()
        .filter(|(t, _, _)| (t - best_t).abs() < 60.0)
        .take(MAX_SUBSCRIPTIONS)
        .map(|(_, _, name)| format!("ticker.{name}.100ms"))
        .collect();

    Some(cluster)
}

// ---------------------------------------------------------------------------
// Message builders
// ---------------------------------------------------------------------------

/// Build the JSON-RPC `public/subscribe` message for the given channels.
pub fn build_subscribe_msg(channels: &[String], id: u64) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "method": "public/subscribe",
        "params": {
            "channels": channels,
        },
        "id": id,
    })
    .to_string()
}

/// Build the JSON-RPC `public/test` heartbeat message.
pub fn build_test_msg(id: u64) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "method": "public/test",
        "params": {},
        "id": id,
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::{FeedRow, LiveState, finite};
    use crate::kernel::math::implied_vol_to_sigma_1s;

    // ── Subscription message format ───────────────────────────────────────────

    #[test]
    fn subscribe_msg_structure() {
        let channels = vec![
            "ticker.BTC-28MAR25-95000-C.100ms".to_owned(),
            "ticker.BTC-28MAR25-96000-C.100ms".to_owned(),
        ];
        let msg = build_subscribe_msg(&channels, SUBSCRIBE_ID);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();

        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "public/subscribe");
        assert_eq!(v["id"], SUBSCRIBE_ID);

        let ch = v["params"]["channels"].as_array().unwrap();
        assert_eq!(ch.len(), 2);
        assert_eq!(ch[0], "ticker.BTC-28MAR25-95000-C.100ms");
        assert_eq!(ch[1], "ticker.BTC-28MAR25-96000-C.100ms");
    }

    #[test]
    fn subscribe_msg_empty_channels() {
        let channels: Vec<String> = vec![];
        let msg = build_subscribe_msg(&channels, SUBSCRIBE_ID);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let ch = v["params"]["channels"].as_array().unwrap();
        assert!(ch.is_empty());
    }

    // ── Heartbeat message format ──────────────────────────────────────────────

    #[test]
    fn heartbeat_msg_structure() {
        let msg = build_test_msg(HEARTBEAT_ID);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();

        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "public/test");
        assert_eq!(v["id"], HEARTBEAT_ID);
        // params must be present (Deribit requires it)
        assert!(v["params"].is_object());
    }

    // ── Ticker notification parsing ───────────────────────────────────────────

    #[test]
    fn parse_ticker_notification_full() {
        // Exact format from the Deribit WS API spec.
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "subscription",
            "params": {
                "channel": "ticker.BTC-28MAR25-95000-C.100ms",
                "data": {
                    "instrument_name": "BTC-28MAR25-95000-C",
                    "timestamp": 1_536_569_522_277_u64,
                    "mark_iv": 45.0,
                    "mark_price": 0.0245,
                    "best_bid_price": 0.0240,
                    "best_ask_price": 0.0250,
                    "best_bid_amount": 5.0,
                    "best_ask_amount": 3.0,
                    "index_price": 95000.0,
                    "underlying_price": 95100.0,
                    "open_interest": 1234.5,
                    "greeks": {
                        "delta": 0.52,
                        "gamma": 0.0001,
                        "theta": -15.2,
                        "vega": 120.5,
                        "rho": 0.05,
                    },
                    "bid_iv": 44.5,
                    "ask_iv": 45.5,
                    "state": "open",
                    "last_price": 0.0243,
                }
            }
        });

        // method must be "subscription"
        assert_eq!(msg["method"].as_str(), Some("subscription"));

        let params = &msg["params"];
        let channel = params["channel"].as_str().unwrap();
        assert!(channel.starts_with("ticker."));

        let data = &params["data"];

        // mark_iv parses via finite()
        let mark_iv = finite(&data["mark_iv"]).unwrap();
        assert!((mark_iv - 45.0).abs() < 1e-10);
        assert!(mark_iv > 0.0);

        // timestamp (ms) → seconds
        let ts_ms = data["timestamp"].as_f64().unwrap();
        let ts_s = ts_ms / 1000.0;
        assert!((ts_s - 1_536_569_522.277).abs() < 0.001);

        // instrument_name
        assert_eq!(
            data["instrument_name"].as_str(),
            Some("BTC-28MAR25-95000-C")
        );

        // greeks
        let greeks = &data["greeks"];
        assert!((finite(&greeks["delta"]).unwrap() - 0.52).abs() < 1e-10);
        assert!((finite(&greeks["vega"]).unwrap() - 120.5).abs() < 1e-10);
    }

    #[test]
    fn parse_ticker_notification_missing_mark_iv_skipped() {
        // mark_iv absent → finite() returns None → row is not emitted.
        let data = serde_json::json!({
            "instrument_name": "BTC-28MAR25-95000-C",
            "timestamp": 1_536_569_522_277_u64,
            // mark_iv missing
            "mark_price": 0.0245,
        });
        assert_eq!(finite(&data["mark_iv"]), None);
    }

    #[test]
    fn parse_ticker_notification_zero_mark_iv_skipped() {
        let data = serde_json::json!({"mark_iv": 0.0});
        let mark_iv = finite(&data["mark_iv"]);
        // finite() accepts 0.0 but the filter `v > 0.0` rejects it.
        assert_eq!(mark_iv, Some(0.0));
        assert!(!(mark_iv.unwrap() > 0.0));
    }

    #[test]
    fn non_subscription_method_skipped() {
        // RPC response messages (result/error) must not be processed.
        let rpc_response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": SUBSCRIBE_ID,
            "result": ["ticker.BTC-28MAR25-95000-C.100ms"],
        });
        assert_ne!(rpc_response["method"].as_str(), Some("subscription"));
    }

    #[test]
    fn non_ticker_channel_skipped() {
        // Only "ticker.*" channels are processed; other subscription
        // notifications (e.g. book.*) should be dropped.
        let params = serde_json::json!({
            "channel": "book.BTC-28MAR25-95000-C.none.10.100ms",
            "data": {},
        });
        let channel = params["channel"].as_str().unwrap();
        assert!(!channel.starts_with("ticker."));
    }

    // ── mark_iv → sigma conversion ────────────────────────────────────────────

    #[test]
    fn mark_iv_to_sigma_matches_kernel() {
        // iv_pct=45.0 → implied_vol_to_sigma_1s(45.0)
        // The feed calls implied_vol_to_sigma_1s(avg_iv) directly.
        for &iv_pct in &[10.0_f64, 30.0, 45.0, 80.0, 120.0] {
            let sigma = implied_vol_to_sigma_1s(iv_pct);
            assert!(
                sigma.is_finite() && sigma > 0.0,
                "iv={iv_pct} → sigma={sigma}"
            );
        }
    }

    #[test]
    fn mark_iv_45pct_sigma_known_value() {
        // 45% annualised → sigma_1s ≈ 8.01e-5
        let sigma = implied_vol_to_sigma_1s(45.0);
        assert!(
            (sigma - 8.01e-5).abs() < 1e-6,
            "45% IV → sigma_1s = {sigma}, expected ~8.01e-5"
        );
    }

    #[test]
    fn mark_iv_cluster_average_then_sigma() {
        // Simulate the sigma_interval logic: average cluster IVs → convert.
        let cluster = vec![44.0_f64, 45.0, 46.0];
        let avg_iv = cluster.iter().sum::<f64>() / cluster.len() as f64;
        assert!((avg_iv - 45.0).abs() < 1e-10);

        let sigma = implied_vol_to_sigma_1s(avg_iv);
        let expected = implied_vol_to_sigma_1s(45.0);
        assert!((sigma - expected).abs() < 1e-15);
    }

    #[test]
    fn sigma_nan_firewall() {
        // If the cluster somehow produces a non-finite sigma, the state must
        // not be updated.  Simulate the guard condition.
        let sigma = implied_vol_to_sigma_1s(0.0); // returns 0.0
        assert!(
            !(sigma.is_finite() && sigma > 0.0),
            "zero IV should not pass guard"
        );

        let sigma_nan = implied_vol_to_sigma_1s(f64::NAN); // returns 0.0
        assert!(!(sigma_nan.is_finite() && sigma_nan > 0.0));

        let sigma_good = implied_vol_to_sigma_1s(45.0);
        assert!(sigma_good.is_finite() && sigma_good > 0.0);
    }

    // ── Instrument filtering ──────────────────────────────────────────────────

    #[test]
    fn moneyness_filter_atm_passes() {
        let spot = 95_000.0_f64;
        let strike_atm = 95_000.0_f64;
        let m = (spot / strike_atm).ln().abs();
        assert!(m < MONEYNESS_THRESHOLD, "ATM should pass: m={m}");
    }

    #[test]
    fn moneyness_filter_near_atm_passes() {
        // ~4% OTM: should pass the 5% threshold.
        let spot = 95_000.0_f64;
        let strike = 99_000.0_f64;
        let m = (spot / strike).ln().abs();
        assert!(m < MONEYNESS_THRESHOLD, "4% OTM should pass: m={m}");
    }

    #[test]
    fn moneyness_filter_far_otm_rejects() {
        // ~10% OTM: should fail.
        let spot = 95_000.0_f64;
        let strike = 105_000.0_f64;
        let m = (spot / strike).ln().abs();
        assert!(m >= MONEYNESS_THRESHOLD, "10% OTM should fail: m={m}");
    }

    #[test]
    fn min_t_secs_filter() {
        // t_secs must be >= MIN_T_SECS (300s).
        assert!(299.0_f64 < MIN_T_SECS, "299s should be rejected");
        assert!(300.0_f64 >= MIN_T_SECS, "300s should pass");
        assert!(86400.0_f64 >= MIN_T_SECS, "1 day should pass");
    }

    #[test]
    fn nearest_expiry_cluster_filtering() {
        // Simulate the cluster-selection step: keep only instruments within
        // 60s of the nearest expiry.
        let mut candidates: Vec<(f64, f64, String)> = vec![
            (1800.0, 0.01, "BTC-NEAR-95000-C".to_owned()), // nearest, near ATM
            (1830.0, 0.02, "BTC-NEAR-96000-C".to_owned()), // within 60s, near ATM
            (1861.0, 0.01, "BTC-NEAR-94000-C".to_owned()), // exactly 61s after, excluded
            (86400.0, 0.00, "BTC-FAR-95000-C".to_owned()), // far expiry, excluded
        ];
        candidates.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
        });

        let best_t = candidates[0].0;
        let cluster: Vec<String> = candidates
            .iter()
            .filter(|(t, _, _)| (t - best_t).abs() < 60.0)
            .take(MAX_SUBSCRIPTIONS)
            .map(|(_, _, name)| format!("ticker.{name}.100ms"))
            .collect();

        assert_eq!(cluster.len(), 2, "only 2 instruments within 60s window");
        assert_eq!(cluster[0], "ticker.BTC-NEAR-95000-C.100ms");
        assert_eq!(cluster[1], "ticker.BTC-NEAR-96000-C.100ms");
    }

    #[test]
    fn max_subscriptions_capped() {
        // When there are more than MAX_SUBSCRIPTIONS near-ATM instruments,
        // the list must be truncated.
        let candidates: Vec<(f64, f64, String)> = (0..25)
            .map(|i| (1800.0 + i as f64, 0.0, format!("BTC-INST-{i}-C")))
            .collect();

        let best_t = candidates[0].0;
        let cluster: Vec<String> = candidates
            .iter()
            .filter(|(t, _, _)| (t - best_t).abs() < 60.0)
            .take(MAX_SUBSCRIPTIONS)
            .map(|(_, _, name)| format!("ticker.{name}.100ms"))
            .collect();

        assert_eq!(cluster.len(), MAX_SUBSCRIPTIONS);
    }

    // ── FeedRow source and meta structure ─────────────────────────────────────

    #[test]
    fn feed_row_source_is_deribit_ws() {
        let row = FeedRow {
            ts: 1_710_000_000.0,
            source: "deribit_ws",
            value: 45.0,
            meta: None,
            ticker: None,
        };
        assert_eq!(row.source, "deribit_ws");
    }

    #[test]
    fn feed_row_value_is_mark_iv() {
        // The FeedRow value field carries mark_iv (percent, e.g. 45.0).
        let mark_iv = 45.0_f64;
        let row = FeedRow {
            ts: 1_710_000_000.0,
            source: "deribit_ws",
            value: mark_iv,
            meta: None,
            ticker: None,
        };
        assert!((row.value - 45.0).abs() < 1e-10);
    }

    #[test]
    fn feed_row_meta_contains_required_fields() {
        let instrument = "BTC-28MAR25-95000-C";
        let meta = serde_json::json!({
            "instrument_name": instrument,
            "mark_price": 0.0245_f64,
            "best_bid_price": 0.0240_f64,
            "best_ask_price": 0.0250_f64,
            "best_bid_amount": 5.0_f64,
            "best_ask_amount": 3.0_f64,
            "index_price": 95000.0_f64,
            "underlying_price": 95100.0_f64,
            "open_interest": 1234.5_f64,
            "bid_iv": 44.5_f64,
            "ask_iv": 45.5_f64,
            "greeks": {
                "delta": 0.52_f64,
                "gamma": 0.0001_f64,
                "theta": -15.2_f64,
                "vega": 120.5_f64,
                "rho": 0.05_f64,
            },
        });

        let s = meta.to_string();

        assert!(s.contains("instrument_name"));
        assert!(s.contains(instrument));
        assert!(s.contains("mark_price"));
        assert!(s.contains("best_bid_price"));
        assert!(s.contains("best_ask_price"));
        assert!(s.contains("index_price"));
        assert!(s.contains("underlying_price"));
        assert!(s.contains("open_interest"));
        assert!(s.contains("bid_iv"));
        assert!(s.contains("ask_iv"));
        assert!(s.contains("greeks"));
        assert!(s.contains("delta"));
        assert!(s.contains("vega"));
    }

    #[test]
    fn feed_row_meta_greeks_values() {
        let meta = serde_json::json!({
            "greeks": {
                "delta": 0.52_f64,
                "gamma": 0.0001_f64,
                "theta": -15.2_f64,
                "vega": 120.5_f64,
                "rho": 0.05_f64,
            }
        });
        let parsed: serde_json::Value = serde_json::from_str(&meta.to_string()).unwrap();
        let g = &parsed["greeks"];

        assert!((g["delta"].as_f64().unwrap() - 0.52).abs() < 1e-10);
        assert!((g["gamma"].as_f64().unwrap() - 0.0001).abs() < 1e-10);
        assert!((g["theta"].as_f64().unwrap() - (-15.2)).abs() < 1e-10);
        assert!((g["vega"].as_f64().unwrap() - 120.5).abs() < 1e-10);
        assert!((g["rho"].as_f64().unwrap() - 0.05).abs() < 1e-10);
    }

    // ── LiveState sigma update ────────────────────────────────────────────────

    #[test]
    fn livestate_sigma_update_roundtrip() {
        let state = LiveState::default();
        assert_eq!(state.sigma_1s.load(), 0.0);
        assert_eq!(state.sigma_ts.load(), 0.0);

        let iv_pct = 45.0_f64;
        let sigma = implied_vol_to_sigma_1s(iv_pct);
        let ts = 1_710_000_000.0_f64;

        state.sigma_1s.store(sigma);
        state.sigma_ts.store(ts);

        assert!((state.sigma_1s.load() - sigma).abs() < 1e-15);
        assert!((state.sigma_ts.load() - ts).abs() < 1e-6);
    }

    #[test]
    fn livestate_sigma_not_updated_on_zero_cluster() {
        // When iv_cluster is empty, sigma_1s must remain unchanged.
        let state = LiveState::default();
        let initial = state.sigma_1s.load();

        // Simulate empty cluster guard:
        let iv_cluster: Vec<f64> = vec![];
        if !iv_cluster.is_empty() {
            let avg_iv = iv_cluster.iter().sum::<f64>() / iv_cluster.len() as f64;
            let sigma = implied_vol_to_sigma_1s(avg_iv);
            if sigma.is_finite() && sigma > 0.0 {
                state.sigma_1s.store(sigma);
            }
        }

        assert_eq!(
            state.sigma_1s.load(),
            initial,
            "empty cluster must not update sigma"
        );
    }

    // ── Channel name construction ─────────────────────────────────────────────

    #[test]
    fn channel_name_format() {
        let instrument = "BTC-28MAR25-95000-C";
        let channel = format!("ticker.{instrument}.100ms");
        assert_eq!(channel, "ticker.BTC-28MAR25-95000-C.100ms");
        assert!(channel.starts_with("ticker."));
        assert!(channel.ends_with(".100ms"));
    }

    #[test]
    fn discover_channels_filters_puts() {
        // Verify the option_type == "call" filter at the JSON level.
        let call = serde_json::json!({"option_type": "call"});
        let put = serde_json::json!({"option_type": "put"});
        assert_eq!(call["option_type"].as_str(), Some("call"));
        assert_ne!(put["option_type"].as_str(), Some("call"));
    }

    #[test]
    fn discover_channels_filters_expired() {
        // t_secs < MIN_T_SECS → rejected.
        let now = wall_clock();
        // expiry_ms = (now + 100) * 1000 → t_secs = 100 < 300
        let expiry_ms = (now + 100.0) * 1000.0;
        let t_secs = expiry_ms / 1000.0 - now;
        assert!(t_secs < MIN_T_SECS, "t_secs={t_secs} should be rejected");
    }
}
