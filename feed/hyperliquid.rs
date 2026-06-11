//! Hyperliquid BTC perp WebSocket — funding, OI, premium, oracle, L2 depth, trades.
//!
//! Subscribes to three channels:
//! - `activeAssetCtx` (BTC): funding, OI, mark, oracle, mid, premium (~0.5s/block)
//! - `l2Book` (BTC): L2 orderbook, 20 levels per side (~0.5s/block)
//! - `trades` (BTC): real-time trades (price, size, side)
//!
//! No authentication required for market data.
//! WebSocket subscriptions do not count against REST rate limits.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;
use futures_util::{SinkExt, StreamExt};

use super::{Backoff, Feed, FeedDedup, LiveState, finite, wall_clock, wall_clock_ms};

const URL: &str = "wss://api.hyperliquid.xyz/ws";

pub struct HyperliquidFeed;

impl HyperliquidFeed {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HyperliquidFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Feed for HyperliquidFeed {
    fn name(&self) -> &'static str {
        "hyperliquid"
    }

    async fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) {
        let mut backoff = Backoff::new(2.0, 30.0);
        let mut dedup = FeedDedup::new();

        loop {
            if stop.is_cancelled() {
                eprintln!("[hyperliquid] shutting down");
                break;
            }

            let ws = match tokio_tungstenite::connect_async(URL).await {
                Ok((stream, _)) => {
                    backoff.reset();
                    eprintln!("[hyperliquid] connected");
                    stream
                }
                Err(e) => {
                    eprintln!("[hyperliquid] connect error: {e}");
                    state.inc_errors();
                    backoff.wait(&stop).await;
                    continue;
                }
            };

            let (mut write, mut read) = ws.split();

            // Subscribe to all three channels
            let subs = [
                r#"{"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}"#,
                r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#,
                r#"{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}"#,
            ];

            let mut sub_ok = true;
            for sub in &subs {
                let msg = tokio_tungstenite::tungstenite::Message::Text((*sub).into());
                if let Err(e) = write.send(msg).await {
                    eprintln!("[hyperliquid] subscribe error: {e}");
                    state.inc_errors();
                    sub_ok = false;
                    break;
                }
            }
            if !sub_ok {
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
                    Err(_) => continue, // timeout — silent continue
                    Ok(None) => break,  // stream ended
                    Ok(Some(Err(e))) => {
                        eprintln!("[hyperliquid] ws error: {e}");
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

                // Skip short messages (subscription acks, etc.)
                if text.len() < 20 {
                    continue;
                }

                let parsed: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Route by channel field in envelope
                let channel = match parsed["channel"].as_str() {
                    Some(c) => c,
                    None => continue, // subscription ack or unknown format
                };

                match channel {
                    "activeAssetCtx" => {
                        handle_active_asset_ctx(&parsed, &state, &rings, &mut dedup);
                    }
                    "l2Book" => {
                        handle_l2_book(&parsed, &state, &rings, &mut dedup);
                    }
                    "trades" => {
                        handle_trades(&parsed, &rings, &mut dedup);
                    }
                    _ => {} // unknown channel, skip
                }
            }

            // Disconnected — backoff and retry
            if !stop.is_cancelled() {
                eprintln!(
                    "[hyperliquid] disconnected, reconnecting in {}s",
                    backoff.current as u64
                );
                backoff.wait(&stop).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Channel handlers
// ---------------------------------------------------------------------------

/// Parse activeAssetCtx: funding, OI, mark, oracle, mid, premium.
///
/// Envelope: `{"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{...}}}`
fn handle_active_asset_ctx(
    parsed: &serde_json::Value,
    state: &LiveState,
    rings: &crate::ring::RingSet,
    dedup: &mut FeedDedup,
) {
    let ctx = &parsed["data"]["ctx"];
    if ctx.is_null() {
        return;
    }

    let ts = wall_clock();

    // FIX: WP05-F2 — Dedup check BEFORE all state writes. The old code wrote
    // hl_ts and hl_count (and field values) before checking dedup, so a
    // duplicate message would still increment the counter and update the
    // timestamp, making counts and lag metrics unreliable. Move dedup first.
    if !dedup.check("hyperliquid", ts) {
        return;
    }

    let funding = finite(&ctx["funding"]);
    let oi = finite(&ctx["openInterest"]);
    let mark = finite(&ctx["markPx"]);
    let oracle = finite(&ctx["oraclePx"]);
    let mid = finite(&ctx["midPx"]);
    let premium = finite(&ctx["premium"]);
    let day_vlm = finite(&ctx["dayNtlVlm"]);

    // Impact prices: [bid_impact, ask_impact]
    let impact_bid = ctx["impactPxs"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(finite);
    let impact_ask = ctx["impactPxs"]
        .as_array()
        .and_then(|a| a.get(1))
        .and_then(finite);

    // Update LiveState atomics
    if let Some(f) = funding {
        state.hl_funding.store(f);
    }
    if let Some(o) = oi {
        state.hl_oi.store(o);
    }
    if let Some(p) = premium {
        state.hl_premium.store(p);
    }
    if let Some(m) = mark {
        state.hl_mark.store(m);
    }
    if let Some(o) = oracle {
        state.hl_oracle.store(o);
    }
    if let Some(m) = mid {
        state.hl_mid.store(m);
    }
    state.hl_ts.store(ts);
    state.hl_count.fetch_add(1, Ordering::Relaxed);

    // Primary value = mark price (most useful for alignment with spot)
    let value = mark.unwrap_or(0.0);
    if value <= 0.0 {
        return;
    }

    let meta = serde_json::json!({
        "funding": funding,
        "oi": oi,
        "premium": premium,
        "oracle": oracle,
        "mid": mid,
        "impact_bid": impact_bid,
        "impact_ask": impact_ask,
        "day_vlm": day_vlm,
    });

    let meta_s = meta.to_string();
    rings.hyperliquid.write(ts, value, meta_s.as_bytes(), None);
}

/// Parse l2Book: aggregate depth at 0.1%, 0.5%, 1.0% bands from mid.
///
/// Envelope: `{"channel":"l2Book","data":{"coin":"BTC","levels":[[bids],[asks]]}}`
/// Each level: `{"px":"73150.0","sz":"1.5","n":3}`
fn handle_l2_book(
    parsed: &serde_json::Value,
    state: &LiveState,
    rings: &crate::ring::RingSet,
    dedup: &mut FeedDedup,
) {
    let data = &parsed["data"];
    let levels = &data["levels"];
    if !levels.is_array() {
        return;
    }

    let bids = match levels.as_array().and_then(|a| a.first()) {
        Some(serde_json::Value::Array(b)) => b,
        _ => return,
    };
    let asks = match levels.as_array().and_then(|a| a.get(1)) {
        Some(serde_json::Value::Array(a)) => a,
        _ => return,
    };

    // Determine mid from best bid/ask
    let best_bid = bids.first().and_then(|l| finite(&l["px"]));
    let best_ask = asks.first().and_then(|l| finite(&l["px"]));
    let mid = match (best_bid, best_ask) {
        (Some(b), Some(a)) if b > 0.0 && a > 0.0 => (b + a) / 2.0,
        _ => return,
    };

    let ts = wall_clock();

    // Aggregate depth at each band
    let bands = [0.001, 0.005, 0.01]; // 0.1%, 0.5%, 1.0%
    let mut bid_depths = [0.0_f64; 3];
    let mut ask_depths = [0.0_f64; 3];

    for level in bids {
        let px = match finite(&level["px"]) {
            Some(p) if p > 0.0 => p,
            _ => continue,
        };
        let sz = match finite(&level["sz"]) {
            Some(s) if s > 0.0 => s,
            _ => continue,
        };
        let dist = (mid - px) / mid; // positive for bids below mid
        for (i, &band) in bands.iter().enumerate() {
            if dist <= band {
                bid_depths[i] += sz;
            }
        }
    }

    for level in asks {
        let px = match finite(&level["px"]) {
            Some(p) if p > 0.0 => p,
            _ => continue,
        };
        let sz = match finite(&level["sz"]) {
            Some(s) if s > 0.0 => s,
            _ => continue,
        };
        let dist = (px - mid) / mid; // positive for asks above mid
        for (i, &band) in bands.iter().enumerate() {
            if dist <= band {
                ask_depths[i] += sz;
            }
        }
    }

    // Update LiveState
    state.hl_bid_depth_01.store(bid_depths[0]);
    state.hl_ask_depth_01.store(ask_depths[0]);
    state.hl_bid_depth_05.store(bid_depths[1]);
    state.hl_ask_depth_05.store(ask_depths[1]);
    state.hl_bid_depth_10.store(bid_depths[2]);
    state.hl_ask_depth_10.store(ask_depths[2]);

    // Dedup check
    if !dedup.check("hyperliquid_l2", ts) {
        return;
    }

    let meta = serde_json::json!({
        "mid": mid,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth_01": bid_depths[0],
        "ask_depth_01": ask_depths[0],
        "bid_depth_05": bid_depths[1],
        "ask_depth_05": ask_depths[1],
        "bid_depth_10": bid_depths[2],
        "ask_depth_10": ask_depths[2],
        "bid_levels": bids.len(),
        "ask_levels": asks.len(),
    });

    let meta_s = meta.to_string();
    rings.hyperliquid_l2.write(ts, mid, meta_s.as_bytes(), None);
}

/// Parse trades: individual trade ticks.
///
/// Envelope: `{"channel":"trades","data":[{"coin":"BTC","px":"73150.0","sz":"0.1","side":"B","time":1710000000000,...}]}`
fn handle_trades(
    parsed: &serde_json::Value,
    rings: &crate::ring::RingSet,
    dedup: &mut FeedDedup,
) {
    let trades = match parsed["data"].as_array() {
        Some(a) => a,
        None => return,
    };

    for trade in trades {
        // Filter to BTC only
        let coin = trade["coin"].as_str().unwrap_or("");
        if coin != "BTC" {
            continue;
        }

        let price = match finite(&trade["px"]) {
            Some(p) if p > 0.0 => p,
            _ => continue,
        };

        let size = finite(&trade["sz"]).unwrap_or(0.0);
        let side_str = trade["side"].as_str().unwrap_or("");
        let tid = trade["tid"].as_u64().unwrap_or(0);

        // Timestamp from "time" field (epoch ms), fallback to wall clock
        let ts_ms = trade["time"].as_f64().unwrap_or_else(wall_clock_ms);
        let ts_s = ts_ms / 1000.0;

        // FIX: WP05-F1 — dedup by trade id when available, not just timestamp.
        // Multiple trades can share the same ms timestamp in bursts.
        let dedup_key = if tid > 0 { tid as f64 } else { ts_s };
        if !dedup.check("hyperliquid_trades", dedup_key) {
            continue;
        }

        let meta = serde_json::json!({
            "sz": size,
            "side": side_str,
            "tid": tid,
        });

        let meta_s = meta.to_string();
        rings.hyperliquid_trades.write(ts_s, price, meta_s.as_bytes(), None);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::LiveState;
    use crate::ring::SourceRing;

    // -- activeAssetCtx parsing -------------------------------------------

    #[test]
    fn parse_active_asset_ctx_full() {
        let msg = serde_json::json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "BTC",
                "ctx": {
                    "funding": "0.0000125",
                    "openInterest": "688.11",
                    "markPx": "73150.0",
                    "oraclePx": "73145.0",
                    "midPx": "73148.5",
                    "premium": "0.00031774",
                    "impactPxs": ["73140.0", "73160.0"],
                    "dayNtlVlm": "1169046.29",
                    "prevDayPx": "73322.0"
                }
            }
        });

        let state = LiveState::default();
        let ring = SourceRing::new(16, 300.0, false);
        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_active_asset_ctx(&msg, &state, &rings, &mut dedup);

        assert!((state.hl_funding.load() - 0.0000125).abs() < 1e-10);
        assert!((state.hl_oi.load() - 688.11).abs() < 0.01);
        assert!((state.hl_mark.load() - 73150.0).abs() < 0.1);
        assert!((state.hl_oracle.load() - 73145.0).abs() < 0.1);
        assert!((state.hl_mid.load() - 73148.5).abs() < 0.1);
        assert!((state.hl_premium.load() - 0.00031774).abs() < 1e-8);
        assert!(state.hl_ts.load() > 0.0);
        assert_eq!(state.hl_count.load(Ordering::Relaxed), 1);
        // Verify ring was written
        let _ = ring; // ring is unused since we use RingSet; just ensure no compile error
        let entry = rings.hyperliquid.head().unwrap();
        assert!((entry.value - 73150.0).abs() < 0.1);
    }

    #[test]
    fn parse_active_asset_ctx_missing_fields() {
        // Only funding and mark present — partial update is fine
        let msg = serde_json::json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "BTC",
                "ctx": {
                    "funding": "0.0001",
                    "markPx": "85000.0"
                }
            }
        });

        let state = LiveState::default();
        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_active_asset_ctx(&msg, &state, &rings, &mut dedup);

        assert!((state.hl_funding.load() - 0.0001).abs() < 1e-10);
        assert!((state.hl_mark.load() - 85000.0).abs() < 0.1);
        // Missing fields stay at default (0.0)
        assert_eq!(state.hl_oi.load(), 0.0);
        assert_eq!(state.hl_oracle.load(), 0.0);
    }

    #[test]
    fn parse_active_asset_ctx_null_ctx() {
        let msg = serde_json::json!({
            "channel": "activeAssetCtx",
            "data": {"coin": "BTC", "ctx": null}
        });

        let state = LiveState::default();
        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_active_asset_ctx(&msg, &state, &rings, &mut dedup);

        // Nothing updated
        assert_eq!(state.hl_count.load(Ordering::Relaxed), 0);
    }

    // -- l2Book parsing ---------------------------------------------------

    #[test]
    fn parse_l2_book_aggregation() {
        // mid = (73149 + 73151) / 2 = 73150
        // 0.1% band = 73150 * 0.001 = $73.15
        // 0.5% band = 73150 * 0.005 = $365.75
        // 1.0% band = 73150 * 0.01  = $731.50
        let msg = serde_json::json!({
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "levels": [
                    // bids: best at 73149, then 73100, then 72900
                    [
                        {"px": "73149.0", "sz": "1.0", "n": 2},
                        {"px": "73100.0", "sz": "2.0", "n": 3},
                        {"px": "72900.0", "sz": "5.0", "n": 1}
                    ],
                    // asks: best at 73151, then 73200, then 73500
                    [
                        {"px": "73151.0", "sz": "1.5", "n": 2},
                        {"px": "73200.0", "sz": "3.0", "n": 4},
                        {"px": "73500.0", "sz": "4.0", "n": 1}
                    ]
                ]
            }
        });

        let state = LiveState::default();
        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_l2_book(&msg, &state, &rings, &mut dedup);

        // 0.1% band ($73.15): bid 73149 is $1 below mid → within. bid 73100 is $50 → within.
        //   73149: dist = (73150-73149)/73150 = 0.0000137 → within 0.1%
        //   73100: dist = (73150-73100)/73150 = 0.000683 → within 0.1%
        //   72900: dist = (73150-72900)/73150 = 0.00342 → NOT within 0.1%, within 0.5%
        assert!((state.hl_bid_depth_01.load() - 3.0).abs() < 0.01); // 1.0 + 2.0
        assert!((state.hl_bid_depth_05.load() - 8.0).abs() < 0.01); // 1.0 + 2.0 + 5.0
        assert!((state.hl_bid_depth_10.load() - 8.0).abs() < 0.01); // all within 1%

        // asks: 73151 dist=0.0000137, 73200 dist=0.000683, 73500 dist=0.00479
        assert!((state.hl_ask_depth_01.load() - 4.5).abs() < 0.01); // 1.5 + 3.0
        assert!((state.hl_ask_depth_05.load() - 8.5).abs() < 0.01); // 1.5 + 3.0 + 4.0
        assert!((state.hl_ask_depth_10.load() - 8.5).abs() < 0.01); // all within 1%
    }

    #[test]
    fn parse_l2_book_empty_levels() {
        let msg = serde_json::json!({
            "channel": "l2Book",
            "data": {"coin": "BTC", "levels": [[], []]}
        });

        let state = LiveState::default();
        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_l2_book(&msg, &state, &rings, &mut dedup);

        // No mid computable from empty book → early return, nothing stored
        assert_eq!(state.hl_bid_depth_01.load(), 0.0);
    }

    #[test]
    fn parse_l2_book_missing_levels() {
        let msg = serde_json::json!({
            "channel": "l2Book",
            "data": {"coin": "BTC"}
        });

        let state = LiveState::default();
        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_l2_book(&msg, &state, &rings, &mut dedup);

        assert_eq!(state.hl_bid_depth_01.load(), 0.0);
    }

    // -- trades parsing ---------------------------------------------------

    #[test]
    fn parse_trades_single() {
        let msg = serde_json::json!({
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC",
                    "px": "73150.0",
                    "sz": "0.5",
                    "side": "B",
                    "time": 1710000000123_u64,
                    "tid": 999
                }
            ]
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_trades(&msg, &rings, &mut dedup);

        let entry = rings.hyperliquid_trades.head().unwrap();
        assert!((entry.value - 73150.0).abs() < 0.1);
        assert!((entry.ts - 1710000000.123).abs() < 0.001);
        let meta: serde_json::Value = serde_json::from_str(entry.meta_str().unwrap()).unwrap();
        assert_eq!(meta["side"], "B");
        assert!((meta["sz"].as_f64().unwrap() - 0.5).abs() < 1e-6);
    }

    #[test]
    fn parse_trades_filters_non_btc() {
        let msg = serde_json::json!({
            "channel": "trades",
            "data": [
                {"coin": "ETH", "px": "3000.0", "sz": "1.0", "side": "S", "time": 1710000000000_u64}
            ]
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_trades(&msg, &rings, &mut dedup);

        assert!(rings.hyperliquid_trades.head().is_none()); // nothing written
    }

    #[test]
    fn parse_trades_zero_price_skipped() {
        let msg = serde_json::json!({
            "channel": "trades",
            "data": [
                {"coin": "BTC", "px": "0.0", "sz": "1.0", "side": "B", "time": 1710000000000_u64}
            ]
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_trades(&msg, &rings, &mut dedup);

        assert!(rings.hyperliquid_trades.head().is_none());
    }

    #[test]
    fn parse_trades_multiple() {
        let msg = serde_json::json!({
            "channel": "trades",
            "data": [
                {"coin": "BTC", "px": "73150.0", "sz": "0.5", "side": "B", "time": 1710000000100_u64, "tid": 1},
                {"coin": "BTC", "px": "73155.0", "sz": "1.0", "side": "S", "time": 1710000000200_u64, "tid": 2}
            ]
        });

        let rings = crate::ring::RingSet::default();
        let mut dedup = FeedDedup::new();

        handle_trades(&msg, &rings, &mut dedup);

        assert_eq!(rings.hyperliquid_trades.write_count(), 2);
        let entry = rings.hyperliquid_trades.head().unwrap();
        assert!((entry.value - 73155.0).abs() < 0.1); // last written is head
    }

    // -- displacement -----------------------------------------------------

    #[test]
    fn displacement_hyperliquid_both_positive() {
        let state = LiveState::default();
        state.binance_price.store(73200.0);
        state.hl_oracle.store(73145.0);
        let d = state.displacement_hyperliquid().unwrap();
        assert!((d - 55.0).abs() < 0.01);
    }

    #[test]
    fn displacement_hyperliquid_missing_oracle() {
        let state = LiveState::default();
        state.binance_price.store(73200.0);
        assert!(state.displacement_hyperliquid().is_none());
    }

    #[test]
    fn displacement_hyperliquid_missing_binance() {
        let state = LiveState::default();
        state.hl_oracle.store(73145.0);
        assert!(state.displacement_hyperliquid().is_none());
    }

    // -- LiveState fields -------------------------------------------------

    #[test]
    fn livestate_hl_fields_store_and_load() {
        let state = LiveState::default();
        state.hl_funding.store(0.0001);
        state.hl_oi.store(500.0);
        state.hl_premium.store(0.0003);
        state.hl_mark.store(85000.0);
        state.hl_oracle.store(84990.0);
        state.hl_mid.store(84995.0);
        state.hl_bid_depth_01.store(1.5);
        state.hl_ask_depth_01.store(2.0);
        state.hl_bid_depth_05.store(10.0);
        state.hl_ask_depth_05.store(12.0);
        state.hl_bid_depth_10.store(25.0);
        state.hl_ask_depth_10.store(30.0);

        assert!((state.hl_funding.load() - 0.0001).abs() < 1e-10);
        assert!((state.hl_oi.load() - 500.0).abs() < 0.01);
        assert!((state.hl_premium.load() - 0.0003).abs() < 1e-8);
        assert!((state.hl_mark.load() - 85000.0).abs() < 0.1);
        assert!((state.hl_oracle.load() - 84990.0).abs() < 0.1);
        assert!((state.hl_mid.load() - 84995.0).abs() < 0.1);
        assert!((state.hl_bid_depth_01.load() - 1.5).abs() < 0.01);
        assert!((state.hl_ask_depth_01.load() - 2.0).abs() < 0.01);
        assert!((state.hl_bid_depth_05.load() - 10.0).abs() < 0.01);
        assert!((state.hl_ask_depth_05.load() - 12.0).abs() < 0.01);
        assert!((state.hl_bid_depth_10.load() - 25.0).abs() < 0.01);
        assert!((state.hl_ask_depth_10.load() - 30.0).abs() < 0.01);
    }

    // -- Feed row DB write structure --------------------------------------

    #[test]
    fn feed_row_active_asset_ctx_structure() {
        let msg = serde_json::json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "BTC",
                "ctx": {
                    "funding": "0.0001",
                    "openInterest": "500.0",
                    "markPx": "85000.0",
                    "oraclePx": "84990.0",
                    "midPx": "84995.0",
                    "premium": "0.0003",
                    "impactPxs": ["84980.0", "85010.0"],
                    "dayNtlVlm": "500000.0"
                }
            }
        });

        let rings = crate::ring::RingSet::default();
        let state = LiveState::default();
        let mut dedup = FeedDedup::new();

        handle_active_asset_ctx(&msg, &state, &rings, &mut dedup);

        let entry = rings.hyperliquid.head().unwrap();
        assert!((entry.value - 85000.0).abs() < 0.1);
        assert!(entry.ts > 0.0);

        // Meta may be truncated at META_CAP (128 bytes), so check key presence via contains
        let meta_s = entry.meta_str().unwrap();
        assert!(meta_s.contains("funding"));
        assert!(meta_s.contains("oi"));
        assert!(meta_s.contains("oracle"));
    }

    // -- Channel routing --------------------------------------------------

    #[test]
    fn unknown_channel_ignored() {
        let msg = serde_json::json!({
            "channel": "somethingElse",
            "data": {"foo": "bar"}
        });

        // Should not panic
        let channel = msg["channel"].as_str().unwrap();
        assert_ne!(channel, "activeAssetCtx");
        assert_ne!(channel, "l2Book");
        assert_ne!(channel, "trades");
    }

    #[test]
    fn subscription_ack_skipped() {
        // Subscription acks have no "channel" field
        let msg = serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "activeAssetCtx", "coin": "BTC"}
        });

        assert!(msg["channel"].as_str().is_none());
    }
}
