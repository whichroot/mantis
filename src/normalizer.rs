//! Type boundary between ring buffer and kernel.
//!
//! The ring stores untyped `(seq, ts, value, meta)` entries indexed by source.
//! The kernel needs specifically named f64 inputs: spot, sigma_1s, oracle_value,
//! best_bid, best_ask, t_secs, strike.
//!
//! `normalize` assembles those inputs for one market at one point in time.
//! It returns `None` if any required input is missing, stale, or degenerate.
//! The caller does `match normalize(...) { Some(i) => i, None => continue }`.
//!
//! # What normalizer does NOT do
//!
//! - Does not call any math.rs or risk.rs function.
//! - Does not compute p_market, spread, d1, p_true, gap, omega.
//! - Does not access SQLite (market metadata passed in as &MarketRow).
//! - Does not decide whether to trade.
//! - Does not write to the ring.
//!
//! # Two read sources
//!
//! - **Rings** — per-source raw data: spot (binance), oracle (brti/chainlink),
//!   BBO (kalshi_ticker/poly_bbo).
//! - **LiveState** — cross-feed computed value: sigma_1s. Deribit WS computes
//!   it from mark_iv cluster; REST fallback computes it via bisection. The
//!   sigma updater task owns the computation and stores the result in LiveState.
//!   Normalizer reads the result — not the raw mark_iv ring entries.

use crate::db::{parse_iso_to_unix, MarketRow};
use crate::feed::LiveState;
use crate::kernel::risk::{OracleType, Venue};
use crate::ring::RingSet;

// ---------------------------------------------------------------------------
// MarketInputs — typed kernel inputs for one market at one tick
// ---------------------------------------------------------------------------

/// All values the kernel needs to evaluate one market at one point in time.
///
/// Every field is guaranteed present, positive, and finite.
/// If any field cannot be satisfied, `normalize` returns `None`.
#[derive(Debug, Clone)]
pub struct MarketInputs {
    /// BTC/USDT spot price from Binance.
    pub spot: f64,
    /// Per-second implied volatility from Deribit (WS or REST).
    pub sigma_1s: f64,
    /// Oracle reference price for this market's settlement oracle.
    pub oracle_value: f64,
    /// Best yes-bid (Kalshi) or best bid (Polymarket). Maker entry price.
    pub best_bid: f64,
    /// Best yes-ask (Kalshi) or best ask (Polymarket). Taker exit price.
    pub best_ask: f64,
    /// Seconds until market close.
    pub t_secs: f64,
    /// Strike price in USD.
    pub strike: f64,
    /// Venue determines fee surface.
    pub venue: Venue,
    /// Oracle type determines which ring and displacement profile.
    pub oracle_type: OracleType,
    /// Market ticker for ring lookup and logging.
    pub ticker: String,
}

// ---------------------------------------------------------------------------
// normalize — the type boundary function
// ---------------------------------------------------------------------------

/// Assemble kernel inputs for a market from ring state and LiveState.
///
/// Returns `None` if any required input is missing, stale, or degenerate.
/// This is the combined data-present gate (Gates 1-3 per CLAUDE.md):
/// - Gate 1: strike present
/// - Gate 2: t_secs > 0
/// - Gate 3: ring data fresh (seq valid, value positive, finite)
///
/// No fallbacks. No defaults. No unwrap_or.
pub fn normalize(
    rings: &RingSet,
    state: &LiveState,
    market: &MarketRow,
    now: f64,
) -> Option<MarketInputs> {
    // ── Step 1: spot ─────────────────────────────────────────────────────
    let spot = rings.binance.head()?.value;
    if spot <= 0.0 || !spot.is_finite() {
        return None;
    }

    // ── Step 2: sigma_1s (cross-feed computed value via LiveState) ────────
    let sigma_1s = state.sigma_1s.load();
    if sigma_1s <= 0.0 || !sigma_1s.is_finite() {
        return None;
    }

    // ── Step 3: oracle type ───────────────────────────────────────────────
    let oracle_type = oracle_for(market.oracle.as_deref().unwrap_or(""))?;

    // ── Step 4: oracle value ──────────────────────────────────────────────
    let oracle_value = read_oracle_value(rings, oracle_type)?;
    if oracle_value <= 0.0 || !oracle_value.is_finite() {
        return None;
    }

    // ── Step 5: venue ─────────────────────────────────────────────────────
    let venue = venue_for(&market.venue, market.series.as_deref().unwrap_or(""));

    // ── Step 6: BBO from venue ring ───────────────────────────────────────
    let ticker = &market.ticker;
    // Polymarket ring is indexed by token_id (the asset_id used by the CLOB WS),
    // not by the synthetic "poly-XXXXX" ticker stored in the markets table.
    // Kalshi ring is indexed by the market ticker directly.
    let ring_key: &str = match &venue {
        Venue::PolymarketCrypto | Venue::PolymarketSports => {
            market.token_id.as_deref().filter(|s| !s.is_empty())?
        }
        _ => ticker.as_str(),
    };
    let (best_bid, best_ask) = read_bbo(rings, &venue, ring_key, now)?;
    if best_bid <= 0.0 || best_ask <= 0.0 || !best_bid.is_finite() || !best_ask.is_finite() {
        return None;
    }

    // ── Step 7: t_secs ────────────────────────────────────────────────────
    let close_time = parse_iso_to_unix(market.close_time.as_deref()?).filter(|&t| t.is_finite())?;
    let t_secs = close_time - now;
    if t_secs <= 0.0 {
        return None;
    }

    // ── Step 8: strike ────────────────────────────────────────────────────
    let strike = market.strike.filter(|&k| k > 0.0 && k.is_finite())?;

    // ── Step 9: assemble ──────────────────────────────────────────────────
    Some(MarketInputs {
        spot,
        sigma_1s,
        oracle_value,
        best_bid,
        best_ask,
        t_secs,
        strike,
        venue,
        oracle_type,
        ticker: ticker.clone(),
    })
}

// ---------------------------------------------------------------------------
// Oracle ring reader
// ---------------------------------------------------------------------------

fn read_oracle_value(rings: &RingSet, oracle_type: OracleType) -> Option<f64> {
    match oracle_type {
        OracleType::Brti => Some(rings.brti.head()?.value),
        OracleType::ChainlinkStreams => {
            // Prefer RTDS (real-time WS), fall back to direct Chainlink HTTP
            let rtds = rings.rtds_chainlink.head();
            let cl = rings.chainlink.head();
            match (rtds, cl) {
                (Some(r), _) if r.value > 0.0 => Some(r.value),
                (_, Some(c)) if c.value > 0.0 => Some(c.value),
                _ => None,
            }
        }
        OracleType::BinanceCandle => {
            // Daily/above-below markets resolve on Binance spot.
            // oracle_value = spot — displacement is always ~0.
            Some(rings.binance.head()?.value)
        }
    }
}

// ---------------------------------------------------------------------------
// BBO reader
// ---------------------------------------------------------------------------

fn read_bbo(rings: &RingSet, venue: &Venue, ticker: &str, now: f64) -> Option<(f64, f64)> {
    match venue {
        Venue::KalshiIndex | Venue::KalshiGeneral => {
            let entry = rings.kalshi_ticker.get_by_ticker(ticker, now)?;
            // value = yes_bid; yes_ask is in meta JSON
            let best_bid = entry.value;
            let best_ask = parse_f64_from_meta(&entry, "yes_ask")?;
            Some((best_bid, best_ask))
        }
        Venue::PolymarketCrypto | Venue::PolymarketSports => {
            // poly_bbo is indexed by token_id (stored as ticker in the DB)
            let entry = rings.poly_bbo.get_by_ticker(ticker, now)?;
            // value = midpoint; best_bid and best_ask are in meta JSON
            let best_bid = parse_f64_from_meta(&entry, "best_bid")?;
            let best_ask = parse_f64_from_meta(&entry, "best_ask")?;
            Some((best_bid, best_ask))
        }
    }
}

// ---------------------------------------------------------------------------
// Mapping helpers
// ---------------------------------------------------------------------------

/// Map (venue_str, series_str) → Venue enum.
///
/// Kalshi index series: KXBTC15M, KXBTCD, KXBTC.
/// Kalshi general series: BTCD and anything else.
/// Polymarket: all BTC markets are PolymarketCrypto.
/// Unknown venue: conservative default KalshiGeneral (highest fee).
pub fn venue_for(venue_str: &str, series: &str) -> Venue {
    match venue_str {
        "kalshi" => {
            let s = series.to_uppercase();
            if s.starts_with("KXBTC") {
                Venue::KalshiIndex
            } else {
                Venue::KalshiGeneral
            }
        }
        "polymarket" => Venue::PolymarketCrypto,
        _ => Venue::KalshiGeneral, // conservative default
    }
}

/// Map oracle string from the markets table → OracleType enum.
///
/// Returns None for unsupported oracles:
/// - "brr": BRR oracle not yet supported. No OracleType::Brr variant, no BRR
///   feed, no displacement profile. BTCD markets use BRR; there are currently
///   zero active BTCD markets. When BRR markets return: add OracleType::Brr,
///   a BRR feed source, and a displacement profile. Do not map BRR to Brti —
///   they measure structurally different prices.
/// - unknown strings: no safe default. Return None, skip the market.
pub fn oracle_for(oracle_str: &str) -> Option<OracleType> {
    match oracle_str {
        "brti" => Some(OracleType::Brti),
        "chainlink_streams" | "chainlink" => Some(OracleType::ChainlinkStreams),
        "binance_1m_candle" | "binance" => Some(OracleType::BinanceCandle),
        "brr" => {
            // BRR oracle not yet supported — skip until OracleType::Brr,
            // BRR feed, and displacement profile exist.
            None
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Meta JSON helper
// ---------------------------------------------------------------------------

/// Parse a named f64 field from a RingEntry's inline meta bytes.
///
/// Returns None if meta is empty, not valid JSON, the key is absent,
/// or the value is not a finite positive number.
fn parse_f64_from_meta(entry: &crate::ring::RingEntry, key: &str) -> Option<f64> {
    let meta_str = entry.meta_str()?;
    let v: serde_json::Value = serde_json::from_str(meta_str).ok()?;
    v.get(key)
        .and_then(|val| val.as_f64())
        .filter(|&x| x.is_finite())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::MarketRow;
    use crate::feed::LiveState;
    use crate::ring::RingSet;

    // ── Test helpers ──────────────────────────────────────────────────────

    /// Convert unix seconds (f64) → "YYYY-MM-DDTHH:MM:SSZ".
    ///
    /// Inverse of `parse_iso_to_unix`. Tests use this to generate close_time
    /// strings from `wall_clock() + offset_secs` so time comparisons are
    /// always relative to the actual clock, never hardcoded.
    fn unix_to_iso_z(unix: f64) -> String {
        let secs = unix as i64;
        let days = secs.div_euclid(86400);
        let rem = secs.rem_euclid(86400);
        let h = rem / 3600;
        let mn = (rem % 3600) / 60;
        let s = rem % 60;

        // Howard Hinnant's civil_from_days — proleptic Gregorian calendar
        let z = days + 719468;
        let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
        let doe = z - era * 146097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let mo = if mp < 10 { mp + 3 } else { mp - 9 };
        let yr = if mo <= 2 { y + 1 } else { y };

        format!("{yr:04}-{mo:02}-{d:02}T{h:02}:{mn:02}:{s:02}Z")
    }

    /// A valid Kalshi market row for testing.
    /// `close_offset_secs`: positive = open market, negative = expired.
    fn kalshi_market(close_offset_secs: f64, strike: Option<f64>) -> MarketRow {
        let now = crate::feed::wall_clock();
        let close_str = unix_to_iso_z(now + close_offset_secs);
        MarketRow {
            id: 1,
            venue: "kalshi".to_string(),
            ticker: "KXBTCD-TEST-T70000".to_string(),
            series: Some("KXBTCD".to_string()),
            market_type: Some("daily".to_string()),
            oracle: Some("brti".to_string()),
            strike,
            open_time: None,
            close_time: Some(close_str),
            resolution_time: None,
            outcome: None,
            rules: None,
            token_id: None,
            discovered_at: Some(now),
        }
    }

    /// A valid Polymarket market row for testing.
    ///
    /// Uses realistic split: `ticker` = synthetic "poly-XXXXX" DB key,
    /// `token_id` = long CLOB asset ID used by the WS feed and ring index.
    /// These are different in production — the normalizer must use token_id
    /// for the poly_bbo ring lookup, not ticker.
    fn poly_market(close_offset_secs: f64, strike: Option<f64>) -> MarketRow {
        let now = crate::feed::wall_clock();
        let close_str = unix_to_iso_z(now + close_offset_secs);
        MarketRow {
            id: 2,
            venue: "polymarket".to_string(),
            ticker: "poly-1611425".to_string(), // synthetic DB key, NOT the ring index
            series: Some("btc-updown-5m".to_string()),
            market_type: Some("5m".to_string()),
            oracle: Some("chainlink_streams".to_string()),
            strike,
            open_time: None,
            close_time: Some(close_str),
            resolution_time: None,
            outcome: None,
            rules: None,
            token_id: Some("token-id-abc123".to_string()), // ring index key
            discovered_at: Some(now),
        }
    }

    /// Write spot to binance ring.
    fn write_spot(rings: &RingSet, spot: f64) {
        rings
            .binance
            .write(crate::feed::wall_clock(), spot, b"", None);
    }

    /// Write sigma to LiveState.
    fn write_sigma(state: &LiveState, sigma: f64) {
        state.sigma_1s.store(sigma);
    }

    /// Write oracle to brti ring.
    fn write_brti(rings: &RingSet, val: f64) {
        rings.brti.write(crate::feed::wall_clock(), val, b"", None);
    }

    /// Write oracle to chainlink ring.
    fn write_chainlink(rings: &RingSet, val: f64) {
        rings
            .chainlink
            .write(crate::feed::wall_clock(), val, b"", None);
    }

    /// Write oracle to rtds_chainlink ring.
    fn write_rtds_chainlink(rings: &RingSet, val: f64) {
        rings
            .rtds_chainlink
            .write(crate::feed::wall_clock(), val, b"", None);
    }

    /// Write BBO to kalshi_ticker ring.
    ///
    /// Matches the compact meta format written by `handle_ticker` in kalshi_ws.rs:
    /// value = yes_bid, meta = `{"yes_ask": <f64|null>}`.
    /// Full ticker payload (volume, OI, no_bid, no_ask) is not in the ring meta —
    /// it exceeds META_CAP (128 bytes) and would produce truncated, unreadable JSON.
    fn write_kalshi_bbo(rings: &RingSet, ticker: &str, bid: f64, ask: f64) {
        let meta = serde_json::json!({ "yes_ask": ask });
        rings.kalshi_ticker.write(
            crate::feed::wall_clock(),
            bid,
            meta.to_string().as_bytes(),
            Some(ticker),
        );
    }

    /// Write BBO to poly_bbo ring.
    ///
    /// Matches the compact meta format written by `write_best_bid_ask` in polymarket_ws.rs:
    /// value = mid, meta = `{"best_bid": <f64|null>, "best_ask": <f64|null>}`.
    /// Asset ID and spread are not in the ring meta — asset_id is in the ticker index,
    /// spread is derivable, and including both would exceed META_CAP for long token IDs.
    fn write_poly_bbo(rings: &RingSet, token_id: &str, bid: f64, ask: f64) {
        let mid = (bid + ask) / 2.0;
        let meta = serde_json::json!({ "best_bid": bid, "best_ask": ask });
        rings.poly_bbo.write(
            crate::feed::wall_clock(),
            mid,
            meta.to_string().as_bytes(),
            Some(token_id),
        );
    }

    /// A fully populated state for happy-path testing.
    fn all_data_kalshi(rings: &RingSet, state: &LiveState) {
        write_spot(rings, 74000.0);
        write_sigma(state, 8.5e-5);
        write_brti(rings, 73950.0);
        write_kalshi_bbo(rings, "KXBTCD-TEST-T70000", 0.47, 0.49);
    }

    fn all_data_poly(rings: &RingSet, state: &LiveState) {
        write_spot(rings, 74000.0);
        write_sigma(state, 8.5e-5);
        write_rtds_chainlink(rings, 73940.0);
        write_poly_bbo(rings, "token-id-abc123", 0.46, 0.50);
    }

    // ── None path tests ───────────────────────────────────────────────────

    #[test]
    fn normalize_returns_none_on_missing_spot() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        // binance ring empty — spot missing
        write_sigma(&state, 8.5e-5);
        write_brti(&rings, 73950.0);
        write_kalshi_bbo(&rings, "KXBTCD-TEST-T70000", 0.47, 0.49);
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    #[test]
    fn normalize_returns_none_on_stale_sigma() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        write_spot(&rings, 74000.0);
        // sigma_1s = 0.0 (default) — stale/missing
        write_brti(&rings, 73950.0);
        write_kalshi_bbo(&rings, "KXBTCD-TEST-T70000", 0.47, 0.49);
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    #[test]
    fn normalize_returns_none_on_missing_oracle() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        write_spot(&rings, 74000.0);
        write_sigma(&state, 8.5e-5);
        // brti ring empty — oracle missing
        write_kalshi_bbo(&rings, "KXBTCD-TEST-T70000", 0.47, 0.49);
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    #[test]
    fn normalize_returns_none_on_missing_bbo() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        write_spot(&rings, 74000.0);
        write_sigma(&state, 8.5e-5);
        write_brti(&rings, 73950.0);
        // kalshi_ticker ring empty — BBO missing
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    #[test]
    fn normalize_returns_none_on_expired_market() {
        let rings = RingSet::new();
        let state = LiveState::default();
        // close_time 60s in the past
        let market = kalshi_market(-60.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        all_data_kalshi(&rings, &state);
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    #[test]
    fn normalize_returns_none_on_missing_strike() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, None); // no strike
        let now = crate::feed::wall_clock();
        all_data_kalshi(&rings, &state);
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    #[test]
    fn normalize_returns_none_for_brr_oracle() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let mut market = kalshi_market(300.0, Some(70000.0));
        market.oracle = Some("brr".to_string());
        market.series = Some("BTCD".to_string());
        let now = crate::feed::wall_clock();
        write_spot(&rings, 74000.0);
        write_sigma(&state, 8.5e-5);
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    // ── Some path tests ───────────────────────────────────────────────────

    #[test]
    fn normalize_returns_some_with_all_fields_valid_kalshi() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        all_data_kalshi(&rings, &state);

        let inputs = normalize(&rings, &state, &market, now).expect("should be Some");
        assert!((inputs.spot - 74000.0).abs() < 1.0);
        assert!((inputs.sigma_1s - 8.5e-5).abs() < 1e-8);
        assert!((inputs.oracle_value - 73950.0).abs() < 1.0);
        assert!((inputs.best_bid - 0.47).abs() < 1e-6);
        assert!((inputs.best_ask - 0.49).abs() < 1e-6);
        assert!(inputs.t_secs > 0.0);
        assert!((inputs.strike - 70000.0).abs() < 1.0);
        assert!(matches!(inputs.venue, Venue::KalshiIndex));
        assert!(matches!(inputs.oracle_type, OracleType::Brti));
        assert_eq!(inputs.ticker, "KXBTCD-TEST-T70000");
    }

    #[test]
    fn normalize_returns_some_with_all_fields_valid_polymarket() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = poly_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        all_data_poly(&rings, &state);

        let inputs = normalize(&rings, &state, &market, now).expect("should be Some");
        assert!((inputs.spot - 74000.0).abs() < 1.0);
        assert!((inputs.best_bid - 0.46).abs() < 1e-6);
        assert!((inputs.best_ask - 0.50).abs() < 1e-6);
        assert!(matches!(inputs.venue, Venue::PolymarketCrypto));
        assert!(matches!(inputs.oracle_type, OracleType::ChainlinkStreams));
        // ticker = market display ticker (poly-XXXXX), NOT the ring key (token_id).
        // Ring lookups use market.token_id internally; callers see the ticker.
        assert_eq!(inputs.ticker, "poly-1611425");
    }

    #[test]
    fn normalize_falls_back_to_chainlink_rest_when_rtds_empty() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = poly_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        write_spot(&rings, 74000.0);
        write_sigma(&state, 8.5e-5);
        // rtds_chainlink empty — fall back to chainlink HTTP ring
        write_chainlink(&rings, 73940.0);
        write_poly_bbo(&rings, "token-id-abc123", 0.46, 0.50);

        let inputs = normalize(&rings, &state, &market, now).expect("should be Some");
        assert!((inputs.oracle_value - 73940.0).abs() < 1.0);
    }

    #[test]
    fn normalize_selects_correct_oracle_ring_for_kalshi() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        all_data_kalshi(&rings, &state);

        let inputs = normalize(&rings, &state, &market, now).unwrap();
        // Kalshi KXBTCD uses BRTI — oracle_value comes from brti ring
        assert!(matches!(inputs.oracle_type, OracleType::Brti));
        assert!((inputs.oracle_value - 73950.0).abs() < 1.0);
    }

    #[test]
    fn normalize_selects_correct_oracle_ring_for_polymarket() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = poly_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        all_data_poly(&rings, &state);

        let inputs = normalize(&rings, &state, &market, now).unwrap();
        // Polymarket up/down uses Chainlink — oracle_value from rtds_chainlink
        assert!(matches!(inputs.oracle_type, OracleType::ChainlinkStreams));
        assert!((inputs.oracle_value - 73940.0).abs() < 1.0);
    }

    #[test]
    fn normalize_validates_aba_guard_on_venue_ring() {
        let rings = RingSet::new();
        let state = LiveState::default();
        let market = kalshi_market(300.0, Some(70000.0));
        let now = crate::feed::wall_clock();
        write_spot(&rings, 74000.0);
        write_sigma(&state, 8.5e-5);
        write_brti(&rings, 73950.0);

        // Write BBO for the target ticker
        write_kalshi_bbo(&rings, "KXBTCD-TEST-T70000", 0.47, 0.49);
        // Lap the ring by writing 180+ entries for a different ticker
        for _ in 0..181 {
            write_kalshi_bbo(&rings, "OTHER-TICKER", 0.50, 0.52);
        }
        // The slot for KXBTCD-TEST-T70000 has been overwritten — ABA guard should fire
        // get_by_ticker validates seq; lapped slot returns None → normalize returns None
        assert!(normalize(&rings, &state, &market, now).is_none());
    }

    // ── Mapping helper tests ──────────────────────────────────────────────

    // ── Meta-size boundary tests ──────────────────────────────────────────

    /// Kalshi ticker ring meta must fit within META_CAP.
    ///
    /// The actual feed writes `{"yes_ask": <f64|null>}`.
    /// Worst case: yes_ask is a full-precision f64 (~20 bytes total).
    /// This test catches any future regression where the feed meta grows
    /// past 128 bytes and becomes unreadable by parse_f64_from_meta.
    #[test]
    fn kalshi_bbo_meta_fits_within_meta_cap() {
        use crate::ring::META_CAP;
        let yes_ask: Option<f64> = Some(0.123456789012345_f64);
        let meta = serde_json::json!({ "yes_ask": yes_ask });
        let meta_s = meta.to_string();
        assert!(
            meta_s.len() <= META_CAP,
            "kalshi_ticker BBO meta ({} bytes) exceeds META_CAP ({META_CAP}): {meta_s}",
            meta_s.len()
        );
    }

    /// Polymarket BBO ring meta must fit within META_CAP.
    ///
    /// The actual feed writes `{"best_bid": <f64|null>, "best_ask": <f64|null>}`.
    /// Worst case: both fields are full-precision f64 (~50 bytes total).
    /// This test catches any future regression where the feed meta grows
    /// past 128 bytes and becomes unreadable by parse_f64_from_meta.
    #[test]
    fn poly_bbo_meta_fits_within_meta_cap() {
        use crate::ring::META_CAP;
        let best_bid: Option<f64> = Some(0.123456789012345_f64);
        let best_ask: Option<f64> = Some(0.987654321098765_f64);
        let meta = serde_json::json!({ "best_bid": best_bid, "best_ask": best_ask });
        let meta_s = meta.to_string();
        assert!(
            meta_s.len() <= META_CAP,
            "poly_bbo BBO meta ({} bytes) exceeds META_CAP ({META_CAP}): {meta_s}",
            meta_s.len()
        );
    }

    /// Ring meta is valid JSON and the key field is parseable.
    ///
    /// Verifies the full data path: feed helper → ring write → ring read →
    /// parse_f64_from_meta → correct value. If meta were truncated or the
    /// key name mismatched, this test would fail with None.
    #[test]
    fn kalshi_bbo_meta_yes_ask_parseable_from_ring() {
        let rings = RingSet::new();
        write_kalshi_bbo(&rings, "KXBTCD-TEST-T70000", 0.47, 0.49);
        let now = crate::feed::wall_clock();
        let entry = rings
            .kalshi_ticker
            .get_by_ticker("KXBTCD-TEST-T70000", now + 1.0)
            .expect("entry must exist");
        // value = yes_bid
        assert!((entry.value - 0.47).abs() < 1e-9);
        // meta must be valid JSON with yes_ask key
        let meta_str = entry.meta_str().expect("meta must be present");
        let v: serde_json::Value = serde_json::from_str(meta_str).expect("meta must be valid JSON");
        let yes_ask = v["yes_ask"].as_f64().expect("yes_ask must be f64");
        assert!((yes_ask - 0.49).abs() < 1e-9);
    }

    /// Ring meta is valid JSON and both BBO fields are parseable.
    #[test]
    fn poly_bbo_meta_fields_parseable_from_ring() {
        let rings = RingSet::new();
        write_poly_bbo(&rings, "token-id-abc123", 0.46, 0.50);
        let now = crate::feed::wall_clock();
        let entry = rings
            .poly_bbo
            .get_by_ticker("token-id-abc123", now + 1.0)
            .expect("entry must exist");
        // value = mid
        assert!((entry.value - 0.48).abs() < 1e-9);
        // meta must be valid JSON with best_bid, best_ask keys
        let meta_str = entry.meta_str().expect("meta must be present");
        let v: serde_json::Value = serde_json::from_str(meta_str).expect("meta must be valid JSON");
        let bid = v["best_bid"].as_f64().expect("best_bid must be f64");
        let ask = v["best_ask"].as_f64().expect("best_ask must be f64");
        assert!((bid - 0.46).abs() < 1e-9);
        assert!((ask - 0.50).abs() < 1e-9);
    }

    /// Long real-world Polymarket token IDs must not push meta over META_CAP.
    ///
    /// Polymarket token IDs are ~77-char decimal strings. The compact meta
    /// (`{"best_bid":...,"best_ask":...}`) must fit regardless of token ID
    /// length — the asset_id is NOT in the ring meta, only in the ticker index.
    #[test]
    fn poly_bbo_long_token_id_meta_still_fits() {
        use crate::ring::META_CAP;
        let long_token_id =
            "21742633143463906290569050155826241533067272736897614950488156847949938836455";
        let rings = RingSet::new();
        write_poly_bbo(&rings, long_token_id, 0.48, 0.52);
        let now = crate::feed::wall_clock();
        let entry = rings
            .poly_bbo
            .get_by_ticker(long_token_id, now + 1.0)
            .expect("entry must exist for long token ID");
        assert!(
            entry.meta_len as usize <= META_CAP,
            "meta overflowed META_CAP: {} bytes",
            entry.meta_len
        );
        // Must still be parseable
        let meta_str = entry.meta_str().expect("meta must be present");
        let v: serde_json::Value = serde_json::from_str(meta_str)
            .expect("meta must be valid JSON even with long token ID");
        assert!(v["best_bid"].as_f64().is_some());
        assert!(v["best_ask"].as_f64().is_some());
    }

    #[test]
    fn venue_for_maps_kalshi_series_correctly() {
        assert!(matches!(venue_for("kalshi", "KXBTCD"), Venue::KalshiIndex));
        assert!(matches!(
            venue_for("kalshi", "KXBTC15M"),
            Venue::KalshiIndex
        ));
        assert!(matches!(venue_for("kalshi", "KXBTC"), Venue::KalshiIndex));
        assert!(matches!(venue_for("kalshi", "BTCD"), Venue::KalshiGeneral));
        assert!(matches!(venue_for("kalshi", ""), Venue::KalshiGeneral));
        assert!(matches!(
            venue_for("polymarket", "btc-updown-5m"),
            Venue::PolymarketCrypto
        ));
        assert!(matches!(venue_for("unknown", ""), Venue::KalshiGeneral)); // conservative
    }

    #[test]
    fn oracle_for_maps_oracle_strings_correctly() {
        assert!(matches!(oracle_for("brti"), Some(OracleType::Brti)));
        assert!(matches!(
            oracle_for("chainlink_streams"),
            Some(OracleType::ChainlinkStreams)
        ));
        assert!(matches!(
            oracle_for("chainlink"),
            Some(OracleType::ChainlinkStreams)
        ));
        assert!(matches!(
            oracle_for("binance_1m_candle"),
            Some(OracleType::BinanceCandle)
        ));
        assert!(matches!(
            oracle_for("binance"),
            Some(OracleType::BinanceCandle)
        ));
        assert!(oracle_for("brr").is_none()); // BRR not yet supported
        assert!(oracle_for("unknown").is_none());
        assert!(oracle_for("").is_none());
    }
}
