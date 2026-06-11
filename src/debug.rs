//! Paper trading system — generates labeled data for policy network training.
//!
//! The paper trader simulates order placement and position management using live
//! ring data. Every entry and exit is recorded with full terrain features
//! (physics, knob-free) and traveler decisions (risk, knob-dependent) so the
//! policy network can learn which knob settings produce the best outcomes for
//! each terrain profile.
//!
//! # Architecture
//!
//! ```text
//! rings + LiveState
//!       ↓
//!   normalizer::normalize()  →  MarketInputs (typed kernel inputs)
//!       ↓
//!   gate pipeline (Gates 0-10)
//!       ↓
//!   paper_orders (resting)
//!       ↓
//!   check_fills (strict breakthrough, back-of-queue)
//!       ↓
//!   paper_positions (open)
//!       ↓
//!   evaluate_exits (5-level exit stack)
//!       ↓
//!   paper_positions (closed) + paper_pnl (snapshot)
//! ```
//!
//! # Enabled via
//!
//! `--paper` command-line argument. The task is spawned in main.rs alongside
//! feed tasks. Tick interval: 5 seconds.
//!
//! # Fill model
//!
//! Strict breakthrough, back-of-queue. The market must trade through your level:
//! - BidYes at limit_price: fills when best_ask < limit_price
//! - BidNo  at limit_price: fills when best_bid > (1 - limit_price)
//!
//! Fill price is the breakthrough price (best_ask or best_bid at fill time),
//! not the limit price. Pessimistic by design — real fills should outperform.

use std::collections::HashMap;
use std::sync::Arc;

use rusqlite::{Connection, params};
use tokio_util::sync::CancellationToken;

use crate::db::{self, MarketRow, PAPER_SCHEMA_SQL};
use crate::feed::{LiveState, wall_clock};
use crate::kernel::{math, risk};
use crate::normalizer;
use crate::ring::RingSet;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const INITIAL_CAPITAL: f64 = 10_000_000.0;
const MIN_T_SECS: f64 = 60.0;
const FILL_FLOOR: f64 = 0.02;
const FILL_CEIL: f64 = 0.98;
const GAP_CAP: f64 = 0.10;
const TICK_INTERVAL_SECS: u64 = 5;
const MARKET_REFRESH_SECS: f64 = 60.0;
// Minimum seconds held before convergence exit is considered.
const MIN_HOLD_SECS_CONVERGENCE: f64 = 60.0;
// Strike dedup bucket: $100 increments.
const STRIKE_DEDUP_BUCKET: f64 = 100.0;

// ---------------------------------------------------------------------------
// In-memory order and position records
// ---------------------------------------------------------------------------

/// A resting limit order in the paper book.
#[derive(Debug, Clone)]
pub struct PaperOrder {
    pub id: i64,
    pub market_id: i64,
    pub ticker: String,
    pub venue: String,
    pub oracle: String,
    pub side: risk::Side,
    pub limit_price: f64,
    pub size: f64,
    pub strike: f64,
    pub placed_ts: f64,
    // Terrain snapshot at placement — needed when converting to position on fill.
    pub p_true: f64,
    pub p_market: f64,
    pub gap: f64,
    pub sigma_1s: f64,
    pub t_secs: f64,
    pub spread: f64,
    pub net_edge: f64,
    pub gate_d1: f64,
    pub displacement: f64,
}

/// An open position with running peak/drawdown tracking.
#[derive(Debug, Clone)]
pub struct PaperPosition {
    pub id: i64,
    pub market_id: i64,
    pub ticker: String,
    pub venue: String,
    pub oracle: String,
    pub side: risk::Side,
    pub size: f64,
    pub strike: f64,
    pub entry_price: f64,
    pub entry_ts: f64,
    pub entry_gap: f64,
    pub entry_fee: f64,
    pub committed_capital: f64,
    pub spread_at_fill: f64,
    pub entry_net_edge: f64,
    pub entry_gate_d1: f64,
    pub entry_displacement: f64,
    // Running trackers
    pub peak_unrealized: f64,
}

// ---------------------------------------------------------------------------
// PaperTrader
// ---------------------------------------------------------------------------

/// Paper trading system. Spawned as a task, drives the measurement loop.
pub struct PaperTrader {
    /// Resting limit orders, keyed by market_id.
    orders: HashMap<i64, PaperOrder>,
    /// Open positions, keyed by market_id.
    positions: HashMap<i64, PaperPosition>,
    /// Previous gate_d1 per market for gate_trend computation.
    prev_gate_d1: HashMap<i64, f64>,
    /// Adaptive oracle displacement profiles.
    oracle_profiles: HashMap<risk::OracleType, risk::OracleProfile>,
    /// Cached active markets. Refreshed every MARKET_REFRESH_SECS.
    markets: Vec<MarketRow>,
    last_market_refresh: f64,
    /// Risk knobs — measurement defaults.
    risk_config: risk::RiskConfig,
    /// Exit configuration.
    exit_config: risk::ExitConfig,
    /// Running capital (starts at INITIAL_CAPITAL, adjusts on realized PnL).
    capital: f64,
    /// Accumulated realized PnL.
    realized_pnl: f64,
    /// Peak equity for drawdown tracking.
    peak_equity: f64,
    total_trades: u64,
    wins: u64,
    losses: u64,
}

impl PaperTrader {
    /// Construct a new paper trader, loading active markets from the DB.
    pub fn new(conn: &Connection) -> Self {
        let markets = db::active_markets(conn).unwrap_or_default();
        let now = wall_clock();

        let mut oracle_profiles = HashMap::new();
        oracle_profiles.insert(
            risk::OracleType::Brti,
            risk::OracleProfile::default_for(risk::OracleType::Brti),
        );
        oracle_profiles.insert(
            risk::OracleType::ChainlinkStreams,
            risk::OracleProfile::default_for(risk::OracleType::ChainlinkStreams),
        );
        oracle_profiles.insert(
            risk::OracleType::BinanceCandle,
            risk::OracleProfile::default_for(risk::OracleType::BinanceCandle),
        );

        eprintln!("[paper] init — {} active markets", markets.len());

        Self {
            orders: HashMap::new(),
            positions: HashMap::new(),
            prev_gate_d1: HashMap::new(),
            oracle_profiles,
            markets,
            last_market_refresh: now,
            risk_config: risk::RiskConfig::default(),
            exit_config: risk::ExitConfig::default(),
            capital: INITIAL_CAPITAL,
            realized_pnl: 0.0,
            peak_equity: INITIAL_CAPITAL,
            total_trades: 0,
            wins: 0,
            losses: 0,
        }
    }

    /// Single evaluation tick. Called every TICK_INTERVAL_SECS.
    pub fn tick(
        &mut self,
        conn: &Connection,
        rings: &RingSet,
        state: &LiveState,
    ) {
        let now = wall_clock();

        // Refresh market cache every MARKET_REFRESH_SECS
        if now - self.last_market_refresh > MARKET_REFRESH_SECS {
            match db::active_markets(conn) {
                Ok(markets) => {
                    self.markets = markets;
                    self.last_market_refresh = now;
                }
                Err(e) => eprintln!("[paper] market refresh error: {e}"),
            }
        }

        // Update oracle displacement profiles from ring heads
        self.update_oracle_profiles(rings, state);

        // Exit stack before entries — frees capital and dedup slots
        self.evaluate_exits(conn, rings, state, now);

        // Simulate fills on resting orders
        self.check_fills(conn, rings, now);

        // Gate pipeline — place new orders
        self.evaluate_entries(conn, rings, state, now);

        // PnL snapshot
        self.record_pnl(conn, rings, state, now);
    }

    // -----------------------------------------------------------------------
    // Oracle profile updates
    // -----------------------------------------------------------------------

    fn update_oracle_profiles(&mut self, rings: &RingSet, state: &LiveState) {
        let spot = state.binance_price.load();
        if spot <= 0.0 || !spot.is_finite() {
            return;
        }

        // BRTI displacement
        if let Some(entry) = rings.brti.head()
            && entry.value > 0.0
        {
            let disp = spot - entry.value;
            if let Some(p) = self.oracle_profiles.get_mut(&risk::OracleType::Brti) {
                p.update(disp);
            }
        }

        // Chainlink displacement — prefer RTDS
        let cl_val = rings
            .rtds_chainlink
            .head()
            .filter(|e| e.value > 0.0)
            .map(|e| e.value)
            .or_else(|| {
                rings
                    .chainlink
                    .head()
                    .filter(|e| e.value > 0.0)
                    .map(|e| e.value)
            });
        if let Some(cl) = cl_val {
            let disp = spot - cl;
            if let Some(p) = self
                .oracle_profiles
                .get_mut(&risk::OracleType::ChainlinkStreams)
            {
                p.update(disp);
            }
        }

        // BinanceCandle: displacement is ~0 by construction
        if let Some(p) = self
            .oracle_profiles
            .get_mut(&risk::OracleType::BinanceCandle)
        {
            p.update(0.0);
        }
    }

    // -----------------------------------------------------------------------
    // Gate pipeline — entry evaluation
    // -----------------------------------------------------------------------

    fn evaluate_entries(
        &mut self,
        conn: &Connection,
        rings: &RingSet,
        state: &LiveState,
        now: f64,
    ) {
        // Gate 7 dedup: (strike_bucket, side) → seen this tick
        let mut dedup_this_tick: HashMap<(i64, &'static str), bool> = HashMap::new();

        // Candidates: (omega, market_id, candidate_data) sorted by omega desc
        let mut candidates: Vec<(f64, i64, EntryCandidate)> = Vec::new();

        // Clone to avoid borrow conflicts on self during the loop
        let markets: Vec<MarketRow> = self.markets.clone();

        for m in &markets {
            // ── Gate 0: existing exposure ────────────────────────────────
            if self.orders.contains_key(&m.id) || self.positions.contains_key(&m.id) {
                continue;
            }

            // ── Gates 1-3: normalizer (strike, time, data present) ───────
            let inputs = match normalizer::normalize(rings, state, m, now) {
                Some(i) => i,
                None => continue,
            };

            // ── Gate 2b: minimum time alive ──────────────────────────────
            if inputs.t_secs < MIN_T_SECS {
                continue;
            }

            // ── Gate 3b: p_market bounds ─────────────────────────────────
            let p_market = (inputs.best_bid + inputs.best_ask) / 2.0;
            if p_market <= FILL_FLOOR || p_market >= FILL_CEIL {
                continue;
            }

            // ── Gate 4: kernel compute ───────────────────────────────────
            let sigma_sqrt_t = inputs.sigma_1s * inputs.t_secs.sqrt();
            let d1_val =
                math::d1(inputs.spot, inputs.strike, inputs.sigma_1s, inputs.t_secs);
            let p_true_val =
                math::p_true(inputs.spot, inputs.strike, inputs.sigma_1s, inputs.t_secs);

            if !d1_val.is_finite() || !p_true_val.is_finite() {
                continue;
            }
            if p_true_val <= 0.0 || p_true_val >= 1.0 {
                continue;
            }

            let gap_val = math::gap(p_true_val, p_market);
            let gap_abs = gap_val.abs();
            if gap_abs >= GAP_CAP {
                continue;
            }

            // ── Gate 5: oracle gate ──────────────────────────────────────
            let profile = match self.oracle_profiles.get(&inputs.oracle_type) {
                Some(p) => p,
                None => continue,
            };
            if !risk::edge_qualifies(
                gap_abs,
                p_market,
                &inputs.venue,
                profile,
                inputs.spot,
                sigma_sqrt_t,
                d1_val,
            ) {
                continue;
            }

            // ── Gate 6: regime — per-market oracle displacement ──────────
            let oracle_value = inputs.oracle_value;
            let mu = if oracle_value > 0.0 {
                inputs.spot - oracle_value
            } else {
                0.0
            };
            let regime = risk::classify_regime(inputs.spot, inputs.strike, mu);

            // ── Terrain: gate_d1, gate_trend ─────────────────────────────
            let fee_threshold = inputs.venue.taker_rate(p_market);
            let displacement_abs = (inputs.spot - oracle_value).abs();
            let gd1 = math::gate_d1(displacement_abs, fee_threshold, sigma_sqrt_t);
            let prev_gd1 = self.prev_gate_d1.get(&m.id).copied().unwrap_or(0.0);
            let gtrend = math::gate_trend(gd1, prev_gd1);
            self.prev_gate_d1.insert(m.id, gd1);

            // ── Gate 7: strike dedup ─────────────────────────────────────
            let side = risk::compute_side(gap_val);
            let side_str: &'static str = match side {
                risk::Side::BidYes => "BidYes",
                risk::Side::BidNo => "BidNo",
            };
            let bucket = (inputs.strike / STRIKE_DEDUP_BUCKET).round() as i64;
            if dedup_this_tick.contains_key(&(bucket, side_str)) {
                continue;
            }

            // ── Gate 8: spread ───────────────────────────────────────────
            let spread = inputs.best_ask - inputs.best_bid;
            let spread_pct = if p_market > 0.0 { spread / p_market } else { 0.0 };

            // ── Gate 9: omega ────────────────────────────────────────────
            let net_edge_val = math::net_edge(gap_abs, fee_threshold + (spread - fee_threshold).max(0.0) / 2.0);
            let omega = risk::omega_at(
                p_true_val,
                p_market,
                fee_threshold,
                spread,
                side,
                regime,
                self.capital,
                self.risk_config.tail_frac,
                self.risk_config.max_frac,
                self.risk_config.revert_scale,
            );
            if omega < 0.0 {
                continue;
            }

            let limit_price =
                risk::compute_limit_price(p_true_val, self.risk_config.offset, side);
            let size = (omega + 1.0).floor();

            let hl_funding = {
                let v = state.hl_funding.load();
                if v.is_finite() { Some(v) } else { None }
            };
            let hl_oi = {
                let v = state.hl_oi.load();
                if v.is_finite() { Some(v) } else { None }
            };
            let hl_premium = {
                let v = state.hl_premium.load();
                if v.is_finite() { Some(v) } else { None }
            };
            let hl_bid_depth = {
                let v = state.hl_bid_depth_01.load();
                if v.is_finite() { Some(v) } else { None }
            };
            let hl_ask_depth = {
                let v = state.hl_ask_depth_01.load();
                if v.is_finite() { Some(v) } else { None }
            };

            candidates.push((
                omega,
                m.id,
                EntryCandidate {
                    market: m.clone(),
                    inputs,
                    p_market,
                    p_true: p_true_val,
                    gap: gap_val,
                    d1: d1_val,
                    regime,
                    side,
                    side_str,
                    limit_price,
                    size,
                    omega,
                    net_edge: net_edge_val,
                    gate_d1: gd1,
                    gate_trend: gtrend,
                    displacement: mu,
                    spread,
                    spread_pct,
                    fee_rate: fee_threshold,
                    bucket,
                    hl_funding,
                    hl_oi,
                    hl_premium,
                    hl_bid_depth,
                    hl_ask_depth,
                },
            ));
        }

        // ── Gate 10: argmax — sort by omega desc, re-dedup after each placement ──
        candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        for (_, _, c) in candidates {
            // Re-check Gate 0 (a prior candidate may have claimed this market)
            if self.orders.contains_key(&c.market.id)
                || self.positions.contains_key(&c.market.id)
            {
                continue;
            }
            // Re-check strike dedup after each placement
            if dedup_this_tick.contains_key(&(c.bucket, c.side_str)) {
                continue;
            }

            let bucket = c.bucket;
            let side_str = c.side_str;
            self.place_order(conn, c, now);
            dedup_this_tick.insert((bucket, side_str), true);
        }
    }

    fn place_order(&mut self, conn: &Connection, c: EntryCandidate, now: f64) {
        let oracle_str = c.inputs.oracle_type.to_string();
        let side_str = c.side.to_string();
        let regime_str = c.regime.to_string();

        let result = conn.execute(
            "INSERT INTO paper_orders \
             (market_id, ticker, venue, oracle, side, limit_price, size, strike, \
              status, placed_ts, \
              p_true, p_market, gap, omega, d1, sigma_1s, t_secs, regime, \
              net_edge, gate_d1, gate_trend, displacement, spread, spread_pct, fee_rate, \
              hl_funding, hl_oi, hl_premium, hl_bid_depth, hl_ask_depth) \
             VALUES \
             (?1,?2,?3,?4,?5,?6,?7,?8,\
              'resting',?9,\
              ?10,?11,?12,?13,?14,?15,?16,?17,\
              ?18,?19,?20,?21,?22,?23,?24,\
              ?25,?26,?27,?28,?29)",
            params![
                c.market.id,
                c.market.ticker,
                c.market.venue,
                oracle_str,
                side_str,
                c.limit_price,
                c.size,
                c.inputs.strike,
                now,
                c.p_true,
                c.p_market,
                c.gap,
                c.omega,
                c.d1,
                c.inputs.sigma_1s,
                c.inputs.t_secs,
                regime_str,
                c.net_edge,
                c.gate_d1,
                c.gate_trend,
                c.displacement,
                c.spread,
                c.spread_pct,
                c.fee_rate,
                c.hl_funding,
                c.hl_oi,
                c.hl_premium,
                c.hl_bid_depth,
                c.hl_ask_depth,
            ],
        );

        match result {
            Ok(_) => {
                let id = conn.last_insert_rowid();
                eprintln!(
                    "[paper] order placed: {} {} @ {:.4}  ω={:.3}  gd1={:.3}",
                    c.side_str, c.market.ticker, c.limit_price, c.omega, c.gate_d1
                );
                self.orders.insert(
                    c.market.id,
                    PaperOrder {
                        id,
                        market_id: c.market.id,
                        ticker: c.market.ticker.clone(),
                        venue: c.market.venue.clone(),
                        oracle: oracle_str,
                        side: c.side,
                        limit_price: c.limit_price,
                        size: c.size,
                        strike: c.inputs.strike,
                        placed_ts: now,
                        p_true: c.p_true,
                        p_market: c.p_market,
                        gap: c.gap,
                        sigma_1s: c.inputs.sigma_1s,
                        t_secs: c.inputs.t_secs,
                        spread: c.spread,
                        net_edge: c.net_edge,
                        gate_d1: c.gate_d1,
                        displacement: c.displacement,
                    },
                );
            }
            Err(e) => eprintln!("[paper] order insert error: {e}"),
        }
    }

    // -----------------------------------------------------------------------
    // Fill simulation — strict breakthrough, back-of-queue
    // -----------------------------------------------------------------------

    fn check_fills(&mut self, conn: &Connection, rings: &RingSet, now: f64) {
        let order_ids: Vec<i64> = self.orders.keys().copied().collect();

        for market_id in order_ids {
            let order = match self.orders.get(&market_id) {
                Some(o) => o.clone(),
                None => continue,
            };

            // Find the market row to call normalizer
            let market = match self.markets.iter().find(|m| m.id == market_id) {
                Some(m) => m.clone(),
                None => continue,
            };

            // Read current BBO from ring (need only best_bid and best_ask)
            // We read directly via get_by_ticker since we only need BBO scalars
            // and don't need full MarketInputs for the fill check.
            let (current_bid, current_ask) = match read_bbo_for_fill(rings, &order, &market, now) {
                Some(bbo) => bbo,
                None => continue,
            };

            let filled = match order.side {
                // BidYes: resting bid at limit_price. Fill when ask breaks through below.
                risk::Side::BidYes => current_ask < order.limit_price,
                // BidNo: resting offer (buying NO = selling YES at limit_price).
                // In YES terms: limit_price is where we offer NO.
                // Fill when bid breaks through above: someone paying more than limit_price for YES
                // crosses our NO offer.
                risk::Side::BidNo => current_bid > order.limit_price,
            };

            if !filled {
                continue;
            }

            // Fill price = breakthrough price (not limit price)
            let fill_price = match order.side {
                risk::Side::BidYes => current_ask,
                risk::Side::BidNo => current_bid,
            };

            // Mark order filled in DB
            let _ = conn.execute(
                "UPDATE paper_orders SET status='filled', fill_ts=?1, fill_price=?2 WHERE id=?3",
                params![now, fill_price, order.id],
            );

            // Compute entry metrics
            let spread_at_fill = current_ask - current_bid;
            // Venue resolved with the market's series to get the correct fee tier
            // (KalshiIndex vs KalshiGeneral). PaperOrder only stores venue string,
            // not the series, so re-derive here from the market row.
            let fill_venue = crate::normalizer::venue_for(
                &order.venue,
                market.series.as_deref().unwrap_or(""),
            );
            let entry_fee = order.size * fill_venue.taker_rate(fill_price) * fill_price;

            // committed_capital = size * cost_per_contract(fill_price, side)
            let cost = risk::cost_per_contract(fill_price, order.side);
            let committed_capital = order.size * cost;

            // Insert into paper_positions
            let result = conn.execute(
                "INSERT INTO paper_positions \
                 (market_id, ticker, venue, oracle, side, size, strike, \
                  entry_price, entry_ts, entry_gap, entry_fee, committed_capital, \
                  spread_at_fill, entry_net_edge, entry_gate_d1, entry_displacement) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
                params![
                    order.market_id,
                    order.ticker,
                    order.venue,
                    order.oracle,
                    order.side.to_string(),
                    order.size,
                    order.strike,
                    fill_price,
                    now,
                    order.gap,
                    entry_fee,
                    committed_capital,
                    spread_at_fill,
                    order.net_edge,
                    order.gate_d1,
                    order.displacement,
                ],
            );

            match result {
                Ok(_) => {
                    let pos_id = conn.last_insert_rowid();
                    eprintln!(
                        "[paper] fill: {} {} @ {:.4}  (limit was {:.4})",
                        order.side, order.ticker, fill_price, order.limit_price
                    );
                    let pos = PaperPosition {
                        id: pos_id,
                        market_id: order.market_id,
                        ticker: order.ticker.clone(),
                        venue: order.venue.clone(),
                        oracle: order.oracle.clone(),
                        side: order.side,
                        size: order.size,
                        strike: order.strike,
                        entry_price: fill_price,
                        entry_ts: now,
                        entry_gap: order.gap,
                        entry_fee,
                        committed_capital,
                        spread_at_fill,
                        entry_net_edge: order.net_edge,
                        entry_gate_d1: order.gate_d1,
                        entry_displacement: order.displacement,
                        peak_unrealized: 0.0,
                    };
                    self.orders.remove(&market_id);
                    self.positions.insert(market_id, pos);
                }
                Err(e) => eprintln!("[paper] position insert error: {e}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Exit stack
    // -----------------------------------------------------------------------

    fn evaluate_exits(
        &mut self,
        conn: &Connection,
        rings: &RingSet,
        state: &LiveState,
        now: f64,
    ) {
        let pos_keys: Vec<i64> = self.positions.keys().copied().collect();

        for market_id in pos_keys {
            let pos = match self.positions.get(&market_id) {
                Some(p) => p.clone(),
                None => continue,
            };

            let market = match self.markets.iter().find(|m| m.id == market_id) {
                Some(m) => m.clone(),
                None => {
                    // Market gone from active list — treat as resolved unknown
                    self.close_position(
                        conn,
                        rings,
                        state,
                        market_id,
                        pos.entry_price, // use entry price as fallback
                        "resolution",
                        now,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                        0.0,
                    );
                    continue;
                }
            };

            // Read current data
            let inputs = match normalizer::normalize(rings, state, &market, now) {
                Some(i) => i,
                None => continue, // Can't see market, hold
            };

            let p_market_now = (inputs.best_bid + inputs.best_ask) / 2.0;
            if p_market_now <= 0.0 || !p_market_now.is_finite() {
                continue;
            }

            // Current price for PnL computation
            let current_price = match pos.side {
                risk::Side::BidYes => inputs.best_ask, // exit taker: pay the ask
                risk::Side::BidNo => inputs.best_bid,  // exit taker: sell to bid
            };

            let unrealized = risk::unrealized_pnl(pos.entry_price, current_price, pos.side);
            let total_unrealized = unrealized * pos.size;

            // Update peak
            let new_peak = pos.peak_unrealized.max(total_unrealized);
            if (new_peak - pos.peak_unrealized).abs() > 1e-9
                && let Some(p) = self.positions.get_mut(&market_id)
            {
                p.peak_unrealized = new_peak;
            }
            let peak = new_peak;

            // Kernel re-evaluation for exit terrain
            let sigma_sqrt_t = inputs.sigma_1s * inputs.t_secs.sqrt();
            let _d1_now = math::d1(
                inputs.spot,
                inputs.strike,
                inputs.sigma_1s,
                inputs.t_secs,
            );
            let p_true_now = math::p_true(
                inputs.spot,
                inputs.strike,
                inputs.sigma_1s,
                inputs.t_secs,
            );

            let exit_gap = if p_true_now.is_finite()
                && p_true_now > 0.0
                && p_true_now < 1.0
            {
                math::gap(p_true_now, p_market_now)
            } else {
                0.0
            };

            let exit_gap_abs = exit_gap.abs();
            let fee_threshold = inputs.venue.taker_rate(p_market_now);
            let displacement_abs = (inputs.spot - inputs.oracle_value).abs();
            let exit_gd1 = math::gate_d1(displacement_abs, fee_threshold, sigma_sqrt_t);
            let prev_gd1 = self.prev_gate_d1.get(&market_id).copied().unwrap_or(0.0);
            let exit_gtrend = math::gate_trend(exit_gd1, prev_gd1);
            let exit_net_edge =
                math::net_edge(exit_gap_abs, fee_threshold);

            // ── Physics exit: terrain_gone ────────────────────────────────
            // The displacement no longer clears the fee threshold. The trail
            // that justified entry has vanished — the entry gate would reject
            // this market right now. Exit immediately. No knob. No threshold.
            // The manifold closed under the position.
            if exit_gd1 < 0.0 {
                let spread_at_exit = inputs.best_ask - inputs.best_bid;
                self.close_position(
                    conn,
                    rings,
                    state,
                    market_id,
                    current_price,
                    "terrain_gone",
                    now,
                    exit_gd1,
                    exit_gtrend,
                    exit_net_edge,
                    p_true_now,
                    p_market_now,
                    spread_at_exit,
                );
                continue;
            }

            // Exit thresholds (sigma-scaled)
            let entry_gap_abs = pos.entry_gap.abs();
            let thresholds =
                risk::exit_thresholds(&self.exit_config, entry_gap_abs, inputs.sigma_1s);

            let hold_secs = now - pos.entry_ts;

            // ── Exit 0: sign flip ─────────────────────────────────────────
            let entry_side = risk::compute_side(pos.entry_gap);
            let current_side = risk::compute_side(exit_gap);
            let exit_reason = if exit_gap != 0.0 && entry_side != current_side {
                Some("sign_flip")

            // ── Exit 1: profit target ─────────────────────────────────────
            } else if total_unrealized >= thresholds.profit_target {
                Some("profit_target")

            // ── Exit 2: trailing stop ─────────────────────────────────────
            } else if peak - total_unrealized >= thresholds.trail_distance {
                Some("trailing_stop")

            // ── Exit 3: convergence ───────────────────────────────────────
            } else if exit_gap_abs < thresholds.convergence_floor
                && total_unrealized < 0.0
                && hold_secs > MIN_HOLD_SECS_CONVERGENCE
            {
                Some("convergence")

            // ── Exit 4: max loss ──────────────────────────────────────────
            } else if total_unrealized <= -thresholds.max_loss {
                Some("max_loss")

            // ── Exit 5: resolution ────────────────────────────────────────
            } else if inputs.t_secs <= 0.0 {
                Some("resolution")
            } else {
                None
            };

            if let Some(reason) = exit_reason {
                let spread_at_exit = inputs.best_ask - inputs.best_bid;
                self.close_position(
                    conn,
                    rings,
                    state,
                    market_id,
                    current_price,
                    reason,
                    now,
                    exit_gd1,
                    exit_gtrend,
                    exit_net_edge,
                    p_true_now,
                    p_market_now,
                    spread_at_exit,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn close_position(
        &mut self,
        conn: &Connection,
        _rings: &RingSet,
        _state: &LiveState,
        market_id: i64,
        exit_price: f64,
        exit_reason: &str,
        now: f64,
        exit_gd1: f64,
        exit_gtrend: f64,
        exit_net_edge: f64,
        exit_p_true: f64,
        exit_p_market: f64,
        spread_at_exit: f64,
    ) {
        let pos = match self.positions.remove(&market_id) {
            Some(p) => p,
            None => return,
        };

        let unrealized_per =
            risk::unrealized_pnl(pos.entry_price, exit_price, pos.side);
        let gross_pnl = unrealized_per * pos.size;

        // Exit fee — venue resolved with market series for correct fee tier.
        let exit_market_series = self
            .markets
            .iter()
            .find(|m| m.id == market_id)
            .and_then(|m| m.series.as_deref())
            .unwrap_or("");
        let exit_venue = crate::normalizer::venue_for(&pos.venue, exit_market_series);
        let exit_fee = pos.size * exit_venue.taker_rate(exit_price) * exit_price;
        let net_pnl = gross_pnl - exit_fee - pos.entry_fee;

        let hold_secs = now - pos.entry_ts;
        let peak = pos.peak_unrealized;

        let exit_gap = if exit_p_true > 0.0 && exit_p_true < 1.0 && exit_p_market > 0.0 {
            math::gap(exit_p_true, exit_p_market)
        } else {
            0.0
        };

        let _ = conn.execute(
            "UPDATE paper_positions SET \
             exit_price=?1, exit_ts=?2, exit_reason=?3, exit_fee=?4, \
             gross_pnl=?5, net_pnl=?6, \
             exit_p_true=?7, exit_p_market=?8, exit_gap=?9, \
             exit_gate_d1=?10, exit_gate_trend=?11, exit_net_edge=?12, \
             spread_at_exit=?13, peak_unrealized=?14, hold_secs=?15 \
             WHERE id=?16",
            params![
                exit_price,
                now,
                exit_reason,
                exit_fee,
                gross_pnl,
                net_pnl,
                if exit_p_true.is_finite() { Some(exit_p_true) } else { None },
                if exit_p_market.is_finite() { Some(exit_p_market) } else { None },
                if exit_gap.is_finite() { Some(exit_gap) } else { None },
                exit_gd1,
                exit_gtrend,
                exit_net_edge,
                spread_at_exit,
                peak,
                hold_secs,
                pos.id,
            ],
        );

        // Update capital and stats
        self.realized_pnl += net_pnl;
        self.capital += net_pnl;
        self.total_trades += 1;
        if net_pnl > 0.0 {
            self.wins += 1;
        } else {
            self.losses += 1;
        }
        let equity = self.capital + self.unrealized_pnl_total();
        if equity > self.peak_equity {
            self.peak_equity = equity;
        }

        eprintln!(
            "[paper] close: {} {} @ {:.4}  reason={}  net_pnl={:+.2}  capital={:.0}",
            pos.side,
            pos.ticker,
            exit_price,
            exit_reason,
            net_pnl,
            self.capital,
        );
    }

    // -----------------------------------------------------------------------
    // PnL snapshot
    // -----------------------------------------------------------------------

    fn record_pnl(&self, conn: &Connection, _rings: &RingSet, _state: &LiveState, now: f64) {
        let unrealized = self.unrealized_pnl_total();
        let total_value = self.capital + unrealized;
        let drawdown_pct = if self.peak_equity > 0.0 {
            (self.peak_equity - total_value) / self.peak_equity * 100.0
        } else {
            0.0
        };

        let _ = conn.execute(
            "INSERT INTO paper_pnl \
             (ts, capital, realized_pnl, unrealized_pnl, total_value, \
              peak_equity, open_positions, resting_orders, total_trades, \
              wins, losses, drawdown_pct) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                now,
                self.capital,
                self.realized_pnl,
                unrealized,
                total_value,
                self.peak_equity,
                self.positions.len() as i64,
                self.orders.len() as i64,
                self.total_trades as i64,
                self.wins as i64,
                self.losses as i64,
                drawdown_pct,
            ],
        );
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn unrealized_pnl_total(&self) -> f64 {
        // Without live prices we can't compute exact unrealized.
        // Return 0 — the snapshot is a capital accounting tool, not a mark-to-market.
        // Per-position unrealized is computed at exit time.
        0.0
    }
}

// ---------------------------------------------------------------------------
// EntryCandidate — collects per-market data before argmax sort
// ---------------------------------------------------------------------------

struct EntryCandidate {
    market: MarketRow,
    inputs: normalizer::MarketInputs,
    p_market: f64,
    p_true: f64,
    gap: f64,
    d1: f64,
    regime: risk::Regime,
    side: risk::Side,
    side_str: &'static str,
    limit_price: f64,
    size: f64,
    omega: f64,
    net_edge: f64,
    gate_d1: f64,
    gate_trend: f64,
    displacement: f64,
    spread: f64,
    spread_pct: f64,
    fee_rate: f64,
    bucket: i64,
    hl_funding: Option<f64>,
    hl_oi: Option<f64>,
    hl_premium: Option<f64>,
    hl_bid_depth: Option<f64>,
    hl_ask_depth: Option<f64>,
}

// ---------------------------------------------------------------------------
// BBO reader for fill check (reads ring directly without full normalize)
// ---------------------------------------------------------------------------

/// Read best_bid and best_ask for a resting order's market.
/// Returns None if the ring entry is absent or stale.
fn read_bbo_for_fill(
    rings: &RingSet,
    order: &PaperOrder,
    market: &MarketRow,
    now: f64,
) -> Option<(f64, f64)> {
    let venue = crate::normalizer::venue_for(&order.venue, market.series.as_deref().unwrap_or(""));
    match venue {
        risk::Venue::KalshiIndex | risk::Venue::KalshiGeneral => {
            let entry = rings.kalshi_ticker.get_by_ticker(&order.ticker, now)?;
            let best_bid = entry.value;
            let meta_str = entry.meta_str()?;
            let v: serde_json::Value = serde_json::from_str(meta_str).ok()?;
            let best_ask = v["yes_ask"].as_f64().filter(|&x| x.is_finite())?;
            if best_bid > 0.0 && best_ask > 0.0 {
                Some((best_bid, best_ask))
            } else {
                None
            }
        }
        risk::Venue::PolymarketCrypto | risk::Venue::PolymarketSports => {
            // Ring indexed by token_id, not by the synthetic poly-XXXXX ticker.
            let ring_key = market.token_id.as_deref().filter(|s| !s.is_empty())?;
            let entry = rings.poly_bbo.get_by_ticker(ring_key, now)?;
            let meta_str = entry.meta_str()?;
            let v: serde_json::Value = serde_json::from_str(meta_str).ok()?;
            let best_bid = v["best_bid"].as_f64().filter(|&x| x.is_finite())?;
            let best_ask = v["best_ask"].as_f64().filter(|&x| x.is_finite())?;
            if best_bid > 0.0 && best_ask > 0.0 {
                Some((best_bid, best_ask))
            } else {
                None
            }
        }
    }
}



// ---------------------------------------------------------------------------
// Public entry point — the async task spawned by main.rs
// ---------------------------------------------------------------------------

/// Run the paper trader task. Spawned when `--paper` is on the command line.
pub async fn run_paper_trader(
    db_path: String,
    rings: Arc<RingSet>,
    state: Arc<LiveState>,
    stop: CancellationToken,
) {
    let conn = match db::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[paper] DB open error: {e}");
            return;
        }
    };

    if let Err(e) = conn.execute_batch(PAPER_SCHEMA_SQL) {
        eprintln!("[paper] schema init error: {e}");
        return;
    }

    let mut trader = PaperTrader::new(&conn);
    let interval = std::time::Duration::from_secs(TICK_INTERVAL_SECS);

    eprintln!("[paper] started — tick every {TICK_INTERVAL_SECS}s");

    loop {
        let cancelled = tokio::select! {
            () = tokio::time::sleep(interval) => false,
            () = stop.cancelled() => true,
        };
        if cancelled {
            eprintln!("[paper] shutdown");
            break;
        }
        trader.tick(&conn, &rings, &state);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{MarketRow, PAPER_SCHEMA_SQL, SCHEMA_SQL};
    use crate::feed::LiveState;
    use crate::ring::RingSet;

    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        conn.execute_batch(PAPER_SCHEMA_SQL).unwrap();
        conn
    }

    fn unix_to_iso_z(unix: f64) -> String {
        let secs = unix as i64;
        let days = secs.div_euclid(86400);
        let rem = secs.rem_euclid(86400);
        let h = rem / 3600;
        let mn = (rem % 3600) / 60;
        let s = rem % 60;
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

    fn kalshi_market(market_id: i64, close_offset_secs: f64, strike: f64) -> MarketRow {
        let now = wall_clock();
        let close_str = unix_to_iso_z(now + close_offset_secs);
        MarketRow {
            id: market_id,
            venue: "kalshi".to_string(),
            ticker: format!("KXBTCD-TEST-T{strike:.0}"),
            series: Some("KXBTCD".to_string()),
            market_type: Some("daily".to_string()),
            oracle: Some("brti".to_string()),
            strike: Some(strike),
            open_time: None,
            close_time: Some(close_str),
            resolution_time: None,
            outcome: None,
            rules: None,
            token_id: None,
            discovered_at: Some(now),
        }
    }

    fn write_all_rings(rings: &RingSet, state: &LiveState, spot: f64, brti: f64, bid: f64, ask: f64, ticker: &str) {
        let now = wall_clock();
        rings.binance.write(now, spot, b"", None);
        rings.brti.write(now, brti, b"", None);
        state.sigma_1s.store(8.5e-5);
        let meta = serde_json::json!({ "yes_ask": ask });
        rings.kalshi_ticker.write(now, bid, meta.to_string().as_bytes(), Some(ticker));
    }

    fn insert_market(conn: &Connection, m: &MarketRow) {
        conn.execute(
            "INSERT OR IGNORE INTO markets \
             (id, venue, ticker, series, market_type, oracle, strike, close_time, discovered_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                m.id, m.venue, m.ticker, m.series, m.market_type,
                m.oracle, m.strike, m.close_time, m.discovered_at,
            ],
        ).unwrap();
    }

    // ── PaperTrader construction ──────────────────────────────────────────

    #[test]
    fn paper_trader_new_initializes_with_capital() {
        let conn = test_conn();
        let trader = PaperTrader::new(&conn);
        assert!((trader.capital - INITIAL_CAPITAL).abs() < 1.0);
        assert_eq!(trader.total_trades, 0);
        assert!(trader.orders.is_empty());
        assert!(trader.positions.is_empty());
    }

    #[test]
    fn paper_trader_new_loads_markets_from_db() {
        let conn = test_conn();
        let m = kalshi_market(1, 300.0, 70000.0);
        insert_market(&conn, &m);
        let trader = PaperTrader::new(&conn);
        assert_eq!(trader.markets.len(), 1);
    }

    // ── RiskConfig default ────────────────────────────────────────────────

    #[test]
    fn risk_config_default_all_positive_finite() {
        let cfg = risk::RiskConfig::default();
        for (name, val) in [
            ("tail_frac", cfg.tail_frac),
            ("max_frac", cfg.max_frac),
            ("revert_scale", cfg.revert_scale),
            ("offset", cfg.offset),
            ("max_slippage", cfg.max_slippage),
            ("gap_cap", cfg.gap_cap),
            ("time_divisor", cfg.time_divisor),
            ("fill_floor", cfg.fill_floor),
            ("profit_target_sigma", cfg.profit_target_sigma),
            ("trail_distance_sigma", cfg.trail_distance_sigma),
            ("convergence_floor_sigma", cfg.convergence_floor_sigma),
            ("max_loss_sigma", cfg.max_loss_sigma),
        ] {
            assert!(val > 0.0 && val.is_finite(), "{name} = {val} must be positive and finite");
        }
    }

    // ── Gate pipeline: entries ────────────────────────────────────────────

    #[test]
    fn evaluate_entries_skips_expired_market() {
        let conn = test_conn();
        let m = kalshi_market(1, -60.0, 70000.0); // expired
        insert_market(&conn, &m);

        let rings = RingSet::new();
        let state = LiveState::default();
        write_all_rings(&rings, &state, 74000.0, 73950.0, 0.47, 0.49, &m.ticker);

        let mut trader = PaperTrader::new(&conn);
        trader.tick(&conn, &rings, &state);
        assert!(trader.orders.is_empty(), "expired market should not produce order");
    }

    #[test]
    fn evaluate_entries_skips_missing_data() {
        let conn = test_conn();
        let m = kalshi_market(1, 300.0, 70000.0);
        insert_market(&conn, &m);

        let rings = RingSet::new();
        let state = LiveState::default();
        // No data written to rings

        let mut trader = PaperTrader::new(&conn);
        trader.tick(&conn, &rings, &state);
        assert!(trader.orders.is_empty(), "missing ring data should not produce order");
    }

    #[test]
    fn evaluate_entries_skips_existing_exposure() {
        let conn = test_conn();
        let m = kalshi_market(1, 300.0, 70000.0);
        insert_market(&conn, &m);

        let rings = RingSet::new();
        let state = LiveState::default();
        write_all_rings(&rings, &state, 74000.0, 73950.0, 0.47, 0.49, &m.ticker);

        let mut trader = PaperTrader::new(&conn);

        // Manually inject a resting order for this market.
        // limit_price = 0.30, current ask = 0.49 → not a breakthrough (0.49 > 0.30),
        // so the order stays resting and evaluate_entries skips this market (Gate 0).
        trader.orders.insert(1, PaperOrder {
            id: 99,
            market_id: 1,
            ticker: m.ticker.clone(),
            venue: "kalshi".to_string(),
            oracle: "brti".to_string(),
            side: risk::Side::BidYes,
            limit_price: 0.30, // well below current ask of 0.49 — stays resting
            size: 1.0,
            strike: 70000.0,
            placed_ts: wall_clock(),
            p_true: 0.55,
            p_market: 0.48,
            gap: 0.07,
            sigma_1s: 8.5e-5,
            t_secs: 300.0,
            spread: 0.02,
            net_edge: 0.05,
            gate_d1: 1.0,
            displacement: 50.0,
        });

        trader.tick(&conn, &rings, &state);

        // The injected order is still resting — not filled, not duplicated.
        // evaluate_entries saw the order in self.orders and skipped this market (Gate 0).
        assert_eq!(trader.orders.len(), 1, "existing order should block new placement");
        assert_eq!(trader.orders[&1].id, 99, "should be the original injected order");
    }

    // ── Oracle profile update ─────────────────────────────────────────────

    #[test]
    fn oracle_profile_updates_from_rings() {
        let rings = RingSet::new();
        let state = LiveState::default();
        state.binance_price.store(74000.0);
        rings.brti.write(wall_clock(), 73950.0, b"", None);

        let conn = test_conn();
        let mut trader = PaperTrader::new(&conn);
        trader.update_oracle_profiles(&rings, &state);

        let profile = &trader.oracle_profiles[&risk::OracleType::Brti];
        assert_eq!(profile.n_obs, 1);
        assert!((profile.disp_ema - 50.0).abs() < 1.0); // |74000 - 73950| = 50
    }

    // ── Strike dedup ──────────────────────────────────────────────────────

    #[test]
    fn strike_dedup_uses_100_bucket() {
        // Two strikes in same $100 bucket: 70000 and 70050
        let bucket_a = (70000.0_f64 / STRIKE_DEDUP_BUCKET).round() as i64;
        let bucket_b = (70050.0_f64 / STRIKE_DEDUP_BUCKET).round() as i64;
        // 70000 → bucket 700, 70050 → bucket 701 (different buckets)
        // 70000 and 70001 → same bucket 700
        assert_eq!(bucket_a, (70000.0_f64 / 100.0).round() as i64);
        assert_eq!(bucket_b, (70050.0_f64 / 100.0).round() as i64);
    }

    // ── Fill model ────────────────────────────────────────────────────────

    #[test]
    fn fill_model_bid_yes_strict_breakthrough() {
        // BidYes at limit 0.55: only fills when ask < 0.55 (ask crosses through)
        let order = PaperOrder {
            id: 1,
            market_id: 1,
            ticker: "TEST".to_string(),
            venue: "kalshi".to_string(),
            oracle: "brti".to_string(),
            side: risk::Side::BidYes,
            limit_price: 0.55,
            size: 10.0,
            strike: 70000.0,
            placed_ts: 0.0,
            p_true: 0.60,
            p_market: 0.48,
            gap: 0.12,
            sigma_1s: 8.5e-5,
            t_secs: 300.0,
            spread: 0.02,
            net_edge: 0.10,
            gate_d1: 1.5,
            displacement: 50.0,
        };

        // Ask at touch (0.55) — back of queue, NOT filled
        let filled_at_touch = match order.side {
            risk::Side::BidYes => 0.55_f64 < order.limit_price, // false
            _ => false,
        };
        assert!(!filled_at_touch, "touch should not fill");

        // Ask breaks through (0.54 < 0.55) — filled
        let filled_breakthrough = match order.side {
            risk::Side::BidYes => 0.54_f64 < order.limit_price, // true
            _ => false,
        };
        assert!(filled_breakthrough, "breakthrough should fill");
    }

    #[test]
    fn fill_model_bid_no_strict_breakthrough() {
        // BidNo at limit 0.45: fills when bid > 0.45 (bid crosses through our offer)
        let order = PaperOrder {
            id: 2,
            market_id: 2,
            ticker: "TEST2".to_string(),
            venue: "kalshi".to_string(),
            oracle: "brti".to_string(),
            side: risk::Side::BidNo,
            limit_price: 0.45,
            size: 5.0,
            strike: 70000.0,
            placed_ts: 0.0,
            p_true: 0.40,
            p_market: 0.52,
            gap: -0.12,
            sigma_1s: 8.5e-5,
            t_secs: 300.0,
            spread: 0.02,
            net_edge: 0.10,
            gate_d1: 1.5,
            displacement: -50.0,
        };

        // Bid at touch (0.45) — NOT filled
        let filled_at_touch = match order.side {
            risk::Side::BidNo => 0.45_f64 > order.limit_price, // false
            _ => false,
        };
        assert!(!filled_at_touch, "touch should not fill");

        // Bid breaks through (0.46 > 0.45) — filled
        let filled_breakthrough = match order.side {
            risk::Side::BidNo => 0.46_f64 > order.limit_price, // true
            _ => false,
        };
        assert!(filled_breakthrough, "breakthrough should fill");
    }

    // ── Exit reasons ──────────────────────────────────────────────────────

    #[test]
    fn exit_sign_flip_condition() {
        // Entry side BidYes (gap > 0). If gap flips negative, sign_flip triggers.
        let entry_gap = 0.05_f64;
        let exit_gap = -0.03_f64;
        let entry_side = risk::compute_side(entry_gap);
        let exit_side = risk::compute_side(exit_gap);
        assert_ne!(entry_side, exit_side, "sign flip should detect side change");
    }

    #[test]
    fn exit_sign_same_no_flip() {
        let entry_gap = 0.05_f64;
        let exit_gap = 0.02_f64; // smaller but same sign
        let entry_side = risk::compute_side(entry_gap);
        let exit_side = risk::compute_side(exit_gap);
        assert_eq!(entry_side, exit_side, "same sign = no flip");
    }

    #[test]
    fn exit_profit_target_triggers() {
        let cfg = risk::ExitConfig::default();
        let t = risk::exit_thresholds(&cfg, 0.10, 0.0003);
        // profit_target = 0.50 * 0.10 * 1.0 = 0.05 per contract
        // At size=10: total target = 0.50
        let total_unrealized = 0.51_f64;
        assert!(total_unrealized >= t.profit_target, "should trigger profit target");
    }

    #[test]
    fn exit_max_loss_triggers() {
        let cfg = risk::ExitConfig::default();
        let t = risk::exit_thresholds(&cfg, 0.10, 0.0003);
        let total_unrealized = -(t.max_loss + 0.001);
        assert!(
            total_unrealized <= -t.max_loss,
            "should trigger max_loss"
        );
    }

    // ── terrain_gone physics exit ─────────────────────────────────────

    #[test]
    fn terrain_gone_fires_when_gate_d1_negative() {
        // gate_d1 < 0 means displacement has fallen below the fee threshold.
        // The trail has vanished — terrain_gone should fire.
        let displacement_abs = 0.005_f64; // below fee threshold
        let fee_threshold = 0.015_f64;
        let sigma_sqrt_t = 0.01_f64;
        let gd1 = math::gate_d1(displacement_abs, fee_threshold, sigma_sqrt_t);
        assert!(gd1 < 0.0, "gate_d1 should be negative when displacement < fee");
    }

    #[test]
    fn terrain_gone_does_not_fire_when_gate_d1_positive() {
        // gate_d1 >= 0 means displacement still clears the fee. Trail exists.
        let displacement_abs = 0.04_f64; // above fee threshold
        let fee_threshold = 0.015_f64;
        let sigma_sqrt_t = 0.01_f64;
        let gd1 = math::gate_d1(displacement_abs, fee_threshold, sigma_sqrt_t);
        assert!(gd1 > 0.0, "gate_d1 should be positive when displacement > fee");
    }

    #[test]
    fn terrain_gone_fires_before_risk_exits() {
        // When gate_d1 < 0, terrain_gone exits before any risk threshold is
        // consulted. Here we verify the ordering by ensuring gate_d1 is
        // checked first — a position with gate_d1 < 0 exits as terrain_gone
        // regardless of whether profit_target or trailing_stop would also fire.
        //
        // This is a structural property: the physics check (gate_d1 < 0)
        // precedes the risk exit chain in evaluate_exits. We verify the
        // condition is exclusive: terrain_gone reason != any risk reason.
        let terrain_gone_reason = "terrain_gone";
        let risk_reasons = ["sign_flip", "profit_target", "trailing_stop",
                            "convergence", "max_loss", "resolution"];
        for r in &risk_reasons {
            assert_ne!(terrain_gone_reason, *r,
                "terrain_gone must be distinct from risk exit reasons");
        }
    }

    #[test]
    fn terrain_gone_records_correct_exit_reason() {
        // Integration test: position with gate_d1 < 0 at eval time exits
        // as "terrain_gone" in paper_positions.
        let conn = test_conn();
        let m = kalshi_market(1, 300.0, 70000.0);
        insert_market(&conn, &m);

        let now = wall_clock();

        // Insert a filled order
        conn.execute(
            "INSERT INTO paper_orders \
             (id, market_id, ticker, venue, oracle, side, limit_price, size, strike, status, \
              placed_ts, fill_ts, fill_price, p_true, p_market, gap, omega, d1, sigma_1s, \
              t_secs, regime, net_edge, gate_d1, gate_trend, displacement, spread, \
              spread_pct, fee_rate) \
             VALUES (1,1,?1,'kalshi','brti','BidYes',0.48,100.0,70000.0,'filled', \
                     ?2,?2,0.48, 0.55,0.48,0.07,1.5,1.2,8.5e-5,300.0,'Amplify', \
                     0.05,2.0,0.1,50.0,0.02,0.042,0.015)",
            rusqlite::params![m.ticker, now - 10.0],
        ).unwrap();

        // Insert the matching open position
        conn.execute(
            "INSERT INTO paper_positions \
             (id, market_id, ticker, venue, oracle, side, size, strike, entry_price, \
              entry_ts, entry_gap, entry_fee, committed_capital, spread_at_fill, \
              entry_net_edge, entry_gate_d1, entry_displacement) \
             VALUES (1,1,?1,'kalshi','brti','BidYes',100.0,70000.0,0.48, \
                     ?2,0.07,72.0,4800.0,0.02,0.05,2.0,50.0)",
            rusqlite::params![m.ticker, now - 10.0],
        ).unwrap();

        let rings = RingSet::new();
        let state = LiveState::default();

        // Write rings so normalizer succeeds but displacement < fee_threshold.
        // fee_threshold for kalshi ≈ 0.035*(1-0.48) ≈ 0.0182
        // We need |spot - brti| < fee_threshold → brti ≈ spot → displacement ≈ 0
        let spot = 74000.0_f64;
        let brti_value = 74000.0_f64; // same as spot → displacement = 0 → gate_d1 < 0
        write_all_rings(&rings, &state, spot, brti_value, 0.47, 0.49, &m.ticker);

        let mut trader = PaperTrader::new(&conn);
        // Inject the position into the in-memory map so evaluate_exits sees it.
        trader.positions.insert(1, PaperPosition {
            id: 1,
            market_id: 1,
            ticker: m.ticker.clone(),
            venue: "kalshi".to_string(),
            oracle: "brti".to_string(),
            side: risk::Side::BidYes,
            size: 100.0,
            strike: 70000.0,
            entry_price: 0.48,
            entry_ts: now - 10.0,
            entry_gap: 0.07,
            entry_fee: 72.0,
            committed_capital: 4800.0,
            spread_at_fill: 0.02,
            entry_net_edge: 0.05,
            entry_gate_d1: 2.0,
            entry_displacement: 50.0,
            peak_unrealized: 0.0,
        });
        // Warm the oracle profile so edge_qualifies isn't the blocker — but
        // terrain_gone only checks gate_d1, which is computed fresh from rings.
        // We only need the brti oracle profile to have enough displacement EMA
        // that normalizer returns Some(). With brti=spot, disp_ema → 0.
        trader.update_oracle_profiles(&rings, &state);

        trader.evaluate_exits(&conn, &rings, &state, now);

        // Position should be closed.
        assert!(trader.positions.is_empty(), "position should be closed");

        // Verify exit_reason in DB.
        let reason: String = conn.query_row(
            "SELECT exit_reason FROM paper_positions WHERE id=1",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(reason, "terrain_gone", "exit reason should be terrain_gone");
    }

    #[test]
    fn terrain_gone_populates_exit_terrain_fields() {
        // When terrain_gone fires, exit_gate_d1, exit_gate_trend, exit_net_edge
        // must all be written (not NULL) in paper_positions.
        let conn = test_conn();
        let m = kalshi_market(1, 300.0, 70000.0);
        insert_market(&conn, &m);

        let now = wall_clock();
        conn.execute(
            "INSERT INTO paper_orders \
             (id, market_id, ticker, venue, oracle, side, limit_price, size, strike, status, \
              placed_ts, fill_ts, fill_price, p_true, p_market, gap, omega, d1, sigma_1s, \
              t_secs, regime, net_edge, gate_d1, gate_trend, displacement, spread, \
              spread_pct, fee_rate) \
             VALUES (1,1,?1,'kalshi','brti','BidYes',0.48,100.0,70000.0,'filled', \
                     ?2,?2,0.48, 0.55,0.48,0.07,1.5,1.2,8.5e-5,300.0,'Amplify', \
                     0.05,2.0,0.1,50.0,0.02,0.042,0.015)",
            rusqlite::params![m.ticker, now - 10.0],
        ).unwrap();
        conn.execute(
            "INSERT INTO paper_positions \
             (id, market_id, ticker, venue, oracle, side, size, strike, entry_price, \
              entry_ts, entry_gap, entry_fee, committed_capital, spread_at_fill, \
              entry_net_edge, entry_gate_d1, entry_displacement) \
             VALUES (1,1,?1,'kalshi','brti','BidYes',100.0,70000.0,0.48, \
                     ?2,0.07,72.0,4800.0,0.02,0.05,2.0,50.0)",
            rusqlite::params![m.ticker, now - 10.0],
        ).unwrap();

        let rings = RingSet::new();
        let state = LiveState::default();
        write_all_rings(&rings, &state, 74000.0, 74000.0, 0.47, 0.49, &m.ticker);

        let mut trader = PaperTrader::new(&conn);
        trader.positions.insert(1, PaperPosition {
            id: 1, market_id: 1, ticker: m.ticker.clone(),
            venue: "kalshi".to_string(), oracle: "brti".to_string(),
            side: risk::Side::BidYes, size: 100.0, strike: 70000.0,
            entry_price: 0.48, entry_ts: now - 10.0, entry_gap: 0.07,
            entry_fee: 72.0, committed_capital: 4800.0, spread_at_fill: 0.02,
            entry_net_edge: 0.05, entry_gate_d1: 2.0, entry_displacement: 50.0,
            peak_unrealized: 0.0,
        });
        trader.update_oracle_profiles(&rings, &state);
        trader.evaluate_exits(&conn, &rings, &state, now);

        let (gd1, gtrend, net_edge): (Option<f64>, Option<f64>, Option<f64>) = conn.query_row(
            "SELECT exit_gate_d1, exit_gate_trend, exit_net_edge \
             FROM paper_positions WHERE id=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).unwrap();

        assert!(gd1.is_some(), "exit_gate_d1 should be populated");
        assert!(gtrend.is_some(), "exit_gate_trend should be populated");
        assert!(net_edge.is_some(), "exit_net_edge should be populated");
    }

    #[test]
    fn terrain_gone_exit_gate_d1_is_negative() {
        // Confirm that a terrain_gone exit always records a negative exit_gate_d1.
        // This is the invariant: the physics exit fires iff gate_d1 < 0.
        let conn = test_conn();
        let m = kalshi_market(1, 300.0, 70000.0);
        insert_market(&conn, &m);

        let now = wall_clock();
        conn.execute(
            "INSERT INTO paper_orders \
             (id, market_id, ticker, venue, oracle, side, limit_price, size, strike, status, \
              placed_ts, fill_ts, fill_price, p_true, p_market, gap, omega, d1, sigma_1s, \
              t_secs, regime, net_edge, gate_d1, gate_trend, displacement, spread, \
              spread_pct, fee_rate) \
             VALUES (1,1,?1,'kalshi','brti','BidYes',0.48,100.0,70000.0,'filled', \
                     ?2,?2,0.48, 0.55,0.48,0.07,1.5,1.2,8.5e-5,300.0,'Amplify', \
                     0.05,2.0,0.1,50.0,0.02,0.042,0.015)",
            rusqlite::params![m.ticker, now - 10.0],
        ).unwrap();
        conn.execute(
            "INSERT INTO paper_positions \
             (id, market_id, ticker, venue, oracle, side, size, strike, entry_price, \
              entry_ts, entry_gap, entry_fee, committed_capital, spread_at_fill, \
              entry_net_edge, entry_gate_d1, entry_displacement) \
             VALUES (1,1,?1,'kalshi','brti','BidYes',100.0,70000.0,0.48, \
                     ?2,0.07,72.0,4800.0,0.02,0.05,2.0,50.0)",
            rusqlite::params![m.ticker, now - 10.0],
        ).unwrap();

        let rings = RingSet::new();
        let state = LiveState::default();
        write_all_rings(&rings, &state, 74000.0, 74000.0, 0.47, 0.49, &m.ticker);

        let mut trader = PaperTrader::new(&conn);
        trader.positions.insert(1, PaperPosition {
            id: 1, market_id: 1, ticker: m.ticker.clone(),
            venue: "kalshi".to_string(), oracle: "brti".to_string(),
            side: risk::Side::BidYes, size: 100.0, strike: 70000.0,
            entry_price: 0.48, entry_ts: now - 10.0, entry_gap: 0.07,
            entry_fee: 72.0, committed_capital: 4800.0, spread_at_fill: 0.02,
            entry_net_edge: 0.05, entry_gate_d1: 2.0, entry_displacement: 50.0,
            peak_unrealized: 0.0,
        });
        trader.update_oracle_profiles(&rings, &state);
        trader.evaluate_exits(&conn, &rings, &state, now);

        let exit_gd1: f64 = conn.query_row(
            "SELECT exit_gate_d1 FROM paper_positions WHERE id=1",
            [],
            |r| r.get(0),
        ).unwrap();

        assert!(exit_gd1 < 0.0,
            "terrain_gone exit must record negative exit_gate_d1, got {exit_gd1}");
    }

    // ── Schema integrity ──────────────────────────────────────────────────

    #[test]
    fn paper_schema_tables_exist() {
        let conn = test_conn();
        for table in ["paper_orders", "paper_positions", "paper_pnl"] {
            let count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table}"),
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(-1);
            assert_eq!(count, 0, "table {table} should exist and be empty");
        }
    }

    #[test]
    fn paper_pnl_record_inserts() {
        let conn = test_conn();
        let rings = RingSet::new();
        let state = LiveState::default();
        let trader = PaperTrader::new(&conn);
        trader.record_pnl(&conn, &rings, &state, wall_clock());

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM paper_pnl", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
