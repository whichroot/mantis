//! RiskConfig (12 knobs) + domain types + 14 parameterized functions.
//!
//! Every function here either takes a free parameter from the operator
//! or operates on a domain type (Side, Regime, Venue).
//! Imports from math.rs only.

use super::math;

// ---------------------------------------------------------------------------
// The 12 free parameters — the operator's rulebook
// ---------------------------------------------------------------------------

/// Risk configuration: the 12 free parameters that separate debug from executor.
///
/// The physics (math.rs) has zero knobs. These 12 are all risk management.
/// Loading a different RiskConfig is how you change the agent's posture.
#[derive(Debug, Clone)]
pub struct RiskConfig {
    /// #1: Fractional Kelly — how much of full Kelly to use.
    pub tail_frac: f64,
    /// #2: Kelly cap — max fraction of bankroll per trade.
    pub max_frac: f64,
    /// #3: Regime dampener — Kelly multiplier in mean-reverting regime.
    pub revert_scale: f64,
    /// #4: Limit price cushion from p_true.
    pub offset: f64,
    /// #5: Exit price floor/ceiling (slippage protection).
    pub max_slippage: f64,
    /// #6: Score table gap ceiling.
    pub gap_cap: f64,
    /// #7: Score table time urgency scaling.
    pub time_divisor: f64,
    /// #8: Score table minimum fill rate.
    pub fill_floor: f64,
    /// #9: Exit: take profit threshold (sigma-scaled).
    pub profit_target_sigma: f64,
    /// #10: Exit: trailing stop distance (sigma-scaled).
    pub trail_distance_sigma: f64,
    /// #11: Exit: gap convergence floor (sigma-scaled).
    pub convergence_floor_sigma: f64,
    /// #12: Exit: max loss threshold (sigma-scaled).
    pub max_loss_sigma: f64,
}

impl Default for RiskConfig {
    /// Equilibrium defaults from first experimental runs.
    ///
    /// These are measurement defaults for data collection, not optimized values.
    /// The policy network learns better values from labeled outcomes.
    /// Defaults are conservative-permissive: wide enough that trades get placed
    /// and measured, tight enough to survive the collection period.
    fn default() -> Self {
        Self {
            tail_frac: 0.25,
            max_frac: 0.05,
            revert_scale: 0.10,
            offset: 0.005,
            max_slippage: 0.02,
            gap_cap: 0.10,
            time_divisor: 60.0,
            fill_floor: 0.05,
            profit_target_sigma: 2.0,
            trail_distance_sigma: 1.5,
            convergence_floor_sigma: 0.5,
            max_loss_sigma: 4.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// Trade direction for binary YES/NO prediction markets.
///
/// BidYes = buying the YES outcome token (bullish on the event).
/// BidNo  = buying the NO outcome token (bearish on the event).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    BidYes,
    BidNo,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::BidYes => write!(f, "BidYes"),
            Side::BidNo => write!(f, "BidNo"),
        }
    }
}

/// Market regime: the 2×2 matrix of position × momentum.
///
/// Amplify = position and momentum aligned → trade with full conviction.
/// Revert  = position and momentum opposed → edge is dissolving, reduce exposure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    /// Above strike + rising, or below strike + falling. Aligned.
    Amplify,
    /// Above strike + falling, or below strike + rising. Opposed.
    Revert,
}

impl std::fmt::Display for Regime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Regime::Amplify => write!(f, "amplify"),
            Regime::Revert => write!(f, "revert"),
        }
    }
}

// ---------------------------------------------------------------------------
// Oracle types and adaptive displacement profile
// ---------------------------------------------------------------------------

/// What data source resolves this market.
///
/// The oracle determines the structural lag — and therefore whether
/// a gap represents a tradeable edge or just noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OracleType {
    /// CF Benchmarks Real-Time Index. 60s trailing average.
    /// Kalshi resolution oracle. Structural lag: ~$7-17.
    Brti,
    /// Chainlink Decentralized Oracle Network consensus.
    /// Polymarket up/down resolution. Structural lag: ~$10-26.
    ChainlinkStreams,
    /// Binance 1-minute candle Close. Same source as our feed.
    /// Polymarket daily resolution. Structural lag: ~$0.
    BinanceCandle,
}

impl std::fmt::Display for OracleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OracleType::Brti => write!(f, "BRTI"),
            OracleType::ChainlinkStreams => write!(f, "ChainlinkStreams"),
            OracleType::BinanceCandle => write!(f, "BinanceCandle"),
        }
    }
}

/// Adaptive oracle displacement profile — rolling stats from beacon measurements.
///
/// Updated continuously by the beacon as it measures the displacement
/// between Binance spot and each oracle feed. All values in USD.
///
/// Zero state: `n_obs = 0` means no data yet. `edge_qualifies` returns
/// false until the profile has enough observations.
#[derive(Debug, Clone)]
pub struct OracleProfile {
    pub oracle: OracleType,
    /// Exponential moving average of |displacement| in USD.
    pub disp_ema: f64,
    /// Rolling max |displacement| (decayed).
    pub disp_max: f64,
    /// Welford online variance accumulator (M2).
    m2: f64,
    /// Running mean for Welford.
    welford_mean: f64,
    /// EMA decay factor (0 < alpha < 1). Smaller = longer memory.
    alpha: f64,
    /// Observation count.
    pub n_obs: u64,
    /// Minimum observations before profile is considered warm.
    pub min_obs: u64,
}

impl OracleProfile {
    /// Create a cold profile for the given oracle type.
    ///
    /// `alpha`: EMA decay factor. 0.02 ≈ 50-sample half-life.
    /// `min_obs`: minimum samples before edge qualification is allowed.
    pub fn new(oracle: OracleType, alpha: f64, min_obs: u64) -> Self {
        Self {
            oracle,
            disp_ema: 0.0,
            disp_max: 0.0,
            m2: 0.0,
            welford_mean: 0.0,
            alpha: alpha.clamp(0.001, 0.5),
            n_obs: 0,
            min_obs,
        }
    }

    /// Create with reasonable defaults per oracle type.
    pub fn default_for(oracle: OracleType) -> Self {
        match oracle {
            // BRTI: 1/s cadence, want ~60s warmup
            OracleType::Brti => Self::new(oracle, 0.02, 30),
            // Chainlink: ~1/s via RTDS, want ~60s warmup
            OracleType::ChainlinkStreams => Self::new(oracle, 0.02, 30),
            // Binance candle: displacement is structurally ~0
            OracleType::BinanceCandle => Self::new(oracle, 0.02, 30),
        }
    }

    /// Is the profile warm enough to make decisions?
    #[inline]
    pub fn is_warm(&self) -> bool {
        self.n_obs >= self.min_obs
    }

    /// Feed a new displacement observation (in USD, signed: spot - oracle).
    ///
    /// Updates EMA, max (with decay), and Welford variance.
    pub fn update(&mut self, displacement: f64) {
        if !displacement.is_finite() {
            return;
        }
        let abs_d = displacement.abs();
        self.n_obs += 1;

        // EMA of |displacement|
        if self.n_obs == 1 {
            self.disp_ema = abs_d;
            self.disp_max = abs_d;
            self.welford_mean = displacement;
            self.m2 = 0.0;
        } else {
            self.disp_ema = self.alpha * abs_d + (1.0 - self.alpha) * self.disp_ema;
            // Decayed max: max drifts toward EMA, snaps up on new highs
            self.disp_max = (self.disp_max * (1.0 - self.alpha * 0.1)).max(abs_d);
            // Welford online variance
            let delta = displacement - self.welford_mean;
            self.welford_mean += delta / self.n_obs as f64;
            let delta2 = displacement - self.welford_mean;
            self.m2 += delta * delta2;
        }
    }

    /// Standard deviation of displacement (population).
    #[inline]
    pub fn disp_std(&self) -> f64 {
        if self.n_obs < 2 {
            return 0.0;
        }
        let var = self.m2 / self.n_obs as f64;
        if var > 0.0 {
            var.sqrt()
        } else {
            0.0
        }
    }

    /// Convert a USD displacement to a probability-space gap estimate.
    ///
    /// The probability surface has gamma: a $1 price move creates different
    /// probability changes depending on where you are on the curve and how
    /// much time remains. Local gamma at moneyness d1: φ(d1)/(σ√T × S).
    ///
    /// Δp = φ(d1) × displacement / (σ√T × spot)
    ///
    /// Uses local φ(d1), not ATM φ(0). At ATM nothing changes. At d1=1,
    /// capacity drops 24%. At d1=2, drops 86%. The gate becomes properly
    /// restrictive deep in/out of the money where the probability surface
    /// is flatter and dollar displacement creates less probability movement.
    #[inline]
    pub fn displacement_as_gap(&self, spot: f64, sigma_sqrt_t: f64, d1: f64) -> f64 {
        if spot <= 0.0 || !spot.is_finite() || self.disp_ema <= 0.0 {
            return 0.0;
        }
        if sigma_sqrt_t <= 0.0 || !sigma_sqrt_t.is_finite() {
            return 0.0;
        }
        if !d1.is_finite() {
            return 0.0; // can't see → no capacity → gate rejects
        }
        math::phi_pdf(d1) * self.disp_ema / (sigma_sqrt_t * spot)
    }
}

/// Does this gap represent a structural, tradeable edge?
///
/// Three conditions must ALL hold:
/// 1. Gap exceeds venue fee (pure math — existing check)
/// 2. Oracle profile is warm (enough data to trust)
/// 3. Oracle has meaningful displacement (the edge is structural, not noise)
///
/// `sigma_sqrt_t` = σ_1s × √T_seconds. Required for the gamma-amplified
/// conversion from USD displacement to probability gap. Without it, the
/// conversion is off by ~φ(0)/σ√T (typically 50-100x).
///
/// `d1` = ln(S/K) / (σ√T). Required for local gamma at the current
/// moneyness. Using ATM gamma (φ(0)) everywhere overestimates oracle gap
/// capacity at OTM by up to ~7x. The gate must use local φ(d1).
///
/// For BinanceCandle oracle, condition 3 naturally fails because
/// disp_ema ≈ $0 — the market resolves on the same feed we trade from.
/// No hardcoded blocklist needed; the data decides.
#[inline]
pub fn edge_qualifies(
    gap_abs: f64,
    p_market: f64,
    venue: &Venue,
    profile: &OracleProfile,
    spot: f64,
    sigma_sqrt_t: f64,
    d1: f64,
) -> bool {
    // 1. Must exceed venue fee
    if !venue.gap_exceeds_fee(gap_abs, p_market) {
        return false;
    }
    // 2. Must have enough observations
    if !profile.is_warm() {
        return false;
    }
    // 3. Oracle displacement (gamma-amplified at local moneyness) must exceed
    //    the fee surface. If the oracle's structural lag can't generate gaps
    //    bigger than fees at this d1, any observed gap is noise.
    let oracle_gap_capacity = profile.displacement_as_gap(spot, sigma_sqrt_t, d1);
    let fee_rate = venue.taker_rate(p_market);
    oracle_gap_capacity > fee_rate
}

// ---------------------------------------------------------------------------
// Venue / fee types
// ---------------------------------------------------------------------------

/// Venue identity for fee surface dispatch.
///
/// Fee models are pure math — polynomial/linear surfaces mapping p_market -> fee_rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    /// Polymarket crypto: taker = 0.25 * (p(1-p))^2, maker = 0.
    PolymarketCrypto,
    /// Polymarket sports: taker = 0.0175 * p(1-p), maker = 0.
    PolymarketSports,
    /// Kalshi general: taker = 0.07 * (1-p), maker = 0.0175 * (1-p).
    KalshiGeneral,
    /// Kalshi index: taker = 0.035 * (1-p), maker = 0.0175 * (1-p).
    KalshiIndex,
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Venue::PolymarketCrypto => write!(f, "PolymarketCrypto"),
            Venue::PolymarketSports => write!(f, "PolymarketSports"),
            Venue::KalshiGeneral => write!(f, "KalshiGeneral"),
            Venue::KalshiIndex => write!(f, "KalshiIndex"),
        }
    }
}

impl Venue {
    /// Taker fee rate at price `p` (fraction of notional).
    #[inline(always)]
    pub fn taker_rate(&self, p: f64) -> f64 {
        match self {
            Self::PolymarketCrypto => math::poly_crypto_taker(p),
            Self::PolymarketSports => math::poly_sports_taker(p),
            Self::KalshiGeneral => math::kalshi_general_taker(p),
            Self::KalshiIndex => math::kalshi_index_taker(p),
        }
    }

    /// Maker fee rate at price `p`.
    #[inline(always)]
    pub fn maker_rate(&self, p: f64) -> f64 {
        match self {
            Self::PolymarketCrypto | Self::PolymarketSports => 0.0,
            Self::KalshiGeneral | Self::KalshiIndex => math::kalshi_maker(p),
        }
    }

    /// Round raw fee to venue precision.
    #[inline(always)]
    pub fn round_fee(&self, raw: f64) -> f64 {
        match self {
            Self::PolymarketCrypto | Self::PolymarketSports => math::poly_round(raw),
            Self::KalshiGeneral | Self::KalshiIndex => math::kalshi_round(raw),
        }
    }

    /// Absolute taker fee (USD/USDC) for `contracts` at price `p`.
    #[inline(always)]
    pub fn taker_fee(&self, p: f64, contracts: f64) -> f64 {
        self.round_fee(self.taker_rate(p) * contracts * p)
    }

    /// Absolute maker fee. Zero on Polymarket.
    #[inline(always)]
    pub fn maker_fee(&self, p: f64, contracts: f64) -> f64 {
        self.round_fee(self.maker_rate(p) * contracts * p)
    }

    /// Is `|gap|` large enough to justify crossing the spread as taker?
    #[inline(always)]
    pub fn gap_exceeds_fee(&self, gap_abs: f64, p_market: f64) -> bool {
        gap_abs > self.taker_rate(p_market)
    }
}

// ---------------------------------------------------------------------------
// Ω scalar field — the entry/exit decision surface
// ---------------------------------------------------------------------------

/// The entry/exit decision scalar field.
///
/// Ω(p_true, p_market, fee, spread, side, regime; θ, C) =
///   min( (|p_true − p_market| − fee − excess/2) · τ · R / L,  κ )  ×  C / cost  −  1
///
/// where:
///   excess = max(spread − fee, 0)
///   L      = loss_frac(p_true, side)
///   cost   = cost_per_contract(p_market, side)
///   R      = regime_kelly_scale(regime, θ₃)
///   τ      = tail_frac (θ₁)
///   κ      = max_frac  (θ₂)
///
/// The excess term accounts for the observed bid-ask spread above the fee
/// floor. Entry is maker (free), exit is taker (spread/2). When spread is
/// unknown (0.0) or at the fee floor, excess = 0 and Ω is unchanged.
///
/// Sign determines action:
///   Ω ≥ 0  →  system acts, size = floor(Ω + 1) contracts
///   Ω < 0  →  no trade
///
/// The topology of {Ω ≥ 0} over (S, K, σ, T, p, μ) is the complete
/// description of where the system can exist.
///
/// The caller passes taker_fee for entries, maker_fee for exits.
/// Four evaluations per point (buy_yes, buy_no, sell_yes, sell_no)
/// with position holding as the mask on exit actions.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
pub fn omega_at(
    p_true_val: f64,
    p_market: f64,
    fee_rate: f64,
    spread: f64,
    side: Side,
    regime: Regime,
    capital: f64,
    tail_frac: f64,
    max_frac: f64,
    revert_scale: f64,
) -> f64 {
    // NaN firewall — every boundary guarded
    if !(p_true_val > 0.0 && p_true_val < 1.0) {
        return -1.0;
    }
    if !(p_market > 0.0 && p_market < 1.0) {
        return -1.0;
    }
    if !fee_rate.is_finite() || fee_rate < 0.0 {
        return -1.0;
    }
    if !(capital > 0.0 && capital.is_finite()) {
        return -1.0;
    }
    if !(tail_frac > 0.0 && tail_frac.is_finite()) {
        return -1.0;
    }
    if !(max_frac > 0.0 && max_frac.is_finite()) {
        return -1.0;
    }
    if !revert_scale.is_finite() {
        return -1.0;
    }

    let g_abs = (p_true_val - p_market).abs();
    let excess = (spread - fee_rate).max(0.0);
    let ne = math::net_edge(g_abs, fee_rate + excess / 2.0);
    if ne <= 0.0 {
        return -1.0;
    }

    let loss = compute_loss_frac(p_true_val, side);
    let cost = cost_per_contract(p_market, side);
    if cost <= 0.0 {
        return -1.0;
    }

    let r = regime_kelly_scale(regime, revert_scale);
    let kr = kelly_raw(ne, loss);
    // R inside the min: regime scales perceived edge before the cap
    let kelly_eff = (kr * tail_frac * r).min(max_frac);

    kelly_eff * capital / cost - 1.0
}

// ---------------------------------------------------------------------------
// Exit configuration
// ---------------------------------------------------------------------------

/// Configuration for sigma-scaled exit thresholds.
#[derive(Debug, Clone)]
pub struct ExitConfig {
    /// Reference sigma for scaling (median sigma_1s). Default 0.0003; adaptive via SigmaEma.
    pub sigma_reference: f64,
    /// Profit target as fraction of entry_gap. Default 0.50.
    pub base_profit_target: f64,
    /// Trailing stop distance as fraction of entry_gap. Default 0.40.
    pub base_trail_distance: f64,
    /// Convergence floor (absolute gap). Default 0.005.
    pub base_convergence_floor: f64,
    /// Max loss as fraction of entry_gap. Default 1.0.
    pub max_loss_fraction: f64,
    /// Sigma ratio clamp floor. Default 0.25.
    pub sigma_ratio_min: f64,
    /// Sigma ratio clamp ceiling. Default 4.0.
    pub sigma_ratio_max: f64,
}

impl Default for ExitConfig {
    fn default() -> Self {
        Self {
            sigma_reference: 0.0003,
            base_profit_target: 0.50,
            base_trail_distance: 0.40,
            base_convergence_floor: 0.005,
            max_loss_fraction: 1.0,
            sigma_ratio_min: 0.25,
            sigma_ratio_max: 4.0,
        }
    }
}

/// Pre-computed exit thresholds. All fields guaranteed finite and > 0.
#[derive(Debug, Clone)]
pub struct ExitThresholds {
    /// Unrealized PnL target to exit.
    pub profit_target: f64,
    /// Max pullback from peak PnL.
    pub trail_distance: f64,
    /// Min |gap| to consider converged.
    pub convergence_floor: f64,
    /// Max unrealized loss before cut.
    pub max_loss: f64,
}

// ---------------------------------------------------------------------------
// 14 parameterized functions (functions 24–37)
// ---------------------------------------------------------------------------

/// Binary Kelly criterion: `f* = net_edge / loss_fraction`
///
/// Returns 0.0 when no positive edge or zero loss fraction.
#[inline(always)]
pub fn kelly_raw(net_edge: f64, loss_frac: f64) -> f64 {
    if net_edge <= 0.0 || loss_frac <= 0.0 {
        return 0.0;
    }
    net_edge / loss_frac
}

/// Fractional Kelly with tail protection + hard cap.
///
/// `fraction = kelly_raw * tail_frac`, clamped to `max_frac`.
/// Uses free parameters #1 (tail_frac) and #2 (max_frac).
#[inline(always)]
pub fn kelly_fraction(net_edge: f64, loss_frac: f64, tail_frac: f64, max_frac: f64) -> f64 {
    (kelly_raw(net_edge, loss_frac) * tail_frac).min(max_frac)
}

/// Cost per contract for the given side.
///
/// The complementary token relationship: YES + NO = $1.
/// BidYes: buy YES token at p_market. Cost = p_market.
/// BidNo:  buy NO token at 1 - p_market. Cost = 1 - p_market.
///
/// This is the primitive that `compute_size` and `capital_at_risk` derive from.
/// Returns 0.0 on degenerate inputs (p outside (0,1), non-finite).
#[inline(always)]
pub fn cost_per_contract(p_market: f64, side: Side) -> f64 {
    if !p_market.is_finite() || p_market <= 0.0 || p_market >= 1.0 {
        return 0.0;
    }
    match side {
        Side::BidYes => p_market,
        Side::BidNo => 1.0 - p_market,
    }
}

/// Order size in whole contracts: `floor(kelly * capital / cost)`.
///
/// The caller passes `cost_per_contract(p_market, side)` as the third argument.
/// This function is pure arithmetic — it doesn't know what a Side is.
///
/// Returns 0.0 when result < 1.0 (never round up to avoid oversizing).
#[inline(always)]
pub fn compute_size(kelly: f64, capital: f64, cost: f64) -> f64 {
    if kelly <= 0.0
        || capital <= 0.0
        || cost <= 0.0
        || !kelly.is_finite()
        || !capital.is_finite()
        || !cost.is_finite()
    {
        return 0.0;
    }
    let raw = (kelly * capital / cost).floor();
    if raw < 1.0 {
        0.0
    } else {
        raw
    }
}

/// Table scoring for allocator capital distribution.
///
/// Uses free parameters #6 (gap_cap), #7 (time_divisor), #8 (fill_floor).
#[inline(always)]
pub fn score_table(
    gap_abs: f64,
    fee_rate: f64,
    t_secs: f64,
    fill_rate: f64,
    gap_cap: f64,
    time_divisor: f64,
    fill_floor: f64,
) -> f64 {
    if !gap_abs.is_finite() || !t_secs.is_finite() || !fill_rate.is_finite() {
        return 0.0;
    }

    let gap_component = if fee_rate > 0.0 {
        (gap_abs / fee_rate).min(gap_cap)
    } else {
        gap_abs.min(1.0) * gap_cap
    };

    let time_component = 1.0 / (1.0 + t_secs / time_divisor.max(1.0));
    let fill_component = fill_rate.clamp(fill_floor.max(0.01), 1.0);

    gap_component * time_component * fill_component
}

/// Classify regime from spot displacement and per-tick drift (mu).
///
/// mu > 0 = upward momentum, mu < 0 = downward momentum.
/// Returns Amplify when position and momentum agree, Revert when opposed.
///
/// Unknown flow (mu = 0 or NaN) defaults to Revert: when you can't read
/// the flow, act as if it's against you. The cost of a false Revert is
/// reduced position size. The cost of a false Amplify is full-sized entry
/// with no momentum confirmation. The asymmetry favors Revert.
#[inline(always)]
pub fn classify_regime(spot: f64, strike: f64, mu: f64) -> Regime {
    if !mu.is_finite() || mu == 0.0 {
        return Regime::Revert;
    }
    let above_strike = spot > strike;
    let rising = mu > 0.0;
    if above_strike == rising {
        Regime::Amplify
    } else {
        Regime::Revert
    }
}

/// Kelly scaling factor for regime.
///
/// Amplify: full Kelly (1.0). Revert: reduced by free parameter #3 (revert_scale).
#[inline(always)]
pub fn regime_kelly_scale(regime: Regime, revert_scale: f64) -> f64 {
    match regime {
        Regime::Amplify => 1.0,
        Regime::Revert => revert_scale.clamp(0.0, 1.0),
    }
}

/// Determine trade side from gap sign.
/// gap >= 0: market underprices UP -> BidYes
/// gap < 0:  market overprices UP  -> BidNo
#[inline(always)]
pub fn compute_side(gap: f64) -> Side {
    if gap >= 0.0 {
        Side::BidYes
    } else {
        Side::BidNo
    }
}

/// Loss fraction for Kelly denominator.
/// BidYes: lose if NO resolves -> loss = 1 - p_true
/// BidNo:  lose if YES resolves -> loss = p_true
/// Clamped to (0.0, 1.0) — never exactly 0 or 1 to avoid div-by-zero.
#[inline(always)]
pub fn compute_loss_frac(p_true_val: f64, side: Side) -> f64 {
    let raw = match side {
        Side::BidYes => 1.0 - p_true_val,
        Side::BidNo => p_true_val,
    };
    raw.clamp(1e-10, 1.0 - 1e-10)
}

/// Limit order price, offset from p_true toward conservative side.
/// Uses free parameter #4 (offset).
///
/// BidYes:  p_true - offset (buy below true value)
/// BidNo:   p_true + offset (sell above true value)
/// Clamped to [0.01, 0.99].
#[inline(always)]
pub fn compute_limit_price(p_true_val: f64, offset: f64, side: Side) -> f64 {
    if !p_true_val.is_finite() || !offset.is_finite() {
        return 0.5;
    }
    let raw = match side {
        Side::BidYes => p_true_val - offset,
        Side::BidNo => p_true_val + offset,
    };
    raw.clamp(0.01, 0.99)
}

/// Worst acceptable price for market exit (slippage protection).
/// Uses free parameter #5 (max_slippage).
///
/// BidYes exit (selling YES): floor = p_market * (1 - slippage), min 0.01
/// BidNo exit (buying YES back): ceil = p_market * (1 + slippage), max 0.99
#[inline(always)]
pub fn compute_worst_price(p_market: f64, side: Side, max_slippage: f64) -> f64 {
    let slippage = max_slippage.clamp(0.0, 0.5);
    match side {
        Side::BidYes => (p_market * (1.0 - slippage)).max(0.01),
        Side::BidNo => (p_market * (1.0 + slippage)).min(0.99),
    }
}

/// Unrealized PnL per contract.
/// BidYes:  current - entry (profit when price rises)
/// BidNo:   entry - current (profit when price falls)
#[inline(always)]
pub fn unrealized_pnl(entry_price: f64, current_price: f64, side: Side) -> f64 {
    if !entry_price.is_finite() || !current_price.is_finite() {
        return 0.0;
    }
    match side {
        Side::BidYes => current_price - entry_price,
        Side::BidNo => entry_price - current_price,
    }
}

/// Total unrealized PnL = per_contract * size.
#[inline(always)]
pub fn total_unrealized_pnl(entry_price: f64, current_price: f64, size: f64, side: Side) -> f64 {
    if !size.is_finite() || size <= 0.0 {
        return 0.0;
    }
    unrealized_pnl(entry_price, current_price, side) * size
}

/// Capital committed to a position.
///
/// Uses `cost_per_contract` — the same primitive as `compute_size`.
/// capital_at_risk = size * cost_per_contract(entry_price, side)
#[inline(always)]
pub fn capital_at_risk(size: f64, entry_price: f64, side: Side) -> f64 {
    if !size.is_finite()
        || size <= 0.0
        || !entry_price.is_finite()
        || entry_price <= 0.0
        || entry_price >= 1.0
    {
        return 0.0;
    }
    size * cost_per_contract(entry_price, side)
}

/// Compute sigma-scaled exit thresholds.
/// Uses free parameters #9-#12 (via ExitConfig).
///
/// sigma_ratio = (sigma_1s / sigma_reference).clamp(min, max)
/// Hot regime (ratio > 1): wider targets. Cold regime (ratio < 1): tighter.
/// max_loss is NOT sigma-scaled — survival is absolute.
pub fn exit_thresholds(cfg: &ExitConfig, entry_gap: f64, sigma_1s: f64) -> ExitThresholds {
    let entry_gap = if entry_gap.is_finite() && entry_gap > 0.0 {
        entry_gap
    } else {
        0.001
    };
    let sigma_ratio = if cfg.sigma_reference > 0.0 && sigma_1s.is_finite() && sigma_1s > 0.0 {
        (sigma_1s / cfg.sigma_reference).clamp(cfg.sigma_ratio_min, cfg.sigma_ratio_max)
    } else {
        cfg.sigma_ratio_min // unknown vol → tightest exits → protect capital
    };

    ExitThresholds {
        profit_target: (cfg.base_profit_target * entry_gap * sigma_ratio).max(0.001),
        trail_distance: (cfg.base_trail_distance * entry_gap * sigma_ratio).max(0.001),
        convergence_floor: (cfg.base_convergence_floor * sigma_ratio).max(0.0001),
        max_loss: (cfg.max_loss_fraction * entry_gap).max(0.001),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    const EPS: f64 = 1e-10;

    // -- kelly (5 tests) --------------------------------------------------

    #[test]
    fn kelly_raw_positive() {
        assert!((kelly_raw(0.05, 0.4) - 0.125).abs() < EPS);
    }

    #[test]
    fn kelly_raw_no_edge() {
        assert_eq!(kelly_raw(0.0, 0.4), 0.0);
        assert_eq!(kelly_raw(-0.01, 0.4), 0.0);
    }

    #[test]
    fn kelly_raw_zero_loss() {
        assert_eq!(kelly_raw(0.05, 0.0), 0.0);
    }

    #[test]
    fn kelly_fraction_quarter() {
        let f = kelly_fraction(0.05, 0.4, 0.25, 1.0);
        assert!((f - 0.03125).abs() < EPS);
    }

    #[test]
    fn kelly_fraction_capped() {
        let f = kelly_fraction(0.5, 0.1, 0.5, 0.25);
        assert_eq!(f, 0.25);
    }

    // -- cost_per_contract (4 tests) --------------------------------------

    #[test]
    fn cost_bid_yes() {
        assert!((cost_per_contract(0.60, Side::BidYes) - 0.60).abs() < EPS);
    }

    #[test]
    fn cost_bid_no() {
        assert!((cost_per_contract(0.60, Side::BidNo) - 0.40).abs() < EPS);
    }

    #[test]
    fn cost_at_center() {
        assert!((cost_per_contract(0.50, Side::BidYes) - 0.50).abs() < EPS);
        assert!((cost_per_contract(0.50, Side::BidNo) - 0.50).abs() < EPS);
    }

    #[test]
    fn cost_degenerate() {
        assert_eq!(cost_per_contract(0.0, Side::BidYes), 0.0);
        assert_eq!(cost_per_contract(1.0, Side::BidNo), 0.0);
        assert_eq!(cost_per_contract(f64::NAN, Side::BidYes), 0.0);
        assert_eq!(cost_per_contract(-0.5, Side::BidYes), 0.0);
    }

    // -- compute_size (7 tests) -------------------------------------------

    #[test]
    fn size_normal() {
        // floor(0.1 * 5000 / 0.5) = 1000
        assert!((compute_size(0.1, 5000.0, 0.5) - 1000.0).abs() < EPS);
    }

    #[test]
    fn size_high_cost() {
        // floor(0.1 * 5000 / 0.70) = 714
        assert!((compute_size(0.1, 5000.0, 0.70) - 714.0).abs() < EPS);
    }

    #[test]
    fn size_low_cost() {
        // floor(0.1 * 5000 / 0.30) = 1666
        assert!((compute_size(0.1, 5000.0, 0.30) - 1666.0).abs() < EPS);
    }

    #[test]
    fn size_zero_kelly() {
        assert_eq!(compute_size(0.0, 5000.0, 0.5), 0.0);
    }

    #[test]
    fn size_sub_one_contract() {
        assert_eq!(compute_size(0.001, 100.0, 0.5), 0.0);
    }

    #[test]
    fn size_zero_capital() {
        assert_eq!(compute_size(0.1, 0.0, 0.5), 0.0);
    }

    #[test]
    fn size_nan_input() {
        assert_eq!(compute_size(f64::NAN, 5000.0, 0.5), 0.0);
        assert_eq!(compute_size(0.1, f64::NAN, 0.5), 0.0);
        assert_eq!(compute_size(0.1, 5000.0, f64::NAN), 0.0);
        assert_eq!(compute_size(0.1, 5000.0, 0.0), 0.0);
    }

    // -- Kelly capital invariant (1 test) ---------------------------------

    #[test]
    fn kelly_capital_invariant() {
        // For any p_market and side, size * cost ≈ kelly * capital
        // (within one contract's cost of floor rounding)
        let kelly = 0.10;
        let capital = 5000.0;
        let expected = kelly * capital; // $500

        for &p in &[0.10, 0.20, 0.30, 0.50, 0.70, 0.80, 0.90] {
            for &side in &[Side::BidYes, Side::BidNo] {
                let cost = cost_per_contract(p, side);
                let size = compute_size(kelly, capital, cost);
                let committed = size * cost;
                // Must be within one contract's cost of expected
                assert!(
                    (committed - expected).abs() <= cost + EPS,
                    "Kelly invariant violated: side={side}, p={p}, \
                     committed={committed}, expected={expected}, cost={cost}"
                );
                // Must not exceed expected (floor never oversizes)
                assert!(
                    committed <= expected + EPS,
                    "Oversized: side={side}, p={p}, \
                     committed={committed}, expected={expected}"
                );
            }
        }
    }

    // -- score_table (4 tests) --------------------------------------------

    #[test]
    fn score_gap_dominance() {
        let s1 = score_table(0.05, 0.01, 300.0, 0.5, 10.0, 600.0, 0.1);
        let s2 = score_table(0.10, 0.01, 300.0, 0.5, 10.0, 600.0, 0.1);
        assert!(s2 > s1, "s2={s2} s1={s1}");
    }

    #[test]
    fn score_time_urgency() {
        let s_far = score_table(0.05, 0.01, 3600.0, 0.5, 10.0, 600.0, 0.1);
        let s_near = score_table(0.05, 0.01, 60.0, 0.5, 10.0, 600.0, 0.1);
        assert!(s_near > s_far, "s_near={s_near} s_far={s_far}");
    }

    #[test]
    fn score_fill_rate() {
        let s_low = score_table(0.05, 0.01, 300.0, 0.2, 10.0, 600.0, 0.1);
        let s_high = score_table(0.05, 0.01, 300.0, 0.8, 10.0, 600.0, 0.1);
        assert!(s_high > s_low, "s_high={s_high} s_low={s_low}");
    }

    #[test]
    fn score_zero_fee() {
        let s = score_table(0.05, 0.0, 300.0, 0.5, 10.0, 600.0, 0.1);
        let expected_gap = 0.05_f64.min(1.0) * 10.0;
        let expected_time = 1.0 / (1.0 + 300.0 / 600.0);
        let expected_fill = 0.5_f64.clamp(0.1_f64.max(0.01), 1.0);
        let expected = expected_gap * expected_time * expected_fill;
        assert!((s - expected).abs() < 1e-9, "s={s} expected={expected}");
    }

    // -- compute_side (2 tests) -------------------------------------------

    #[test]
    fn side_bid_yes_on_positive_gap() {
        assert_eq!(compute_side(0.05), Side::BidYes);
    }

    #[test]
    fn side_bid_no_on_negative_gap() {
        assert_eq!(compute_side(-0.05), Side::BidNo);
    }

    // -- compute_loss_frac (3 tests) --------------------------------------

    #[test]
    fn loss_frac_bid_yes() {
        let v = compute_loss_frac(0.7, Side::BidYes);
        assert!((v - 0.3).abs() < EPS);
    }

    #[test]
    fn loss_frac_bid_no() {
        let v = compute_loss_frac(0.7, Side::BidNo);
        assert!((v - 0.7).abs() < EPS);
    }

    #[test]
    fn loss_frac_clamped() {
        let v = compute_loss_frac(1.0, Side::BidYes);
        assert!(v > 0.0, "must not be exactly 0");
        assert!((v - 1e-10).abs() < EPS);
    }

    // -- compute_limit_price (4 tests) ------------------------------------

    #[test]
    fn limit_buy_below_p_true() {
        let p = compute_limit_price(0.6, 0.01, Side::BidYes);
        assert!((p - 0.59).abs() < EPS);
    }

    #[test]
    fn limit_sell_above_p_true() {
        let p = compute_limit_price(0.6, 0.01, Side::BidNo);
        assert!((p - 0.61).abs() < EPS);
    }

    #[test]
    fn limit_clamped_low() {
        let p = compute_limit_price(0.01, 0.02, Side::BidYes);
        assert!((p - 0.01).abs() < EPS);
    }

    #[test]
    fn limit_zero_offset() {
        let p = compute_limit_price(0.6, 0.0, Side::BidYes);
        assert!((p - 0.6).abs() < EPS);
    }

    // -- compute_worst_price (3 tests) ------------------------------------

    #[test]
    fn worst_buy_exit() {
        let w = compute_worst_price(0.6, Side::BidYes, 0.05);
        assert!((w - 0.57).abs() < EPS);
    }

    #[test]
    fn worst_sell_exit() {
        let w = compute_worst_price(0.4, Side::BidNo, 0.05);
        assert!((w - 0.42).abs() < EPS);
    }

    #[test]
    fn worst_floor() {
        let w = compute_worst_price(0.02, Side::BidYes, 0.5);
        assert!((w - 0.01).abs() < EPS);
    }

    // -- unrealized_pnl (4 tests) -----------------------------------------

    #[test]
    fn pnl_buy_profit() {
        let p = unrealized_pnl(0.4, 0.6, Side::BidYes);
        assert!((p - 0.2).abs() < EPS);
    }

    #[test]
    fn pnl_buy_loss() {
        let p = unrealized_pnl(0.6, 0.4, Side::BidYes);
        assert!((p - (-0.2)).abs() < EPS);
    }

    #[test]
    fn pnl_sell_profit() {
        let p = unrealized_pnl(0.6, 0.4, Side::BidNo);
        assert!((p - 0.2).abs() < EPS);
    }

    #[test]
    fn pnl_sell_loss() {
        let p = unrealized_pnl(0.4, 0.6, Side::BidNo);
        assert!((p - (-0.2)).abs() < EPS);
    }

    // -- capital_at_risk (3 tests) ----------------------------------------

    #[test]
    fn car_buy() {
        let c = capital_at_risk(100.0, 0.6, Side::BidYes);
        assert!((c - 60.0).abs() < EPS);
    }

    #[test]
    fn car_sell() {
        let c = capital_at_risk(100.0, 0.6, Side::BidNo);
        assert!((c - 40.0).abs() < EPS);
    }

    #[test]
    fn car_zero_size() {
        assert_eq!(capital_at_risk(0.0, 0.6, Side::BidYes), 0.0);
    }

    // -- classify_regime / regime_kelly_scale (4 tests) --------------------

    #[test]
    fn regime_amplify_aligned() {
        assert_eq!(classify_regime(70_100.0, 70_000.0, 0.001), Regime::Amplify);
        assert_eq!(classify_regime(69_900.0, 70_000.0, -0.001), Regime::Amplify);
    }

    #[test]
    fn regime_revert_opposed() {
        assert_eq!(classify_regime(70_100.0, 70_000.0, -0.001), Regime::Revert);
        assert_eq!(classify_regime(69_900.0, 70_000.0, 0.001), Regime::Revert);
    }

    #[test]
    fn regime_kelly_amplify_full() {
        assert_eq!(regime_kelly_scale(Regime::Amplify, 0.25), 1.0);
    }

    #[test]
    fn regime_kelly_revert_scaled() {
        assert!((regime_kelly_scale(Regime::Revert, 0.25) - 0.25).abs() < EPS);
    }

    // -- venue dispatch (8 tests) -----------------------------------------

    #[test]
    fn venue_taker_dispatch() {
        let v = Venue::PolymarketCrypto;
        assert!((v.taker_rate(0.50) - 0.015625).abs() < EPS);

        let v = Venue::KalshiGeneral;
        assert!((v.taker_rate(0.50) - 0.035).abs() < EPS);
    }

    #[test]
    fn venue_maker_dispatch() {
        assert_eq!(Venue::PolymarketCrypto.maker_rate(0.50), 0.0);
        assert!(Venue::KalshiGeneral.maker_rate(0.50) > 0.0);
    }

    #[test]
    fn venue_gap_exceeds_fee_poly() {
        let v = Venue::PolymarketCrypto;
        assert!(v.gap_exceeds_fee(0.01, 0.05));
        assert!(!v.gap_exceeds_fee(0.01, 0.50));
        assert!(v.gap_exceeds_fee(0.02, 0.50));
    }

    #[test]
    fn venue_gap_exceeds_fee_kalshi() {
        let v = Venue::KalshiGeneral;
        assert!(!v.gap_exceeds_fee(0.03, 0.50));
        assert!(v.gap_exceeds_fee(0.04, 0.50));
    }

    #[test]
    fn poly_taker_fee_100_contracts_center() {
        let v = Venue::PolymarketCrypto;
        let fee = v.taker_fee(0.50, 100.0);
        assert!((fee - 0.7813).abs() < EPS, "fee={fee}");
    }

    #[test]
    fn poly_maker_fee_always_zero() {
        let v = Venue::PolymarketCrypto;
        assert_eq!(v.maker_fee(0.50, 100.0), 0.0);
        assert_eq!(v.maker_fee(0.10, 1000.0), 0.0);
    }

    #[test]
    fn kalshi_maker_fee_nonzero() {
        let v = Venue::KalshiGeneral;
        let fee = v.maker_fee(0.50, 100.0);
        assert!((fee - 0.44).abs() < EPS, "fee={fee}");
    }

    // -- exit_thresholds (6 tests) ----------------------------------------

    #[test]
    fn exit_hot_regime_widens() {
        let cfg = ExitConfig::default();
        let t = exit_thresholds(&cfg, 0.10, 0.0006);
        assert!(
            (t.profit_target - 0.10).abs() < 1e-9,
            "profit_target={}",
            t.profit_target
        );
        assert!(
            (t.trail_distance - 0.08).abs() < 1e-9,
            "trail_distance={}",
            t.trail_distance
        );
    }

    #[test]
    fn exit_cold_regime_tightens() {
        let cfg = ExitConfig::default();
        let t = exit_thresholds(&cfg, 0.10, 0.00015);
        assert!(
            (t.profit_target - 0.025).abs() < 1e-9,
            "profit_target={}",
            t.profit_target
        );
        assert!(
            (t.trail_distance - 0.02).abs() < 1e-9,
            "trail_distance={}",
            t.trail_distance
        );
    }

    #[test]
    fn exit_sigma_unknown_tightens() {
        // sigma_reference = 0 → condition fails → sigma_ratio_min (0.25)
        // profit_target = 0.50 * 0.10 * 0.25 = 0.0125
        let cfg = ExitConfig {
            sigma_reference: 0.0,
            ..Default::default()
        };
        let t = exit_thresholds(&cfg, 0.10, 0.0003);
        assert!(
            (t.profit_target - 0.0125).abs() < 1e-9,
            "unknown sigma should tighten, got {}",
            t.profit_target
        );
    }

    #[test]
    fn exit_all_finite_positive() {
        let cfg = ExitConfig::default();
        let t = exit_thresholds(&cfg, 0.05, 0.0003);
        assert!(t.profit_target.is_finite() && t.profit_target > 0.0);
        assert!(t.trail_distance.is_finite() && t.trail_distance > 0.0);
        assert!(t.convergence_floor.is_finite() && t.convergence_floor > 0.0);
        assert!(t.max_loss.is_finite() && t.max_loss > 0.0);
    }

    #[test]
    fn exit_max_loss_not_scaled() {
        let cfg = ExitConfig::default();
        let t_hot = exit_thresholds(&cfg, 0.10, 0.0006);
        let t_cold = exit_thresholds(&cfg, 0.10, 0.00015);
        assert!(
            (t_hot.max_loss - t_cold.max_loss).abs() < EPS,
            "hot={} cold={}",
            t_hot.max_loss,
            t_cold.max_loss
        );
        assert!((t_hot.max_loss - 0.10).abs() < EPS);
    }

    #[test]
    fn exit_entry_gap_zero() {
        let cfg = ExitConfig::default();
        let t = exit_thresholds(&cfg, 0.0, 0.0003);
        assert!(t.profit_target >= 0.001);
        assert!(t.max_loss >= 0.001);
    }

    // -- omega_at (13 tests) ----------------------------------------------

    #[test]
    fn omega_negative_when_no_edge() {
        // fee exceeds gap → ne ≤ 0 → Ω = -1
        let pt = 0.52;
        let pm = 0.50;
        let fee = 0.05; // fee > |gap| = 0.02
        let o = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.25,
        );
        assert!(o < 0.0, "omega should be negative, got {o}");
    }

    #[test]
    fn omega_positive_with_edge() {
        // Large gap, small fee → should produce positive Ω
        let pt = 0.70;
        let pm = 0.50;
        let fee = 0.015; // poly crypto at 0.50
        let o = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.25,
        );
        assert!(o > 0.0, "omega should be positive, got {o}");
    }

    #[test]
    fn omega_size_encoding() {
        // size = floor(Ω + 1) must match compute_size
        let pt = 0.65;
        let pm = 0.50;
        let fee = 0.015;
        let capital = 5000.0;
        let tau = 0.25;
        let kappa = 0.10;

        let o = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            capital,
            tau,
            kappa,
            0.25,
        );
        let size_from_omega = if o >= 0.0 { (o + 1.0).floor() } else { 0.0 };

        // Reproduce via existing functions (R=1 for Amplify)
        let ne = math::net_edge((pt - pm).abs(), fee);
        let loss = compute_loss_frac(pt, Side::BidYes);
        let kr = kelly_raw(ne, loss);
        let kelly_eff = (kr * tau * 1.0).min(kappa); // R=1 for Amplify
        let cost = cost_per_contract(pm, Side::BidYes);
        let size_from_pipeline = compute_size(kelly_eff, capital, cost);

        assert!(
            (size_from_omega - size_from_pipeline).abs() < EPS,
            "omega size={size_from_omega}, pipeline size={size_from_pipeline}, omega={o}"
        );
    }

    #[test]
    fn omega_correct_side_wins() {
        // When gap > 0, BidYes should have higher Ω than BidNo
        // Use high cap so it doesn't bind — lets the loss/cost asymmetry show
        let pt = 0.65;
        let pm = 0.50;
        let fee = 0.015;
        let args = (100.0, 0.25, 1.0, 0.25); // small capital, no cap

        let o_yes = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        let o_no = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidNo,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        assert!(
            o_yes > o_no,
            "BidYes should win when gap>0: yes={o_yes}, no={o_no}"
        );

        // When gap < 0, BidNo should win
        let pt2 = 0.35;
        let o_yes2 = omega_at(
            pt2,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        let o_no2 = omega_at(
            pt2,
            pm,
            fee,
            0.0,
            Side::BidNo,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        assert!(
            o_no2 > o_yes2,
            "BidNo should win when gap<0: no={o_no2}, yes={o_yes2}"
        );
    }

    #[test]
    fn omega_regime_revert_reduces() {
        let pt = 0.65;
        let pm = 0.50;
        let fee = 0.015;

        let o_amp = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.25,
        );
        let o_rev = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Revert,
            10_000.0,
            0.25,
            0.05,
            0.25,
        );
        assert!(
            o_amp > o_rev,
            "Amplify should produce higher Ω: amp={o_amp}, rev={o_rev}"
        );
    }

    #[test]
    fn omega_r_inside_min() {
        // Verify R is inside the min, not outside.
        // When cap binds in Amplify but not after R scaling in Revert:
        //   min(raw * τ * R, κ) ≠ min(raw * τ, κ) * R
        let pt: f64 = 0.70;
        let pm: f64 = 0.50;
        let fee = 0.01;
        let tau = 0.25;
        let kappa = 0.05;
        let r_scale = 0.25;

        let ne: f64 = math::net_edge((pt - pm).abs(), fee);
        let loss = compute_loss_frac(pt, Side::BidYes);
        let kr = kelly_raw(ne, loss);
        // raw * τ = large enough to bind the cap in Amplify
        let raw_tau = kr * tau;
        assert!(
            raw_tau > kappa,
            "test requires cap to bind: raw_tau={raw_tau}"
        );
        // raw * τ * R should be below cap
        let raw_tau_r = kr * tau * r_scale;

        let o = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Revert,
            10_000.0,
            tau,
            kappa,
            r_scale,
        );
        let cost = cost_per_contract(pm, Side::BidYes);

        if raw_tau_r < kappa {
            // R brought it below cap: kelly_eff = raw_tau_r, not κ * R
            let expected = raw_tau_r * 10_000.0 / cost - 1.0;
            assert!(
                (o - expected).abs() < 1e-9,
                "R inside min: omega={o}, expected={expected}"
            );
        }
    }

    #[test]
    fn omega_maker_vs_taker() {
        // Maker fee (lower) should produce higher Ω than taker fee
        // Use high cap so it doesn't bind — lets the fee difference show
        let pt = 0.60;
        let pm = 0.50;
        let taker = 0.015;
        let maker = 0.0; // Polymarket maker = 0

        let o_taker = omega_at(
            pt,
            pm,
            taker,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            100.0,
            0.25,
            1.0,
            0.25,
        );
        let o_maker = omega_at(
            pt,
            pm,
            maker,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            100.0,
            0.25,
            1.0,
            0.25,
        );
        assert!(
            o_maker > o_taker,
            "maker fee should give higher Ω: maker={o_maker}, taker={o_taker}"
        );
    }

    #[test]
    fn omega_four_state_exits_dominate() {
        // With maker_fee < taker_fee, exit actions dominate entry actions
        // for the same directional edge.
        let pt = 0.65;
        let pm = 0.50;
        let taker = 0.015;
        let maker = 0.0;
        let args = (10_000.0, 0.25, 0.05, 0.25);

        // BuyYes (entry long) vs SellNo (exit short) — same side, different fee
        let o_buy_yes = omega_at(
            pt,
            pm,
            taker,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        let o_sell_no = omega_at(
            pt,
            pm,
            maker,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        assert!(
            o_sell_no >= o_buy_yes,
            "sell_no should >= buy_yes: sell={o_sell_no}, buy={o_buy_yes}"
        );

        // BuyNo (entry short) vs SellYes (exit long) — same side, different fee
        let o_buy_no = omega_at(
            pt,
            pm,
            taker,
            0.0,
            Side::BidNo,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        let o_sell_yes = omega_at(
            pt,
            pm,
            maker,
            0.0,
            Side::BidNo,
            Regime::Amplify,
            args.0,
            args.1,
            args.2,
            args.3,
        );
        assert!(
            o_sell_yes >= o_buy_no,
            "sell_yes should >= buy_no: sell={o_sell_yes}, buy={o_buy_no}"
        );
    }

    #[test]
    fn omega_threshold_at_zero() {
        // Find parameters where Ω ≈ 0 (boundary of habitable zone)
        // At Ω = 0: kelly_eff * C / cost = 1, meaning exactly 1 contract
        let pm = 0.50;
        let fee = 0.01;
        let capital = 100.0; // small capital to hit the boundary
        let tau = 0.25;
        let kappa = 0.10;

        // Sweep pt to find where Ω crosses zero
        let mut last_sign = -1.0_f64;
        let mut crossing_found = false;
        for i in 1..99 {
            let pt = i as f64 / 100.0;
            let o = omega_at(
                pt,
                pm,
                fee,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                capital,
                tau,
                kappa,
                0.25,
            );
            if o >= 0.0 && last_sign < 0.0 {
                crossing_found = true;
                // At the boundary, size should be exactly 1
                let size = (o + 1.0).floor();
                assert!(size >= 1.0, "at boundary, size should be ≥ 1");
            }
            last_sign = o;
        }
        assert!(crossing_found, "should find an Ω = 0 crossing somewhere");
    }

    #[test]
    fn omega_capital_scales_linearly() {
        let pt = 0.65;
        let pm = 0.50;
        let fee = 0.015;

        let o1 = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            5_000.0,
            0.25,
            0.05,
            0.25,
        );
        let o2 = omega_at(
            pt,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.25,
        );

        // Ω = kelly * C / cost - 1, so (Ω+1) scales linearly with C
        let ratio = (o2 + 1.0) / (o1 + 1.0);
        assert!(
            (ratio - 2.0).abs() < EPS,
            "capital should scale linearly: ratio={ratio}"
        );
    }

    #[test]
    fn omega_nan_firewall() {
        let args = (10_000.0, 0.25, 0.05, 0.25);
        assert_eq!(
            omega_at(
                f64::NAN,
                0.5,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                0.6,
                f64::NAN,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                0.6,
                0.5,
                f64::NAN,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                0.6,
                0.5,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                f64::NAN,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                0.0,
                0.5,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                1.0,
                0.5,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                0.6,
                0.0,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                0.6,
                1.0,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        assert_eq!(
            omega_at(
                f64::INFINITY,
                0.5,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Amplify,
                args.0,
                args.1,
                args.2,
                args.3
            ),
            -1.0
        );
        // revert_scale NaN: must not produce NaN omega that escapes entry filter
        assert_eq!(
            omega_at(
                0.6,
                0.5,
                0.01,
                0.0,
                Side::BidYes,
                Regime::Revert,
                args.0,
                args.1,
                args.2,
                f64::NAN
            ),
            -1.0
        );
    }

    #[test]
    fn omega_zero_fee_highest() {
        // Zero fee (maker on Polymarket) should give highest Ω
        let pt = 0.55;
        let pm = 0.50;
        let o_zero = omega_at(
            pt,
            pm,
            0.0,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.10,
            0.25,
        );
        let o_fee = omega_at(
            pt,
            pm,
            0.04,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.10,
            0.25,
        );
        assert!(
            o_zero > o_fee,
            "zero fee should give higher Ω: zero={o_zero}, fee={o_fee}"
        );
    }

    #[test]
    fn omega_symmetric_at_center() {
        // At pm=0.50, BidYes and BidNo should have equal cost,
        // so when cap binds for both, Ω should be identical
        let pm = 0.50;
        let fee = 0.01;
        let capital = 10_000.0;
        // Use parameters where cap definitely binds for both
        let tau = 0.25;
        let kappa = 0.01; // very low cap

        // BidYes with pt=0.65 (gap=+0.15)
        let o_yes = omega_at(
            0.65,
            pm,
            fee,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            capital,
            tau,
            kappa,
            0.25,
        );
        // BidNo with pt=0.35 (gap=-0.15, same |gap|)
        let o_no = omega_at(
            0.35,
            pm,
            fee,
            0.0,
            Side::BidNo,
            Regime::Amplify,
            capital,
            tau,
            kappa,
            0.25,
        );

        // At pm=0.50, cost_yes = cost_no = 0.50
        // If cap binds: kelly_eff = κ for both
        // So Ω = κ * C / 0.5 - 1 should be identical
        assert!(
            (o_yes - o_no).abs() < EPS,
            "symmetric at center: yes={o_yes}, no={o_no}"
        );
    }

    // -- omega_at spread-excess (7 tests) ---------------------------------

    #[test]
    fn omega_spread_unknown_no_effect() {
        // spread=0.0 (unknown): excess=0, Omega unchanged from fee-only
        let o_no_spread = omega_at(
            0.55,
            0.50,
            0.015,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        let o_at_floor = omega_at(
            0.55,
            0.50,
            0.015,
            0.015,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        assert!(
            (o_no_spread - o_at_floor).abs() < 1e-9,
            "spread=0 and spread=fee_floor should give identical Omega: \
             no_spread={o_no_spread}, at_floor={o_at_floor}"
        );
    }

    #[test]
    fn omega_spread_excess_shrinks_omega() {
        // spread > fee_floor: Omega should decrease
        let o_tight = omega_at(
            0.55,
            0.50,
            0.015,
            0.015,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        let o_wide = omega_at(
            0.55,
            0.50,
            0.015,
            0.045,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        assert!(
            o_wide < o_tight,
            "wider spread should produce lower Omega: tight={o_tight}, wide={o_wide}"
        );
    }

    #[test]
    fn omega_spread_below_floor_no_credit() {
        // spread < fee_floor: no credit, same as spread=0
        let o_zero = omega_at(
            0.55,
            0.50,
            0.015,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        let o_below = omega_at(
            0.55,
            0.50,
            0.015,
            0.005,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        assert!(
            (o_zero - o_below).abs() < 1e-9,
            "spread below fee floor should give same Omega as spread=0: \
             zero={o_zero}, below={o_below}"
        );
    }

    #[test]
    fn omega_spread_eats_entire_edge() {
        // spread so wide it consumes the edge: Omega should be negative
        let o = omega_at(
            0.55,
            0.50,
            0.015,
            0.20,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        assert!(
            o < 0.0,
            "spread wider than edge should produce negative Omega: {o}"
        );
    }

    #[test]
    fn omega_spread_negative_clamped() {
        // crossed book (negative spread): clamped to 0, same as unknown
        let o_zero = omega_at(
            0.55,
            0.50,
            0.015,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        let o_neg = omega_at(
            0.55,
            0.50,
            0.015,
            -0.02,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        assert!(
            (o_zero - o_neg).abs() < 1e-9,
            "negative spread should clamp to zero excess: \
             zero={o_zero}, neg={o_neg}"
        );
    }

    #[test]
    fn omega_spread_nan_degrades_safely() {
        // NaN spread: Rust's f64::max ignores NaN, so (NaN - fee).max(0.0) = 0.0.
        // Result: NaN spread degrades to "unknown spread" = no excess.
        let o_nan = omega_at(
            0.55,
            0.50,
            0.015,
            f64::NAN,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        let o_zero = omega_at(
            0.55,
            0.50,
            0.015,
            0.0,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        // NaN spread should produce identical result to spread=0
        assert!(
            (o_nan - o_zero).abs() < 1e-9,
            "NaN spread should degrade to spread=0: nan={o_nan}, zero={o_zero}"
        );
    }

    #[test]
    fn omega_spread_excess_is_half_roundtrip() {
        // Verify the excess/2 math: entry is maker (free), exit is taker (spread/2)
        let fee = 0.015;
        let spread = 0.045; // excess = 0.030, half = 0.015
        let _o_tight = omega_at(
            0.55,
            0.50,
            fee,
            fee,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        let o_wide = omega_at(
            0.55,
            0.50,
            fee,
            spread,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        // The effective fee increased by excess/2 = 0.015
        // So o_wide should equal omega_at with fee_rate = 0.030, spread = 0.030:
        let o_equiv = omega_at(
            0.55,
            0.50,
            fee + 0.015,
            fee + 0.015,
            Side::BidYes,
            Regime::Amplify,
            10_000.0,
            0.25,
            0.05,
            0.10,
        );
        assert!(
            (o_wide - o_equiv).abs() < 1e-9,
            "excess/2 should be equivalent to increasing fee_rate by that amount: \
             wide={o_wide}, equiv={o_equiv}"
        );
    }

    // -- NaN firewall (1 test) --------------------------------------------

    #[test]
    fn nan_firewall_domain_types() {
        let lp = compute_limit_price(f64::NAN, 0.01, Side::BidYes);
        assert!(lp.is_finite());
        let lp2 = compute_limit_price(0.6, f64::NAN, Side::BidYes);
        assert!(lp2.is_finite());
        let lp3 = compute_limit_price(f64::INFINITY, 0.01, Side::BidNo);
        assert!(lp3.is_finite());

        assert_eq!(unrealized_pnl(f64::NAN, 0.6, Side::BidYes), 0.0);
        assert_eq!(unrealized_pnl(0.4, f64::NAN, Side::BidYes), 0.0);
        assert_eq!(unrealized_pnl(f64::INFINITY, 0.6, Side::BidYes), 0.0);

        let cfg = ExitConfig::default();
        let t = exit_thresholds(&cfg, 0.05, f64::NAN);
        assert!(t.profit_target.is_finite() && t.profit_target > 0.0);
        assert!(t.trail_distance.is_finite() && t.trail_distance > 0.0);
        assert!(t.convergence_floor.is_finite() && t.convergence_floor > 0.0);
        assert!(t.max_loss.is_finite() && t.max_loss > 0.0);

        assert_eq!(capital_at_risk(f64::NAN, 0.6, Side::BidYes), 0.0);
        assert_eq!(capital_at_risk(100.0, f64::NAN, Side::BidYes), 0.0);
        assert_eq!(capital_at_risk(0.0, 0.6, Side::BidYes), 0.0);
        assert_eq!(capital_at_risk(f64::INFINITY, 0.6, Side::BidYes), 0.0);
    }

    // -- OracleProfile (8 tests) ------------------------------------------

    #[test]
    fn oracle_profile_cold_start() {
        let p = OracleProfile::default_for(OracleType::Brti);
        assert!(!p.is_warm());
        assert_eq!(p.n_obs, 0);
        assert_eq!(p.disp_ema, 0.0);
        assert_eq!(p.disp_std(), 0.0);
    }

    #[test]
    fn oracle_profile_warmup() {
        let mut p = OracleProfile::default_for(OracleType::Brti);
        for _ in 0..30 {
            p.update(10.0);
        }
        assert!(p.is_warm());
        assert!((p.disp_ema - 10.0).abs() < 0.5); // EMA converges to constant input
    }

    #[test]
    fn oracle_profile_ema_tracks() {
        let mut p = OracleProfile::new(OracleType::ChainlinkStreams, 0.1, 5);
        // Feed constant $10 displacement
        for _ in 0..50 {
            p.update(10.0);
        }
        assert!((p.disp_ema - 10.0).abs() < 0.01, "ema={}", p.disp_ema);
        // Shift to $20
        for _ in 0..50 {
            p.update(20.0);
        }
        assert!((p.disp_ema - 20.0).abs() < 0.1, "ema={}", p.disp_ema);
    }

    #[test]
    fn oracle_profile_std_zero_on_constant() {
        let mut p = OracleProfile::new(OracleType::Brti, 0.1, 5);
        for _ in 0..20 {
            p.update(10.0);
        }
        assert!(p.disp_std() < 0.001, "std={}", p.disp_std());
    }

    #[test]
    fn oracle_profile_std_nonzero_on_varying() {
        let mut p = OracleProfile::new(OracleType::Brti, 0.1, 5);
        for i in 0..50 {
            p.update(if i % 2 == 0 { 10.0 } else { -10.0 });
        }
        assert!(p.disp_std() > 5.0, "std={}", p.disp_std());
    }

    #[test]
    fn oracle_profile_max_decays() {
        let mut p = OracleProfile::new(OracleType::Brti, 0.1, 5);
        p.update(100.0); // spike
        for _ in 0..200 {
            p.update(1.0); // settle
        }
        // Max should have decayed from 100 toward 1
        assert!(p.disp_max < 50.0, "max should decay, got {}", p.disp_max);
    }

    #[test]
    fn oracle_profile_nan_rejected() {
        let mut p = OracleProfile::default_for(OracleType::Brti);
        p.update(f64::NAN);
        assert_eq!(p.n_obs, 0);
        p.update(10.0);
        p.update(f64::NAN);
        assert_eq!(p.n_obs, 1);
    }

    #[test]
    fn oracle_displacement_as_gap() {
        let mut p = OracleProfile::default_for(OracleType::ChainlinkStreams);
        for _ in 0..50 {
            p.update(10.0); // $10 displacement
        }
        // σ√T for 5m market: σ_1s=3e-4, T=300s → σ√T = 3e-4 × √300 ≈ 0.005196
        let sigma_sqrt_t = 3e-4 * 300.0_f64.sqrt();
        let gap = p.displacement_as_gap(70_000.0, sigma_sqrt_t, 0.0);
        // At ATM (d1=0): φ(0) × $10 / (σ√T × $70,000)
        // = 0.3989 × 10 / (0.005196 × 70000) = 0.3989 × 10 / 363.7 = 0.01097
        let phi_0 = 1.0 / (2.0 * std::f64::consts::PI).sqrt(); // 0.39894...
        let expected = phi_0 * 10.0 / (sigma_sqrt_t * 70_000.0);
        assert!(
            (gap - expected).abs() < 1e-6,
            "gap={gap}, expected={expected}"
        );
    }

    // -- edge_qualifies (5 tests) -----------------------------------------

    #[test]
    fn edge_qualifies_warm_chainlink() {
        // Chainlink with $15 displacement at $83k, 5m market
        // σ√T = 3e-4 × √300 ≈ 0.005196
        // Oracle gap capacity = φ(0) × 15 / (0.005196 × 83000) = 0.01387
        // Poly crypto taker at p=0.30 = 0.011025 → 0.01387 > 0.01103 → PASSES
        // Poly crypto taker at p=0.50 = 0.015625 → 0.01387 < 0.01563 → BLOCKED
        let mut p = OracleProfile::default_for(OracleType::ChainlinkStreams);
        for _ in 0..50 {
            p.update(15.0); // $15 displacement (realistic calm market)
        }
        let venue = Venue::PolymarketCrypto;
        let svt = 3e-4 * 300.0_f64.sqrt();
        // At p=0.30 (ATM, d1=0), fee is low enough for $15 displacement to qualify
        assert!(edge_qualifies(0.05, 0.30, &venue, &p, 83_000.0, svt, 0.0));
        // At p=0.50 (ATM, d1=0), fee ridge blocks even with gamma correction
        assert!(!edge_qualifies(0.05, 0.50, &venue, &p, 83_000.0, svt, 0.0));
    }

    #[test]
    fn edge_qualifies_cold_profile_blocks() {
        let p = OracleProfile::default_for(OracleType::Brti);
        let svt = 3e-4 * 300.0_f64.sqrt();
        // Cold profile (0 obs) — should block even with huge gap
        assert!(!edge_qualifies(
            0.50,
            0.50,
            &Venue::KalshiIndex,
            &p,
            70_000.0,
            svt,
            0.0,
        ));
    }

    #[test]
    fn edge_qualifies_binance_candle_blocked() {
        // BinanceCandle with near-zero displacement — should naturally fail
        let mut p = OracleProfile::default_for(OracleType::BinanceCandle);
        for _ in 0..50 {
            p.update(0.5); // $0.50 displacement ≈ noise
        }
        let venue = Venue::PolymarketCrypto;
        let svt = 3e-4 * 300.0_f64.sqrt();
        // gap capacity = φ(0) × 0.5 / (0.005196 × 70000) ≈ 0.000548 — below fee at p=0.30
        assert!(!edge_qualifies(0.05, 0.30, &venue, &p, 70_000.0, svt, 0.0));
    }

    #[test]
    fn edge_qualifies_gap_below_fee_blocked() {
        let mut p = OracleProfile::default_for(OracleType::Brti);
        for _ in 0..50 {
            p.update(500.0);
        }
        let svt = 3e-4 * 300.0_f64.sqrt();
        // Gap is below fee threshold — condition 1 fails
        let venue = Venue::KalshiGeneral;
        // taker at p=0.50 = 0.035; gap = 0.01 < 0.035
        assert!(!edge_qualifies(0.01, 0.50, &venue, &p, 70_000.0, svt, 0.0));
    }

    #[test]
    fn edge_qualifies_all_conditions_met() {
        let mut p = OracleProfile::default_for(OracleType::Brti);
        for _ in 0..50 {
            p.update(50.0); // $50 cascade displacement
        }
        let venue = Venue::KalshiIndex;
        let svt = 3e-4 * 300.0_f64.sqrt();
        // gap capacity = φ(0) × 50 / (0.005196 × 70000) = 0.05482
        // taker_rate(0.30) = 0.035 × 0.70 = 0.0245
        // 0.05482 > 0.0245 → qualifies (ATM, d1=0)
        assert!(edge_qualifies(0.10, 0.30, &venue, &p, 70_000.0, svt, 0.0));
    }

    // -- regime: unknown momentum (1 test) --------------------------------

    #[test]
    fn regime_unknown_momentum_defaults_revert() {
        // mu=0.0: identical consecutive ticks, no flow information → Revert
        assert_eq!(classify_regime(70_100.0, 70_000.0, 0.0), Regime::Revert);
        // mu=NaN: feed gap, missing data → Revert
        assert_eq!(
            classify_regime(70_100.0, 70_000.0, f64::NAN),
            Regime::Revert
        );
        // Both sides of the strike
        assert_eq!(classify_regime(69_900.0, 70_000.0, 0.0), Regime::Revert);
    }

    // -- exit: unknown sigma (1 test) -------------------------------------

    #[test]
    fn exit_sigma_nan_tightens() {
        let cfg = ExitConfig::default();
        // sigma_1s = NaN → condition fails → sigma_ratio_min (0.25)
        // profit_target = 0.50 * 0.10 * 0.25 = 0.0125
        let t = exit_thresholds(&cfg, 0.10, f64::NAN);
        assert!(
            (t.profit_target - 0.0125).abs() < 1e-9,
            "NaN sigma should tighten, got {}",
            t.profit_target
        );
        // trail_distance = 0.40 * 0.10 * 0.25 = 0.01
        assert!(
            (t.trail_distance - 0.01).abs() < 1e-9,
            "NaN sigma trail should tighten, got {}",
            t.trail_distance
        );
        // max_loss unaffected (not sigma-scaled)
        assert!(
            (t.max_loss - 0.10).abs() < 1e-9,
            "max_loss should be unaffected, got {}",
            t.max_loss
        );
    }

    // -- displacement_as_gap: local gamma (1 test) ------------------------

    #[test]
    fn displacement_as_gap_local_gamma() {
        let mut p = OracleProfile::default_for(OracleType::ChainlinkStreams);
        for _ in 0..50 {
            p.update(15.0); // $15 displacement
        }
        let spot = 83_000.0;
        let svt = 3e-4 * 300.0_f64.sqrt(); // sigma_sqrt_t

        // ATM: d1=0, φ(0)=0.3989
        let cap_atm = p.displacement_as_gap(spot, svt, 0.0);
        assert!(cap_atm > 0.0);

        // OTM: d1=2.0, φ(2.0)=0.05399
        let cap_otm = p.displacement_as_gap(spot, svt, 2.0);
        assert!(cap_otm > 0.0);

        // OTM capacity should be ~13.5% of ATM (φ(2)/φ(0) ≈ 0.1353)
        let ratio = cap_otm / cap_atm;
        assert!(
            (ratio - 0.1353).abs() < 0.01,
            "OTM/ATM ratio should be ~0.135, got {ratio}"
        );

        // NaN d1 → 0.0 (gate rejects)
        assert_eq!(p.displacement_as_gap(spot, svt, f64::NAN), 0.0);
    }

    // -- edge_qualifies: ATM vs OTM (1 test) -----------------------------

    #[test]
    fn edge_qualifies_atm_vs_otm() {
        let mut p = OracleProfile::default_for(OracleType::ChainlinkStreams);
        for _ in 0..50 {
            p.update(15.0); // $15 Chainlink displacement
        }
        let venue = Venue::PolymarketCrypto;
        let svt = 3e-4 * 300.0_f64.sqrt();
        let spot = 83_000.0;

        // ATM (d1=0): full gamma, gate should pass at p=0.30
        // capacity = φ(0) × 15 / (svt × 83000) = 0.3989 × 15 / (0.005196 × 83000)
        //          = 5.984 / 431.3 = 0.01388
        // taker_rate(0.30) = 0.25 × (0.3 × 0.7)^2 = 0.25 × 0.0441 = 0.01103
        // 0.01388 > 0.01103 → passes
        assert!(edge_qualifies(0.05, 0.30, &venue, &p, spot, svt, 0.0));

        // OTM (d1=2.0): γ drops ~7.4x, capacity = 0.01388 × 0.1353 = 0.001878
        // 0.001878 < 0.01103 → fails (correctly rejected at OTM)
        assert!(!edge_qualifies(0.05, 0.30, &venue, &p, spot, svt, 2.0));
    }
}
