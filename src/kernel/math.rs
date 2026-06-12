//! 23 parameter-free formulas. f64 → f64.
//!
//! Zero free parameters. Zero domain types. Zero I/O. Zero state.

use core::f64::consts::{FRAC_1_SQRT_2, FRAC_2_SQRT_PI};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Seconds in one year (365.25 days).
pub const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;

// ---------------------------------------------------------------------------
// Fee polynomials — Polymarket
// ---------------------------------------------------------------------------

/// Polymarket crypto taker: `0.25 * (p(1-p))²`
///
/// Peak 1.5625% at p=0.50, near-zero at extremes.
#[inline(always)]
pub fn poly_crypto_taker(p: f64) -> f64 {
    let p = p.clamp(0.001, 0.999);
    let pq = p * (1.0 - p);
    0.25 * pq * pq
}

/// Polymarket sports taker: `0.0175 * p(1-p)`
///
/// Peak 0.4375% at p=0.50.
#[inline(always)]
pub fn poly_sports_taker(p: f64) -> f64 {
    let p = p.clamp(0.001, 0.999);
    0.0175 * p * (1.0 - p)
}

/// Polymarket maker: always zero.
#[inline(always)]
pub fn poly_maker(_p: f64) -> f64 {
    0.0
}

/// Polymarket fee rounding: round to 4 decimals, min 0.0001 USDC.
#[inline(always)]
pub fn poly_round(raw: f64) -> f64 {
    if raw <= 0.0 {
        return 0.0;
    }
    (raw * 10_000.0).round().max(1.0) / 10_000.0
}

// ---------------------------------------------------------------------------
// Fee polynomials — Kalshi
// ---------------------------------------------------------------------------

/// Kalshi general taker: `0.07 * (1-p)`. At p=0.50: 3.50%.
#[inline(always)]
pub fn kalshi_general_taker(p: f64) -> f64 {
    let p = p.clamp(0.01, 0.99);
    0.07 * (1.0 - p)
}

/// Kalshi index taker: `0.035 * (1-p)`. At p=0.50: 1.75%.
#[inline(always)]
pub fn kalshi_index_taker(p: f64) -> f64 {
    let p = p.clamp(0.01, 0.99);
    0.035 * (1.0 - p)
}

/// Kalshi maker: `0.0175 * (1-p)`. At p=0.50: 0.875%.
#[inline(always)]
pub fn kalshi_maker(p: f64) -> f64 {
    let p = p.clamp(0.01, 0.99);
    0.0175 * (1.0 - p)
}

/// Kalshi fee rounding: ceil to cent, min $0.01.
///
/// Epsilon guard prevents fp artifacts from bumping exact cents.
#[inline(always)]
pub fn kalshi_round(raw: f64) -> f64 {
    if raw <= 0.0 {
        return 0.0;
    }
    (raw * 100.0 - 1e-9).ceil().max(1.0) / 100.0
}

// ---------------------------------------------------------------------------
// Error function — zero dependencies, derived from first principles
// ---------------------------------------------------------------------------

/// Maclaurin coefficients cₙ = (-1)^n / (n! · (2n+1)) for n = 0..17.
/// Used in: erf(x) = (2/√π) · x · Σ cₙ · x^{2n}
const MACLAURIN_C: [f64; 18] = [
    1.0,                             //  n=0:   1 / (0! · 1)
    -1.0 / 3.0,                      //  n=1:  -1 / (1! · 3)
    1.0 / 10.0,                      //  n=2:   1 / (2! · 5)
    -1.0 / 42.0,                     //  n=3:  -1 / (3! · 7)
    1.0 / 216.0,                     //  n=4:   1 / (4! · 9)
    -1.0 / 1_320.0,                  //  n=5:  -1 / (5! · 11)
    1.0 / 9_360.0,                   //  n=6:   1 / (6! · 13)
    -1.0 / 75_600.0,                 //  n=7:  -1 / (7! · 15)
    1.0 / 685_440.0,                 //  n=8:   1 / (8! · 17)
    -1.0 / 6_894_720.0,              //  n=9:  -1 / (9! · 19)
    1.0 / 76_204_800.0,              //  n=10:  1 / (10! · 21)
    -1.0 / 918_086_400.0,            //  n=11: -1 / (11! · 23)
    1.0 / 11_975_040_000.0,          //  n=12:  1 / (12! · 25)
    -1.0 / 168_129_561_600.0,        //  n=13: -1 / (13! · 27)
    1.0 / 2_528_170_444_800.0,       //  n=14:  1 / (14! · 29)
    -1.0 / 40_537_905_408_000.0,     //  n=15: -1 / (15! · 31)
    1.0 / 690_572_006_304_000.0,     //  n=16:  1 / (16! · 33)
    -1.0 / 12_449_059_983_360_000.0, //  n=17: -1 / (17! · 35)
];

/// Error function: erf(x) = (2/√π) ∫₀ˣ e^{-t²} dt
///
/// Two-region piecewise + saturation, derived from first principles:
///
/// - **|x| ≤ 1.0 — Maclaurin series.** Expand e^{-t²}, integrate term by term.
///   18 terms via Horner in u = x². Truncation error < 1e-16, ~1 bit cancellation.
///
/// - **|x| > 1.0 — Continued fraction for erfc.** Laplace CF from repeated
///   integration by parts of ∫_x^∞ e^{-t²} dt. Modified Lentz evaluation.
///   erf = 1 − erfc. Converges in ~33 iterations at boundary, ~8 at x = 6.
///
/// - **|x| > 27 — Saturation.** e^{-729} ≈ 10^{-317} < f64 subnormal.
///
/// Odd function: erf(−x) = −erf(x). NaN → NaN. ±∞ → ±1.
#[inline]
pub fn erf(x: f64) -> f64 {
    if x.is_nan() {
        return f64::NAN;
    }

    let a = x.abs();

    // Saturation: e^{-27²} ≈ 10^{-317} underflows f64
    if a > 27.0 {
        return if x > 0.0 { 1.0 } else { -1.0 };
    }

    let result = if a <= 1.0 {
        // Region A: Maclaurin series
        // erf(a) = (2/√π) · a · P(a²)
        // P(u) = Σ_{n=0}^{17} cₙ · uⁿ  via Horner
        let u = a * a;
        let mut p = MACLAURIN_C[17];
        for i in (0..17).rev() {
            p = MACLAURIN_C[i] + u * p;
        }
        FRAC_2_SQRT_PI * a * p
    } else {
        // Region B: erfc via Laplace continued fraction, then erf = 1 - erfc
        //
        // erfc(a) = (e^{-a²} / √π) · (1 / G(a))
        // G(a) = a + (1/2)/(a + (2/2)/(a + (3/2)/(a + ...)))
        //
        // Modified Lentz's algorithm evaluates G(a).
        const TINY: f64 = 1e-30;
        const EPS: f64 = 1e-16;
        const MAX_ITER: usize = 100;

        let mut f = a;
        let mut c = a;
        let mut d = 0.0_f64;

        for j in 1..=MAX_ITER {
            let aj = j as f64 * 0.5;

            d = a + aj * d;
            if d.abs() < TINY {
                d = TINY;
            }
            d = 1.0 / d;

            c = a + aj / c;
            if c.abs() < TINY {
                c = TINY;
            }

            let delta = c * d;
            f *= delta;

            if (delta - 1.0).abs() < EPS {
                break;
            }
        }

        // erfc(a) = e^{-a²} · (2/√π) / (2 · G(a))
        let erfc_a = (-a * a).exp() * FRAC_2_SQRT_PI / (2.0 * f);
        1.0 - erfc_a
    };

    if x < 0.0 {
        -result
    } else {
        result
    }
}

// ---------------------------------------------------------------------------
// Core geometry — the displacement pipeline
// ---------------------------------------------------------------------------

/// Standard normal CDF: Φ(x) = 0.5 * (1 + erf(x/√2))
#[inline(always)]
pub fn phi(x: f64) -> f64 {
    0.5 * (1.0 + erf(x * FRAC_1_SQRT_2))
}

/// Standard normal PDF: φ(x) = (1/√2π) * exp(-x²/2)
#[inline(always)]
pub fn phi_pdf(x: f64) -> f64 {
    const INV_SQRT_2PI: f64 = 0.398_942_280_401_432_7;
    INV_SQRT_2PI * (-0.5 * x * x).exp()
}

/// d1 displacement: `ln(S/K) / (σ√T)`
///
/// Measures distance from fair value through the 1/√T lens (R²=0.97).
/// Returns 0.0 on degenerate inputs (T≤0, σ≤0, K≤0, S≤0) or non-finite inputs.
#[inline(always)]
pub fn d1(spot: f64, strike: f64, sigma_1s: f64, t_secs: f64) -> f64 {
    if spot <= 0.0 || strike <= 0.0 || sigma_1s <= 0.0 || t_secs <= 0.0 {
        return 0.0;
    }
    if !spot.is_finite() || !strike.is_finite() || !sigma_1s.is_finite() || !t_secs.is_finite() {
        return 0.0;
    }
    (spot / strike).ln() / (sigma_1s * t_secs.sqrt())
}

/// True probability from geometry: `P_true = Φ(d1)`
///
/// When no data (d1=0), returns 0.5 — no edge, no bet.
#[inline(always)]
pub fn p_true(spot: f64, strike: f64, sigma_1s: f64, t_secs: f64) -> f64 {
    phi(d1(spot, strike, sigma_1s, t_secs))
}

/// Gap = P_true − P_market.
///
/// Positive → market underprices UP. Negative → market overprices UP.
#[inline(always)]
pub fn gap(p_true: f64, p_market: f64) -> f64 {
    p_true - p_market
}

/// Net edge after fee hurdle. Expects `|gap|`, not signed gap. Negative → fold.
#[inline(always)]
pub fn net_edge(gap_abs: f64, fee_rate: f64) -> f64 {
    gap_abs - fee_rate
}

// ---------------------------------------------------------------------------
// Gate coupling — terrain durability (gate_d1, gate_trend)
// ---------------------------------------------------------------------------

/// Gate margin: distance from the edge-qualifies threshold, normalized by
/// volatility and time.
///
/// Same geometry as `d1`: measures distance-to-boundary in sigma*sqrt(T) units,
/// but applied to the gate boundary rather than the strike boundary.
///
/// - `d1` answers: how likely is the binary to resolve in the money?
/// - `gate_d1` answers: how likely is the edge to survive until resolution?
///
/// Positive = displacement clears the fee by this many sigma units (wide trail).
/// Zero = displacement exactly at threshold (gate marginal).
/// Negative = displacement below threshold (trail gone).
///
/// Larger = more durable edge. It is a first-passage metric: Phi(gate_d1) is
/// the probability that displacement (a random walk with diffusion rate sigma)
/// does not cross back below the fee threshold before time T.
///
/// NaN firewall: returns 0.0 on any degenerate input.
/// `sigma_sqrt_t <= 0` returns 0.0 (same as no-data behavior — conservative).
#[inline(always)]
pub fn gate_d1(displacement_abs: f64, fee_threshold: f64, sigma_sqrt_t: f64) -> f64 {
    if sigma_sqrt_t <= 0.0 || !sigma_sqrt_t.is_finite() {
        return 0.0;
    }
    if !displacement_abs.is_finite() || !fee_threshold.is_finite() {
        return 0.0;
    }
    (displacement_abs - fee_threshold) / sigma_sqrt_t
}

/// Gate trend: is the trail widening or narrowing?
///
/// The time derivative of `gate_d1` — the difference between two consecutive
/// evaluations. Positive = displacement moving away from the fee threshold
/// (trail widening). Negative = displacement collapsing toward threshold
/// (trail narrowing). Zero = stable or first observation.
///
/// Both inputs are already in sigma*sqrt(T) units, so the difference is
/// a dimensionless rate of change — no additional normalization needed.
///
/// NaN firewall: returns 0.0 on non-finite input.
/// First observation (no prev): pass `gate_d1_prev = 0.0` → trend = gate_d1_now.
#[inline(always)]
pub fn gate_trend(gate_d1_now: f64, gate_d1_prev: f64) -> f64 {
    if !gate_d1_now.is_finite() || !gate_d1_prev.is_finite() {
        return 0.0;
    }
    gate_d1_now - gate_d1_prev
}

// ---------------------------------------------------------------------------
// Sigma utilities
// ---------------------------------------------------------------------------

/// Convert annualized IV (percentage, e.g. 45.0 = 45%) to per-second sigma.
///
/// sigma_1s = (iv_annual / 100) / sqrt(seconds_per_year)
#[inline(always)]
pub fn implied_vol_to_sigma_1s(iv_annual_pct: f64) -> f64 {
    if !iv_annual_pct.is_finite() || iv_annual_pct <= 0.0 {
        return 0.0;
    }
    (iv_annual_pct / 100.0) / SECS_PER_YEAR.sqrt()
}

/// Convert per-second sigma to annualized IV (percentage).
///
/// iv_annual = sigma_1s * sqrt(seconds_per_year) * 100
#[inline(always)]
pub fn sigma_1s_to_iv_annual(sigma_1s: f64) -> f64 {
    if !sigma_1s.is_finite() || sigma_1s <= 0.0 {
        return 0.0;
    }
    sigma_1s * SECS_PER_YEAR.sqrt() * 100.0
}

/// Sigma z-score for entropy detection.
/// z = (current - mean) / std
/// Returns 0.0 if std <= 0 or any input is non-finite.
#[inline(always)]
pub fn sigma_z_score(current: f64, mean: f64, std: f64) -> f64 {
    if std <= 0.0 || !current.is_finite() || !mean.is_finite() || !std.is_finite() {
        return 0.0;
    }
    (current - mean) / std
}

/// Map contract duration to correlation group.
/// Bucket boundaries match actual Polymarket/Kalshi contract types.
/// Same oracle + same duration bucket = correlated positions.
pub fn correlation_group(duration_minutes: i64) -> &'static str {
    match duration_minutes {
        ..=5 => "5m",
        6..=15 => "15m",
        16..=60 => "1h",
        61..=240 => "4h",
        241..=1440 => "daily",
        _ => "weekly",
    }
}

// ---------------------------------------------------------------------------
// Black-Scholes pricing
// ---------------------------------------------------------------------------

/// Black-Scholes European call price (r = 0).
///
/// C = S·Φ(d1) - K·Φ(d2)
/// d1 = ln(S/K) / (σ√T) + σ√T/2
/// d2 = d1 - σ√T
///
/// Note: this uses the FULL BS d1 (with drift term σ√T/2), not our simplified
/// binary d1. The drift matters when pricing options (not just probabilities).
/// Returns 0.0 on degenerate inputs.
#[inline(always)]
pub fn bs_call_price(spot: f64, strike: f64, sigma_1s: f64, t_secs: f64) -> f64 {
    if spot <= 0.0
        || strike <= 0.0
        || sigma_1s <= 0.0
        || t_secs <= 0.0
        || !spot.is_finite()
        || !strike.is_finite()
        || !sigma_1s.is_finite()
        || !t_secs.is_finite()
    {
        return 0.0;
    }
    let sqrt_t = t_secs.sqrt();
    let sv = sigma_1s * sqrt_t;
    if sv <= 0.0 {
        return 0.0;
    }
    let d1_full = (spot / strike).ln() / sv + sv / 2.0;
    let d2_full = d1_full - sv;
    let price = spot * phi(d1_full) - strike * phi(d2_full);
    if price.is_finite() {
        price.max(0.0)
    } else {
        0.0
    }
}

/// Black-Scholes European put price via put-call parity (r = 0).
///
/// P = C - S + K = K·Φ(-d2) - S·Φ(-d1)
/// Returns 0.0 on degenerate inputs.
#[inline(always)]
pub fn bs_put_price(spot: f64, strike: f64, sigma_1s: f64, t_secs: f64) -> f64 {
    if spot <= 0.0
        || strike <= 0.0
        || sigma_1s <= 0.0
        || t_secs <= 0.0
        || !spot.is_finite()
        || !strike.is_finite()
        || !sigma_1s.is_finite()
        || !t_secs.is_finite()
    {
        return 0.0;
    }
    let call = bs_call_price(spot, strike, sigma_1s, t_secs);
    let put = call - spot + strike;
    if put.is_finite() {
        put.max(0.0)
    } else {
        0.0
    }
}

/// Black-Scholes vega: ∂C/∂σ = S · φ(d1) · √T
///
/// This is the Newton-Raphson derivative for the implied vol solver.
/// Same for calls and puts (vega is identical for both).
/// Returns 0.0 on degenerate inputs.
#[inline(always)]
pub fn bs_vega(spot: f64, strike: f64, sigma_1s: f64, t_secs: f64) -> f64 {
    if spot <= 0.0
        || strike <= 0.0
        || sigma_1s <= 0.0
        || t_secs <= 0.0
        || !spot.is_finite()
        || !strike.is_finite()
        || !sigma_1s.is_finite()
        || !t_secs.is_finite()
    {
        return 0.0;
    }
    let sqrt_t = t_secs.sqrt();
    let sv = sigma_1s * sqrt_t;
    if sv <= 0.0 {
        return 0.0;
    }
    let d1_full = (spot / strike).ln() / sv + sv / 2.0;
    let v = spot * phi_pdf(d1_full) * sqrt_t;
    if v.is_finite() {
        v.max(0.0)
    } else {
        0.0
    }
}

/// Black-Scholes gamma: ∂²C/∂S² = φ(d1) / (S · σ · √T)
///
/// Same for calls and puts. This is the curvature of the option price
/// with respect to spot — the hedging pressure per dollar of spot movement.
/// Multiply by OI to get strike-level gamma exposure (GEX).
/// Returns 0.0 on degenerate inputs.
#[inline(always)]
pub fn bs_gamma(spot: f64, strike: f64, sigma_1s: f64, t_secs: f64) -> f64 {
    if spot <= 0.0
        || strike <= 0.0
        || sigma_1s <= 0.0
        || t_secs <= 0.0
        || !spot.is_finite()
        || !strike.is_finite()
        || !sigma_1s.is_finite()
        || !t_secs.is_finite()
    {
        return 0.0;
    }
    let sqrt_t = t_secs.sqrt();
    let sv = sigma_1s * sqrt_t;
    if sv <= 0.0 {
        return 0.0;
    }
    let d1_full = (spot / strike).ln() / sv + sv / 2.0;
    let g = phi_pdf(d1_full) / (spot * sv);
    if g.is_finite() {
        g.max(0.0)
    } else {
        0.0
    }
}

/// Implied volatility via Newton-Raphson.
///
/// Given an observed option price, solves for sigma_1s (per-second vol) such that
/// `bs_{call|put}_price(S, K, sigma_1s, T) ≈ observed_price`.
///
/// Returns `Some(sigma_1s)` on convergence, `None` if:
/// - degenerate inputs (price <= 0, S <= 0, K <= 0, T <= 0)
/// - no convergence within 20 iterations
/// - vega too flat (sigma insensitive region)
///
/// Initial guess: 0.0003 (median BTC sigma_1s).
/// Tolerance: 1e-10 absolute price difference.
/// Sigma clamp: [1e-12, 1.0] per iteration.
#[inline]
pub fn implied_vol(
    option_price: f64,
    spot: f64,
    strike: f64,
    t_secs: f64,
    is_call: bool,
) -> Option<f64> {
    if option_price <= 0.0
        || spot <= 0.0
        || strike <= 0.0
        || t_secs <= 0.0
        || !option_price.is_finite()
        || !spot.is_finite()
        || !strike.is_finite()
        || !t_secs.is_finite()
    {
        return None;
    }

    if is_call {
        if option_price > spot {
            return None;
        }
    } else {
        if option_price > strike {
            return None;
        }
    }

    const MAX_ITER: usize = 20;
    const TOL: f64 = 1e-10;
    const SIGMA_MIN: f64 = 1e-12;
    const SIGMA_MAX: f64 = 1.0;
    const VEGA_FLOOR: f64 = 1e-15;

    let mut sigma = 0.0003_f64;

    for _ in 0..MAX_ITER {
        let model_price = if is_call {
            bs_call_price(spot, strike, sigma, t_secs)
        } else {
            bs_put_price(spot, strike, sigma, t_secs)
        };

        let diff = model_price - option_price;
        if diff.abs() < TOL {
            return Some(sigma);
        }

        let v = bs_vega(spot, strike, sigma, t_secs);
        if v < VEGA_FLOOR {
            return None;
        }

        sigma -= diff / v;
        sigma = sigma.clamp(SIGMA_MIN, SIGMA_MAX);
    }

    let final_price = if is_call {
        bs_call_price(spot, strike, sigma, t_secs)
    } else {
        bs_put_price(spot, strike, sigma, t_secs)
    };
    if (final_price - option_price).abs() < TOL * 100.0 {
        Some(sigma)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    const EPS: f64 = 1e-10;

    // -- phi / phi_pdf (8 tests) ------------------------------------------

    #[test]
    fn phi_at_zero() {
        assert!((phi(0.0) - 0.5).abs() < EPS);
    }

    #[test]
    fn phi_symmetry() {
        for &x in &[0.5, 1.0, 2.0, 3.0] {
            assert!((phi(x) + phi(-x) - 1.0).abs() < EPS);
        }
    }

    #[test]
    fn phi_monotonic() {
        let mut prev = phi(-5.0);
        for i in -49..50 {
            let x = i as f64 * 0.1;
            let curr = phi(x);
            assert!(curr >= prev, "not monotonic at x={x}");
            prev = curr;
        }
    }

    #[test]
    fn phi_extremes() {
        assert!(phi(-10.0) < 1e-15);
        assert!((phi(10.0) - 1.0).abs() < 1e-15);
    }

    #[test]
    fn phi_known_values() {
        assert!((phi(1.0) - 0.8413447460685429).abs() < 1e-12);
        assert!((phi(-1.0) - 0.15865525393145702).abs() < 1e-12);
        assert!((phi(2.0) - 0.9772498680518208).abs() < 1e-12);
    }

    #[test]
    fn phi_pdf_at_zero() {
        assert!((phi_pdf(0.0) - 0.398_942_280_401_432_7).abs() < 1e-12);
    }

    #[test]
    fn phi_pdf_symmetry() {
        for &x in &[0.5, 1.0, 2.5] {
            assert!((phi_pdf(x) - phi_pdf(-x)).abs() < EPS);
        }
    }

    #[test]
    fn phi_pdf_derivative_of_cdf() {
        let h = 1e-7;
        for &x in &[0.0, 1.0, -1.0, 2.0] {
            let numerical = (phi(x + h) - phi(x - h)) / (2.0 * h);
            let analytic = phi_pdf(x);
            assert!(
                (numerical - analytic).abs() < 1e-5,
                "x={x}: numerical={numerical}, analytic={analytic}"
            );
        }
    }

    // -- d1 (8 tests) -----------------------------------------------------

    #[test]
    fn d1_at_the_money() {
        assert!(d1(70_000.0, 70_000.0, 0.0001, 300.0).abs() < EPS);
    }

    #[test]
    fn d1_above_strike() {
        let v = d1(70_100.0, 70_000.0, 0.0001, 300.0);
        assert!(v > 0.0, "d1={v}");
    }

    #[test]
    fn d1_below_strike() {
        let v = d1(69_900.0, 70_000.0, 0.0001, 300.0);
        assert!(v < 0.0, "d1={v}");
    }

    #[test]
    fn d1_degenerate() {
        assert_eq!(d1(0.0, 70_000.0, 0.0001, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 0.0, 0.0001, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 70_000.0, 0.0, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 70_000.0, 0.0001, 0.0), 0.0);
        assert_eq!(d1(-1.0, 70_000.0, 0.0001, 300.0), 0.0);
    }

    #[test]
    fn d1_kyle_sqrt_t_scaling() {
        let d300 = d1(70_100.0, 70_000.0, 0.0001, 300.0);
        let d150 = d1(70_100.0, 70_000.0, 0.0001, 150.0);
        let ratio = d150 / d300;
        let expected = (300.0_f64 / 150.0).sqrt();
        assert!(
            (ratio - expected).abs() < 1e-10,
            "ratio={ratio} expected={expected}"
        );
    }

    #[test]
    fn d1_antisymmetric_around_strike() {
        let above = d1(70_100.0, 70_000.0, 0.0001, 300.0);
        let below = d1(69_900.143, 70_000.0, 0.0001, 300.0);
        assert!(above > 0.0 && below < 0.0);
    }

    #[test]
    fn d1_nan_returns_zero() {
        assert_eq!(d1(f64::NAN, 70_000.0, 0.0001, 300.0), 0.0);
        assert_eq!(d1(70_000.0, f64::NAN, 0.0001, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 70_000.0, f64::NAN, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 70_000.0, 0.0001, f64::NAN), 0.0);
    }

    #[test]
    fn d1_infinity_returns_zero() {
        assert_eq!(d1(f64::INFINITY, 70_000.0, 0.0001, 300.0), 0.0);
        assert_eq!(d1(70_000.0, f64::INFINITY, 0.0001, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 70_000.0, f64::INFINITY, 300.0), 0.0);
        assert_eq!(d1(70_000.0, 70_000.0, 0.0001, f64::INFINITY), 0.0);
    }

    // -- p_true (5 tests) -------------------------------------------------

    #[test]
    fn p_true_at_the_money() {
        assert!((p_true(70_000.0, 70_000.0, 0.0001, 300.0) - 0.5).abs() < EPS);
    }

    #[test]
    fn p_true_above() {
        assert!(p_true(70_100.0, 70_000.0, 0.0001, 300.0) > 0.5);
    }

    #[test]
    fn p_true_below() {
        assert!(p_true(69_900.0, 70_000.0, 0.0001, 300.0) < 0.5);
    }

    #[test]
    fn p_true_convergence_near_expiry() {
        let p = p_true(70_100.0, 70_000.0, 0.0001, 1.0);
        assert!(p > 0.99, "p={p}");
    }

    #[test]
    fn p_true_no_data_returns_half() {
        assert!((p_true(0.0, 70_000.0, 0.0001, 300.0) - 0.5).abs() < EPS);
    }

    // -- gap / net_edge (4 tests) -----------------------------------------

    #[test]
    fn gap_positive_when_underpriced() {
        assert!((gap(0.70, 0.55) - 0.15).abs() < EPS);
    }

    #[test]
    fn gap_negative_when_overpriced() {
        assert!((gap(0.30, 0.55) - (-0.25)).abs() < EPS);
    }

    #[test]
    fn net_edge_subtracts_fee() {
        assert!((net_edge(0.05, 0.015) - 0.035).abs() < EPS);
    }

    #[test]
    fn net_edge_negative_when_fee_exceeds() {
        assert!(net_edge(0.01, 0.015) < 0.0);
    }

    // -- gate coupling: gate_d1, gate_trend (9 tests) ---------------------

    #[test]
    fn gate_d1_positive_when_displacement_exceeds_fee() {
        // displacement = 0.04, fee = 0.015, sigma_sqrt_t = 0.01
        // gate_d1 = (0.04 - 0.015) / 0.01 = 2.5
        let gd1 = gate_d1(0.04, 0.015, 0.01);
        assert!((gd1 - 2.5).abs() < EPS, "expected 2.5, got {gd1}");
    }

    #[test]
    fn gate_d1_zero_when_at_threshold() {
        // displacement == fee → gate_d1 = 0.0
        let gd1 = gate_d1(0.015, 0.015, 0.01);
        assert!(gd1.abs() < EPS, "expected 0.0, got {gd1}");
    }

    #[test]
    fn gate_d1_negative_when_below_threshold() {
        // displacement < fee → trail is gone
        let gd1 = gate_d1(0.01, 0.015, 0.01);
        assert!(gd1 < 0.0, "expected negative, got {gd1}");
        assert!((gd1 - (-0.5)).abs() < EPS, "expected -0.5, got {gd1}");
    }

    #[test]
    fn gate_d1_zero_on_nan_inputs() {
        assert_eq!(gate_d1(f64::NAN, 0.015, 0.01), 0.0);
        assert_eq!(gate_d1(0.04, f64::NAN, 0.01), 0.0);
        assert_eq!(gate_d1(f64::INFINITY, 0.015, 0.01), 0.0);
    }

    #[test]
    fn gate_d1_zero_on_zero_sigma() {
        assert_eq!(gate_d1(0.04, 0.015, 0.0), 0.0);
        assert_eq!(gate_d1(0.04, 0.015, -1.0), 0.0);
        assert_eq!(gate_d1(0.04, 0.015, f64::NAN), 0.0);
    }

    #[test]
    fn gate_trend_positive_when_widening() {
        // gate_d1 grew from 1.0 to 1.5 → trail widening
        let trend = gate_trend(1.5, 1.0);
        assert!((trend - 0.5).abs() < EPS, "expected 0.5, got {trend}");
    }

    #[test]
    fn gate_trend_negative_when_narrowing() {
        // gate_d1 shrank from 2.0 to 1.2 → trail narrowing
        let trend = gate_trend(1.2, 2.0);
        assert!((trend - (-0.8)).abs() < EPS, "expected -0.8, got {trend}");
    }

    #[test]
    fn gate_trend_zero_on_nan() {
        assert_eq!(gate_trend(f64::NAN, 1.0), 0.0);
        assert_eq!(gate_trend(1.0, f64::NAN), 0.0);
        assert_eq!(gate_trend(f64::INFINITY, 1.0), 0.0);
    }

    #[test]
    fn gate_trend_zero_on_first_observation() {
        // First tick: prev = 0.0. Trend = gate_d1_now - 0.0 = gate_d1_now.
        // When gate_d1 itself is 0.0 (at threshold), trend is also 0.0.
        let trend = gate_trend(0.0, 0.0);
        assert_eq!(trend, 0.0);
        // Non-zero first observation returns the gate_d1 value itself
        let trend2 = gate_trend(2.0, 0.0);
        assert!((trend2 - 2.0).abs() < EPS);
    }

    // -- fee surfaces (17 tests) ------------------------------------------

    #[test]
    fn poly_crypto_peak_at_center() {
        assert!((poly_crypto_taker(0.50) - 0.015625).abs() < EPS);
    }

    #[test]
    fn poly_crypto_near_zero_at_extremes() {
        assert!(poly_crypto_taker(0.05) < 0.001);
        assert!(poly_crypto_taker(0.95) < 0.001);
    }

    #[test]
    fn poly_crypto_symmetric() {
        for &p in &[0.10, 0.20, 0.30, 0.40] {
            let lo = poly_crypto_taker(p);
            let hi = poly_crypto_taker(1.0 - p);
            assert!((lo - hi).abs() < EPS, "asymmetric at p={p}");
        }
    }

    #[test]
    fn poly_crypto_known_rates() {
        assert!((poly_crypto_taker(0.10) - 0.002025).abs() < EPS);
        assert!((poly_crypto_taker(0.20) - 0.0064).abs() < EPS);
        assert!((poly_crypto_taker(0.30) - 0.011025).abs() < EPS);
    }

    #[test]
    fn poly_sports_peak() {
        assert!((poly_sports_taker(0.50) - 0.004375).abs() < EPS);
    }

    #[test]
    fn poly_maker_always_zero() {
        for &p in &[0.01, 0.25, 0.50, 0.75, 0.99] {
            assert_eq!(poly_maker(p), 0.0);
        }
    }

    #[test]
    fn kalshi_general_at_center() {
        assert!((kalshi_general_taker(0.50) - 0.035).abs() < EPS);
    }

    #[test]
    fn kalshi_general_at_extreme() {
        assert!((kalshi_general_taker(0.10) - 0.063).abs() < EPS);
    }

    #[test]
    fn kalshi_index_at_center() {
        assert!((kalshi_index_taker(0.50) - 0.0175).abs() < EPS);
    }

    #[test]
    fn kalshi_maker_at_center() {
        assert!((kalshi_maker(0.50) - 0.00875).abs() < EPS);
    }

    #[test]
    fn kalshi_maker_nonzero() {
        for &p in &[0.10, 0.50, 0.90] {
            assert!(kalshi_maker(p) > 0.0, "maker fee should be >0 at p={p}");
        }
    }

    #[test]
    fn poly_round_4_decimals() {
        assert!((poly_round(0.02025) - 0.0203).abs() < EPS);
        assert!((poly_round(0.12800) - 0.1280).abs() < EPS);
    }

    #[test]
    fn poly_round_min_fee() {
        assert!((poly_round(0.00001) - 0.0001).abs() < EPS);
    }

    #[test]
    fn poly_round_zero() {
        assert_eq!(poly_round(0.0), 0.0);
        assert_eq!(poly_round(-1.0), 0.0);
    }

    #[test]
    fn kalshi_round_ceil() {
        assert!((kalshi_round(0.011) - 0.02).abs() < EPS);
        assert!((kalshi_round(0.005) - 0.01).abs() < EPS);
    }

    #[test]
    fn kalshi_round_exact_cent() {
        assert!((kalshi_round(0.01) - 0.01).abs() < EPS);
        assert!((kalshi_round(0.03) - 0.03).abs() < EPS);
        assert!((kalshi_round(0.10) - 0.10).abs() < EPS);
    }

    #[test]
    fn kalshi_round_zero() {
        assert_eq!(kalshi_round(0.0), 0.0);
        assert_eq!(kalshi_round(-1.0), 0.0);
    }

    // -- sigma_z_score (2 tests) ------------------------------------------

    #[test]
    fn z_score_normal() {
        let z = sigma_z_score(0.0006, 0.0003, 0.0001);
        assert!((z - 3.0).abs() < EPS, "z={z}");
    }

    #[test]
    fn z_score_zero_std() {
        assert_eq!(sigma_z_score(0.0006, 0.0003, 0.0), 0.0);
    }

    // -- correlation_group (6 tests) --------------------------------------

    #[test]
    fn group_5m() {
        assert_eq!(correlation_group(5), "5m");
        assert_eq!(correlation_group(1), "5m");
    }

    #[test]
    fn group_15m() {
        assert_eq!(correlation_group(10), "15m");
        assert_eq!(correlation_group(15), "15m");
    }

    #[test]
    fn group_1h() {
        assert_eq!(correlation_group(60), "1h");
        assert_eq!(correlation_group(30), "1h");
    }

    #[test]
    fn group_4h() {
        assert_eq!(correlation_group(120), "4h");
        assert_eq!(correlation_group(240), "4h");
    }

    #[test]
    fn group_daily() {
        assert_eq!(correlation_group(1440), "daily");
        assert_eq!(correlation_group(500), "daily");
    }

    #[test]
    fn group_weekly() {
        assert_eq!(correlation_group(10080), "weekly");
        assert_eq!(correlation_group(1441), "weekly");
    }

    // -- Black-Scholes + IV (15 tests) ------------------------------------

    #[test]
    fn bs_call_atm() {
        let c = bs_call_price(100.0, 100.0, 0.0003, 86400.0);
        assert!(c > 0.0, "ATM call must be positive, got {c}");
        assert!(c < 100.0, "ATM call must be < spot, got {c}");
        assert!(
            (c - 3.52).abs() < 1.0,
            "ATM call price {c} not near expected ~3.52"
        );
    }

    #[test]
    fn bs_put_call_parity() {
        let s = 90_000.0;
        let k = 89_000.0;
        let sigma = 0.0003;
        let t = 3600.0;

        let c = bs_call_price(s, k, sigma, t);
        let p = bs_put_price(s, k, sigma, t);

        let parity = c - p;
        let expected = s - k;
        assert!(
            (parity - expected).abs() < 0.01,
            "put-call parity violated: C-P={parity}, S-K={expected}"
        );
    }

    #[test]
    fn bs_vega_positive() {
        let v = bs_vega(90_000.0, 90_000.0, 0.0003, 86400.0);
        assert!(v > 0.0, "vega must be positive, got {v}");
    }

    #[test]
    fn bs_vega_atm_peak() {
        let s = 90_000.0;
        let sigma = 0.0003;
        let t = 86400.0;

        let v_atm = bs_vega(s, s, sigma, t);
        let v_itm = bs_vega(s, s * 0.95, sigma, t);
        let v_otm = bs_vega(s, s * 1.05, sigma, t);

        assert!(
            v_atm > v_itm,
            "ATM vega ({v_atm}) should exceed ITM ({v_itm})"
        );
        assert!(
            v_atm > v_otm,
            "ATM vega ({v_atm}) should exceed OTM ({v_otm})"
        );
    }

    #[test]
    fn bs_gamma_positive() {
        let g = bs_gamma(90_000.0, 90_000.0, 0.0003, 86400.0);
        assert!(g > 0.0, "gamma must be positive, got {g}");
    }

    #[test]
    fn bs_gamma_atm_peak() {
        let s = 90_000.0;
        let sigma = 0.0003;
        let t = 86400.0;

        let g_atm = bs_gamma(s, s, sigma, t);
        let g_itm = bs_gamma(s, s * 0.95, sigma, t);
        let g_otm = bs_gamma(s, s * 1.05, sigma, t);

        assert!(
            g_atm > g_itm,
            "ATM gamma ({g_atm}) should exceed ITM ({g_itm})"
        );
        assert!(
            g_atm > g_otm,
            "ATM gamma ({g_atm}) should exceed OTM ({g_otm})"
        );
    }

    #[test]
    fn bs_gamma_vega_relationship() {
        let s = 70_000.0;
        let sigma = 0.000_092_5;
        let t = 3600.0;

        let g = bs_gamma(s, s, sigma, t);
        let v = bs_vega(s, s, sigma, t);

        let reconstructed_vega = g * s * s * sigma * t;
        assert!(
            (reconstructed_vega - v).abs() / v < 1e-10,
            "gamma * S^2 * sigma * T ({reconstructed_vega}) should equal vega ({v})"
        );
    }

    #[test]
    fn bs_degenerate_inputs() {
        assert_eq!(bs_call_price(0.0, 100.0, 0.0003, 86400.0), 0.0);
        assert_eq!(bs_call_price(100.0, 0.0, 0.0003, 86400.0), 0.0);
        assert_eq!(bs_call_price(100.0, 100.0, 0.0, 86400.0), 0.0);
        assert_eq!(bs_call_price(100.0, 100.0, 0.0003, 0.0), 0.0);
        assert_eq!(bs_call_price(f64::NAN, 100.0, 0.0003, 86400.0), 0.0);
        assert_eq!(bs_put_price(f64::NAN, 100.0, 0.0003, 86400.0), 0.0);
        assert_eq!(bs_vega(f64::NAN, 100.0, 0.0003, 86400.0), 0.0);
        assert_eq!(bs_gamma(f64::NAN, 100.0, 0.0003, 86400.0), 0.0);
        assert_eq!(bs_gamma(100.0, 100.0, 0.0, 86400.0), 0.0);
        assert_eq!(bs_gamma(0.0, 100.0, 0.0003, 86400.0), 0.0);
    }

    #[test]
    fn iv_roundtrip_call() {
        let s = 90_000.0;
        let k = 90_000.0;
        let sigma = 0.0003;
        let t = 86400.0;

        let price = bs_call_price(s, k, sigma, t);
        let recovered = implied_vol(price, s, k, t, true);

        assert!(
            recovered.is_some(),
            "IV solver should converge for ATM call"
        );
        let r = recovered.unwrap();
        assert!(
            (r - sigma).abs() < 1e-8,
            "roundtrip sigma: expected {sigma}, got {r}"
        );
    }

    #[test]
    fn iv_roundtrip_put() {
        let s = 90_000.0;
        let k = 91_000.0;
        let sigma = 0.00025;
        let t = 604800.0;

        let price = bs_put_price(s, k, sigma, t);
        let recovered = implied_vol(price, s, k, t, false);

        assert!(recovered.is_some(), "IV solver should converge for put");
        let r = recovered.unwrap();
        assert!(
            (r - sigma).abs() < 1e-8,
            "roundtrip put sigma: expected {sigma}, got {r}"
        );
    }

    #[test]
    fn iv_deep_itm() {
        let s = 100_000.0;
        let k = 80_000.0;
        let sigma = 0.0004;
        let t = 86400.0;

        let price = bs_call_price(s, k, sigma, t);
        let recovered = implied_vol(price, s, k, t, true);

        assert!(recovered.is_some(), "should converge for deep ITM");
    }

    #[test]
    fn iv_deep_otm() {
        let s = 80_000.0;
        let k = 100_000.0;
        let sigma = 0.0005;
        let t = 604800.0;

        let price = bs_call_price(s, k, sigma, t);
        if price > 1e-10 {
            let recovered = implied_vol(price, s, k, t, true);
            assert!(recovered.is_some(), "should converge for OTM if price > 0");
        }
    }

    #[test]
    fn iv_no_convergence() {
        assert!(implied_vol(100_001.0, 100_000.0, 90_000.0, 86400.0, true).is_none());
        assert!(implied_vol(100_001.0, 100_000.0, 90_000.0, 86400.0, false).is_none());
        assert!(implied_vol(0.0, 100_000.0, 90_000.0, 86400.0, true).is_none());
        assert!(implied_vol(100.0, 0.0, 90_000.0, 86400.0, true).is_none());
        assert!(implied_vol(f64::NAN, 100_000.0, 90_000.0, 86400.0, true).is_none());
    }

    #[test]
    fn iv_conversion_roundtrip() {
        let iv = 45.0;
        let sigma = implied_vol_to_sigma_1s(iv);
        assert!(sigma > 0.0);
        let back = sigma_1s_to_iv_annual(sigma);
        assert!(
            (back - iv).abs() < 1e-10,
            "IV conversion roundtrip: {iv} -> {sigma} -> {back}"
        );
        assert!(
            (sigma - 8.01e-5).abs() < 1e-6,
            "45% IV sigma_1s = {sigma}, expected ~8.01e-5"
        );
    }

    #[test]
    fn iv_nan_firewall() {
        assert!(implied_vol(f64::NAN, 100.0, 100.0, 86400.0, true).is_none());
        assert!(implied_vol(5.0, f64::INFINITY, 100.0, 86400.0, true).is_none());
        assert!(implied_vol(5.0, 100.0, f64::NAN, 86400.0, true).is_none());
        assert!(implied_vol(5.0, 100.0, 100.0, f64::NAN, true).is_none());
        assert_eq!(implied_vol_to_sigma_1s(f64::NAN), 0.0);
        assert_eq!(implied_vol_to_sigma_1s(-10.0), 0.0);
        assert_eq!(sigma_1s_to_iv_annual(f64::NAN), 0.0);
        assert_eq!(sigma_1s_to_iv_annual(-1.0), 0.0);
    }

    // -- erf vs libm ground truth -----------------------------------------

    #[test]
    fn erf_vs_libm_dense_sweep() {
        // Sweep 100K points across [-10, 10] and find worst disagreement.
        // Threshold 1e-12: matches phi_known_values (tightest kernel test).
        // CF roundoff from ~40 Lentz iterations limits precision to ~1e-13.
        let mut worst_err: f64 = 0.0;
        let mut worst_x: f64 = 0.0;
        let n = 100_000;
        for i in 0..=n {
            let x = -10.0 + 20.0 * (i as f64) / (n as f64);
            let ours = erf(x);
            let theirs = libm::erf(x);
            let err = (ours - theirs).abs();
            if err > worst_err {
                worst_err = err;
                worst_x = x;
            }
        }
        assert!(
            worst_err < 1e-12,
            "erf disagreement: {worst_err:.2e} at x={worst_x}"
        );
    }

    #[test]
    fn erf_vs_libm_boundary_region() {
        // Dense sweep around the Maclaurin/CF boundary at 1.0
        let mut worst_err: f64 = 0.0;
        let mut worst_x: f64 = 0.0;
        let n = 10_000;
        for i in 0..=n {
            let x = 0.96 + 0.08 * (i as f64) / (n as f64);
            let ours = erf(x);
            let theirs = libm::erf(x);
            let err = (ours - theirs).abs();
            if err > worst_err {
                worst_err = err;
                worst_x = x;
            }
        }
        assert!(
            worst_err < 1e-12,
            "erf boundary disagreement: {worst_err:.2e} at x={worst_x}"
        );
    }

    #[test]
    fn erf_vs_libm_real_d1_range() {
        // d1 values that actually occur in BTC binary markets:
        // spot ~70K-100K, strike ~70K-100K, sigma ~8e-5, T ~60-86400s
        // d1 = ln(S/K) / (sigma * sqrt(T)), phi(d1) calls erf(d1/sqrt(2))
        let test_d1s = [
            0.0, 0.01, 0.1, 0.5, 0.84, 0.85, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0,
            10.0, 15.0, 20.0, 27.0, -0.5, -1.0, -2.0, -5.0, -10.0,
        ];
        for d1 in test_d1s {
            let x = d1 * FRAC_1_SQRT_2;
            let ours = erf(x);
            let theirs = libm::erf(x);
            let err = (ours - theirs).abs();
            assert!(
                err < 1e-12,
                "erf mismatch at d1={d1} (x={x}): ours={ours}, libm={theirs}, err={err:.2e}"
            );
        }
    }

    #[test]
    fn erf_edge_cases() {
        assert!(erf(f64::NAN).is_nan());
        assert_eq!(erf(f64::INFINITY), 1.0);
        assert_eq!(erf(f64::NEG_INFINITY), -1.0);
        assert_eq!(erf(0.0), 0.0);
        assert_eq!(erf(28.0), 1.0); // saturation
        assert_eq!(erf(-28.0), -1.0);
        // odd symmetry
        for &x in &[0.1, 0.5, 0.84, 0.85, 1.0, 3.0, 10.0] {
            assert_eq!(erf(-x), -erf(x), "odd symmetry broken at x={x}");
        }
    }
}
