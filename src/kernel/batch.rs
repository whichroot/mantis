//! 6 batch operations for throughput.
//!
//! Takes scalar functions from math and risk, applies them across arrays.
//! The gain field computation loops over 45+ strikes at multiple price grid
//! points — the batch layer is where that gets fast.

use super::math;
use super::risk::Venue;

// ---------------------------------------------------------------------------
// Fee batch
// ---------------------------------------------------------------------------

/// Taker fee rates for N prices on Polymarket crypto.
#[inline]
pub fn poly_crypto_taker_batch(prices: &[f64], out: &mut [f64]) {
    debug_assert_eq!(prices.len(), out.len());
    for i in 0..prices.len() {
        out[i] = math::poly_crypto_taker(prices[i]);
    }
}

/// Taker fee rates for N prices on arbitrary venue.
#[inline]
pub fn taker_rate_batch(venue: Venue, prices: &[f64], out: &mut [f64]) {
    debug_assert_eq!(prices.len(), out.len());
    for i in 0..prices.len() {
        out[i] = venue.taker_rate(prices[i]);
    }
}

// ---------------------------------------------------------------------------
// Geometry batch — sequential
// ---------------------------------------------------------------------------

/// Compute d1 for N tables. All slices must be equal length.
#[inline]
pub fn d1_batch(spots: &[f64], strikes: &[f64], sigmas: &[f64], ts: &[f64], out: &mut [f64]) {
    let n = spots.len();
    debug_assert!(n == strikes.len() && n == sigmas.len() && n == ts.len() && n == out.len());
    for i in 0..n {
        out[i] = math::d1(spots[i], strikes[i], sigmas[i], ts[i]);
    }
}

/// Compute P_true for N tables.
#[inline]
pub fn p_true_batch(spots: &[f64], strikes: &[f64], sigmas: &[f64], ts: &[f64], out: &mut [f64]) {
    d1_batch(spots, strikes, sigmas, ts, out);
    for v in out.iter_mut() {
        *v = math::phi(*v);
    }
}

/// Compute gap for N tables: `out[i] = p_true[i] - p_market[i]`.
#[inline]
pub fn gap_batch(p_trues: &[f64], p_markets: &[f64], out: &mut [f64]) {
    let n = p_trues.len();
    debug_assert!(n == p_markets.len() && n == out.len());
    for i in 0..n {
        out[i] = p_trues[i] - p_markets[i];
    }
}

/// Compute |gap| for N tables.
#[inline]
pub fn gap_abs_batch(p_trues: &[f64], p_markets: &[f64], out: &mut [f64]) {
    gap_batch(p_trues, p_markets, out);
    for v in out.iter_mut() {
        *v = v.abs();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    const EPS: f64 = 1e-10;

    #[test]
    fn batch_poly_matches_scalar() {
        let prices = [0.10, 0.30, 0.50, 0.70, 0.90];
        let mut out = [0.0; 5];
        poly_crypto_taker_batch(&prices, &mut out);
        for i in 0..5 {
            assert!((out[i] - math::poly_crypto_taker(prices[i])).abs() < EPS);
        }
    }

    #[test]
    fn venue_batch_matches_scalar() {
        let v = Venue::KalshiIndex;
        let prices = [0.20, 0.50, 0.80];
        let mut out = [0.0; 3];
        taker_rate_batch(v, &prices, &mut out);
        for i in 0..3 {
            assert!((out[i] - v.taker_rate(prices[i])).abs() < EPS);
        }
    }

    #[test]
    fn d1_batch_matches_scalar() {
        let spots = [70_100.0, 69_900.0, 70_000.0, 70_050.0];
        let strikes = [70_000.0; 4];
        let sigmas = [0.0001; 4];
        let ts = [300.0; 4];
        let mut out = [0.0; 4];
        d1_batch(&spots, &strikes, &sigmas, &ts, &mut out);
        for i in 0..4 {
            assert!((out[i] - math::d1(spots[i], strikes[i], sigmas[i], ts[i])).abs() < EPS);
        }
    }

    #[test]
    fn p_true_batch_matches_scalar() {
        let spots = [70_100.0, 69_900.0, 70_000.0];
        let strikes = [70_000.0; 3];
        let sigmas = [0.0001; 3];
        let ts = [300.0; 3];
        let mut out = [0.0; 3];
        p_true_batch(&spots, &strikes, &sigmas, &ts, &mut out);
        for i in 0..3 {
            assert!((out[i] - math::p_true(spots[i], strikes[i], sigmas[i], ts[i])).abs() < EPS);
        }
    }

    #[test]
    fn gap_batch_matches_scalar() {
        let pt = [0.7, 0.3, 0.5];
        let pm = [0.55, 0.55, 0.50];
        let mut out = [0.0; 3];
        gap_batch(&pt, &pm, &mut out);
        for i in 0..3 {
            assert!((out[i] - math::gap(pt[i], pm[i])).abs() < EPS);
        }
    }

    #[test]
    fn gap_abs_batch_all_positive() {
        let pt = [0.7, 0.3, 0.5];
        let pm = [0.55, 0.55, 0.50];
        let mut out = [0.0; 3];
        gap_abs_batch(&pt, &pm, &mut out);
        for v in &out {
            assert!(*v >= 0.0);
        }
        assert!((out[0] - 0.15).abs() < EPS);
        assert!((out[1] - 0.25).abs() < EPS);
        assert!(out[2].abs() < EPS);
    }
}
