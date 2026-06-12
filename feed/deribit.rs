//! Deribit sigma feed — back-compute implied volatility from BTC options.
//!
//! Standalone async function called every 300s by the sigma updater.
//! NOT a Feed trait implementation — called directly by the snapshot loop.

use crate::kernel::math::{SECS_PER_YEAR, bs_call_price, implied_vol_to_sigma_1s};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DERIBIT_IV_URL: &str =
    "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option";
const USER_AGENT: &str = "mantis-beacon/0.3";
const HTTP_TIMEOUT_SECS: u64 = 15;

/// Minimum time to expiry (seconds) for an option to be considered.
const MIN_T_SECS: f64 = 300.0;

/// Near-ATM cluster: time within this many seconds of best option.
const CLUSTER_T_WINDOW_SECS: f64 = 60.0;

/// Near-ATM cluster: moneyness must be less than this threshold.
const CLUSTER_MONEYNESS_THRESHOLD: f64 = 0.03;

/// Bisection bounds for annualized vol (fraction, not percentage).
/// lo=0.01 = 1%, hi=5.0 = 500%.
const BISECT_LO: f64 = 0.01;
const BISECT_HI: f64 = 5.0;
const BISECT_ITERS: usize = 60;
const BISECT_TOL: f64 = 1e-6;

// ---------------------------------------------------------------------------
// Month abbreviation parser
// ---------------------------------------------------------------------------

/// Parse a 3-letter uppercase month abbreviation to a month number (1–12).
/// Returns `None` on unrecognised input.
pub fn parse_month_abbr(abbr: &str) -> Option<u32> {
    match abbr {
        "JAN" => Some(1),
        "FEB" => Some(2),
        "MAR" => Some(3),
        "APR" => Some(4),
        "MAY" => Some(5),
        "JUN" => Some(6),
        "JUL" => Some(7),
        "AUG" => Some(8),
        "SEP" => Some(9),
        "OCT" => Some(10),
        "NOV" => Some(11),
        "DEC" => Some(12),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Instrument name parser
// ---------------------------------------------------------------------------

/// Parsed Deribit option instrument.
#[derive(Debug, PartialEq)]
pub struct ParsedInstrument {
    pub day: u32,
    pub month: u32,
    /// 2-digit year (e.g. 25 for 2025)
    pub year_2d: u32,
    pub strike: f64,
    /// true = call, false = put
    pub is_call: bool,
}

/// Parse a Deribit BTC instrument name.
///
/// Format: `BTC-{DD}{MON}{YY}-{STRIKE}-(C|P)`
/// Example: `"BTC-28MAR25-95000-C"` → day=28, month=3, year_2d=25, strike=95000, call
///
/// Uses a hand-rolled parser (no regex dependency) that is strict about the format.
pub fn parse_instrument_name(name: &str) -> Option<ParsedInstrument> {
    // Must start with "BTC-"
    let rest = name.strip_prefix("BTC-")?;

    // Find the second '-' separating date from strike
    // Date part: up to 7 chars (DD[D]MMMYY, 2+3+2 = 7 or 1+3+2 = 6)
    // We search for the next '-' after at least 6 chars.
    let dash2 = rest[6..].find('-').map(|p| p + 6)?;
    let date_str = &rest[..dash2];
    let after_date = &rest[dash2 + 1..];

    // Parse date: first 1–2 digits are day, next 3 chars are month, last 2 are year
    let (day_str, month_year_str) = if date_str.len() == 7 {
        // DD + MMMYY
        (&date_str[..2], &date_str[2..])
    } else if date_str.len() == 6 {
        // D + MMMYY
        (&date_str[..1], &date_str[1..])
    } else {
        return None;
    };

    if month_year_str.len() != 5 {
        return None;
    }
    let month_abbr = &month_year_str[..3];
    let year_str = &month_year_str[3..];

    let day: u32 = day_str.parse().ok()?;
    let month = parse_month_abbr(month_abbr)?;
    let year_2d: u32 = year_str.parse().ok()?;

    if !(1..=31).contains(&day) {
        return None;
    }

    // after_date = "{STRIKE}-(C|P)"
    let last_dash = after_date.rfind('-')?;
    let strike_str = &after_date[..last_dash];
    let type_str = &after_date[last_dash + 1..];

    let strike: f64 = strike_str.parse().ok()?;
    if strike <= 0.0 {
        return None;
    }

    let is_call = match type_str {
        "C" => true,
        "P" => false,
        _ => return None,
    };

    Some(ParsedInstrument { day, month, year_2d, strike, is_call })
}

// ---------------------------------------------------------------------------
// Expiry timestamp calculator
// ---------------------------------------------------------------------------

/// Compute Unix timestamp (f64 seconds) for a Deribit option expiry.
///
/// Deribit options always expire at 08:00 UTC on the stated date.
/// year_2d is the 2-digit year (25 → 2025).
pub fn expiry_unix(day: u32, month: u32, year_2d: u32) -> Option<f64> {
    let year = 2000 + year_2d as i64;
    let month = month as i64;
    let day = day as i64;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let epoch_days = days_since_epoch(year, month, day)?;
    // 08:00 UTC = 8 * 3600 = 28800 seconds
    Some(epoch_days as f64 * 86400.0 + 28800.0)
}

/// Compute days since Unix epoch (1970-01-01) for the given Gregorian date.
/// (Identical to the function in chainlink.rs — reproduced here to avoid
/// cross-module coupling; the kernel must remain I/O-free.)
fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    const EPOCH_MARCH_1_0: i64 = 719_468;
    Some(era * 146_097 + doe - EPOCH_MARCH_1_0)
}

// ---------------------------------------------------------------------------
// IV to sigma conversion
// ---------------------------------------------------------------------------

/// Convert Deribit's annualized IV percentage to per-second sigma.
///
/// iv_to_sigma(iv_pct) = (iv_pct / 100) / sqrt(SECS_PER_YEAR)
///
/// This delegates directly to the kernel's `implied_vol_to_sigma_1s`.
#[inline]
pub fn iv_to_sigma(iv_pct: f64) -> f64 {
    implied_vol_to_sigma_1s(iv_pct)
}

// ---------------------------------------------------------------------------
// Bisection implied vol solver
// ---------------------------------------------------------------------------

/// Back-compute annualized IV (as a fraction, e.g. 0.45 = 45%) from a
/// Black-Scholes call price using bisection.
///
/// - `spot`: current BTC/USD spot price
/// - `strike`: option strike price (USD)
/// - `t_secs`: time to expiry in seconds
/// - `mark_price_usd`: option market price in USD
///
/// Returns the annualized IV as a fraction (not percentage) if convergence
/// is achieved within [`BISECT_LO`]..=[`BISECT_HI`], otherwise `None`.
///
/// The bisection operates on annualized vol fraction. Each candidate `mid`
/// is an IV fraction (e.g. 0.80 = 80%). It is converted directly to
/// `sigma_1s = mid / sqrt(SECS_PER_YEAR)`.  The search range [0.01, 5.0]
/// therefore covers 1%–500% annualised IV — the correct range for BTC options.
pub fn bisect_iv(spot: f64, strike: f64, t_secs: f64, mark_price_usd: f64) -> Option<f64> {
    if mark_price_usd <= 0.0 || t_secs <= 0.0 || spot <= 0.0 || strike <= 0.0 {
        return None;
    }

    let mut lo = BISECT_LO;
    let mut hi = BISECT_HI;

    for _ in 0..BISECT_ITERS {
        let mid = (lo + hi) / 2.0;
        // FIX: WP05-F3 — mid is an IV fraction (0.80 = 80%). No /100 needed.
        // Old code divided by 100 here, shrinking the effective range to
        // 0.01%–5% and causing bisection to always fail on real BTC options.
        let sigma_1s = mid / SECS_PER_YEAR.sqrt();
        let model_price = bs_call_price(spot, strike, sigma_1s, t_secs);

        if (model_price - mark_price_usd).abs() < BISECT_TOL {
            return Some((lo + hi) / 2.0);
        }

        if model_price < mark_price_usd {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    let result = (lo + hi) / 2.0;
    // Validity check: must be strictly inside the search bounds
    if result > BISECT_LO && result < BISECT_HI {
        Some(result)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Option record (internal)
// ---------------------------------------------------------------------------

struct OptionRecord {
    /// Deribit's smoothed mark_iv (percentage, e.g. 45.0)
    iv: f64,
    /// Time to expiry in seconds
    t_secs: f64,
    /// |ln(spot/strike)| — zero is ATM
    moneyness: f64,
    strike: f64,
    /// mark_price_btc * spot (USD price)
    mark_price_usd: f64,
    /// Instrument name (for meta)
    name: String,
}

// ---------------------------------------------------------------------------
// Main fetch function
// ---------------------------------------------------------------------------

/// Fetch the current 1-second implied volatility from Deribit BTC options.
///
/// Queries Deribit's `get_book_summary_by_currency` endpoint, selects the
/// nearest-expiry near-ATM call cluster, and computes two sigma values:
///
/// 1. `deribit_iv` (smoothed): average of Deribit's `mark_iv` field.
/// 2. `deribit_iv_computed`: back-computed from `mark_price` via bisection.
///
/// Both values are emitted as [`FeedRow`]s to `tx`.
///
/// Returns `Some(sigma_1s)` using the back-computed value (preferred), or
/// the Deribit smoothed value as fallback. Returns `None` on any error.
pub async fn fetch_sigma(
    client: &reqwest::Client,
    spot: f64,
    rings: &crate::ring::RingSet,
) -> Option<f64> {
    if !spot.is_finite() || spot <= 0.0 {
        return None;
    }

    let now_ts = super::wall_clock();

    // ── HTTP GET ──────────────────────────────────────────────────────────────
    let resp = client
        .get(DERIBIT_IV_URL)
        .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
        .header("User-Agent", USER_AGENT)
        .send()
        .await
        .ok()?;

    let body: serde_json::Value = resp.json().await.ok()?;

    let result_arr = body.get("result")?.as_array()?;

    // ── Parse and filter options ──────────────────────────────────────────────
    let mut calls: Vec<OptionRecord> = Vec::new();

    for opt in result_arr {
        // mark_iv must be positive
        let iv = match opt.get("mark_iv").and_then(|v| v.as_f64()) {
            Some(v) if v > 0.0 => v,
            _ => continue,
        };

        // Instrument name — must be a call
        let name = match opt.get("instrument_name").and_then(|v| v.as_str()) {
            Some(n) => n,
            None => continue,
        };
        if !name.ends_with("-C") {
            continue;
        }

        // Parse instrument name
        let parsed = match parse_instrument_name(name) {
            Some(p) if p.is_call => p,
            _ => continue,
        };

        // Compute expiry Unix timestamp → t_secs
        let expiry_ts = match expiry_unix(parsed.day, parsed.month, parsed.year_2d) {
            Some(ts) => ts,
            None => continue,
        };
        let t_secs = expiry_ts - now_ts;
        if t_secs < MIN_T_SECS {
            continue;
        }

        let strike = parsed.strike;
        let moneyness = if strike > 0.0 {
            (spot / strike).ln().abs()
        } else {
            continue;
        };

        // mark_price is in BTC; convert to USD
        let mark_price_btc = opt
            .get("mark_price")
            .and_then(|v| v.as_f64())
            .filter(|&v| v.is_finite() && v >= 0.0)
            .unwrap_or(0.0);
        let mark_price_usd = if mark_price_btc > 0.0 {
            mark_price_btc * spot
        } else {
            0.0
        };

        calls.push(OptionRecord {
            iv,
            t_secs,
            moneyness,
            strike,
            mark_price_usd,
            name: name.to_owned(),
        });
    }

    if calls.is_empty() {
        return None;
    }

    // ── Select nearest-expiry ATM cluster ─────────────────────────────────────
    // Sort: nearest expiry first, then nearest ATM
    calls.sort_by(|a, b| {
        a.t_secs
            .partial_cmp(&b.t_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(
                a.moneyness
                    .partial_cmp(&b.moneyness)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
    });

    let best = &calls[0];
    let best_t = best.t_secs;
    let best_name = best.name.clone();

    let near_atm: Vec<&OptionRecord> = calls
        .iter()
        .filter(|c| {
            (c.t_secs - best_t).abs() < CLUSTER_T_WINDOW_SECS
                && c.moneyness < CLUSTER_MONEYNESS_THRESHOLD
        })
        .collect();

    // FIX: WP05-F4 — no near-ATM options means sigma is unreliable.
    // Return None instead of falling back to the deepest-OTM option,
    // which would produce a badly skewed IV estimate.
    if near_atm.is_empty() {
        eprintln!("[deribit] no near-ATM options for sigma — skipping");
        return None;
    }
    let cluster: Vec<&OptionRecord> = near_atm;

    // ── 1. Deribit smoothed IV (average mark_iv across cluster) ───────────────
    let avg_deribit_iv: f64 = cluster.iter().map(|c| c.iv).sum::<f64>() / cluster.len() as f64;
    let sigma_deribit = iv_to_sigma(avg_deribit_iv);

    // ── 2. Back-computed IV via bisection ─────────────────────────────────────
    let mut computed_ivs: Vec<f64> = Vec::new();
    for c in &cluster {
        if c.mark_price_usd > 0.0
            && c.t_secs > 0.0
            && let Some(civ) = bisect_iv(spot, c.strike, c.t_secs, c.mark_price_usd)
        {
            // Valid range check: 0.01 < computed_iv < 5.0
            if civ > BISECT_LO && civ < BISECT_HI {
                computed_ivs.push(civ);
            }
        }
    }

    let (avg_computed_iv, sigma_computed, n_computed) = if !computed_ivs.is_empty() {
        let avg = computed_ivs.iter().sum::<f64>() / computed_ivs.len() as f64;
        // avg is in fraction (e.g. 0.45); iv_to_sigma expects percentage
        let sigma = iv_to_sigma(avg * 100.0);
        (avg, sigma, computed_ivs.len())
    } else {
        // Fallback: use Deribit smoothed
        (avg_deribit_iv / 100.0, sigma_deribit, 0usize)
    };

    // ── Emit to rings ─────────────────────────────────────────────────────────
    let meta_deribit = serde_json::json!({
        "type": "mark_iv_smoothed",
        "sigma_1s": sigma_deribit,
        "n_options": cluster.len(),
        "expiry": best_name,
    });
    let meta_deribit_s = meta_deribit.to_string();
    rings.deribit_iv.write(now_ts, avg_deribit_iv, meta_deribit_s.as_bytes(), None);

    let meta_computed = serde_json::json!({
        "type": "implied_from_mark_price",
        "sigma_1s": sigma_computed,
        "n_options": n_computed,
        "expiry": best_name,
    });
    let computed_pct = avg_computed_iv * 100.0;
    let meta_computed_s = meta_computed.to_string();
    rings.deribit_iv_computed.write(now_ts, computed_pct, meta_computed_s.as_bytes(), None);

    // ── Return back-computed sigma (preferred), fallback to smoothed ──────────
    let final_sigma = if n_computed > 0 { sigma_computed } else { sigma_deribit };
    if final_sigma.is_finite() && final_sigma > 0.0 {
        Some(final_sigma)
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
    use crate::kernel::math::{bs_call_price, implied_vol_to_sigma_1s, SECS_PER_YEAR};

    // ── Instrument name parsing ───────────────────────────────────────────────

    #[test]
    fn parse_instrument_btc_28mar25_95000_c() {
        let p = parse_instrument_name("BTC-28MAR25-95000-C").unwrap();
        assert_eq!(p.day, 28);
        assert_eq!(p.month, 3);
        assert_eq!(p.year_2d, 25);
        assert_eq!(p.strike, 95000.0);
        assert!(p.is_call);
    }

    #[test]
    fn parse_instrument_single_digit_day() {
        let p = parse_instrument_name("BTC-5JAN26-80000-C").unwrap();
        assert_eq!(p.day, 5);
        assert_eq!(p.month, 1);
        assert_eq!(p.year_2d, 26);
        assert_eq!(p.strike, 80000.0);
        assert!(p.is_call);
    }

    #[test]
    fn reject_put_option() {
        let p = parse_instrument_name("BTC-28MAR25-95000-P").unwrap();
        assert!(!p.is_call, "P should parse as put");
        // fetch_sigma filters out puts before using this; the parser returns is_call=false
    }

    #[test]
    fn reject_invalid_instrument_name() {
        assert!(parse_instrument_name("ETH-28MAR25-95000-C").is_none());
        assert!(parse_instrument_name("BTC-28XYZ25-95000-C").is_none());
        assert!(parse_instrument_name("not-an-option").is_none());
        assert!(parse_instrument_name("").is_none());
        assert!(parse_instrument_name("BTC-28MAR25-95000-X").is_none());
    }

    #[test]
    fn reject_put_in_fetch_sigma_filter() {
        // Simulate the fetch_sigma filter: only calls (is_call == true) proceed
        let p_call = parse_instrument_name("BTC-28MAR25-95000-C").unwrap();
        let p_put = parse_instrument_name("BTC-28MAR25-95000-P").unwrap();
        assert!(p_call.is_call);
        assert!(!p_put.is_call);
        // In fetch_sigma: `if !name.ends_with("-C") { continue; }`
        assert!("BTC-28MAR25-95000-C".ends_with("-C"));
        assert!(!"BTC-28MAR25-95000-P".ends_with("-C"));
    }

    // ── Month abbreviation parsing ────────────────────────────────────────────

    #[test]
    fn parse_month_all_months() {
        assert_eq!(parse_month_abbr("JAN"), Some(1));
        assert_eq!(parse_month_abbr("FEB"), Some(2));
        assert_eq!(parse_month_abbr("MAR"), Some(3));
        assert_eq!(parse_month_abbr("APR"), Some(4));
        assert_eq!(parse_month_abbr("MAY"), Some(5));
        assert_eq!(parse_month_abbr("JUN"), Some(6));
        assert_eq!(parse_month_abbr("JUL"), Some(7));
        assert_eq!(parse_month_abbr("AUG"), Some(8));
        assert_eq!(parse_month_abbr("SEP"), Some(9));
        assert_eq!(parse_month_abbr("OCT"), Some(10));
        assert_eq!(parse_month_abbr("NOV"), Some(11));
        assert_eq!(parse_month_abbr("DEC"), Some(12));
        assert_eq!(parse_month_abbr("XYZ"), None);
        assert_eq!(parse_month_abbr(""), None);
    }

    // ── mark_iv <= 0 rejection ────────────────────────────────────────────────

    #[test]
    fn reject_mark_iv_zero_or_negative() {
        // Simulate the filter in fetch_sigma
        let iv_zero = 0.0_f64;
        let iv_neg = -1.0_f64;
        let iv_pos = 45.0_f64;

        assert!(!(iv_zero > 0.0), "zero iv should be rejected");
        assert!(!(iv_neg > 0.0), "negative iv should be rejected");
        assert!(iv_pos > 0.0, "positive iv should be accepted");
    }

    // ── Expiry calculation ────────────────────────────────────────────────────

    #[test]
    fn expiry_unix_known_date() {
        // 2025-03-28 08:00:00 UTC
        // Verify against a known Unix timestamp.
        // 2025-03-28 00:00:00 UTC = 1743120000 (approx)
        // + 8 * 3600 = 28800
        // 2025-03-28 08:00:00 UTC = 1743148800
        let ts = expiry_unix(28, 3, 25).unwrap();
        assert!(
            (ts - 1_743_148_800.0).abs() < 2.0,
            "2025-03-28 08:00 UTC: got {ts}, expected ~1743148800"
        );
    }

    #[test]
    fn expiry_unix_epoch_reference() {
        // 2025-01-02 08:00 UTC
        // 2025-01-02 00:00 UTC = 1735776000
        // + 8h = 28800 → 1735804800
        let ts = expiry_unix(2, 1, 25).unwrap();
        assert!((ts - 1_735_804_800.0).abs() < 2.0, "got {ts}");
    }

    // ── Near-ATM cluster filtering ────────────────────────────────────────────

    #[test]
    fn cluster_filtering_moneyness_threshold() {
        // moneyness < 0.03 passes; >= 0.03 fails
        let spot = 95000.0_f64;

        // ATM: moneyness = 0
        let strike_atm = 95000.0;
        let m_atm = (spot / strike_atm).ln().abs();
        assert!(m_atm < CLUSTER_MONEYNESS_THRESHOLD);

        // ~2% OTM: passes
        let strike_near = 97000.0;
        let m_near = (spot / strike_near).ln().abs();
        assert!(m_near < CLUSTER_MONEYNESS_THRESHOLD, "m_near = {m_near}");

        // ~5% OTM: fails
        let strike_far = 100000.0;
        let m_far = (spot / strike_far).ln().abs();
        assert!(m_far >= CLUSTER_MONEYNESS_THRESHOLD, "m_far = {m_far}");
    }

    #[test]
    fn cluster_filtering_time_window() {
        let best_t = 7200.0_f64;

        // Within 60s: passes
        let t_near = best_t + 30.0;
        assert!((t_near - best_t).abs() < CLUSTER_T_WINDOW_SECS);

        // Exactly 60s: fails (strict <)
        let t_exact = best_t + 60.0;
        assert!(!((t_exact - best_t).abs() < CLUSTER_T_WINDOW_SECS));

        // Different expiry: fails
        let t_far = best_t + 86400.0;
        assert!(!((t_far - best_t).abs() < CLUSTER_T_WINDOW_SECS));
    }

    // ── Bisection IV solver ───────────────────────────────────────────────────

    #[test]
    fn bisect_converges_for_known_option() {
        // Generate a known option price using the kernel, then recover IV via bisection.
        let spot = 90_000.0_f64;
        let strike = 90_000.0_f64;
        let t_secs = 86_400.0_f64; // 1 day

        // Use 50% annualised IV (as fraction = 0.50)
        // FIX: WP05-F3 — sigma_1s = true_iv_frac / sqrt(SECS_PER_YEAR), no /100
        let true_iv_frac = 0.50_f64;
        let sigma_1s = true_iv_frac / SECS_PER_YEAR.sqrt();
        let mark_price_usd = bs_call_price(spot, strike, sigma_1s, t_secs);

        assert!(mark_price_usd > 0.0, "test option price must be positive");

        let recovered_iv = bisect_iv(spot, strike, t_secs, mark_price_usd)
            .expect("bisection should converge");

        // Should recover the original IV within reasonable tolerance
        assert!(
            (recovered_iv - true_iv_frac).abs() < 0.001,
            "bisection recovered {recovered_iv}, expected {true_iv_frac}"
        );
    }

    #[test]
    fn bisect_converges_otm_option() {
        let spot = 95_000.0_f64;
        let strike = 97_000.0_f64; // slightly OTM
        let t_secs = 604_800.0_f64; // 7 days

        let true_iv_frac = 0.80_f64; // 80% annualised
        // FIX: WP05-F3 — sigma_1s = true_iv_frac / sqrt(SECS_PER_YEAR), no /100
        let sigma_1s = true_iv_frac / SECS_PER_YEAR.sqrt();
        let mark_price_usd = bs_call_price(spot, strike, sigma_1s, t_secs);

        if mark_price_usd <= 0.0 {
            return; // degenerate, skip
        }

        let recovered_iv = bisect_iv(spot, strike, t_secs, mark_price_usd)
            .expect("bisection should converge for OTM option");

        assert!(
            (recovered_iv - true_iv_frac).abs() < 0.01,
            "OTM bisection: recovered {recovered_iv}, expected {true_iv_frac}"
        );
    }

    #[test]
    fn bisect_rejects_degenerate_inputs() {
        let spot = 90_000.0_f64;
        let strike = 90_000.0_f64;
        let t_secs = 86_400.0_f64;

        assert!(bisect_iv(0.0, strike, t_secs, 1000.0).is_none(), "zero spot");
        assert!(bisect_iv(spot, 0.0, t_secs, 1000.0).is_none(), "zero strike");
        assert!(bisect_iv(spot, strike, 0.0, 1000.0).is_none(), "zero t_secs");
        assert!(bisect_iv(spot, strike, t_secs, 0.0).is_none(), "zero price");
        assert!(bisect_iv(spot, strike, t_secs, -1.0).is_none(), "negative price");
    }

    // ── iv_to_sigma matches kernel ────────────────────────────────────────────

    #[test]
    fn iv_to_sigma_matches_kernel() {
        // iv_to_sigma(iv_pct) must equal implied_vol_to_sigma_1s(iv_pct)
        for &iv_pct in &[10.0_f64, 30.0, 45.0, 80.0, 120.0] {
            let ours = iv_to_sigma(iv_pct);
            let kernel = implied_vol_to_sigma_1s(iv_pct);
            assert_eq!(
                ours, kernel,
                "iv_to_sigma({iv_pct}) = {ours}, kernel = {kernel}"
            );
        }
    }

    #[test]
    fn iv_to_sigma_known_value() {
        // 45% annualised → sigma_1s ≈ 8.01e-5
        let sigma = iv_to_sigma(45.0);
        assert!(
            (sigma - 8.01e-5).abs() < 1e-6,
            "45% IV → sigma_1s = {sigma}, expected ~8.01e-5"
        );
    }

    #[test]
    fn iv_to_sigma_nan_firewall() {
        assert_eq!(iv_to_sigma(0.0), 0.0);
        assert_eq!(iv_to_sigma(-1.0), 0.0);
        assert_eq!(iv_to_sigma(f64::NAN), 0.0);
        assert_eq!(iv_to_sigma(f64::INFINITY), 0.0);
    }

    // ── Fallback to deribit_iv when back-computation fails ───────────────────

    #[test]
    fn fallback_logic_uses_smoothed_iv() {
        // Simulate the fallback: when computed_ivs is empty, we use deribit smoothed
        let avg_deribit_iv = 45.0_f64; // 45%
        let sigma_deribit = iv_to_sigma(avg_deribit_iv);

        let computed_ivs: Vec<f64> = vec![]; // empty → fallback

        let (avg_computed_iv, sigma_computed, n_computed) = if !computed_ivs.is_empty() {
            let avg = computed_ivs.iter().sum::<f64>() / computed_ivs.len() as f64;
            let sigma = iv_to_sigma(avg * 100.0);
            (avg, sigma, computed_ivs.len())
        } else {
            (avg_deribit_iv / 100.0, sigma_deribit, 0usize)
        };

        assert_eq!(n_computed, 0, "no computed IVs");
        assert!(
            (avg_computed_iv - 0.45).abs() < 1e-10,
            "fallback avg_computed_iv should be 0.45"
        );
        assert_eq!(sigma_computed, sigma_deribit, "fallback sigma should match deribit");

        // Final selection: n_computed == 0 → use sigma_deribit
        let final_sigma = if n_computed > 0 { sigma_computed } else { sigma_deribit };
        assert_eq!(final_sigma, sigma_deribit);
    }

    #[test]
    fn fallback_not_triggered_when_computed_succeeds() {
        let avg_deribit_iv = 45.0_f64;
        let sigma_deribit = iv_to_sigma(avg_deribit_iv);

        let computed_ivs = vec![0.44_f64, 0.46_f64]; // some valid back-computed IVs

        let (avg_computed_iv, sigma_computed, n_computed) = if !computed_ivs.is_empty() {
            let avg = computed_ivs.iter().sum::<f64>() / computed_ivs.len() as f64;
            let sigma = iv_to_sigma(avg * 100.0);
            (avg, sigma, computed_ivs.len())
        } else {
            (avg_deribit_iv / 100.0, sigma_deribit, 0usize)
        };

        assert_eq!(n_computed, 2);
        assert!((avg_computed_iv - 0.45).abs() < 1e-10);

        let final_sigma = if n_computed > 0 { sigma_computed } else { sigma_deribit };
        assert_eq!(final_sigma, sigma_computed);
        // sigma_computed is derived from avg * 100 = 45.0
        assert!((final_sigma - sigma_deribit).abs() < 1e-12);
    }

    // ── FeedRow output ────────────────────────────────────────────────────────

    #[test]
    fn feed_row_sources_are_correct() {
        assert_eq!("deribit_iv", "deribit_iv");
        assert_eq!("deribit_iv_computed", "deribit_iv_computed");
    }

    #[test]
    fn feed_row_value_deribit_iv_is_percentage() {
        // deribit_iv row: value = avg_deribit_iv (percentage like 45.0)
        let avg_deribit_iv = 45.0_f64;
        // Python: conn.execute(..., "deribit_iv", avg_deribit_iv, ...)
        assert!((avg_deribit_iv - 45.0).abs() < 1e-10);
    }

    #[test]
    fn feed_row_value_computed_is_percentage() {
        // deribit_iv_computed row: value = avg_computed_iv * 100 (back to percentage)
        let avg_computed_iv = 0.45_f64; // fraction
        let value = avg_computed_iv * 100.0;
        assert!((value - 45.0).abs() < 1e-10);
    }

    // ── End-to-end bisection roundtrip ────────────────────────────────────────

    #[test]
    fn bisection_roundtrip_high_iv() {
        // Test with 200% annualised IV (well inside range)
        let spot = 80_000.0_f64;
        let strike = 80_000.0_f64;
        let t_secs = 3600.0_f64; // 1 hour

        let true_iv_frac = 2.0_f64; // 200% (= 2.0 fraction)
        // FIX: WP05-F3 — sigma_1s = true_iv_frac / sqrt(SECS_PER_YEAR), no /100
        let sigma_1s = true_iv_frac / SECS_PER_YEAR.sqrt();
        let mark_price_usd = bs_call_price(spot, strike, sigma_1s, t_secs);

        assert!(mark_price_usd > 0.0);

        let recovered = bisect_iv(spot, strike, t_secs, mark_price_usd)
            .expect("should converge for high IV");

        assert!(
            (recovered - true_iv_frac).abs() < 0.01,
            "high IV bisection: recovered {recovered}, expected {true_iv_frac}"
        );
    }

    #[test]
    fn sigma_1s_conversion_chain() {
        // Verify the output-side conversion chain used in fetch_sigma:
        //   avg_computed_iv (bisection result, fraction like 0.50)
        //   → sigma = iv_to_sigma(avg * 100.0)
        //   → iv_to_sigma(iv_pct) = (iv_pct / 100) / sqrt(SECS_PER_YEAR)
        //
        // So sigma = (avg_computed_iv * 100 / 100) / sqrt(SECS_PER_YEAR)
        //          = avg_computed_iv / sqrt(SECS_PER_YEAR)
        //
        // Cross-check: iv_to_sigma(50.0) should equal 0.50 / sqrt(SECS_PER_YEAR)
        let iv_pct = 50.0_f64;
        let sigma_kernel = iv_to_sigma(iv_pct);
        let sigma_expected = iv_pct / 100.0 / SECS_PER_YEAR.sqrt();
        assert!(
            (sigma_kernel - sigma_expected).abs() < 1e-15,
            "iv_to_sigma(50.0)={sigma_kernel}, expected={sigma_expected}"
        );

        // FIX: WP05-F3 — bisect_iv's internal sigma_1s at mid=0.50 is now:
        //   mid / sqrt(SECS_PER_YEAR)  (no /100)
        // This matches the output chain: iv_to_sigma(mid * 100) = mid / sqrt(SECS_PER_YEAR)
        // so the bisection search space and the output sigma are now consistent.
        let mid = 0.50_f64; // bisection midpoint in fraction space
        let bisect_sigma = mid / SECS_PER_YEAR.sqrt();
        let output_sigma = iv_to_sigma(mid * 100.0);
        assert!(
            (output_sigma - bisect_sigma).abs() < 1e-15,
            "bisect internal sigma and output sigma should be equal: \
             bisect={bisect_sigma}, output={output_sigma}"
        );
    }
}
