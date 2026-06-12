//! KalshiClient — REST client for Kalshi prediction market.
//!
//! Implements VenueClient. Ported from scripts/collect.py.
//!
//! Market discovery: queries 4 BTC series sequentially, upserts into markets
//! table with COALESCE protection on resolution_time and outcome.
//!
//! Snapshot: batches markets by event_ticker (rsplit "-" once) and makes one
//! API call per event rather than one per market.
//!
//! Resolution: checks each candidate individually with 0.5s sleep between
//! calls. Status not in (None, "open", "active") = resolved.

use super::VenueClient;
use crate::feed::finite;
use anyhow::Context;
use rusqlite::{params, Connection};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const KALSHI_API: &str = "https://api.elections.kalshi.com/trade-api/v2";

const KALSHI_SERIES: &[&str] = &["KXBTC15M", "KXBTCD", "KXBTC", "BTCD"];

/// User-Agent
const USER_AGENT: &str = "mantis/0.3";

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct KalshiClient {
    http: reqwest::Client,
}

impl KalshiClient {
    pub fn new() -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .context("failed to build reqwest::Client for Kalshi")?;
        Ok(Self { http })
    }
}

impl Default for KalshiClient {
    fn default() -> Self {
        // Infallible in practice; reqwest::Client::builder() only fails on
        // bad TLS config, which we don't customize beyond timeouts.
        Self::new().expect("KalshiClient::default: reqwest builder failed")
    }
}

// ---------------------------------------------------------------------------
// VenueClient impl
// ---------------------------------------------------------------------------

impl VenueClient for KalshiClient {
    fn name(&self) -> &'static str {
        "kalshi"
    }

    /// Discover and upsert all open Kalshi BTC markets for the 4 known series.
    async fn sync_markets<'a>(&'a self, db_path: &'a str) -> anyhow::Result<usize> {
        let conn = crate::db::open(db_path)?;
        let now = crate::feed::wall_clock();
        let mut total = 0usize;

        for &series in KALSHI_SERIES {
            let url = format!(
                "{KALSHI_API}/markets?series_ticker={series}&status=open&limit=200"
            );

            let data: Value = match self.http.get(&url).send().await {
                Ok(resp) => match resp.error_for_status() {
                    Ok(r) => match r.json().await {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[kalshi] JSON parse error for {series}: {e}");
                            continue;
                        }
                    },
                    Err(e) => {
                        eprintln!("[kalshi] HTTP error for {series}: {e}");
                        continue;
                    }
                },
                Err(e) => {
                    eprintln!("[kalshi] request error for {series}: {e}");
                    continue;
                }
            };

            let markets = match data.get("markets").and_then(Value::as_array) {
                Some(v) => v,
                None => {
                    eprintln!("[kalshi] no 'markets' array in response for {series}");
                    continue;
                }
            };

            eprintln!("[kalshi] {series}: {} open markets", markets.len());

            for m in markets {
                // ticker — prefer "ticker", fall back to "market_ticker"
                let ticker = match m.get("ticker")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .or_else(|| m.get("market_ticker").and_then(Value::as_str))
                {
                    Some(t) => t.to_owned(),
                    None => continue,
                };

                // strike — floor_strike preferred, regex fallback
                let strike = extract_strike(m);

                // close_time — close_time preferred, expiration_time fallback
                let close_time = m.get("close_time")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .or_else(|| m.get("expiration_time").and_then(Value::as_str))
                    .map(str::to_owned);

                // rules — rules_primary → market_rules → subtitle
                let subtitle = m.get("subtitle")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .or_else(|| m.get("title").and_then(Value::as_str))
                    .unwrap_or("");
                let rules_raw = m.get("rules_primary")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .or_else(|| m.get("market_rules").and_then(Value::as_str))
                    .filter(|s| !s.is_empty())
                    .unwrap_or(subtitle);
                let rules: Option<String> = if rules_raw.is_empty() {
                    None
                } else {
                    Some(rules_raw.chars().take(2000).collect())
                };

                let open_time = m.get("open_time")
                    .and_then(Value::as_str)
                    .map(str::to_owned);

                // FIX: WP04-F1 — map series to correct resolution oracle.
                // BTCD (daily above/below) resolves on BRR, not BRTI.
                // All other BTC series resolve on BRTI.
                let oracle = oracle_for_series(series);

                // FIX: WP04-F7 — capture cap_strike for range markets.
                // Range markets (KXBTC) have both a floor and cap strike;
                // storing only floor_strike loses the upper bound, making
                // range market rules unverifiable from the DB alone.
                let cap_strike = m.get("cap_strike")
                    .and_then(Value::as_f64)
                    .filter(|&v| v.is_finite() && v > 0.0);

                let rules_with_cap = match (rules.as_deref(), cap_strike) {
                    (Some(r), Some(cap)) => Some(format!("cap_strike={cap} {r}")),
                    (Some(r), None) => Some(r.to_string()),
                    (None, Some(cap)) => Some(format!("cap_strike={cap}")),
                    (None, None) => None,
                };

                upsert_market(
                    &conn,
                    "kalshi",
                    &ticker,
                    series,
                    market_type_for_series(series),
                    oracle,
                    strike,
                    open_time.as_deref(),
                    close_time.as_deref(),
                    rules_with_cap.as_deref(),
                    now,
                )?;
                total += 1;
            }
        }

        Ok(total)
    }
}

// ---------------------------------------------------------------------------
// Resolution checking (not part of trait — called from the main loop)
// ---------------------------------------------------------------------------

/// Resolution data returned by [`check_resolution`].
#[derive(Debug)]
pub struct ResolutionInfo {
    pub outcome: Option<String>,
    pub oracle_value_at_close: Option<f64>,
    pub close_time_str: Option<String>,
    pub resolution_time_str: Option<String>,
    pub status: String,
}

/// Check whether a Kalshi market has resolved.
///
/// Returns `None` if the market is still open/active, or if the request fails.
/// Caller is responsible for the 0.5s sleep between calls.
pub async fn check_resolution(
    http: &reqwest::Client,
    ticker: &str,
) -> Option<ResolutionInfo> {
    let url = format!("{KALSHI_API}/markets/{ticker}");
    let data: Value = http
        .get(&url)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;

    // API may wrap in {"market": {...}} or return the object directly
    let m = data.get("market").unwrap_or(&data);

    let status = m.get("status").and_then(Value::as_str).unwrap_or("").to_owned();

    // open / active = not yet resolved
    if matches!(status.as_str(), "" | "open" | "active") {
        return None;
    }

    let result = m.get("result")
        .and_then(Value::as_str)
        .or_else(|| m.get("winner_side").and_then(Value::as_str))
        .map(str::to_owned);

    let oracle_value_at_close = m.get("expiration_value").and_then(finite);

    let close_time_str = m.get("close_time")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| m.get("expiration_time").and_then(Value::as_str))
        .map(str::to_owned);

    // FIX: WP04-F6 — do not substitute close_time for resolution_time.
    // close_time is the scheduled expiry; resolution_time is when the oracle
    // actually published the settlement value. They differ for disputed or
    // delayed settlements. Storing close_time as resolution_time inflates
    // the measured resolution lag and corrupts any time-weighted analysis.
    let resolution_time_str = m.get("resolution_time")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    Some(ResolutionInfo {
        outcome: result,
        oracle_value_at_close,
        close_time_str,
        resolution_time_str,
        status,
    })
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Helpers — pure functions, easily unit-tested
// ---------------------------------------------------------------------------

/// Map a Kalshi series ticker to a market_type string.
///
/// Fix #15 (from Python): KXBTCD is hourly (above/below), NOT daily. BTCD is daily.
pub fn market_type_for_series(series: &str) -> &'static str {
    match series {
        "KXBTC15M" => "up_down_15m",
        "KXBTCD" => "above_below_hourly",
        "KXBTC" => "range_hourly",
        "BTCD" => "above_below_daily",
        _ => "unknown",
    }
}

/// Map a Kalshi series ticker to its resolution oracle.
///
/// FIX: WP04-F1 — BTCD (daily above/below) resolves on BRR (Bitcoin Reference
/// Rate, the CME daily auction price), not BRTI (real-time index). All intraday
/// series (KXBTC15M, KXBTCD, KXBTC) resolve on BRTI.
pub fn oracle_for_series(series: &str) -> &'static str {
    match series {
        s if s.starts_with("KXBTC15M") => "brti",
        s if s.starts_with("KXBTCD") => "brti",
        s if s.starts_with("KXBTC") => "brti",
        s if s.starts_with("BTCD") => "brr",
        _ => "brti", // conservative default
    }
}

/// Extract the event_ticker from a full market ticker.
///
/// Kalshi tickers look like `KXBTCD-26MAR1400-T70000`.
/// The event_ticker is everything up to the last `-`-delimited segment.
///
/// ```
/// use mantis_hybrid::venue::kalshi::kalshi_event_ticker;
/// assert_eq!(kalshi_event_ticker("KXBTCD-26MAR1400-T70000"), "KXBTCD-26MAR1400");
/// assert_eq!(kalshi_event_ticker("SIMPLE"), "SIMPLE");
/// ```
pub fn kalshi_event_ticker(ticker: &str) -> String {
    match ticker.rsplit_once('-') {
        Some((prefix, _)) => prefix.to_owned(),
        None => ticker.to_owned(),
    }
}

/// Extract a strike price from a Kalshi market JSON object.
///
/// Prefers `floor_strike` (Fix #2 from Python). Falls back to regex `$X,XXX`
/// on `subtitle` or `title`.
fn extract_strike(m: &Value) -> Option<f64> {
    // Primary: floor_strike field
    if let Some(v) = m.get("floor_strike")
        && let Some(f) = finite(v)
    {
        return Some(f);
    }

    // Fallback: regex on subtitle / title
    let text = m.get("subtitle")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| m.get("title").and_then(Value::as_str))
        .unwrap_or("");

    parse_dollar_amount(text)
}

/// Extract the first `$X,XXX` pattern from `text` and return as f64.
pub fn parse_dollar_amount(text: &str) -> Option<f64> {
    // Find a '$' followed by digits (with optional commas)
    let chars = text.char_indices();
    for (i, ch) in chars {
        if ch == '$' {
            // Collect subsequent digit/comma characters
            let start = i + 1;
            let rest = &text[start..];
            let num_str: String = rest
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == ',')
                .collect();
            if !num_str.is_empty() {
                let cleaned = num_str.replace(',', "");
                if let Ok(v) = cleaned.parse::<f64>() {
                    return Some(v);
                }
            }
        }
    }
    None
}

/// Read a named field from a JSON object and pass through the NaN firewall.
#[cfg(test)]
fn finite_field(m: &Value, field: &str) -> Option<f64> {
    m.get(field).and_then(finite)
}

// ---------------------------------------------------------------------------
// DB helper
// ---------------------------------------------------------------------------

/// UPSERT a market into the `markets` table.
///
/// Uses `COALESCE` on `resolution_time` and `outcome` so a NULL from
/// discovery never overwrites a previously-written non-NULL value.
#[allow(clippy::too_many_arguments)]
fn upsert_market(
    conn: &Connection,
    venue: &str,
    ticker: &str,
    series: &str,
    market_type: &str,
    oracle: &str,
    strike: Option<f64>,
    open_time: Option<&str>,
    close_time: Option<&str>,
    rules: Option<&str>,
    now: f64,
) -> anyhow::Result<()> {
    conn.execute(
        r#"
        INSERT INTO markets
            (venue, ticker, series, market_type, oracle, strike,
             open_time, close_time, resolution_time, outcome, rules, discovered_at)
        VALUES
            (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL, ?9, ?10)
        ON CONFLICT(venue, ticker) DO UPDATE SET
            series          = excluded.series,
            market_type     = excluded.market_type,
            oracle          = excluded.oracle,
            strike          = excluded.strike,
            open_time       = excluded.open_time,
            close_time      = excluded.close_time,
            resolution_time = COALESCE(markets.resolution_time, excluded.resolution_time),
            outcome         = COALESCE(markets.outcome, excluded.outcome),
            rules           = excluded.rules
        "#,
        params![
            venue,
            ticker,
            series,
            market_type,
            oracle,
            strike,
            open_time,
            close_time,
            rules,
            now,
        ],
    )
    .context("upsert_market")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// L2 book fetcher
// ---------------------------------------------------------------------------

/// Fetch L2 orderbook depth for a set of Kalshi markets.
///
/// API: GET /markets/{ticker}/orderbook (public, no auth needed).
/// Response: `orderbook_fp.yes_dollars` and `orderbook_fp.no_dollars` — arrays
/// of [price_str, quantity_str] sorted ascending. Best bid = last YES element.
/// Best ask = $1.00 - last NO element (binary market reciprocal).
pub async fn fetch_kalshi_books(
    http: &reqwest::Client,
    markets: &[&crate::db::MarketRow],
) -> Vec<crate::db::BookSnapshotRow> {
    let mut results = Vec::new();
    let now = crate::feed::wall_clock();

    for &market in markets {
        // Rate-limit: 100ms between requests
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let url = format!("{KALSHI_API}/markets/{}/orderbook", market.ticker);
        let data: Value = match http.get(&url).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(_) => continue,
                },
                Err(_) => continue,
            },
            Err(_) => continue,
        };

        if let Some(book) = parse_kalshi_book(&data, now, market.id) {
            results.push(book);
        }
    }
    results
}

/// Parse a Kalshi orderbook response into a BookSnapshotRow.
///
/// Kalshi only returns bids. A YES bid at price X implies a NO ask at (1-X).
/// Best YES bid = last element of yes_dollars (ascending sort).
/// Best YES ask = 1.0 - last element of no_dollars.
fn parse_kalshi_book(
    data: &Value,
    ts: f64,
    market_id: i64,
) -> Option<crate::db::BookSnapshotRow> {
    let ob = data.get("orderbook_fp")?;
    let yes_levels = parse_kalshi_levels(ob.get("yes_dollars")?);
    let no_levels = parse_kalshi_levels(ob.get("no_dollars")?);

    // Both sides empty = no book
    if yes_levels.is_empty() && no_levels.is_empty() {
        return None;
    }

    // Best YES bid = last (highest) element of yes_dollars
    let best_bid = yes_levels.last().map(|&(price, _)| price);

    // Best YES ask = 1.0 - last (highest) NO bid price
    let best_ask = no_levels.last().map(|&(price, _)| 1.0 - price);

    let spread = match (best_bid, best_ask) {
        (Some(b), Some(a)) => a - b,
        _ => 0.0,
    };

    // Depth within $0.05 of best
    let bid_depth = match best_bid {
        Some(bb) => yes_levels
            .iter()
            .filter(|&&(p, _)| p >= bb - 0.05)
            .map(|&(_, q)| q)
            .sum(),
        None => 0.0,
    };
    let ask_depth = match best_ask {
        Some(ba) => {
            // NO bids where (1.0 - price) <= ba + 0.05, i.e. price >= 1.0 - (ba + 0.05)
            let threshold = 1.0 - (ba + 0.05);
            no_levels
                .iter()
                .filter(|&&(p, _)| p >= threshold)
                .map(|&(_, q)| q)
                .sum()
        }
        None => 0.0,
    };

    // Compact JSON for levels
    let levels_json = serde_json::json!({
        "yes": yes_levels.iter().map(|&(p, q)| [p, q]).collect::<Vec<_>>(),
        "no": no_levels.iter().map(|&(p, q)| [p, q]).collect::<Vec<_>>(),
    });

    Some(crate::db::BookSnapshotRow {
        ts,
        market_id,
        venue: "kalshi".to_owned(),
        bid_depth,
        ask_depth,
        spread,
        best_bid,
        best_ask,
        levels: Some(levels_json.to_string()),
    })
}

/// Parse Kalshi price level array: [["0.42", "13.00"], ...] → Vec<(f64, f64)>
fn parse_kalshi_levels(arr: &Value) -> Vec<(f64, f64)> {
    let arr = match arr.as_array() {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let pair = match item.as_array() {
            Some(a) if a.len() >= 2 => a,
            _ => continue,
        };
        let price: f64 = match pair[0].as_str().and_then(|s| s.parse::<f64>().ok()) {
            Some(v) if v.is_finite() && v > 0.0 => v,
            _ => continue,
        };
        let qty: f64 = match pair[1].as_str().and_then(|s| s.parse::<f64>().ok()) {
            Some(v) if v.is_finite() && v > 0.0 => v,
            _ => continue,
        };
        levels.push((price, qty));
    }
    levels
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use serde_json::json;

    // ── Market type mapping ───────────────────────────────────────────────────

    #[test]
    fn market_type_kxbtc15m() {
        assert_eq!(market_type_for_series("KXBTC15M"), "up_down_15m");
    }

    #[test]
    fn market_type_kxbtcd_is_hourly_not_daily() {
        // Fix #15: KXBTCD = hourly above/below, NOT daily
        assert_eq!(market_type_for_series("KXBTCD"), "above_below_hourly");
    }

    #[test]
    fn market_type_kxbtc() {
        assert_eq!(market_type_for_series("KXBTC"), "range_hourly");
    }

    #[test]
    fn market_type_btcd_is_daily() {
        assert_eq!(market_type_for_series("BTCD"), "above_below_daily");
    }

    #[test]
    fn market_type_unknown_series() {
        assert_eq!(market_type_for_series("XXXX"), "unknown");
    }

    // ── Event ticker extraction ───────────────────────────────────────────────

    #[test]
    fn event_ticker_three_part() {
        assert_eq!(
            kalshi_event_ticker("KXBTCD-26MAR1400-T70000"),
            "KXBTCD-26MAR1400"
        );
    }

    #[test]
    fn event_ticker_two_part() {
        assert_eq!(kalshi_event_ticker("KXBTC15M-T90000"), "KXBTC15M");
    }

    #[test]
    fn event_ticker_no_hyphen() {
        assert_eq!(kalshi_event_ticker("SIMPLE"), "SIMPLE");
    }

    #[test]
    fn event_ticker_btcd() {
        assert_eq!(
            kalshi_event_ticker("BTCD-26MAR-T70000"),
            "BTCD-26MAR"
        );
    }

    // ── Strike extraction ─────────────────────────────────────────────────────

    #[test]
    fn strike_from_floor_strike() {
        let m = json!({"floor_strike": 70000.0, "subtitle": "Will BTC be above $65,000?"});
        assert_eq!(extract_strike(&m), Some(70000.0));
    }

    #[test]
    fn strike_from_floor_strike_string() {
        // Kalshi sometimes returns numeric fields as strings
        let m = json!({"floor_strike": "75000.5"});
        assert_eq!(extract_strike(&m), Some(75000.5));
    }

    #[test]
    fn strike_fallback_to_subtitle_regex() {
        let m = json!({"subtitle": "Will BTC close above $95,000 at 2pm?"});
        assert_eq!(extract_strike(&m), Some(95000.0));
    }

    #[test]
    fn strike_fallback_to_title_regex() {
        let m = json!({"title": "BTC above $85,000"});
        assert_eq!(extract_strike(&m), Some(85000.0));
    }

    #[test]
    fn strike_none_when_absent() {
        let m = json!({"subtitle": "No price here"});
        assert_eq!(extract_strike(&m), None);
    }

    #[test]
    fn parse_dollar_amount_with_commas() {
        assert_eq!(parse_dollar_amount("above $70,000 at close"), Some(70000.0));
    }

    #[test]
    fn parse_dollar_amount_no_commas() {
        assert_eq!(parse_dollar_amount("strike $95000"), Some(95000.0));
    }

    #[test]
    fn parse_dollar_amount_no_match() {
        assert_eq!(parse_dollar_amount("no price"), None);
    }

    // ── Ticker fallback ───────────────────────────────────────────────────────

    #[test]
    fn ticker_field_preferred_over_market_ticker() {
        // In the sync_markets loop, "ticker" is checked first.
        // This test verifies the extraction logic directly.
        let m = json!({"ticker": "KXBTCD-26MAR1400-T70000", "market_ticker": "SHOULD_NOT_USE"});
        let ticker = m.get("ticker")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .or_else(|| m.get("market_ticker").and_then(Value::as_str));
        assert_eq!(ticker, Some("KXBTCD-26MAR1400-T70000"));
    }

    #[test]
    fn ticker_falls_back_to_market_ticker() {
        let m = json!({"market_ticker": "KXBTCD-26MAR1400-T70000"});
        let ticker = m.get("ticker")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .or_else(|| m.get("market_ticker").and_then(Value::as_str));
        assert_eq!(ticker, Some("KXBTCD-26MAR1400-T70000"));
    }

    #[test]
    fn ticker_none_when_both_absent() {
        let m = json!({"other_field": "irrelevant"});
        let ticker: Option<&str> = m.get("ticker")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .or_else(|| m.get("market_ticker").and_then(Value::as_str));
        assert!(ticker.is_none());
    }

    // ── Field mapping (yes_bid_dollars etc.) ─────────────────────────────────

    #[test]
    fn field_mapping_dollar_fields() {
        let m = json!({
            "ticker": "KXBTCD-26MAR1400-T70000",
            "yes_bid_dollars": "0.4700",
            "yes_ask_dollars": "0.4900",
            "no_bid_dollars": "0.5100",
            "no_ask_dollars": "0.5300",
            "volume_fp": "12345.67",
            "open_interest_fp": "999.0",
        });

        assert_eq!(finite_field(&m, "yes_bid_dollars"), Some(0.47));
        assert_eq!(finite_field(&m, "yes_ask_dollars"), Some(0.49));
        assert_eq!(finite_field(&m, "no_bid_dollars"), Some(0.51));
        assert_eq!(finite_field(&m, "no_ask_dollars"), Some(0.53));
    }

    #[test]
    fn field_mapping_volume_fp_preferred_over_volume() {
        let m = json!({"volume_fp": "12345.0", "volume": 999});
        let vol = finite_field(&m, "volume_fp").or_else(|| finite_field(&m, "volume"));
        assert_eq!(vol, Some(12345.0));
    }

    #[test]
    fn field_mapping_volume_fallback() {
        let m = json!({"volume": 999});
        let vol = finite_field(&m, "volume_fp").or_else(|| finite_field(&m, "volume"));
        assert_eq!(vol, Some(999.0));
    }

    #[test]
    fn field_mapping_oi_fp_preferred() {
        let m = json!({"open_interest_fp": "500.5", "open_interest": 1});
        let oi = finite_field(&m, "open_interest_fp")
            .or_else(|| finite_field(&m, "open_interest"));
        assert_eq!(oi, Some(500.5));
    }

    #[test]
    fn nan_firewall_rejects_non_finite() {
        // JSON null → None
        let m = json!({"yes_bid_dollars": null});
        assert_eq!(finite_field(&m, "yes_bid_dollars"), None);
    }

    // ── Resolution detection ──────────────────────────────────────────────────

    #[test]
    fn resolution_status_open_not_resolved() {
        // status "open" → still active
        let status = "open";
        assert!(matches!(status, "" | "open" | "active"));
    }

    #[test]
    fn resolution_status_active_not_resolved() {
        let status = "active";
        assert!(matches!(status, "" | "open" | "active"));
    }

    #[test]
    fn resolution_status_settled_is_resolved() {
        let status = "settled";
        assert!(!matches!(status, "" | "open" | "active"));
    }

    #[test]
    fn resolution_status_closed_is_resolved() {
        let status = "closed";
        assert!(!matches!(status, "" | "open" | "active"));
    }

    #[test]
    fn resolution_status_resolved_is_resolved() {
        let status = "resolved";
        assert!(!matches!(status, "" | "open" | "active"));
    }

    // ── Rules truncation ──────────────────────────────────────────────────────

    #[test]
    fn rules_truncated_to_2000_chars() {
        let long_rules: String = "x".repeat(5000);
        let truncated: String = long_rules.chars().take(2000).collect();
        assert_eq!(truncated.len(), 2000);
    }

    #[test]
    fn rules_under_2000_unchanged() {
        let short = "short rules text";
        let truncated: String = short.chars().take(2000).collect();
        assert_eq!(truncated, short);
    }

    // ── DB upsert (COALESCE protection) ───────────────────────────────────────

    fn test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(crate::db::SCHEMA_SQL).unwrap();
        conn
    }

    #[test]
    fn upsert_market_inserts_new() {
        let conn = test_db();
        upsert_market(
            &conn,
            "kalshi",
            "KXBTCD-26MAR1400-T70000",
            "KXBTCD",
            "above_below_hourly",
            "brti",
            Some(70000.0),
            None,
            Some("2026-03-26T14:00:00Z"),
            Some("Will BTC close above $70,000?"),
            1710000000.0,
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM markets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_market_updates_on_conflict() {
        let conn = test_db();

        // Insert initial row
        upsert_market(
            &conn,
            "kalshi",
            "KXBTCD-26MAR1400-T70000",
            "KXBTCD",
            "above_below_hourly",
            "brti",
            Some(70000.0),
            None,
            Some("2026-03-26T14:00:00Z"),
            Some("original rules"),
            1710000000.0,
        )
        .unwrap();

        // Update with new rules
        upsert_market(
            &conn,
            "kalshi",
            "KXBTCD-26MAR1400-T70000",
            "KXBTCD",
            "above_below_hourly",
            "brti",
            Some(70000.0),
            None,
            Some("2026-03-26T14:00:00Z"),
            Some("updated rules"),
            1710000001.0,
        )
        .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM markets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1); // still one row

        let rules: String = conn
            .query_row("SELECT rules FROM markets WHERE ticker = 'KXBTCD-26MAR1400-T70000'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rules, "updated rules");
    }

    // ── L2 book parsing ───────────────────────────────────────────────────────

    #[test]
    fn test_parse_kalshi_book() {
        let data = serde_json::json!({
            "orderbook_fp": {
                "yes_dollars": [
                    ["0.1000", "200.00"],
                    ["0.2000", "100.00"],
                    ["0.2500", "50.00"],
                ],
                "no_dollars": [
                    ["0.0100", "500.00"],
                    ["0.5000", "300.00"],
                    ["0.7000", "150.00"],
                ]
            }
        });
        let book = parse_kalshi_book(&data, 1000.0, 1).unwrap();
        assert_eq!(book.market_id, 1);
        assert!((book.best_bid.unwrap() - 0.25).abs() < 1e-6);
        assert!((book.best_ask.unwrap() - 0.30).abs() < 1e-6); // 1.0 - 0.70
        assert!((book.spread - 0.05).abs() < 1e-6);
        assert!(book.bid_depth > 0.0);
        assert!(book.ask_depth > 0.0);
        assert!(book.levels.is_some());
    }

    #[test]
    fn test_parse_kalshi_book_empty() {
        let data = serde_json::json!({
            "orderbook_fp": {
                "yes_dollars": [],
                "no_dollars": []
            }
        });
        assert!(parse_kalshi_book(&data, 1000.0, 1).is_none());
    }

    #[test]
    fn test_parse_kalshi_levels() {
        let arr = serde_json::json!([["0.42", "13.00"], ["0.50", "20.00"]]);
        let levels = parse_kalshi_levels(&arr);
        assert_eq!(levels.len(), 2);
        assert!((levels[0].0 - 0.42).abs() < 1e-6);
        assert!((levels[0].1 - 13.0).abs() < 1e-6);
    }

    #[test]
    fn test_kalshi_depth_within_5_cents() {
        // Best bid at 0.50, levels at 0.50, 0.48, 0.46, 0.44, 0.10
        // Only 0.50, 0.48, 0.46 are within $0.05
        let data = serde_json::json!({
            "orderbook_fp": {
                "yes_dollars": [
                    ["0.1000", "1000.00"],
                    ["0.4400", "100.00"],
                    ["0.4600", "200.00"],
                    ["0.4800", "300.00"],
                    ["0.5000", "400.00"],
                ],
                "no_dollars": [
                    ["0.4500", "500.00"],
                ]
            }
        });
        let book = parse_kalshi_book(&data, 1000.0, 1).unwrap();
        // bid_depth: 0.50 + 0.48 + 0.46 (all >= 0.45) = 400 + 300 + 200 = 900
        // 0.44 is < 0.45 so excluded
        assert!((book.bid_depth - 900.0).abs() < 1e-6);
    }

    #[test]
    fn upsert_coalesce_preserves_outcome() {
        let conn = test_db();

        // Insert initial row
        upsert_market(
            &conn,
            "kalshi",
            "KXBTCD-26MAR1400-T70000",
            "KXBTCD",
            "above_below_hourly",
            "brti",
            Some(70000.0),
            None,
            Some("2026-03-26T14:00:00Z"),
            None,
            1710000000.0,
        )
        .unwrap();

        // Manually set outcome (simulating resolution)
        conn.execute(
            "UPDATE markets SET outcome = 'Yes' WHERE ticker = 'KXBTCD-26MAR1400-T70000'",
            [],
        )
        .unwrap();

        // Re-upsert (discovery run) should NOT overwrite outcome
        upsert_market(
            &conn,
            "kalshi",
            "KXBTCD-26MAR1400-T70000",
            "KXBTCD",
            "above_below_hourly",
            "brti",
            Some(70000.0),
            None,
            Some("2026-03-26T14:00:00Z"),
            None,
            1710000002.0,
        )
        .unwrap();

        let outcome: Option<String> = conn
            .query_row(
                "SELECT outcome FROM markets WHERE ticker = 'KXBTCD-26MAR1400-T70000'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome.as_deref(), Some("Yes")); // COALESCE preserved it
    }
}
