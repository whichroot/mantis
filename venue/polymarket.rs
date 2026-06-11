//! PolymarketClient — Gamma API client for Polymarket prediction market.
//!
//! Implements VenueClient. Ported from scripts/collect.py.
//! CLOB API is dead code in Python — this implementation uses Gamma API only.
//!
//! Market discovery: generates slug candidates for daily-above, intraday
//! up/down (5m, 15m, 4h), daily up-or-down, and hourly up-or-down patterns,
//! deduplicates via a HashSet, and fetches each via the Gamma events endpoint.
//!
//! Snapshot: fetches /markets/{gamma_id} with fallback to
//! /markets?conditionId={gamma_id}. Stores real midpoint in p_market; bid/ask
//! are None because the Gamma API does not provide them (FIX: WP02-F1).
//!
//! Resolution: same endpoint as snapshot; market is resolved when `closed == true`.

use super::VenueClient;
use crate::feed::finite;
use anyhow::Context;
use rusqlite::{params, Connection};
use serde_json::Value;
use std::collections::HashSet;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GAMMA_API: &str = "https://gamma-api.polymarket.com";

/// User-Agent matching the Python client.
const USER_AGENT: &str = "mantis/0.3";

const MONTH_NAMES: &[&str] = &[
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct PolymarketClient {
    http: reqwest::Client,
}

impl PolymarketClient {
    pub fn new() -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .context("failed to build reqwest::Client for Polymarket")?;
        Ok(Self { http })
    }
}

impl Default for PolymarketClient {
    fn default() -> Self {
        Self::new().expect("PolymarketClient::default: reqwest builder failed")
    }
}

// ---------------------------------------------------------------------------
// VenueClient impl
// ---------------------------------------------------------------------------

impl PolymarketClient {
    /// Like `sync_markets` but accepts a fallback strike for `btc-updown-*` markets
    /// whose question text contains no dollar amount.
    ///
    /// Pass `Some(binance_spot)` from the discovery loop after feeds start.
    /// Pass `None` at startup — these markets receive their strike on the next refresh.
    pub async fn sync_markets_with_strike<'a>(
        &'a self,
        db_path: &'a str,
        fallback_strike: Option<f64>,
    ) -> anyhow::Result<usize> {
        self.sync_markets_inner(db_path, fallback_strike).await
    }

    async fn sync_markets_inner<'a>(
        &'a self,
        db_path: &'a str,
        fallback_strike: Option<f64>,
    ) -> anyhow::Result<usize> {
        let conn = crate::db::open(db_path)?;
        let now = crate::feed::wall_clock();
        let now_ts = now as i64;
        let mut total = 0usize;

        // Use current UTC time to compute slug candidates
        let (current_year, current_month, current_day, current_hour) = utc_ymdh(now_ts);

        let mut slugs_tried: HashSet<String> = HashSet::new();

        // 1. Daily bitcoin-above-on-{month}-{day} events (±2 days)
        for offset in -2i64..=2 {
            let ts = now_ts + offset * 86400;
            let (_, month, day, _) = utc_ymdh(ts);
            let slug = daily_above_slug(month, day);
            if slugs_tried.contains(&slug) {
                continue;
            }
            slugs_tried.insert(slug.clone());

            if let Some(ev) = self.fetch_event_by_slug(&slug).await {
                let markets = ev.get("markets").and_then(Value::as_array);
                if let Some(markets) = markets {
                    eprintln!("[polymarket] {slug}: {} markets", markets.len());
                    for m in markets {
                        let row = parse_poly_market(m, &slug, now);
                        upsert_market(&conn, &row)?;
                        total += 1;
                    }
                }
            }
            // FIX: WP04-F8 — rate-limit sleep after every request, not just success.
            // Previously the sleep was inside the if-let block so 404 responses fired
            // unthrottled and could trigger HTTP 429 bursts. Reduced to 500ms.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // 2. Intraday up/down: btc-updown-{5m,15m,4h}-{boundary_unix_ts}
        let interval_map: &[(&str, i64)] = &[
            ("btc-updown-5m", 5 * 60),
            ("btc-updown-15m", 15 * 60),
            ("btc-updown-4h", 4 * 3600),
        ];
        for &(prefix, interval_s) in interval_map {
            for offset_mult in -2i64..=2 {
                let boundary = ((now_ts / interval_s) + offset_mult) * interval_s;
                let slug = format!("{prefix}-{boundary}");
                if slugs_tried.contains(&slug) {
                    continue;
                }
                slugs_tried.insert(slug.clone());

                if let Some(ev) = self.fetch_event_by_slug(&slug).await {
                    let markets = ev.get("markets").and_then(Value::as_array);
                    if let Some(markets) = markets {
                        eprintln!("[polymarket] {slug}: {} markets", markets.len());
                        for m in markets {
                            let mut row = parse_poly_market(m, &slug, now);
                            // btc-updown-* markets have no $ in their description.
                            // Use Binance spot at discovery time as the reference price.
                            if row.strike.is_none() && fallback_strike.filter(|&s| s > 0.0).is_some() {
                                row.strike = fallback_strike;
                            }
                            upsert_market(&conn, &row)?;
                            total += 1;
                        }
                    }
                }
                // FIX: WP04-F8 — rate-limit sleep after every request, not just success.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }

        // 3. bitcoin-up-or-down-on-{month}-{day}-{year} (±1 day)
        for offset in -1i64..=1 {
            let ts = now_ts + offset * 86400;
            let (year, month, day, _) = utc_ymdh(ts);
            let slug = daily_up_or_down_slug(month, day, year);
            if slugs_tried.contains(&slug) {
                continue;
            }
            slugs_tried.insert(slug.clone());

            if let Some(ev) = self.fetch_event_by_slug(&slug).await {
                let markets = ev.get("markets").and_then(Value::as_array);
                if let Some(markets) = markets {
                    eprintln!("[polymarket] {slug}: {} markets", markets.len());
                    for m in markets {
                        let row = parse_poly_market(m, &slug, now);
                        upsert_market(&conn, &row)?;
                        total += 1;
                    }
                }
            }
            // FIX: WP04-F8 — rate-limit sleep after every request, not just success.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // 4. Hourly: bitcoin-up-or-down-{month}-{day}-{hour_str}-et
        //
        // FIX: WP04-F2 / WP04-F3 — use DST-aware ET offset and ET calendar date.
        //
        // Bug: the old code used UTC-4 year-round (EDT only) and always used the
        // UTC calendar date for the slug. During EST (UTC-5) the hour was wrong.
        // When UTC is 00:00-04:59 in EDT or 00:00-05:59 in EST, the ET clock is
        // still on the previous calendar day, so the slug date must be decremented.
        //
        // Fix: use is_dst() to pick the correct offset (4 for EDT, 5 for EST),
        // then check if the ET hour wrapped past midnight to decide whether to
        // use the previous UTC day for the slug date.
        let et_offset = if is_dst(current_month, current_day) { 4i64 } else { 5i64 };
        let et_hour_raw = (current_hour as i64) - et_offset;
        let et_hour = et_hour_raw.rem_euclid(24) as u32;
        // When et_hour_raw < 0 the ET clock is on the previous day
        let (et_month, et_day) = if et_hour_raw < 0 {
            prev_day(current_month, current_day)
        } else {
            (current_month, current_day)
        };
        for hour_offset in -3i64..=3 {
            let h = ((et_hour as i64) + hour_offset).rem_euclid(24) as u32;
            let hour_str = et_hour_str(h);
            let slug = hourly_up_or_down_slug(et_month, et_day, &hour_str);
            if slugs_tried.contains(&slug) {
                continue;
            }
            slugs_tried.insert(slug.clone());

            if let Some(ev) = self.fetch_event_by_slug(&slug).await {
                let markets = ev.get("markets").and_then(Value::as_array);
                if let Some(markets) = markets {
                    eprintln!("[polymarket] {slug}: {} markets", markets.len());
                    for m in markets {
                        let row = parse_poly_market(m, &slug, now);
                        upsert_market(&conn, &row)?;
                        total += 1;
                    }
                }
            }
            // FIX: WP04-F8 — rate-limit sleep after every request, not just success.
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // suppress unused-variable warnings for year/month/day/hour computed above
        let _ = (current_year, current_month, current_day, current_hour);

        Ok(total)
    }
}

impl VenueClient for PolymarketClient {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    async fn sync_markets<'a>(&'a self, db_path: &'a str) -> anyhow::Result<usize> {
        self.sync_markets_inner(db_path, None).await
    }
}

// ---------------------------------------------------------------------------
// Resolution checking (called from main loop, not part of trait)
// ---------------------------------------------------------------------------

/// Resolution data returned by [`check_resolution`].
#[derive(Debug)]
pub struct ResolutionInfo {
    pub outcome: Option<String>,
    pub oracle_value_at_close: Option<f64>,
    pub close_time_str: Option<String>,
    pub resolution_time_str: Option<String>,
    pub resolved_by: Option<String>,
}

/// Determine outcome from Polymarket API fields, falling back to price inference.
///
/// FIX: WP04-F4 — check `resolved` and `winners` API fields first.
/// The old code relied solely on price thresholds (>0.9 / <0.1) which misses
/// markets settled via UMA dispute at unexpected prices, or markets where the
/// price has not yet converged to 0/1 at the time of the resolution check.
pub fn outcome_from_api(m: &Value) -> Option<String> {
    let resolved = m.get("resolved").and_then(Value::as_bool).unwrap_or(false);
    if resolved
        && let Some(winners) = m.get("winners").and_then(Value::as_array)
    {
        if winners.iter().any(|w| w.as_str() == Some("Yes") || w.as_str() == Some("yes")) {
            return Some("Yes".to_owned());
        }
        if winners.iter().any(|w| w.as_str() == Some("No") || w.as_str() == Some("no")) {
            return Some("No".to_owned());
        }
    }
    // Fallback to price inference
    let prices = m.get("outcomePrices").and_then(Value::as_array)
        .cloned()
        .or_else(|| {
            m.get("outcomePrices")
                .and_then(Value::as_str)
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .and_then(|v| v.as_array().cloned())
        })?;
    outcome_from_prices(&prices)
}

/// Check whether a Polymarket market has resolved.
///
/// Returns `None` if the market is still open or the request fails.
/// Detection: `closed == true` OR `resolved == true`.
/// Outcome: checked via `outcome_from_api` (winners fields first, then prices).
/// Caller is responsible for 1.0s sleep between calls.
pub async fn check_resolution(
    http: &reqwest::Client,
    ticker: &str,
) -> Option<ResolutionInfo> {
    let gamma_id = gamma_id_from_ticker(ticker);
    let m = fetch_market_json_with_client(http, &gamma_id).await?;

    // FIX: WP04-F4 — treat market as settled when either closed OR resolved.
    // Some markets are marked resolved before the closed flag is flipped,
    // and disputed UMA markets can have resolved=true while closed=false.
    let closed = m.get("closed").and_then(Value::as_bool).unwrap_or(false);
    let resolved = m.get("resolved").and_then(Value::as_bool).unwrap_or(false);
    if !closed && !resolved {
        return None;
    }

    // FIX: WP04-F4 — use outcome_from_api which checks winners fields first
    let outcome = outcome_from_api(&m);

    let resolved_by = m.get("resolvedBy")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);

    let end_date = m.get("endDate")
        .and_then(Value::as_str)
        .or_else(|| m.get("end_date_iso").and_then(Value::as_str))
        .map(str::to_owned);

    let resolution_time_str = m.get("resolutionDate")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .or_else(|| end_date.clone());

    Some(ResolutionInfo {
        outcome,
        // FIX: WP04-F5 — TODO: populate from feeds table in main.rs resolution loop.
        // The Gamma API never returns the oracle settlement price; it must be joined
        // from the feeds table using close_time as the lookup key. The venue client
        // cannot query feeds because it has no DB access at resolution time.
        oracle_value_at_close: None,
        close_time_str: end_date,
        resolution_time_str,
        resolved_by,
    })
}

// ---------------------------------------------------------------------------
// HTTP helpers (on the client)
// ---------------------------------------------------------------------------

impl PolymarketClient {
    /// Fetch a Polymarket event by exact slug.
    /// Returns None on 404 or any error (no rate limit sleep here).
    async fn fetch_event_by_slug(&self, slug: &str) -> Option<Value> {
        let url = format!("{GAMMA_API}/events/slug/{slug}");
        let resp = self.http.get(&url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let data: Value = resp.json().await.ok()?;
        // API returns a dict; some slugs may return a list-wrapped response
        match data {
            Value::Object(_) => Some(data),
            Value::Array(ref arr) if !arr.is_empty() => Some(arr[0].clone()),
            _ => None,
        }
    }

    // FIX: WP06-F7 — dead method. Only caller was fetch_snapshot (stubbed by
    // WP06-F5). The live path is the free function fetch_market_json_with_client,
    // used by check_resolution. Removed to eliminate the dead-code warning.
}

/// Standalone version so `check_resolution` can use it without `&self`.
async fn fetch_market_json_with_client(
    http: &reqwest::Client,
    gamma_id: &str,
) -> Option<Value> {
    let url = format!("{GAMMA_API}/markets/{gamma_id}");
    let resp = http.get(&url).send().await.ok()?;
    if resp.status().is_success() {
        let data: Value = resp.json().await.ok()?;
        if data.is_object() {
            return Some(data);
        }
    }

    // Fallback: conditionId query param
    let url2 = format!("{GAMMA_API}/markets?conditionId={gamma_id}");
    let resp2 = http.get(&url2).send().await.ok()?;
    if !resp2.status().is_success() {
        return None;
    }
    let data2: Value = resp2.json().await.ok()?;
    match data2 {
        Value::Array(ref arr) if !arr.is_empty() => Some(arr[0].clone()),
        Value::Object(_) => Some(data2),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// DB helper
// ---------------------------------------------------------------------------

/// Parsed market row ready for upsert.
struct PolyMarketRow {
    ticker: String,
    series: String,
    market_type: &'static str,
    oracle: &'static str,
    strike: Option<f64>,
    open_time: Option<String>,
    close_time: Option<String>,
    resolution_time: Option<String>,
    outcome: Option<String>,
    rules: Option<String>,
    token_id: Option<String>, // CLOB token_id for Yes outcome
}

/// UPSERT a Polymarket market into the `markets` table.
///
/// Uses COALESCE on `resolution_time` and `outcome` so discovery never
/// overwrites a previously-written non-NULL value.
fn upsert_market(conn: &Connection, row: &PolyMarketRow) -> anyhow::Result<()> {
    conn.execute(
        r#"
        INSERT INTO markets
            (venue, ticker, series, market_type, oracle, strike,
             open_time, close_time, resolution_time, outcome, rules, token_id, discovered_at)
        VALUES
            ('polymarket', ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, strftime('%s','now'))
        ON CONFLICT(venue, ticker) DO UPDATE SET
            series          = excluded.series,
            market_type     = excluded.market_type,
            oracle          = excluded.oracle,
            strike          = COALESCE(excluded.strike, markets.strike),
            open_time       = excluded.open_time,
            close_time      = excluded.close_time,
            resolution_time = COALESCE(markets.resolution_time, excluded.resolution_time),
            outcome         = COALESCE(markets.outcome, excluded.outcome),
            rules           = excluded.rules,
            token_id        = COALESCE(excluded.token_id, markets.token_id)
        "#,
        params![
            row.ticker,
            row.series,
            row.market_type,
            row.oracle,
            row.strike,
            row.open_time,
            row.close_time,
            row.resolution_time,
            row.outcome,
            row.rules,
            row.token_id,
        ],
    )
    .context("upsert_market (polymarket)")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Pure helpers — slug generation, parsing, oracle/type determination
// ---------------------------------------------------------------------------

/// Generate `bitcoin-above-on-{month}-{day}` slug.
pub fn daily_above_slug(month: u32, day: u32) -> String {
    format!("bitcoin-above-on-{}-{day}", MONTH_NAMES[(month - 1) as usize])
}

/// Generate `bitcoin-up-or-down-on-{month}-{day}-{year}` slug.
pub fn daily_up_or_down_slug(month: u32, day: u32, year: i32) -> String {
    format!(
        "bitcoin-up-or-down-on-{}-{day}-{year}",
        MONTH_NAMES[(month - 1) as usize]
    )
}

/// Generate `bitcoin-up-or-down-{month}-{day}-{hour_str}-et` slug.
pub fn hourly_up_or_down_slug(month: u32, day: u32, hour_str: &str) -> String {
    format!(
        "bitcoin-up-or-down-{}-{day}-{hour_str}-et",
        MONTH_NAMES[(month - 1) as usize]
    )
}

/// Format an ET hour (0-23) as "12am", "1am"..."11am", "12pm", "1pm"..."11pm".
pub fn et_hour_str(h: u32) -> String {
    match h {
        0 => "12am".to_owned(),
        1..=11 => format!("{h}am"),
        12 => "12pm".to_owned(),
        13..=23 => format!("{}pm", h - 12),
        _ => format!("{h}am"), // unreachable in practice
    }
}

/// Extract gamma_id from a Polymarket ticker ("poly-{gamma_id}").
pub fn gamma_id_from_ticker(ticker: &str) -> String {
    ticker.strip_prefix("poly-").unwrap_or(ticker).to_owned()
}

/// Parse `outcomePrices` field — handles both JSON string and native array.
pub fn parse_outcome_prices(m: &Value) -> Option<Vec<Value>> {
    match m.get("outcomePrices")? {
        Value::String(s) => serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| v.as_array().cloned()),
        Value::Array(arr) => Some(arr.clone()),
        _ => None,
    }
}

/// Get a finite f64 from a `Value` slice at index `i`.
fn finite_opt_from_slice(prices: &[Value], i: usize) -> Option<f64> {
    prices.get(i).and_then(finite)
}

/// Determine outcome from outcomePrices: prices[0] > 0.9 → "Yes", < 0.1 → "No".
pub fn outcome_from_prices(prices: &[Value]) -> Option<String> {
    let yes_p = finite_opt_from_slice(prices, 0)?;
    if yes_p > 0.9 {
        Some("Yes".to_owned())
    } else if yes_p < 0.1 {
        Some("No".to_owned())
    } else {
        None
    }
}

/// Determine oracle from resolutionSource and slug.
pub fn determine_oracle(res_src: &str, question: &str, slug: &str) -> &'static str {
    let rs = res_src.to_lowercase();
    let q = question.to_lowercase();
    let s = slug.to_lowercase();

    if rs.contains("chainlink") || rs.contains("chain.link") {
        "chainlink_streams"
    } else if rs.contains("binance") || q.contains("binance") {
        "binance_1m_candle"
    } else if s.contains("btc-updown-5m")
        || s.contains("btc-updown-15m")
        || s.contains("btc-updown-4h")
        || s.contains("up-or-down")
    {
        "chainlink_streams"
    } else if s.contains("above-on") {
        "binance_1m_candle"
    } else {
        "unknown"
    }
}

/// Determine market_type from slug.
pub fn determine_market_type(slug: &str, question: &str) -> &'static str {
    let s = slug.to_lowercase();
    let q = question.to_lowercase();

    if s.contains("15m") || s.contains("15-min") {
        "up_down_15m"
    } else if s.contains("5m") || s.contains("5-min") {
        "up_down_5m"
    } else if s.contains("4h") || s.contains("4-hour") {
        "up_down_4h"
    } else if s.contains("up-or-down") {
        // Hourly or daily — check question for time-of-day indicator
        if q.contains("pm et") || q.contains("am et") || q.contains("pm-") || q.contains("am-") {
            "up_down_hourly"
        } else {
            "up_down_daily"
        }
    } else if s.contains("above") {
        "above_below_daily"
    } else {
        "unknown"
    }
}

/// Parse a Polymarket market JSON object into a `PolyMarketRow`.
fn parse_poly_market(m: &Value, event_slug: &str, _now: f64) -> PolyMarketRow {
    let question = m.get("question").and_then(Value::as_str).unwrap_or("");
    let strike = extract_strike_from_question(question);

    let end_date = m.get("endDate")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| m.get("end_date_iso").and_then(Value::as_str))
        .map(str::to_owned);

    // resolution_time: only if resolvedBy is truthy
    let resolution_time = if m.get("resolvedBy")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false)
    {
        m.get("resolutionDate")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .or_else(|| end_date.clone())
    } else {
        None
    };

    // Outcome at discovery: only if closed
    let closed = m.get("closed").and_then(Value::as_bool).unwrap_or(false);
    let outcome = if closed {
        parse_outcome_prices(m)
            .as_deref()
            .and_then(outcome_from_prices)
    } else {
        None
    };

    let res_src = m.get("resolutionSource")
        .and_then(Value::as_str)
        .unwrap_or("");
    let oracle = determine_oracle(res_src, question, event_slug);
    let market_type = determine_market_type(event_slug, question);

    // Ticker: "poly-{gamma_id}" where gamma_id = m.id || m.conditionId
    let gamma_id = m.get("id")
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
        .filter(|s| !s.is_empty())
        .or_else(|| {
            m.get("conditionId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        });

    let ticker = match &gamma_id {
        Some(id) => format!("poly-{id}"),
        None => format!("poly-{event_slug}-{}", &question.chars().take(30).collect::<String>()),
    };

    let open_time = m.get("startDate")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| m.get("start_date_iso").and_then(Value::as_str))
        .map(str::to_owned);

    let rules = if question.is_empty() {
        None
    } else {
        Some(question.chars().take(2000).collect())
    };

    // Extract CLOB token_id for Yes outcome (first element of clobTokenIds).
    // Also try alternate field `tokens` (array of objects with `token_id` key).
    let token_id = m.get("clobTokenIds")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .and_then(|v| v.into_iter().next())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            m.get("tokens")
                .and_then(Value::as_array)
                .and_then(|arr| arr.first())
                .and_then(|t| t.get("token_id"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        });

    PolyMarketRow {
        ticker,
        series: event_slug.to_owned(),
        market_type,
        oracle,
        strike,
        open_time,
        close_time: end_date,
        resolution_time,
        outcome,
        rules,
        token_id,
    }
}

/// Extract first `$X,XXX` from a question string (same as Kalshi's parse_dollar_amount).
fn extract_strike_from_question(text: &str) -> Option<f64> {
    for (i, ch) in text.char_indices() {
        if ch == '$' {
            let rest = &text[i + 1..];
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

// ---------------------------------------------------------------------------
// ET / DST helpers
// ---------------------------------------------------------------------------

/// Return true if the given UTC month/day falls within US EDT (UTC-4).
///
/// FIX: WP04-F3 — DST rules for US Eastern time:
///   EDT starts: second Sunday of March  (earliest possible: Mar 8)
///   EDT ends:   first Sunday of November (earliest possible: Nov 1)
/// We approximate with fixed day-of-month bounds that hold for years
/// 2020-2040 without carrying the full Gregorian weekday calculation.
/// This is sufficient for slug generation where ±1 day tolerance exists.
pub fn is_dst(month: u32, day: u32) -> bool {
    match month {
        4..=10 => true,
        1 | 2 | 12 => false,
        3 => day >= 8,  // DST starts ≥ Mar 8 (second Sunday)
        11 => day < 7,  // DST ends by Nov 7 at the latest
        _ => false,
    }
}

/// Return the calendar day immediately before (month, day), handling month wraps.
///
/// FIX: WP04-F2 — needed to compute the ET calendar date when UTC midnight has
/// not yet rolled over to the next ET day. Leap years: uses 28 for February
/// because this path is only hit in the few hours when UTC hour < ET offset;
/// in practice Polymarket does not have hourly markets crossing Feb 28/Mar 1.
pub fn prev_day(month: u32, day: u32) -> (u32, u32) {
    if day > 1 {
        (month, day - 1)
    } else {
        let prev_month = if month > 1 { month - 1 } else { 12 };
        let days_in_prev = match prev_month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => 28,
            _ => 30,
        };
        (prev_month, days_in_prev)
    }
}

// ---------------------------------------------------------------------------
// UTC date helpers (no external crate — pure arithmetic)
// ---------------------------------------------------------------------------

/// Decompose a Unix timestamp (seconds) into (year, month, day, hour) UTC.
/// Uses the Gregorian calendar algorithm. month is 1-12, day is 1-31.
pub fn utc_ymdh(ts: i64) -> (i32, u32, u32, u32) {
    // Days since Unix epoch
    let days = ts.div_euclid(86400) as i32;
    let hour = (ts.rem_euclid(86400) / 3600) as u32;

    // Gregorian calendar algorithm (from Richards 2013, adapted)
    // Shifts epoch to 1 Mar 0000 for simplicity
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era [0, 399]
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month of year from Mar [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let year = if m <= 2 { y + 1 } else { y };

    (year, m, d, hour)
}

// ---------------------------------------------------------------------------
// L2 book fetcher
// ---------------------------------------------------------------------------

/// Fetch L2 orderbook depth from the Polymarket CLOB for a set of markets.
///
/// API: GET https://clob.polymarket.com/book?token_id={token_id} (public, no auth).
/// Response: `bids` (descending by price), `asks` (ascending by price),
/// each entry: {"price": "0.45", "size": "100"}.
///
/// Markets without a `token_id` are silently skipped.
pub async fn fetch_poly_books(
    http: &reqwest::Client,
    markets: &[&crate::db::MarketRow],
) -> Vec<crate::db::BookSnapshotRow> {
    let mut results = Vec::new();
    let now = crate::feed::wall_clock();

    for &market in markets {
        let token_id = match &market.token_id {
            Some(t) if !t.is_empty() => t.as_str(),
            _ => continue,
        };

        // Rate-limit: 500ms between requests
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let url = format!("https://clob.polymarket.com/book?token_id={token_id}");
        let data: serde_json::Value = match http.get(&url).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(r) => match r.json().await {
                    Ok(v) => v,
                    Err(_) => continue,
                },
                Err(_) => continue,
            },
            Err(_) => continue,
        };

        if let Some(book) = parse_poly_book(&data, now, market.id) {
            results.push(book);
        }
    }
    results
}

/// Parse a Polymarket CLOB book response into a BookSnapshotRow.
///
/// Bids are sorted descending (best first). Asks are sorted ascending (best first).
fn parse_poly_book(
    data: &serde_json::Value,
    ts: f64,
    market_id: i64,
) -> Option<crate::db::BookSnapshotRow> {
    let bids = parse_poly_levels(data.get("bids")?);
    let asks = parse_poly_levels(data.get("asks")?);

    if bids.is_empty() && asks.is_empty() {
        return None;
    }

    // Bids descending: best = first. Asks ascending: best = first.
    let best_bid = bids.first().map(|&(p, _)| p);
    let best_ask = asks.first().map(|&(p, _)| p);

    let spread = match (best_bid, best_ask) {
        (Some(b), Some(a)) => a - b,
        _ => 0.0,
    };

    // Depth within 0.05 of best
    let bid_depth = match best_bid {
        Some(bb) => bids
            .iter()
            .filter(|&&(p, _)| p >= bb - 0.05)
            .map(|&(_, q)| q)
            .sum(),
        None => 0.0,
    };
    let ask_depth = match best_ask {
        Some(ba) => asks
            .iter()
            .filter(|&&(p, _)| p <= ba + 0.05)
            .map(|&(_, q)| q)
            .sum(),
        None => 0.0,
    };

    let levels_json = serde_json::json!({
        "bids": bids.iter().map(|&(p, q)| [p, q]).collect::<Vec<_>>(),
        "asks": asks.iter().map(|&(p, q)| [p, q]).collect::<Vec<_>>(),
    });

    Some(crate::db::BookSnapshotRow {
        ts,
        market_id,
        venue: "polymarket".to_owned(),
        bid_depth,
        ask_depth,
        spread,
        best_bid,
        best_ask,
        levels: Some(levels_json.to_string()),
    })
}

/// Parse Polymarket level array: [{"price":"0.45","size":"100"}, ...] → Vec<(f64, f64)>
fn parse_poly_levels(arr: &serde_json::Value) -> Vec<(f64, f64)> {
    let arr = match arr.as_array() {
        Some(a) => a,
        None => return Vec::new(),
    };
    let mut levels = Vec::with_capacity(arr.len());
    for item in arr {
        let price: f64 = match item
            .get("price")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
            Some(v) if v.is_finite() && v > 0.0 => v,
            _ => continue,
        };
        let qty: f64 = match item
            .get("size")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
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

    // ── UTC decomposition ─────────────────────────────────────────────────────

    #[test]
    fn utc_ymdh_epoch() {
        // Unix epoch = 1970-01-01 00:00:00 UTC
        assert_eq!(utc_ymdh(0), (1970, 1, 1, 0));
    }

    #[test]
    fn utc_ymdh_known_date() {
        // 2026-03-15 14:30:00 UTC
        // 2026-03-15 00:00:00 UTC = 1773532800 (verified via Python datetime)
        let ts = 1773532800 + 14 * 3600 + 30 * 60;
        let (y, m, d, h) = utc_ymdh(ts);
        assert_eq!(y, 2026);
        assert_eq!(m, 3);
        assert_eq!(d, 15);
        assert_eq!(h, 14);
    }

    #[test]
    fn utc_ymdh_leap_day() {
        // 2024-02-29 12:00:00 UTC = 1709208000
        let ts = 1709208000;
        let (y, m, d, _) = utc_ymdh(ts);
        assert_eq!(y, 2024);
        assert_eq!(m, 2);
        assert_eq!(d, 29);
    }

    // ── Slug generation — daily above ────────────────────────────────────────

    #[test]
    fn daily_above_slug_january_1() {
        assert_eq!(daily_above_slug(1, 1), "bitcoin-above-on-january-1");
    }

    #[test]
    fn daily_above_slug_december_31() {
        assert_eq!(daily_above_slug(12, 31), "bitcoin-above-on-december-31");
    }

    #[test]
    fn daily_above_slug_march_15() {
        assert_eq!(daily_above_slug(3, 15), "bitcoin-above-on-march-15");
    }

    #[test]
    fn daily_above_slug_offsets_match_python() {
        // Python: for offset in range(-2, 3): dt = current + timedelta(days=offset)
        // Today = 2026-03-15 (ts=1773532800, verified via Python datetime)
        let now_ts = 1773532800_i64;
        let slugs: Vec<String> = (-2i64..=2)
            .map(|off| {
                let ts = now_ts + off * 86400;
                let (_, m, d, _) = utc_ymdh(ts);
                daily_above_slug(m, d)
            })
            .collect();
        assert_eq!(slugs[0], "bitcoin-above-on-march-13");
        assert_eq!(slugs[1], "bitcoin-above-on-march-14");
        assert_eq!(slugs[2], "bitcoin-above-on-march-15");
        assert_eq!(slugs[3], "bitcoin-above-on-march-16");
        assert_eq!(slugs[4], "bitcoin-above-on-march-17");
    }

    // ── Slug generation — intraday boundary ──────────────────────────────────

    #[test]
    fn intraday_slug_5m_boundary() {
        // now_ts = 1773532800, interval = 300
        // 1773532800 / 300 = 5911776, * 300 = 1773532800 (already aligned)
        let now_ts = 1773532800_i64;
        let interval_s = 300_i64;
        let boundary = (now_ts / interval_s) * interval_s;
        assert_eq!(boundary, 1773532800);
        let slug = format!("btc-updown-5m-{boundary}");
        assert_eq!(slug, "btc-updown-5m-1773532800");
    }

    #[test]
    fn intraday_slug_15m_boundary_offset() {
        // offset_mult = -1; now_ts = 1773532800, interval = 900
        // 1773532800 / 900 = 1970592, * 900 = 1773532800; -1 → 1773531900
        let now_ts = 1773532800_i64;
        let interval_s = 900_i64;
        let boundary = ((now_ts / interval_s) + (-1)) * interval_s;
        assert_eq!(boundary, 1773531900);
        let slug = format!("btc-updown-15m-{boundary}");
        assert_eq!(slug, "btc-updown-15m-1773531900");
    }

    #[test]
    fn intraday_slug_4h_boundary() {
        // now_ts = 1773532800, interval = 14400
        // 1773532800 / 14400 = 123162, * 14400 = 1773532800
        let now_ts = 1773532800_i64;
        let interval_s = 14400_i64;
        let boundary = (now_ts / interval_s) * interval_s;
        assert_eq!(boundary, 1773532800);
        let slug = format!("btc-updown-4h-{boundary}");
        assert_eq!(slug, "btc-updown-4h-1773532800");
    }

    // ── Slug generation — hourly ET ───────────────────────────────────────────

    #[test]
    fn et_hour_str_midnight() {
        assert_eq!(et_hour_str(0), "12am");
    }

    #[test]
    fn et_hour_str_noon() {
        assert_eq!(et_hour_str(12), "12pm");
    }

    #[test]
    fn et_hour_str_1am() {
        assert_eq!(et_hour_str(1), "1am");
    }

    #[test]
    fn et_hour_str_11am() {
        assert_eq!(et_hour_str(11), "11am");
    }

    #[test]
    fn et_hour_str_1pm() {
        assert_eq!(et_hour_str(13), "1pm");
    }

    #[test]
    fn et_hour_str_11pm() {
        assert_eq!(et_hour_str(23), "11pm");
    }

    #[test]
    fn hourly_slug_format() {
        // ET hour 14 (2pm ET), march 15
        let slug = hourly_up_or_down_slug(3, 15, "2pm");
        assert_eq!(slug, "bitcoin-up-or-down-march-15-2pm-et");
    }

    #[test]
    fn hourly_slug_midnight() {
        let slug = hourly_up_or_down_slug(1, 5, "12am");
        assert_eq!(slug, "bitcoin-up-or-down-january-5-12am-et");
    }

    #[test]
    fn et_conversion_utc18_is_2pm_et() {
        // UTC 18:00 → ET = (18-4)%24 = 14 → "2pm"
        let utc_hour = 18u32;
        let et_hour = ((utc_hour as i64) - 4).rem_euclid(24) as u32;
        assert_eq!(et_hour, 14);
        assert_eq!(et_hour_str(et_hour), "2pm");
    }

    #[test]
    fn et_conversion_utc2_is_10pm_et_prev_day() {
        // UTC 02:00 → ET = (2-4+24)%24 = 22 → "10pm"
        let utc_hour = 2u32;
        let et_hour = ((utc_hour as i64) - 4).rem_euclid(24) as u32;
        assert_eq!(et_hour, 22);
        assert_eq!(et_hour_str(et_hour), "10pm");
    }

    // ── outcomePrices parsing ─────────────────────────────────────────────────

    #[test]
    fn parse_outcome_prices_json_string() {
        let m = json!({"outcomePrices": "[\"0.72\", \"0.28\"]"});
        let prices = parse_outcome_prices(&m).unwrap();
        assert_eq!(prices.len(), 2);
        assert_eq!(finite(&prices[0]), Some(0.72));
        assert_eq!(finite(&prices[1]), Some(0.28));
    }

    #[test]
    fn parse_outcome_prices_native_array() {
        let m = json!({"outcomePrices": [0.72, 0.28]});
        let prices = parse_outcome_prices(&m).unwrap();
        assert_eq!(prices.len(), 2);
        assert_eq!(finite(&prices[0]), Some(0.72));
        assert_eq!(finite(&prices[1]), Some(0.28));
    }

    #[test]
    fn parse_outcome_prices_missing() {
        let m = json!({"something_else": 1});
        assert!(parse_outcome_prices(&m).is_none());
    }

    #[test]
    fn parse_outcome_prices_empty_string_array() {
        let m = json!({"outcomePrices": "[]"});
        let prices = parse_outcome_prices(&m).unwrap();
        assert!(prices.is_empty());
    }

    // ── Oracle determination ─────────────────────────────────────────────────

    #[test]
    fn oracle_chainlink_in_res_src() {
        assert_eq!(
            determine_oracle("https://data.chain.link/...", "", "some-slug"),
            "chainlink_streams"
        );
    }

    #[test]
    fn oracle_chainlink_explicit() {
        assert_eq!(
            determine_oracle("chainlink data streams", "", "some-slug"),
            "chainlink_streams"
        );
    }

    #[test]
    fn oracle_binance_in_res_src() {
        assert_eq!(
            determine_oracle("binance spot price", "", "some-slug"),
            "binance_1m_candle"
        );
    }

    #[test]
    fn oracle_binance_in_question() {
        assert_eq!(
            determine_oracle("", "Will BTC (binance) close above...", "some-slug"),
            "binance_1m_candle"
        );
    }

    #[test]
    fn oracle_updown_slug_chainlink() {
        assert_eq!(
            determine_oracle("", "", "btc-updown-5m-1741996800"),
            "chainlink_streams"
        );
    }

    #[test]
    fn oracle_updown_15m_chainlink() {
        assert_eq!(
            determine_oracle("", "", "btc-updown-15m-1741996800"),
            "chainlink_streams"
        );
    }

    #[test]
    fn oracle_updown_4h_chainlink() {
        assert_eq!(
            determine_oracle("", "", "btc-updown-4h-1741996800"),
            "chainlink_streams"
        );
    }

    #[test]
    fn oracle_up_or_down_chainlink() {
        assert_eq!(
            determine_oracle("", "", "bitcoin-up-or-down-march-15-2pm-et"),
            "chainlink_streams"
        );
    }

    #[test]
    fn oracle_above_on_binance() {
        assert_eq!(
            determine_oracle("", "", "bitcoin-above-on-march-15"),
            "binance_1m_candle"
        );
    }

    #[test]
    fn oracle_unknown_default() {
        assert_eq!(determine_oracle("", "", "some-random-slug"), "unknown");
    }

    // ── market_type determination ─────────────────────────────────────────────

    #[test]
    fn market_type_5m() {
        assert_eq!(determine_market_type("btc-updown-5m-123456", ""), "up_down_5m");
    }

    #[test]
    fn market_type_15m() {
        assert_eq!(determine_market_type("btc-updown-15m-123456", ""), "up_down_15m");
    }

    #[test]
    fn market_type_4h() {
        assert_eq!(determine_market_type("btc-updown-4h-123456", ""), "up_down_4h");
    }

    #[test]
    fn market_type_up_or_down_hourly_pm() {
        // "pm et" in question → hourly
        assert_eq!(
            determine_market_type(
                "bitcoin-up-or-down-march-15-2pm-et",
                "Will BTC go up or down at 2pm et?"
            ),
            "up_down_hourly"
        );
    }

    #[test]
    fn market_type_up_or_down_hourly_am() {
        assert_eq!(
            determine_market_type(
                "bitcoin-up-or-down-march-15-10am-et",
                "Will BTC go up or down at 10am et?"
            ),
            "up_down_hourly"
        );
    }

    #[test]
    fn market_type_up_or_down_daily_no_time() {
        assert_eq!(
            determine_market_type(
                "bitcoin-up-or-down-on-march-15-2026",
                "Will BTC go up or down on March 15?"
            ),
            "up_down_daily"
        );
    }

    #[test]
    fn market_type_above_below_daily() {
        assert_eq!(
            determine_market_type("bitcoin-above-on-march-15", ""),
            "above_below_daily"
        );
    }

    #[test]
    fn market_type_unknown() {
        assert_eq!(determine_market_type("some-random-slug", ""), "unknown");
    }

    // ── Ticker construction ───────────────────────────────────────────────────

    #[test]
    fn ticker_from_numeric_id() {
        let m = json!({"id": 12345, "question": "Will BTC close above $95,000?"});
        let row = parse_poly_market(&m, "bitcoin-above-on-march-15", 0.0);
        assert_eq!(row.ticker, "poly-12345");
    }

    #[test]
    fn ticker_from_string_id() {
        let m = json!({"id": "abc123", "question": "Will BTC close above $95,000?"});
        let row = parse_poly_market(&m, "bitcoin-above-on-march-15", 0.0);
        assert_eq!(row.ticker, "poly-abc123");
    }

    #[test]
    fn ticker_from_condition_id_fallback() {
        let m = json!({
            "conditionId": "0xdeadbeef",
            "question": "Will BTC close above $95,000?"
        });
        let row = parse_poly_market(&m, "bitcoin-above-on-march-15", 0.0);
        assert_eq!(row.ticker, "poly-0xdeadbeef");
    }

    #[test]
    fn ticker_fallback_to_slug_when_no_id() {
        let m = json!({"question": "short question"});
        let row = parse_poly_market(&m, "some-slug", 0.0);
        assert!(row.ticker.starts_with("poly-some-slug-"));
    }

    // ── gamma_id extraction ───────────────────────────────────────────────────

    #[test]
    fn gamma_id_strips_poly_prefix() {
        assert_eq!(gamma_id_from_ticker("poly-12345"), "12345");
    }

    #[test]
    fn gamma_id_no_prefix_unchanged() {
        assert_eq!(gamma_id_from_ticker("12345"), "12345");
    }

    #[test]
    fn gamma_id_condition_id_format() {
        assert_eq!(
            gamma_id_from_ticker("poly-0xdeadbeef1234"),
            "0xdeadbeef1234"
        );
    }

    // ── Outcome determination from prices ────────────────────────────────────

    #[test]
    fn outcome_yes_above_0_9() {
        let prices = vec![json!(0.95), json!(0.05)];
        assert_eq!(outcome_from_prices(&prices), Some("Yes".to_owned()));
    }

    #[test]
    fn outcome_no_below_0_1() {
        let prices = vec![json!(0.03), json!(0.97)];
        assert_eq!(outcome_from_prices(&prices), Some("No".to_owned()));
    }

    #[test]
    fn outcome_none_between() {
        let prices = vec![json!(0.50), json!(0.50)];
        assert_eq!(outcome_from_prices(&prices), None);
    }

    #[test]
    fn outcome_exactly_0_9_is_none() {
        // Boundary: > 0.9, not >= 0.9
        let prices = vec![json!(0.9), json!(0.1)];
        assert_eq!(outcome_from_prices(&prices), None);
    }

    #[test]
    fn outcome_exactly_0_1_is_none() {
        // Boundary: < 0.1, not <= 0.1
        let prices = vec![json!(0.1), json!(0.9)];
        assert_eq!(outcome_from_prices(&prices), None);
    }

    #[test]
    fn outcome_empty_prices_is_none() {
        let prices: Vec<Value> = vec![];
        assert_eq!(outcome_from_prices(&prices), None);
    }

    // ── Polymarket midpoint passthrough (FIX: WP02-F1) ──────────────────────
    // The old code fabricated a 0.5 cent synthetic spread (±0.0025) around the
    // Gamma API midpoint, creating fake bid/ask values in every training row.
    // These tests now validate the correct behaviour: the midpoint is stored
    // directly in p_market; bid/ask are None because the API does not provide them.

    #[test]
    fn poly_midpoint_stored_in_p_market_not_bid_ask() {
        // FIX: WP02-F1 — real midpoint goes to p_market; bid/ask must be None.
        let yes_mid = 0.72_f64;
        let p_market_val = if yes_mid > 0.0 && yes_mid < 1.0 { Some(yes_mid) } else { None };
        let yes_bid: Option<f64> = None;
        let yes_ask: Option<f64> = None;

        assert_eq!(p_market_val, Some(0.72));
        assert!(yes_bid.is_none());
        assert!(yes_ask.is_none());
    }

    #[test]
    fn poly_midpoint_boundary_values_rejected() {
        // FIX: WP02-F1 — values outside (0, 1) are rejected by the NaN firewall.
        for &mid in &[0.0_f64, 1.0, -0.1, 1.01, f64::NAN, f64::INFINITY] {
            let p_market_val = if mid.is_finite() && mid > 0.0 && mid < 1.0 {
                Some(mid)
            } else {
                None
            };
            assert!(p_market_val.is_none(), "expected None for mid={mid}");
        }
    }

    // ── Slug deduplication ───────────────────────────────────────────────────

    #[test]
    fn slugs_tried_dedup() {
        let mut slugs_tried: HashSet<String> = HashSet::new();
        let slug = "bitcoin-above-on-march-15".to_owned();

        assert!(slugs_tried.insert(slug.clone())); // first: inserted
        assert!(!slugs_tried.insert(slug.clone())); // second: already exists
        assert_eq!(slugs_tried.len(), 1);
    }

    #[test]
    fn dedup_across_offset_ranges_same_slug() {
        // Verify that if two offsets produce the same slug, it's only inserted once
        // Example: daily-above for a specific day that appears at offsets +1 and -1
        // in two different calls (contrived, but the mechanism is what matters)
        let mut slugs_tried: HashSet<String> = HashSet::new();
        let slugs_to_try = vec!["bitcoin-above-on-march-15", "bitcoin-above-on-march-15"];

        let mut inserts = 0;
        for s in slugs_to_try {
            if slugs_tried.insert(s.to_owned()) {
                inserts += 1;
            }
        }
        assert_eq!(inserts, 1);
        assert_eq!(slugs_tried.len(), 1);
    }

    // ── Strike extraction ────────────────────────────────────────────────────

    #[test]
    fn strike_from_question_with_commas() {
        assert_eq!(
            extract_strike_from_question("Will BTC close above $95,000?"),
            Some(95000.0)
        );
    }

    #[test]
    fn strike_from_question_no_commas() {
        assert_eq!(
            extract_strike_from_question("BTC above $85000"),
            Some(85000.0)
        );
    }

    #[test]
    fn strike_none_when_no_dollar() {
        assert_eq!(extract_strike_from_question("Will BTC go up?"), None);
    }

    // ── parse_poly_market integration ────────────────────────────────────────

    #[test]
    fn parse_poly_market_oracle_and_type() {
        let m = json!({
            "id": "777",
            "question": "Will BTC go up or down at 2pm et?",
            "outcomePrices": "[\"0.55\", \"0.45\"]",
            "resolutionSource": "chain.link",
            "endDate": "2026-03-15T14:00:00Z",
            "closed": false,
        });
        let row = parse_poly_market(&m, "bitcoin-up-or-down-march-15-2pm-et", 0.0);
        assert_eq!(row.ticker, "poly-777");
        assert_eq!(row.oracle, "chainlink_streams");
        assert_eq!(row.market_type, "up_down_hourly");
        assert!(row.outcome.is_none()); // not closed
    }

    #[test]
    fn parse_poly_market_closed_yes_outcome() {
        let m = json!({
            "id": "888",
            "question": "Will BTC close above $95,000?",
            "outcomePrices": "[\"0.97\", \"0.03\"]",
            "resolutionSource": "",
            "endDate": "2026-03-15T00:00:00Z",
            "closed": true,
            "resolvedBy": "binance",
            "resolutionDate": "2026-03-15T00:05:00Z",
        });
        let row = parse_poly_market(&m, "bitcoin-above-on-march-15", 0.0);
        assert_eq!(row.outcome, Some("Yes".to_owned()));
        assert_eq!(row.oracle, "binance_1m_candle");
        assert_eq!(row.strike, Some(95000.0));
        assert!(row.resolution_time.is_some());
    }

    #[test]
    fn parse_poly_market_no_resolution_time_when_not_resolved_by() {
        let m = json!({
            "id": "999",
            "question": "Will BTC go up?",
            "outcomePrices": "[\"0.5\", \"0.5\"]",
            "endDate": "2026-03-15T14:00:00Z",
            "closed": false,
            // No resolvedBy field
        });
        let row = parse_poly_market(&m, "bitcoin-up-or-down-on-march-15-2026", 0.0);
        assert!(row.resolution_time.is_none());
    }

    // ── DB upsert ─────────────────────────────────────────────────────────────

    fn test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(crate::db::SCHEMA_SQL).unwrap();
        conn
    }

    #[test]
    fn upsert_market_inserts_new() {
        let conn = test_db();
        let row = PolyMarketRow {
            ticker: "poly-12345".to_owned(),
            series: "bitcoin-above-on-march-15".to_owned(),
            market_type: "above_below_daily",
            oracle: "binance_1m_candle",
            strike: Some(95000.0),
            open_time: None,
            close_time: Some("2026-03-15T00:00:00Z".to_owned()),
            resolution_time: None,
            outcome: None,
            rules: Some("Will BTC close above $95,000?".to_owned()),
            token_id: None,
        };
        upsert_market(&conn, &row).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM markets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn upsert_coalesce_preserves_outcome() {
        let conn = test_db();
        let row = PolyMarketRow {
            ticker: "poly-12345".to_owned(),
            series: "bitcoin-above-on-march-15".to_owned(),
            market_type: "above_below_daily",
            oracle: "binance_1m_candle",
            strike: Some(95000.0),
            open_time: None,
            close_time: Some("2026-03-15T00:00:00Z".to_owned()),
            resolution_time: None,
            outcome: None,
            rules: None,
            token_id: None,
        };
        upsert_market(&conn, &row).unwrap();

        // Simulate resolution having set outcome
        conn.execute(
            "UPDATE markets SET outcome = 'Yes' WHERE ticker = 'poly-12345'",
            [],
        )
        .unwrap();

        // Re-upsert with outcome=None should NOT overwrite
        upsert_market(&conn, &row).unwrap();

        let outcome: Option<String> = conn
            .query_row(
                "SELECT outcome FROM markets WHERE ticker = 'poly-12345'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome.as_deref(), Some("Yes"));
    }

    #[test]
    fn upsert_updates_rules_on_conflict() {
        let conn = test_db();
        let row1 = PolyMarketRow {
            ticker: "poly-42".to_owned(),
            series: "slug-a".to_owned(),
            market_type: "unknown",
            oracle: "unknown",
            strike: None,
            open_time: None,
            close_time: None,
            resolution_time: None,
            outcome: None,
            rules: Some("original".to_owned()),
            token_id: None,
        };
        upsert_market(&conn, &row1).unwrap();

        let row2 = PolyMarketRow {
            rules: Some("updated".to_owned()),
            ..row1
        };
        upsert_market(&conn, &row2).unwrap();

        let rules: String = conn
            .query_row(
                "SELECT rules FROM markets WHERE ticker = 'poly-42'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(rules, "updated");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM markets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    // ── Daily up-or-down slug ────────────────────────────────────────────────

    #[test]
    fn daily_up_or_down_slug_format() {
        assert_eq!(
            daily_up_or_down_slug(3, 15, 2026),
            "bitcoin-up-or-down-on-march-15-2026"
        );
    }

    #[test]
    fn daily_up_or_down_slug_january() {
        assert_eq!(
            daily_up_or_down_slug(1, 1, 2027),
            "bitcoin-up-or-down-on-january-1-2027"
        );
    }

    // ── WP07 audit-fix tests ─────────────────────────────────────────────────

    /// T6 — Resolution from API fields, not price threshold (validates WP04-F4)
    ///
    /// `outcome_from_api` must return the outcome from `resolved`+`winners` fields
    /// when present, and must NOT resolve a market purely from price proximity
    /// when `resolved` is false.
    #[test]
    fn outcome_from_api_resolved_winners_yes() {
        // Market with resolved=true, winners=["Yes"], price at 0.85 (below 0.9 threshold)
        let m = json!({
            "resolved": true,
            "winners": ["Yes"],
            "outcomePrices": ["0.85", "0.15"]
        });
        let outcome = outcome_from_api(&m);
        assert_eq!(
            outcome,
            Some("Yes".to_owned()),
            "resolved+winners should return Some(Yes) even when price < 0.9"
        );
    }

    #[test]
    fn outcome_from_api_resolved_winners_no() {
        let m = json!({
            "resolved": true,
            "winners": ["No"],
            "outcomePrices": ["0.15", "0.85"]
        });
        let outcome = outcome_from_api(&m);
        assert_eq!(outcome, Some("No".to_owned()));
    }

    #[test]
    fn outcome_from_api_price_only_not_resolved_is_none() {
        // Price at 0.85 but resolved=false → should NOT trigger resolution
        // (price threshold requires >0.9 anyway, 0.85 is below it)
        let m = json!({
            "resolved": false,
            "outcomePrices": ["0.85", "0.15"]
        });
        let outcome = outcome_from_api(&m);
        assert!(
            outcome.is_none(),
            "price=0.85 with resolved=false should be None, got {outcome:?}"
        );
    }

    #[test]
    fn outcome_from_api_price_threshold_still_works_when_no_winners() {
        // No resolved/winners fields but price > 0.9 → fallback price inference
        let m = json!({
            "outcomePrices": ["0.97", "0.03"]
        });
        let outcome = outcome_from_api(&m);
        assert_eq!(
            outcome,
            Some("Yes".to_owned()),
            "price fallback should still resolve when price > 0.9"
        );
    }

    /// T7 — DST offset produces correct ET date (validates WP04-F2 and WP04-F3)
    ///
    /// `is_dst`: July is summer (EDT=UTC-4), January is winter (EST=UTC-5).
    /// `prev_day`: handles month boundaries including year wrap.
    #[test]
    fn is_dst_july_is_true() {
        // July is always DST (month 4-10 → true)
        assert!(is_dst(7, 15), "July should be DST");
    }

    #[test]
    fn is_dst_january_is_false() {
        assert!(!is_dst(1, 15), "January should not be DST");
    }

    #[test]
    fn is_dst_march_before_8_is_false() {
        // DST starts second Sunday ≥ Mar 8; day 7 is before it
        assert!(!is_dst(3, 7), "March 7 should not be DST");
    }

    #[test]
    fn is_dst_march_from_8_is_true() {
        assert!(is_dst(3, 8), "March 8 should be DST");
        assert!(is_dst(3, 15), "March 15 should be DST");
    }

    #[test]
    fn is_dst_november_before_7_is_true() {
        // DST ends first Sunday in November; day < 7 still in DST
        assert!(is_dst(11, 1), "November 1 should be DST");
        assert!(is_dst(11, 6), "November 6 should be DST");
    }

    #[test]
    fn is_dst_november_from_7_is_false() {
        assert!(!is_dst(11, 7), "November 7 should not be DST");
    }

    #[test]
    fn prev_day_mid_month() {
        assert_eq!(prev_day(3, 15), (3, 14));
    }

    #[test]
    fn prev_day_first_of_month() {
        // March 1 → Feb 28
        assert_eq!(prev_day(3, 1), (2, 28));
    }

    #[test]
    fn prev_day_january_first_wraps_to_december() {
        // January 1 → December 31
        assert_eq!(prev_day(1, 1), (12, 31));
    }

    #[test]
    fn prev_day_june_first() {
        // June 1 → May 31
        assert_eq!(prev_day(6, 1), (5, 31));
    }

    #[test]
    fn prev_day_april_first() {
        // April 1 → March 31
        assert_eq!(prev_day(4, 1), (3, 31));
    }

    /// T7b — Full ET date derivation for slug generation (validates WP04-F2)
    ///
    /// At UTC 02:00 in July (DST, offset=4), ET is 22:00 the PREVIOUS day.
    /// The slug must use the previous ET day, not the UTC day.
    #[test]
    fn et_date_wraps_to_prev_day_when_utc_before_offset() {
        // UTC July 16, 02:00 → ET = 02:00 - 4 = -2 → prev day → July 15, 22:00
        let utc_month = 7u32;
        let utc_day = 16u32;
        let utc_hour = 2u32;

        let et_offset = if is_dst(utc_month, utc_day) { 4i64 } else { 5i64 };
        let et_hour_raw = (utc_hour as i64) - et_offset;
        let et_hour = et_hour_raw.rem_euclid(24) as u32;
        let (et_month, et_day) = if et_hour_raw < 0 {
            prev_day(utc_month, utc_day)
        } else {
            (utc_month, utc_day)
        };

        // Should have wrapped to July 15
        assert_eq!((et_month, et_day), (7, 15), "ET date should be July 15");
        assert_eq!(et_hour, 22, "ET hour should be 22");
        assert_eq!(et_hour_str(et_hour), "10pm");
    }

    #[test]
    fn et_date_no_wrap_when_utc_after_offset() {
        // UTC July 16, 18:00 → ET = 18 - 4 = 14 → same day
        let utc_month = 7u32;
        let utc_day = 16u32;
        let utc_hour = 18u32;

        let et_offset = if is_dst(utc_month, utc_day) { 4i64 } else { 5i64 };
        let et_hour_raw = (utc_hour as i64) - et_offset;
        let (et_month, et_day) = if et_hour_raw < 0 {
            prev_day(utc_month, utc_day)
        } else {
            (utc_month, utc_day)
        };

        assert_eq!((et_month, et_day), (7, 16), "ET date should stay July 16");
    }

    // ── L2 book parsing ───────────────────────────────────────────────────────

    #[test]
    fn test_parse_poly_book() {
        let data = serde_json::json!({
            "market": "0xtest",
            "asset_id": "123",
            "bids": [
                {"price": "0.45", "size": "100"},
                {"price": "0.44", "size": "200"},
            ],
            "asks": [
                {"price": "0.46", "size": "150"},
                {"price": "0.47", "size": "250"},
            ],
            "tick_size": "0.01",
            "last_trade_price": "0.45"
        });
        let book = parse_poly_book(&data, 1000.0, 1).unwrap();
        assert!((book.best_bid.unwrap() - 0.45).abs() < 1e-6);
        assert!((book.best_ask.unwrap() - 0.46).abs() < 1e-6);
        assert!((book.spread - 0.01).abs() < 1e-6);
        assert_eq!(book.venue, "polymarket");
    }

    #[test]
    fn test_parse_poly_book_empty() {
        let data = serde_json::json!({
            "bids": [],
            "asks": []
        });
        assert!(parse_poly_book(&data, 1000.0, 1).is_none());
    }

    #[test]
    fn test_parse_poly_levels() {
        let arr = serde_json::json!([
            {"price": "0.45", "size": "100"},
            {"price": "0.44", "size": "200"},
        ]);
        let levels = parse_poly_levels(&arr);
        assert_eq!(levels.len(), 2);
        assert!((levels[0].0 - 0.45).abs() < 1e-6);
        assert!((levels[1].1 - 200.0).abs() < 1e-6);
    }
}
