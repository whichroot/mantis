//! ChainlinkFeed — Chainlink BTC/USD Data Streams oracle (HTTP poller).
//!
//! Polls the Chainlink dashboard API every 2 seconds. This is an HTTP feed,
//! NOT a WebSocket. Uses `reqwest::Client` with a persistent connection pool.
//!
//! Edge cases from collect.py:
//! - URL: https://data.chain.link/api/live-data-engine-stream-data?feedId=...
//! - Method: GET, headers: User-Agent: "mantis-beacon/0.3", timeout: 8s
//! - Poll interval: 2 seconds
//! - Response deeply nested: data.allStreamValuesGenerics.nodes[]
//! - Only nodes with attributeName == "benchmark" are processed
//! - Value from valueNumeric field through pos() firewall
//! - Timestamp is ISO string in validAfterTs — parsed manually (no chrono)
//! - DEDUPLICATION: seen_ts set (rounded to 3 decimals), bounded at 500 entries
//!   with 120s TTL eviction when > 500 entries
//! - State updates: chainlink_value, chainlink_ts, chainlink_count
//!   (same fields as RTDS chainlink — last writer wins, intentional)
//! - Meta includes recv_lag_ms AND recv_ts (unique to this feed)
//! - Error handling: http error → retry_delay doubles up to 30s,
//!   resets to 2s only inside the `if data:` branch (on successful data fetch)
//!
//! Quirk: retry_delay resets only when a response with parseable data is
//! received, not on any successful HTTP call.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;

use super::{Feed, LiveState, pos};

const API_URL: &str = concat!(
    "https://data.chain.link/api/live-data-engine-stream-data",
    "?feedId=0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
    "&abiIndex=0&queryWindow=1m",
);

const USER_AGENT: &str = "mantis-beacon/0.3";

/// HTTP request timeout (seconds).
const HTTP_TIMEOUT_SECS: u64 = 8;

/// Normal poll interval (seconds).
const POLL_INTERVAL_SECS: f64 = 2.0;

/// Maximum retry delay (seconds).
const MAX_RETRY_DELAY_SECS: f64 = 30.0;

/// Maximum seen_ts set size before TTL eviction.
const DEDUP_MAX: usize = 500;

/// TTL for seen_ts entries (seconds). Entries older than this are evicted.
const DEDUP_TTL_SECS: f64 = 120.0;

// ---------------------------------------------------------------------------
// ISO 8601 / RFC 3339 timestamp parser (no chrono dependency)
// ---------------------------------------------------------------------------

/// Parse an ISO 8601 / RFC 3339 timestamp string to Unix seconds (f64).
///
/// Handles the following formats:
/// - `"2024-01-15T12:34:56Z"`
/// - `"2024-01-15T12:34:56.789Z"`
/// - `"2024-01-15T12:34:56.789+00:00"`
/// - `"2024-01-15T12:34:56+05:30"` (arbitrary UTC offset)
///
/// Returns `None` on any parse error.
pub fn parse_iso_ts(s: &str) -> Option<f64> {
    // Split at 'T' separator between date and time.
    let t_pos = s.find('T')?;
    let date_part = &s[..t_pos];
    let rest = &s[t_pos + 1..];

    // Parse date: YYYY-MM-DD
    let date_parts: Vec<&str> = date_part.splitn(3, '-').collect();
    if date_parts.len() != 3 {
        return None;
    }
    let year: i64 = date_parts[0].parse().ok()?;
    let month: i64 = date_parts[1].parse().ok()?;
    let day: i64 = date_parts[2].parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Split time from timezone. Timezone starts at 'Z', '+', or '-' AFTER
    // the time portion (after position 0 to skip the sign of hours).
    // Format: HH:MM:SS[.sss][Z|(+/-)HH:MM]
    let (time_part, offset_secs) = parse_time_and_offset(rest)?;

    // Parse time: HH:MM:SS[.fractional]
    let colon1 = time_part.find(':')?;
    let after_colon1 = &time_part[colon1 + 1..];
    let colon2 = colon1 + 1 + after_colon1.find(':')?;

    let hh: i64 = time_part[..colon1].parse().ok()?;
    let mm: i64 = time_part[colon1 + 1..colon2].parse().ok()?;

    // Seconds may include fractional part
    let ss_str = &time_part[colon2 + 1..];
    let ss_f: f64 = ss_str.parse().ok()?;
    let ss_whole = ss_f.floor() as i64;
    let ss_frac = ss_f - ss_f.floor();

    if !(0..=23).contains(&hh) || !(0..=59).contains(&mm) || !(0..=60).contains(&ss_whole) {
        return None;
    }

    // Compute days since Unix epoch (1970-01-01).
    let epoch_days = days_since_epoch(year, month, day)?;

    let unix_s = epoch_days as f64 * 86400.0
        + hh as f64 * 3600.0
        + mm as f64 * 60.0
        + ss_whole as f64
        + ss_frac
        - offset_secs;

    Some(unix_s)
}

/// Parse the time-and-optional-timezone portion of an ISO timestamp.
/// Returns `(time_str, offset_seconds_east)`.
fn parse_time_and_offset(s: &str) -> Option<(&str, f64)> {
    // Look for 'Z' suffix.
    if let Some(stripped) = s.strip_suffix('Z') {
        return Some((stripped, 0.0));
    }

    // Look for '+' or '-' after position 0 (to distinguish sign of first char).
    // The offset sign starts AFTER the HH:MM:SS[.sss] portion.
    // Minimum time part is 8 chars ("HH:MM:SS"), so search from index 6.
    let search_start = s.len().min(6);
    let offset_pos = s[search_start..]
        .find(['+', '-'])
        .map(|p| p + search_start);

    if let Some(op) = offset_pos {
        let time_str = &s[..op];
        let tz_str = &s[op..];
        let offset = parse_tz_offset(tz_str)?;
        return Some((time_str, offset));
    }

    // No timezone info — treat as UTC.
    Some((s, 0.0))
}

/// Parse a timezone offset string like "+05:30" or "-07:00" to seconds east.
fn parse_tz_offset(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    let sign: f64 = if s.starts_with('-') { -1.0 } else { 1.0 };
    let inner = &s[1..]; // strip sign
    let colon = inner.find(':')?;
    let off_h: f64 = inner[..colon].parse().ok()?;
    let off_m: f64 = inner[colon + 1..].parse().ok()?;
    Some(sign * (off_h * 3600.0 + off_m * 60.0))
}

/// Compute days since Unix epoch (1970-01-01) for the given Gregorian date.
/// Uses the proleptic Gregorian calendar formula.
fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    // Adjust month/year so March = month 1 (simplifies leap-year math).
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };

    // Days in each month from March: 31 30 31 30 31 31 30 31 30 31 31 28/29
    // Formula: floor((153*m + 2) / 5) gives cumulative days from March 1.
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400); // year-of-era [0, 399]
    let doy = (153 * m + 2) / 5 + day - 1; // day-of-year from March 1
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // day-of-era
    let epoch_march_1_0 = 719_468_i64; // days from 0000-03-01 to 1970-01-01 epoch
    Some(era * 146_097 + doe - epoch_march_1_0)
}

// ---------------------------------------------------------------------------
// ChainlinkFeed
// ---------------------------------------------------------------------------

pub struct ChainlinkFeed;

impl ChainlinkFeed {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ChainlinkFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Feed for ChainlinkFeed {
    fn name(&self) -> &'static str {
        "chainlink"
    }

    async fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) {
        // Build a persistent reqwest client with the required User-Agent.
        let client = match reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(std::time::Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[chainlink] failed to build HTTP client: {e}");
                return;
            }
        };

        // Deduplication set: stores timestamps rounded to 3 decimal places.
        let mut seen_ts: HashSet<u64> = HashSet::new();

        let mut retry_delay = POLL_INTERVAL_SECS;

        loop {
            if stop.is_cancelled() {
                break;
            }

            // ── HTTP GET ────────────────────────────────────────────────────
            let resp = client.get(API_URL).send().await;

            let body: Option<serde_json::Value> = match resp {
                Ok(r) => match r.json::<serde_json::Value>().await {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!("[chainlink] JSON parse error: {e}");
                        state.inc_errors();
                        None
                    }
                },
                Err(e) => {
                    eprintln!("[chainlink] HTTP error: {e}");
                    state.inc_errors();
                    None
                }
            };

            // ── Process response ────────────────────────────────────────────
            if let Some(data) = body {
                // Dig into: data.allStreamValuesGenerics.nodes[]
                let nodes = data
                    .get("data")
                    .and_then(|d| d.get("allStreamValuesGenerics"))
                    .and_then(|a| a.get("nodes"))
                    .and_then(|n| n.as_array());

                if let Some(nodes) = nodes {
                    for node in nodes {
                        // Only process benchmark attribute.
                        if node.get("attributeName").and_then(|v| v.as_str())
                            != Some("benchmark")
                        {
                            continue;
                        }

                        // Value through pos() firewall.
                        let val = match pos(
                            node.get("valueNumeric")
                                .unwrap_or(&serde_json::Value::Null),
                        ) {
                            Some(v) => v,
                            None => continue,
                        };

                        // Parse ISO timestamp.
                        let ts_str = match node
                            .get("validAfterTs")
                            .and_then(|v| v.as_str())
                        {
                            Some(s) if !s.is_empty() => s,
                            _ => continue,
                        };

                        let ts_s = match parse_iso_ts(ts_str) {
                            Some(t) => t,
                            None => continue,
                        };

                        // Dedup by timestamp (rounded to 3 decimal places).
                        // Encode as u64 fixed-point (millis) for Hash.
                        let ts_key = ts_s_to_key(ts_s);
                        if seen_ts.contains(&ts_key) {
                            continue;
                        }
                        seen_ts.insert(ts_key);

                        // Bounded dedup set: evict entries older than TTL
                        // when size exceeds DEDUP_MAX (matches Python).
                        if seen_ts.len() > DEDUP_MAX {
                            let cutoff = ts_s_to_key(super::wall_clock() - DEDUP_TTL_SECS);
                            seen_ts.retain(|&k| k >= cutoff);
                        }

                        // Update shared state (last-writer-wins with RTDS).
                        state.chainlink_value.store(val);
                        state.chainlink_ts.store(ts_s);
                        state.chainlink_count.fetch_add(1, Ordering::Relaxed);

                        // Compute recv lag.
                        let recv_ts = super::wall_clock();
                        let lag_ms = (recv_ts - ts_s) * 1000.0;

                        let meta = serde_json::json!({
                            "recv_lag_ms": (lag_ms * 10.0).round() / 10.0,
                            "recv_ts": (recv_ts * 1000.0).round() / 1000.0,
                        });

                        let meta_s = meta.to_string();
                        rings.chainlink.write(ts_s, val, meta_s.as_bytes(), None);
                    }
                }

                // FIX: WP05-F5 — reset retry on valid response, not just valid nodes.
                // Previously inside `if let Some(nodes)`, so a valid HTTP response
                // with a missing/empty nodes array left retry_delay doubled.
                retry_delay = POLL_INTERVAL_SECS;
            } else {
                // HTTP error or unparseable response: double retry delay.
                retry_delay = (retry_delay * 2.0).min(MAX_RETRY_DELAY_SECS);
            }

            // ── Wait before next poll ────────────────────────────────────────
            tokio::select! {
                () = tokio::time::sleep(
                    std::time::Duration::from_secs_f64(retry_delay)
                ) => {}
                () = stop.cancelled() => break,
            }
        }
    }
}

/// Encode a unix-seconds timestamp (3 decimal precision) as a u64 key.
/// Rounds to nearest millisecond for hash storage.
#[inline]
fn ts_s_to_key(ts_s: f64) -> u64 {
    // Multiply by 1000 and round to get millis, store as u64.
    // Negative timestamps (pre-1970) are clamped to 0 — irrelevant in practice.
    if ts_s < 0.0 {
        return 0;
    }
    (ts_s * 1000.0).round() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    // ── ISO timestamp parsing ───────────────────────────────────────────────

    #[test]
    fn parse_iso_z_suffix() {
        // "2024-01-15T12:34:56Z" — basic UTC with Z suffix
        let ts = parse_iso_ts("2024-01-15T12:34:56Z").unwrap();
        // 2024-01-15 12:34:56 UTC
        // Verify by manual calculation: days_since_epoch should give a reasonable result.
        // We just check it's a reasonable Unix timestamp (after 2020).
        assert!(ts > 1_577_836_800.0, "timestamp should be after 2020-01-01");
        assert!(ts < 2_000_000_000.0, "timestamp should be before 2033-05-18");
    }

    #[test]
    fn parse_iso_fractional_seconds_z() {
        // "2024-01-15T12:34:56.789Z"
        let ts = parse_iso_ts("2024-01-15T12:34:56.789Z").unwrap();
        let ts_no_frac = parse_iso_ts("2024-01-15T12:34:56Z").unwrap();
        // Fractional seconds should differ by ~0.789s
        let diff = ts - ts_no_frac;
        assert!((diff - 0.789).abs() < 0.001, "fractional seconds: got diff {diff}");
    }

    #[test]
    fn parse_iso_positive_offset() {
        // "+05:30" offset = 5.5 hours east = 19800 seconds
        // "2024-01-15T18:04:56+05:30" == "2024-01-15T12:34:56Z"
        let ts_utc = parse_iso_ts("2024-01-15T12:34:56Z").unwrap();
        let ts_off = parse_iso_ts("2024-01-15T18:04:56+05:30").unwrap();
        assert!((ts_utc - ts_off).abs() < 1.0, "offset +05:30 should match UTC");
    }

    #[test]
    fn parse_iso_negative_offset() {
        // "-07:00" = 7 hours west = -25200 seconds
        // "2024-01-15T05:34:56-07:00" == "2024-01-15T12:34:56Z"
        let ts_utc = parse_iso_ts("2024-01-15T12:34:56Z").unwrap();
        let ts_off = parse_iso_ts("2024-01-15T05:34:56-07:00").unwrap();
        assert!((ts_utc - ts_off).abs() < 1.0, "offset -07:00 should match UTC");
    }

    #[test]
    fn parse_iso_known_epoch() {
        // 1970-01-01T00:00:00Z should be exactly 0.0
        let ts = parse_iso_ts("1970-01-01T00:00:00Z").unwrap();
        assert_eq!(ts, 0.0);
    }

    #[test]
    fn parse_iso_known_value() {
        // 2024-01-15 00:00:00 UTC
        // Days from epoch to 2024-01-15: verify within 1 second of expected.
        // Expected: 1705276800 (can verify externally)
        let ts = parse_iso_ts("2024-01-15T00:00:00Z").unwrap();
        assert!((ts - 1_705_276_800.0).abs() < 1.0, "got {ts}");
    }

    #[test]
    fn parse_iso_rejects_garbage() {
        assert_eq!(parse_iso_ts("not-a-timestamp"), None);
        assert_eq!(parse_iso_ts(""), None);
        assert_eq!(parse_iso_ts("2024-13-01T00:00:00Z"), None); // invalid month
    }

    #[test]
    fn parse_iso_rfc3339_plus_zero() {
        // "+00:00" is equivalent to "Z"
        let ts_z = parse_iso_ts("2024-01-15T12:34:56Z").unwrap();
        let ts_p = parse_iso_ts("2024-01-15T12:34:56+00:00").unwrap();
        assert!((ts_z - ts_p).abs() < 0.001);
    }

    // ── JSON response structure parsing ─────────────────────────────────────

    fn make_response(nodes: Vec<serde_json::Value>) -> serde_json::Value {
        serde_json::json!({
            "data": {
                "allStreamValuesGenerics": {
                    "nodes": nodes
                }
            }
        })
    }

    fn make_benchmark_node(value: f64, ts: &str) -> serde_json::Value {
        serde_json::json!({
            "attributeName": "benchmark",
            "valueNumeric": value,
            "validAfterTs": ts,
        })
    }

    #[test]
    fn parse_nested_json_response() {
        let resp = make_response(vec![make_benchmark_node(95000.0, "2024-01-15T12:00:00Z")]);
        let nodes = resp["data"]["allStreamValuesGenerics"]["nodes"]
            .as_array()
            .unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn filter_benchmark_attribute() {
        let resp = make_response(vec![
            make_benchmark_node(95000.0, "2024-01-15T12:00:00Z"),
            serde_json::json!({
                "attributeName": "ask",
                "valueNumeric": 95010.0,
                "validAfterTs": "2024-01-15T12:00:00Z",
            }),
            serde_json::json!({
                "attributeName": "bid",
                "valueNumeric": 94990.0,
                "validAfterTs": "2024-01-15T12:00:00Z",
            }),
        ]);

        let nodes = resp["data"]["allStreamValuesGenerics"]["nodes"]
            .as_array()
            .unwrap();

        let benchmark_nodes: Vec<_> = nodes
            .iter()
            .filter(|n| {
                n.get("attributeName").and_then(|v| v.as_str()) == Some("benchmark")
            })
            .collect();

        assert_eq!(benchmark_nodes.len(), 1);
        assert_eq!(
            pos(benchmark_nodes[0].get("valueNumeric").unwrap()).unwrap(),
            95000.0
        );
    }

    // ── pos() firewall ───────────────────────────────────────────────────────

    #[test]
    fn reject_non_positive_value_numeric() {
        let zero = serde_json::json!(0.0_f64);
        assert_eq!(pos(&zero), None, "zero should be rejected");

        let neg = serde_json::json!(-100.0_f64);
        assert_eq!(pos(&neg), None, "negative should be rejected");

        let null = serde_json::Value::Null;
        assert_eq!(pos(&null), None, "null should be rejected");

        let positive = serde_json::json!(95000.0_f64);
        assert_eq!(pos(&positive), Some(95000.0));
    }

    // ── Deduplication ────────────────────────────────────────────────────────

    #[test]
    fn dedup_same_timestamp_skipped() {
        let mut seen: HashSet<u64> = HashSet::new();

        let ts = 1705276800.0_f64;
        let key = ts_s_to_key(ts);

        // First insert: new.
        assert!(!seen.contains(&key));
        seen.insert(key);

        // Second insert: duplicate.
        assert!(seen.contains(&key), "second time should be seen");
    }

    #[test]
    fn dedup_different_timestamps_processed() {
        let mut seen: HashSet<u64> = HashSet::new();

        let ts1 = 1705276800.000_f64;
        let ts2 = 1705276800.001_f64; // differs by 1ms
        let ts3 = 1705276801.000_f64; // differs by 1s

        let k1 = ts_s_to_key(ts1);
        let k2 = ts_s_to_key(ts2);
        let k3 = ts_s_to_key(ts3);

        assert_ne!(k1, k2, "1ms difference should produce different keys");
        assert_ne!(k1, k3, "1s difference should produce different keys");

        seen.insert(k1);
        assert!(!seen.contains(&k2), "ts2 not yet seen");
        assert!(!seen.contains(&k3), "ts3 not yet seen");
        seen.insert(k2);
        seen.insert(k3);
        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn dedup_3_decimal_precision() {
        // The key encodes timestamps at millisecond (3 decimal place) precision.
        // Two timestamps that are identical produce the same key.
        assert_eq!(ts_s_to_key(1705276800.123), ts_s_to_key(1705276800.123));

        // Timestamps 1ms apart produce different keys.
        let k_a = ts_s_to_key(1705276800.000);
        let k_b = ts_s_to_key(1705276800.001);
        assert_ne!(k_a, k_b, "1ms apart should differ");

        // ts_a * 1000 = 1705276800123.4 → rounds to 1705276800123
        // ts_b * 1000 = 1705276800123.5 → rounds to 1705276800124 (round half away from zero)
        let k_c = ts_s_to_key(1705276800.1234_f64);
        let k_d = ts_s_to_key(1705276800.1235_f64);
        // c and d may differ (the .5 case) — just ensure no panic and stable output.
        let _ = (k_c, k_d);
    }

    #[test]
    fn dedup_set_cleanup_on_overflow() {
        let mut seen: HashSet<u64> = HashSet::new();

        // Fill to just over DEDUP_MAX with old timestamps.
        let old_base = 1_000_000.0_f64; // very old (1970s) — will be below cutoff
        for i in 0..=DEDUP_MAX {
            seen.insert(ts_s_to_key(old_base + i as f64));
        }
        assert!(seen.len() > DEDUP_MAX);

        // Evict entries older than DEDUP_TTL_SECS from now.
        let cutoff = ts_s_to_key(crate::feed::wall_clock() - DEDUP_TTL_SECS);
        seen.retain(|&k| k >= cutoff);

        // All old entries should have been evicted.
        assert_eq!(seen.len(), 0, "all old entries should be evicted");
    }

    #[test]
    fn dedup_set_cleanup_keeps_recent_entries() {
        let mut seen: HashSet<u64> = HashSet::new();

        let now = crate::feed::wall_clock();

        // Mix of old and recent entries.
        let old_ts = now - DEDUP_TTL_SECS - 10.0; // definitely old
        let recent_ts = now - 1.0; // 1 second ago — recent

        for i in 0..300 {
            seen.insert(ts_s_to_key(old_ts + i as f64));
        }
        for i in 0..300 {
            seen.insert(ts_s_to_key(recent_ts + i as f64 * 0.001));
        }

        let initial_len = seen.len();
        assert!(initial_len > DEDUP_MAX, "should exceed limit");

        let cutoff = ts_s_to_key(now - DEDUP_TTL_SECS);
        seen.retain(|&k| k >= cutoff);

        // Recent entries should survive.
        assert!(seen.len() >= 300, "recent entries should be kept");
        // Old entries should be gone.
        assert!(
            seen.len() < initial_len,
            "cleanup should have removed old entries"
        );
    }

    // ── Meta JSON ────────────────────────────────────────────────────────────

    #[test]
    fn meta_includes_recv_lag_ms_and_recv_ts() {
        let ts_s = 1705276800.0_f64;
        let recv_ts = ts_s + 0.5; // 500ms lag
        let lag_ms = (recv_ts - ts_s) * 1000.0;

        let meta = serde_json::json!({
            "recv_lag_ms": (lag_ms * 10.0).round() / 10.0,
            "recv_ts": (recv_ts * 1000.0).round() / 1000.0,
        });

        let s = meta.to_string();
        assert!(s.contains("recv_lag_ms"), "must include recv_lag_ms");
        assert!(s.contains("recv_ts"), "must include recv_ts");
        assert!(s.contains("500"), "lag should be 500ms");
    }

    #[test]
    fn meta_recv_lag_rounded_to_1_decimal() {
        // Lag: 42.768ms → rounded to 42.8
        let lag_ms = 42.768_f64;
        let rounded = (lag_ms * 10.0).round() / 10.0;
        assert!((rounded - 42.8).abs() < 0.0001, "got {rounded}");
    }

    #[test]
    fn meta_recv_ts_rounded_to_3_decimals() {
        let recv_ts = 1705276800.123456_f64;
        let rounded = (recv_ts * 1000.0).round() / 1000.0;
        assert!((rounded - 1705276800.123).abs() < 0.0001, "got {rounded}");
    }

    // ── State updates ────────────────────────────────────────────────────────

    #[test]
    fn state_updates_on_valid_tick() {
        let state = LiveState::default();

        let val = 95000.0_f64;
        let ts_s = 1705276800.0_f64;

        state.chainlink_value.store(val);
        state.chainlink_ts.store(ts_s);
        state.chainlink_count.fetch_add(1, Ordering::Relaxed);

        assert_eq!(state.chainlink_value.load(), 95000.0);
        assert_eq!(state.chainlink_ts.load(), 1705276800.0);
        assert_eq!(state.chainlink_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn state_last_writer_wins() {
        // Both chainlink poller and RTDS write to the same fields.
        let state = LiveState::default();

        // RTDS writes first.
        state.chainlink_value.store(95000.0);
        state.chainlink_ts.store(1705276800.0);
        state.chainlink_count.fetch_add(1, Ordering::Relaxed);

        // Chainlink poller writes second (overwrites).
        state.chainlink_value.store(95010.0);
        state.chainlink_ts.store(1705276802.0);
        state.chainlink_count.fetch_add(1, Ordering::Relaxed);

        assert_eq!(state.chainlink_value.load(), 95010.0, "last writer wins");
        assert_eq!(state.chainlink_ts.load(), 1705276802.0);
        assert_eq!(state.chainlink_count.load(Ordering::Relaxed), 2);
    }

    // ── ts_s_to_key ──────────────────────────────────────────────────────────

    #[test]
    fn ts_key_clamped_negative() {
        assert_eq!(ts_s_to_key(-1.0), 0);
    }

    #[test]
    fn ts_key_zero() {
        assert_eq!(ts_s_to_key(0.0), 0);
    }

    #[test]
    fn ts_key_positive() {
        // 1.234 * 1000 = 1234 -> u64 1234
        assert_eq!(ts_s_to_key(1.234), 1234);
    }

    // ── Feed-level constants ─────────────────────────────────────────────────

    #[test]
    fn api_url_contains_feed_id() {
        // The expected feed ID must be embedded in the API URL.
        let feed_id = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8";
        assert!(API_URL.contains(feed_id), "API_URL must contain the feed ID");
        assert!(API_URL.contains("abiIndex=0"), "API_URL must contain abiIndex");
        assert!(API_URL.contains("queryWindow=1m"), "API_URL must contain queryWindow");
    }
}
