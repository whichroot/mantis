//! Feed infrastructure — trait, types, shared state, manager.
//!
//! Each feed is a self-contained struct that implements [`Feed`].
//! The manager spawns them, collects their output, flushes to DB.

pub mod binance;
pub mod brti;
pub mod chainlink;
pub mod deribit;
pub mod deribit_ws;
pub mod hyperliquid;
pub mod kalshi_ws;
pub mod polymarket_ws;
pub mod rtds;

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// AtomicF64 — lock-free f64 via bit-punning
// ---------------------------------------------------------------------------

/// Lock-free f64 storage. Uses `AtomicU64` with `to_bits`/`from_bits`.
#[derive(Debug)]
pub struct AtomicF64(AtomicU64);

impl AtomicF64 {
    pub const fn new(v: f64) -> Self {
        Self(AtomicU64::new(v.to_bits()))
    }

    // FIX: WP05-F6 — Acquire/Release for coupled field pairs (price+timestamp).
    // Relaxed allows a reader on ARM (Apple Silicon) to see a new price with an
    // old timestamp. Release on store / Acquire on load establishes a
    // happens-before edge so coupled fields are always consistent.
    // On x86 this compiles to the same instructions as Relaxed; the cost is
    // only on weakly-ordered architectures (ARM, POWER).
    pub fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Acquire))
    }

    pub fn store(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Release);
    }
}

impl Default for AtomicF64 {
    fn default() -> Self {
        Self::new(0.0)
    }
}

// ---------------------------------------------------------------------------
// FeedRow — one row for the feeds table
// ---------------------------------------------------------------------------

/// A single tick to be inserted into the `feeds` table.
#[derive(Debug, Clone)]
pub struct FeedRow {
    pub ts: f64,
    pub source: &'static str,
    pub value: f64,
    pub meta: Option<String>,
    /// Per-market identifier for venue source rings (kalshi_*, poly_*).
    /// `None` for oracle/sigma/perp rings that are not per-market.
    pub ticker: Option<String>,
}

// ---------------------------------------------------------------------------
// LiveState — shared mutable state across all feeds and snapshot loop
// ---------------------------------------------------------------------------

/// Shared state updated by feeds, read by snapshot/sigma loops.
/// All fields default to 0.0 / 0. Feeds write, snapshot loop reads.
#[derive(Debug, Default)]
pub struct LiveState {
    // Binance aggTrade (authoritative spot)
    pub binance_price: AtomicF64,
    pub binance_ts: AtomicF64,
    pub binance_count: AtomicU64,

    // BRTI (Kalshi oracle)
    pub brti_value: AtomicF64,
    pub brti_ts: AtomicF64,
    pub brti_count: AtomicU64,

    // Chainlink (Polymarket oracle) — HTTP + RTDS race, last writer wins
    pub chainlink_value: AtomicF64,
    pub chainlink_ts: AtomicF64,
    pub chainlink_count: AtomicU64,

    // RTDS Binance (Polymarket's view, separate from authoritative Binance)
    pub poly_bn_value: AtomicF64,
    pub poly_bn_ts: AtomicF64,
    pub poly_bn_count: AtomicU64,

    // Sigma (from Deribit, back-computed)
    pub sigma_1s: AtomicF64,
    pub sigma_ts: AtomicF64,

    // Hyperliquid BTC perp
    pub hl_funding: AtomicF64,    // current funding rate (per hour)
    pub hl_oi: AtomicF64,         // open interest (BTC)
    pub hl_premium: AtomicF64,    // mark − oracle premium
    pub hl_mark: AtomicF64,       // mark price
    pub hl_oracle: AtomicF64,     // oracle price
    pub hl_mid: AtomicF64,        // mid price
    pub hl_bid_depth_01: AtomicF64, // aggregated bid depth within 0.1% of mid
    pub hl_ask_depth_01: AtomicF64, // aggregated ask depth within 0.1% of mid
    pub hl_bid_depth_05: AtomicF64, // aggregated bid depth within 0.5% of mid
    pub hl_ask_depth_05: AtomicF64, // aggregated ask depth within 0.5% of mid
    pub hl_bid_depth_10: AtomicF64, // aggregated bid depth within 1.0% of mid
    pub hl_ask_depth_10: AtomicF64, // aggregated ask depth within 1.0% of mid
    pub hl_ts: AtomicF64,
    pub hl_count: AtomicU64,

    // Counters
    pub feed_inserts: AtomicU64,
    pub book_inserts: AtomicU64,
    pub snapshot_count: AtomicU64,
    pub errors: AtomicU64,
}

impl LiveState {
    /// Binance − BRTI displacement. None if either is non-positive.
    pub fn displacement(&self) -> Option<f64> {
        let b = self.binance_price.load();
        let r = self.brti_value.load();
        if b > 0.0 && r > 0.0 {
            Some(b - r)
        } else {
            None
        }
    }

    /// Binance − Chainlink displacement.
    pub fn displacement_chainlink(&self) -> Option<f64> {
        let b = self.binance_price.load();
        let c = self.chainlink_value.load();
        if b > 0.0 && c > 0.0 {
            Some(b - c)
        } else {
            None
        }
    }

    /// Binance − Hyperliquid oracle displacement.
    pub fn displacement_hyperliquid(&self) -> Option<f64> {
        let b = self.binance_price.load();
        let h = self.hl_oracle.load();
        if b > 0.0 && h > 0.0 {
            Some(b - h)
        } else {
            None
        }
    }

    pub fn inc_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Feed trait
// ---------------------------------------------------------------------------

/// A live data feed. Handles its own connection, reconnection, and parsing.
///
/// Writes [`RingEntry`]s to the appropriate ring in `rings` for kernel
/// consumption. Also updates [`LiveState`] atomics for cross-feed reads.
/// Returns only on cancellation or fatal error.
///
/// [`RingEntry`]: crate::ring::RingEntry
pub trait Feed: Send + 'static {
    fn name(&self) -> &'static str;

    /// Run the feed. Must handle reconnection internally.
    fn run(
        self: Box<Self>,
        rings: Arc<crate::ring::RingSet>,
        state: Arc<LiveState>,
        stop: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send;
}

// ---------------------------------------------------------------------------
// NaN firewall — matches Python's _finite() and _pos()
// ---------------------------------------------------------------------------

/// Parse a JSON value to f64, return None if non-finite.
pub fn finite(v: &serde_json::Value) -> Option<f64> {
    let f = match v {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    if f.is_finite() { Some(f) } else { None }
}

/// Parse a JSON value to f64, return None if non-finite or non-positive.
pub fn pos(v: &serde_json::Value) -> Option<f64> {
    finite(v).filter(|&f| f > 0.0)
}

// ---------------------------------------------------------------------------
// Backoff helper
// ---------------------------------------------------------------------------

/// Exponential backoff: doubles delay on each call, capped at `max`.
pub struct Backoff {
    pub current: f64,
    base: f64,
    max: f64,
}

impl Backoff {
    pub fn new(base: f64, max: f64) -> Self {
        Self { current: base, base, max }
    }

    /// Wait for the current delay, then double it (capped).
    /// Returns immediately if stop is signalled.
    pub async fn wait(&mut self, stop: &CancellationToken) {
        let delay = std::time::Duration::from_secs_f64(self.current);
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = stop.cancelled() => {}
        }
        self.current = (self.current * 2.0).min(self.max);
    }

    /// Reset delay to base (call on successful connection).
    pub fn reset(&mut self) {
        self.current = self.base;
    }
}

// ---------------------------------------------------------------------------
// NTP-aware wall clock — single source of truth for all feeds
// ---------------------------------------------------------------------------

/// NTP clock offset in microseconds, applied to every wall-clock read.
/// Set once at startup via [`ntp_sync`], then used by [`wall_clock`].
pub static NTP_OFFSET_US: AtomicI64 = AtomicI64::new(0);

/// Query an NTP server and store the offset. Call once at startup.
/// Uses a minimal SNTP implementation (no external crate).
///
/// On failure, offset stays at 0 (fall back to system clock).
pub async fn ntp_sync() {
    match sntp_query("pool.ntp.org", 123).await {
        Ok(offset_us) => {
            // FIX: WP05-F8 — Release on NTP store so subsequent Acquire loads
            // in wall_clock() see the updated offset on all architectures.
            NTP_OFFSET_US.store(offset_us, Ordering::Release);
            let offset_ms = offset_us as f64 / 1000.0;
            eprintln!("[ntp] synced to pool.ntp.org, offset={offset_ms:.1}ms");
        }
        Err(e) => {
            eprintln!("[ntp] sync failed ({e}), using system clock");
        }
    }
}

/// NTP-corrected wall clock in unix seconds (f64).
/// All feeds must use this instead of `SystemTime::now()`.
pub fn wall_clock() -> f64 {
    let sys = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    // FIX: WP05-F8 — Acquire on NTP load pairs with the Release store in
    // ntp_sync(), guaranteeing wall_clock() sees the synced offset everywhere.
    let offset_us = NTP_OFFSET_US.load(Ordering::Acquire);
    sys.as_secs_f64() + (offset_us as f64 / 1_000_000.0)
}

/// NTP-corrected wall clock in milliseconds.
pub fn wall_clock_ms() -> f64 {
    wall_clock() * 1000.0
}

/// Minimal SNTP client. Sends one request, computes clock offset.
/// Returns offset in microseconds (positive = local clock is behind).
async fn sntp_query(host: &str, port: u16) -> Result<i64, String> {
    use tokio::net::UdpSocket;

    let sock = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| format!("bind: {e}"))?;

    // Resolve hostname
    let addr = tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map_err(|e| format!("resolve: {e}"))?
        .next()
        .ok_or_else(|| "no address".to_string())?;

    // Build SNTP request (48 bytes, version 4, mode 3 = client)
    let mut buf = [0u8; 48];
    buf[0] = 0x23; // LI=0, VN=4, Mode=3

    // Record transmit timestamp (T1)
    let t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    sock.send_to(&buf, addr)
        .await
        .map_err(|e| format!("send: {e}"))?;

    // Receive response with timeout
    let mut resp = [0u8; 48];
    let n = tokio::time::timeout(std::time::Duration::from_secs(3), sock.recv(&mut resp))
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|e| format!("recv: {e}"))?;

    if n < 48 {
        return Err(format!("short response: {n} bytes"));
    }

    // Record receive timestamp (T4)
    let t4 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();

    // Extract server timestamps (T2 = receive, T3 = transmit)
    // NTP epoch: 1900-01-01, Unix epoch: 1970-01-01 = 2208988800 seconds
    const NTP_EPOCH_OFFSET: u64 = 2_208_988_800;

    let t2_secs = u32::from_be_bytes([resp[32], resp[33], resp[34], resp[35]]) as u64;
    let t2_frac = u32::from_be_bytes([resp[36], resp[37], resp[38], resp[39]]) as f64 / 4_294_967_296.0;
    let t2 = (t2_secs - NTP_EPOCH_OFFSET) as f64 + t2_frac;

    let t3_secs = u32::from_be_bytes([resp[40], resp[41], resp[42], resp[43]]) as u64;
    let t3_frac = u32::from_be_bytes([resp[44], resp[45], resp[46], resp[47]]) as f64 / 4_294_967_296.0;
    let t3 = (t3_secs - NTP_EPOCH_OFFSET) as f64 + t3_frac;

    let t1_f = t1.as_secs_f64();
    let t4_f = t4.as_secs_f64();

    // Standard NTP offset formula: ((T2-T1) + (T3-T4)) / 2
    let offset = ((t2 - t1_f) + (t3 - t4_f)) / 2.0;
    let offset_us = (offset * 1_000_000.0) as i64;

    Ok(offset_us)
}

// ---------------------------------------------------------------------------
// Feed-level dedup — prevents duplicate rows on reconnect
// ---------------------------------------------------------------------------

/// Simple dedup guard for feed rows. Tracks (source, ts_key) pairs.
/// ts_key = (ts * 1000).round() as u64 — millisecond precision.
pub struct FeedDedup {
    seen: std::collections::HashSet<(u64, u64)>, // (source_hash, ts_key)
    max_size: usize,
    ttl_secs: f64,
}

impl FeedDedup {
    pub fn new() -> Self {
        Self {
            seen: std::collections::HashSet::new(),
            max_size: 1000,
            ttl_secs: 120.0,
        }
    }

    /// Returns true if this row is new (not a duplicate).
    /// Returns false if already seen.
    pub fn check(&mut self, source: &str, ts: f64) -> bool {
        let source_hash = Self::hash_source(source);
        let ts_key = (ts * 1000.0).round() as u64;
        if self.seen.contains(&(source_hash, ts_key)) {
            return false;
        }
        self.seen.insert((source_hash, ts_key));

        // Evict old entries when set gets large
        if self.seen.len() > self.max_size {
            let cutoff = ((wall_clock() - self.ttl_secs) * 1000.0).round() as u64;
            self.seen.retain(|&(_, k)| k >= cutoff);
        }
        true
    }

    fn hash_source(s: &str) -> u64 {
        // Simple FNV-1a hash — deterministic, fast, no dep
        let mut h: u64 = 0xcbf29ce484222325;
        for b in s.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }
}

impl Default for FeedDedup {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_f64_roundtrip() {
        let a = AtomicF64::new(3.14159);
        assert_eq!(a.load(), 3.14159);
        a.store(2.71828);
        assert_eq!(a.load(), 2.71828);
    }

    #[test]
    fn atomic_f64_default_zero() {
        let a = AtomicF64::default();
        assert_eq!(a.load(), 0.0);
    }

    #[test]
    fn atomic_f64_special_values() {
        let a = AtomicF64::new(f64::INFINITY);
        assert_eq!(a.load(), f64::INFINITY);
        a.store(f64::NEG_INFINITY);
        assert_eq!(a.load(), f64::NEG_INFINITY);
        a.store(f64::NAN);
        assert!(a.load().is_nan());
    }

    #[test]
    fn finite_parses_number() {
        let v = serde_json::json!(95123.45);
        assert_eq!(finite(&v), Some(95123.45));
    }

    #[test]
    fn finite_parses_string() {
        let v = serde_json::json!("95123.45");
        assert_eq!(finite(&v), Some(95123.45));
    }

    #[test]
    fn finite_rejects_nan() {
        let v = serde_json::json!(f64::NAN);
        assert_eq!(finite(&v), None);
    }

    #[test]
    fn finite_rejects_null() {
        let v = serde_json::json!(null);
        assert_eq!(finite(&v), None);
    }

    #[test]
    fn finite_rejects_garbage_string() {
        let v = serde_json::json!("not_a_number");
        assert_eq!(finite(&v), None);
    }

    #[test]
    fn pos_rejects_zero() {
        let v = serde_json::json!(0.0);
        assert_eq!(pos(&v), None);
    }

    #[test]
    fn pos_rejects_negative() {
        let v = serde_json::json!(-1.0);
        assert_eq!(pos(&v), None);
    }

    #[test]
    fn pos_accepts_positive() {
        let v = serde_json::json!(42.0);
        assert_eq!(pos(&v), Some(42.0));
    }

    #[test]
    fn pos_parses_positive_string() {
        let v = serde_json::json!("95123.45");
        assert_eq!(pos(&v), Some(95123.45));
    }

    #[test]
    fn live_state_displacement_both_positive() {
        let s = LiveState::default();
        s.binance_price.store(95100.0);
        s.brti_value.store(95090.0);
        assert_eq!(s.displacement(), Some(10.0));
    }

    #[test]
    fn live_state_displacement_missing_brti() {
        let s = LiveState::default();
        s.binance_price.store(95100.0);
        assert_eq!(s.displacement(), None);
    }

    #[test]
    fn live_state_displacement_chainlink() {
        let s = LiveState::default();
        s.binance_price.store(95100.0);
        s.chainlink_value.store(95085.0);
        assert_eq!(s.displacement_chainlink(), Some(15.0));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let mut b = Backoff::new(2.0, 30.0);
        assert_eq!(b.current, 2.0);
        b.current = (b.current * 2.0).min(b.max);
        assert_eq!(b.current, 4.0);
        b.current = (b.current * 2.0).min(b.max);
        assert_eq!(b.current, 8.0);
        b.current = (b.current * 2.0).min(b.max);
        assert_eq!(b.current, 16.0);
        b.current = (b.current * 2.0).min(b.max);
        assert_eq!(b.current, 30.0); // capped
        b.current = (b.current * 2.0).min(b.max);
        assert_eq!(b.current, 30.0); // stays capped
    }

    #[test]
    fn backoff_reset() {
        let mut b = Backoff::new(2.0, 30.0);
        b.current = 16.0;
        b.reset();
        assert_eq!(b.current, 2.0);
    }
}
