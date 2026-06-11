//! Zero-alloc ring buffers — the kernel's hot-path data source.
//!
//! # Architecture
//!
//! ```text
//! Feeds (18 WS/REST streams)
//!     │ write() — lock-free, one writer per ring
//!     ▼
//! RingSet — 19 SourceRings, pre-allocated at startup
//!     │                       │
//!     ▼ head() / get_by_ticker()   ▼ flush_drain() / drain_all()
//! Kernel (hot path)          Flusher (cold path) → SQLite
//! ```
//!
//! # Safety invariant
//!
//! Each [`SourceRing`] has exactly ONE writer: the feed task that owns it.
//! Concurrent writes to the same ring are undefined behaviour. The write
//! protocol uses `Release` on [`SourceRing::write_idx`] and `Acquire` on all
//! reads, establishing a happens-before edge that makes every written entry
//! visible to readers before the index is advanced.
//!
//! [`UnsafeCell`] is the correct Rust idiom here — it tells the compiler
//! "interior mutability, safety maintained by the seq/write_idx protocol",
//! and gives future readers the right signal about what invariant to check.
//! Raw pointers would achieve the same thing but lose that semantic signal.
//!
//! # Design decisions (resolved)
//!
//! - **Single writer per ring** — no lock needed on the write side.
//! - **Timer-driven kernel** — reads ring at its own eval interval; no Notify.
//! - **Flusher via second cursor** — mpsc channel killed; one write path, two
//!   readers. Flusher lapped = acceptable loss (SQLite is the cold archive).
//! - **Ticker index on venue rings** — `HashMap<ticker, write_idx>` updated by
//!   the writer. O(1) kernel lookup. Stale/lapped → skip market.
//! - **BBO only in ring** — full L2 `levels` go nowhere in v1. Kernel needs
//!   only best_bid, best_ask, spread (three scalars). 128-byte inline meta fits.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock,
};

// ---------------------------------------------------------------------------
// RingEntry — fixed-size, zero-alloc, repr(C)
// ---------------------------------------------------------------------------

/// Inline meta capacity in bytes. Sized to hold BBO + small JSON context.
/// `{"best_ask":0.50000,"spread":0.02000,"yes_ask":0.50}` ≈ 54 bytes.
pub const META_CAP: usize = 128;

/// A single observation. Fixed-size, `Copy`, `repr(C)`.
///
/// `source` is implicit — which [`SourceRing`] this entry came from.
/// `seq` is the monotonic write_idx at time of write — the ABA guard.
/// `meta` holds a UTF-8 JSON fragment inline, truncated at [`META_CAP`].
/// Full payloads (large L2 books) are NOT stored; kernel needs BBO only.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RingEntry {
    /// Monotonic write_idx at time of write. ABA guard: reader validates
    /// `ring[idx % cap].seq == idx` before trusting the entry. If seq
    /// doesn't match, the slot was overwritten by a different ticker
    /// after the ring lapped.
    pub seq: u64,
    pub ts: f64,
    pub value: f64,
    pub meta_len: u32,
    pub _pad: u32,
    pub meta: [u8; META_CAP],
}

impl Default for RingEntry {
    fn default() -> Self {
        Self {
            seq: 0,
            ts: 0.0,
            value: 0.0,
            meta_len: 0,
            _pad: 0,
            meta: [0u8; META_CAP],
        }
    }
}

impl RingEntry {
    /// Decode the inline meta as a UTF-8 string slice.
    /// Returns `None` if empty or not valid UTF-8.
    #[inline]
    pub fn meta_str(&self) -> Option<&str> {
        if self.meta_len == 0 {
            return None;
        }
        let len = (self.meta_len as usize).min(META_CAP);
        std::str::from_utf8(&self.meta[..len]).ok()
    }

    /// True if this entry is fresher than `now - window_secs`.
    #[inline]
    pub fn is_fresh(&self, now: f64, window_secs: f64) -> bool {
        self.ts > 0.0 && (now - self.ts) < window_secs
    }
}

// ---------------------------------------------------------------------------
// SourceRing — lock-free ring for one feed source
// ---------------------------------------------------------------------------

/// Lock-free ring buffer for a single feed source.
///
/// # Concurrency model
///
/// - **Writer**: one feed task — calls [`write`] exclusively.
/// - **Reader A**: kernel — calls [`head`] or [`get_by_ticker`] at eval time.
/// - **Reader B**: flusher — calls [`flush_drain`] on a timer.
///
/// Synchronisation is via a single `AtomicUsize` (`write_idx`):
/// - Writer stores with `Release` after the entry is fully written.
/// - Readers load with `Acquire` before dereferencing any slot.
///
/// This is the classic SPSC ring extended to SPMC by making all readers
/// observe the same `write_idx`. Each reader has its own cursor that only
/// it advances.
pub struct SourceRing {
    /// Backing store. `UnsafeCell` because the writer mutates it in-place
    /// while readers may concurrently read other slots.
    buf: UnsafeCell<Box<[RingEntry]>>,
    /// Monotonically increasing write counter. Actual slot = idx % capacity.
    /// Writer advances with `Release`; readers load with `Acquire`.
    write_idx: AtomicUsize,
    /// Flusher's trailing cursor. Only the flusher advances this.
    flush_cursor: AtomicUsize,
    /// Pre-allocated capacity (fixed at construction time).
    pub capacity: usize,
    /// Freshness window in seconds. Entries older than this are considered stale.
    pub window_secs: f64,
    /// Venue rings only: `ticker → monotonic write_idx at last write for that ticker`.
    /// Writer updates before advancing `write_idx` so readers see a consistent pair.
    ticker_index: Option<RwLock<HashMap<String, usize>>>,
    /// Venue rings only: parallel ticker store, same capacity as `buf`.
    /// Slot `i % capacity` holds the ticker written at monotonic index `i`.
    /// The flusher `take()`s from this store when draining, so every historical
    /// entry carries its per-market identity without embedding it in the ring
    /// meta (which is bounded at META_CAP=128 and used by the kernel).
    /// Non-venue rings: `None` — zero allocation, zero cost.
    ticker_store: Option<UnsafeCell<Box<[Option<String>]>>>,
}

// SAFETY: `SourceRing` is `Send + Sync` because:
// 1. The `UnsafeCell` backing store is only mutated by a single writer task
//    (enforced by the single-writer convention documented on every public API).
// 2. All concurrent reads are guarded by `Acquire` load of `write_idx`, which
//    pairs with the writer's `Release` store, establishing happens-before.
// 3. No two threads ever write to the same slot concurrently.
unsafe impl Send for SourceRing {}
unsafe impl Sync for SourceRing {}

impl SourceRing {
    /// Allocate a new ring. `has_ticker_index` should be `true` for venue rings.
    pub fn new(capacity: usize, window_secs: f64, has_ticker_index: bool) -> Self {
        assert!(capacity >= 4, "ring capacity must be >= 4 (got {capacity})");
        let entries = vec![RingEntry::default(); capacity].into_boxed_slice();
        Self {
            buf: UnsafeCell::new(entries),
            write_idx: AtomicUsize::new(0),
            flush_cursor: AtomicUsize::new(0),
            capacity,
            window_secs,
            ticker_index: if has_ticker_index {
                Some(RwLock::new(HashMap::new()))
            } else {
                None
            },
            ticker_store: if has_ticker_index {
                Some(UnsafeCell::new(vec![None; capacity].into_boxed_slice()))
            } else {
                None
            },
        }
    }

    // ── Write (single writer only) ──────────────────────────────────────

    /// Write a new entry. **Must be called only from the owning feed task.**
    ///
    /// If `meta` exceeds [`META_CAP`] it is silently truncated; the inline
    /// copy is sufficient for the kernel's BBO/scalar reads. The flusher
    /// stores what the ring holds — truncated meta is acceptable because
    /// full L2 book data is intentionally excluded from the ring path.
    ///
    /// `ticker` must be provided for venue rings (kalshi_*, poly_*). It
    /// is stored in the ticker index so the kernel can do O(1) lookups.
    ///
    /// # Safety (internal)
    ///
    /// - Sole writer: no concurrent `write()` calls on this ring.
    /// - `write_idx` is advanced with `Release` after the entry is written.
    /// - Slot being written: `write_idx % capacity`. If the ring is full,
    ///   this overwrites the oldest entry — lapping is by design.
    #[inline]
    pub fn write(&self, ts: f64, value: f64, meta: &[u8], ticker: Option<&str>) {
        let widx = self.write_idx.load(Ordering::Relaxed);
        let slot = widx % self.capacity;

        // SAFETY: sole writer, no concurrent writes to this ring.
        // Readers only access slots with index < write_idx (Acquire-loaded),
        // so the slot we're writing to is either freshly initialised or was
        // written in a previous lap — safe to overwrite.
        let buf = unsafe { &mut *self.buf.get() };
        let entry = &mut buf[slot];

        entry.seq = widx as u64;
        entry.ts = ts;
        entry.value = value;
        let copy_len = meta.len().min(META_CAP);
        entry.meta[..copy_len].copy_from_slice(&meta[..copy_len]);
        // Zero remainder to prevent stale bytes from a longer previous entry
        // being misread as part of the new entry's meta.
        entry.meta[copy_len..].fill(0);
        entry.meta_len = copy_len as u32;
        entry._pad = 0;

        // Update ticker index *before* publishing write_idx.
        // Readers who observe the new write_idx will also see this index entry
        // because the RwLock write is sequenced before the Release store.
        if let (Some(t), Some(idx)) = (ticker, &self.ticker_index)
            && let Ok(mut guard) = idx.write()
        {
            guard.insert(t.to_string(), widx);
        }

        // Store ticker in parallel slot for flusher recovery.
        // SAFETY: sole writer, same slot as buf write above.
        // The flusher reads/takes from a disjoint slot range [cursor, widx)
        // under the same Acquire/Release ordering as buf.
        if let Some(store) = &self.ticker_store {
            let store = unsafe { &mut *store.get() };
            store[slot] = ticker.map(|t| t.to_string());
        }

        // Publish: Release pairs with readers' Acquire on write_idx.
        self.write_idx.store(widx + 1, Ordering::Release);
    }

    // ── Kernel reads ───────────────────────────────────────────────────

    /// Read the latest entry (head). Returns `None` if the ring is empty.
    ///
    /// Does **not** apply a freshness check — the kernel checks freshness
    /// after reading if needed (e.g., for oracle warmup validation).
    #[inline]
    pub fn head(&self) -> Option<RingEntry> {
        let widx = self.write_idx.load(Ordering::Acquire);
        if widx == 0 {
            return None;
        }
        let slot = (widx - 1) % self.capacity;
        // SAFETY: widx > 0 and Acquire load ensures the entry at slot is
        // fully written and visible.
        Some(unsafe { (*self.buf.get())[slot] })
    }

    /// O(1) lookup of the latest entry for a specific ticker (venue rings).
    ///
    /// Returns `None` if:
    /// - This ring has no ticker index (non-venue ring).
    /// - No entry has been written for `ticker`.
    /// - **ABA guard**: `entry.seq != mono_idx` — the slot was overwritten
    ///   by a different ticker's data after the ring lapped.
    /// - The entry's timestamp is outside the freshness window.
    pub fn get_by_ticker(&self, ticker: &str, now: f64) -> Option<RingEntry> {
        let index = self.ticker_index.as_ref()?;
        let _widx = self.write_idx.load(Ordering::Acquire);

        let mono_idx = {
            let guard = index.read().ok()?;
            *guard.get(ticker)?
        };

        let slot = mono_idx % self.capacity;
        // SAFETY: Acquire load of write_idx ensures all entries written
        // before write_idx are visible. mono_idx came from the ticker index
        // which is updated before write_idx is advanced.
        let entry = unsafe { (*self.buf.get())[slot] };

        // ABA guard: seq must match the expected write_idx. If the ring
        // lapped and this slot was overwritten with a different ticker's
        // data, seq won't match. Skip — same as no-data behavior.
        if entry.seq != mono_idx as u64 {
            return None;
        }

        if entry.is_fresh(now, self.window_secs) {
            Some(entry)
        } else {
            None
        }
    }

    // ── Flusher reads ──────────────────────────────────────────────────

    /// Drain all entries written since the last flush cursor position.
    ///
    /// `source` is the static source name for this ring (e.g., `"binance"`).
    /// It is embedded in the returned tuples so the flusher can build
    /// [`crate::feed::FeedRow`]s without knowing which ring it's draining.
    ///
    /// If the flusher has been lapped (the ring wrapped past its cursor),
    /// the cursor is advanced to the oldest available entry and a warning
    /// is logged. The missed entries are gone — this is acceptable because
    /// SQLite is the cold archive, not the hot source of truth.
    pub fn flush_drain(
        &self,
        source: &'static str,
    ) -> Vec<(&'static str, RingEntry, Option<String>)> {
        let widx = self.write_idx.load(Ordering::Acquire);
        let mut cursor = self.flush_cursor.load(Ordering::Relaxed);

        // Detect lapped cursor: advance to oldest available entry.
        if widx.saturating_sub(cursor) > self.capacity {
            let missed = widx - cursor - self.capacity;
            eprintln!("[ring::{source}] flusher lapped by {missed} entries — advancing cursor");
            cursor = widx - self.capacity;
        }

        if cursor >= widx {
            return Vec::new();
        }

        let count = widx - cursor;
        let mut out = Vec::with_capacity(count);

        // SAFETY: all entries in [cursor, widx) are fully written and visible
        // after the Acquire load of write_idx above. The ticker_store is
        // accessed mutably only by the flusher (this method), never by the
        // kernel — so take() is safe with no concurrent mutation.
        let buf = unsafe { &*self.buf.get() };
        let tstore = self.ticker_store.as_ref().map(|s| s.get());
        for i in cursor..widx {
            let slot = i % self.capacity;
            // and_then avoids Option<Option<String>>: tstore is Option<*mut ...>,
            // and take() returns Option<String>, so and_then flattens to Option<String>.
            let ticker = tstore.and_then(|ptr| unsafe { (*ptr)[slot].take() });
            out.push((source, buf[slot], ticker));
        }

        // Release so that if flush_cursor is ever observed by another reader,
        // they see the update. In practice only the flusher reads this.
        self.flush_cursor.store(widx, Ordering::Release);
        out
    }

    // ── Diagnostics ────────────────────────────────────────────────────

    /// Total entries ever written (monotonically increasing).
    #[inline]
    pub fn write_count(&self) -> usize {
        self.write_idx.load(Ordering::Acquire)
    }

    /// Entries written but not yet flushed to SQLite.
    #[inline]
    pub fn pending_flush(&self) -> usize {
        let widx = self.write_idx.load(Ordering::Acquire);
        let cursor = self.flush_cursor.load(Ordering::Relaxed);
        widx.saturating_sub(cursor)
    }
}

// ---------------------------------------------------------------------------
// RingSet — all 19 rings, owned by an Arc, shared across tasks
// ---------------------------------------------------------------------------

/// All feed source rings, pre-allocated at startup.
///
/// Feeds write to their designated ring. The kernel reads any ring at
/// eval time. The flusher drains all rings via [`drain_all`].
pub struct RingSet {
    // ── Oracle / sigma sources (300s window) ──────────────────────────
    pub binance: SourceRing,
    pub brti: SourceRing,
    pub rtds_chainlink: SourceRing,
    pub rtds_binance: SourceRing,
    pub chainlink: SourceRing,
    pub deribit_iv: SourceRing,
    pub deribit_iv_computed: SourceRing,
    pub deribit_ws: SourceRing,

    // ── Perp context sources (60s window) ─────────────────────────────
    pub hyperliquid: SourceRing,
    pub hyperliquid_trades: SourceRing,
    pub hyperliquid_l2: SourceRing,

    // ── Venue sources — with ticker index (30s window) ─────────────────
    pub kalshi_book: SourceRing,
    pub kalshi_delta: SourceRing,
    pub kalshi_ticker: SourceRing,
    pub poly_book: SourceRing,
    pub poly_bbo: SourceRing,
    pub poly_price: SourceRing,
    pub poly_trade: SourceRing,
    pub poly_resolved: SourceRing,
}

/// Capacity and window for each source ring.
/// (capacity, window_secs, has_ticker_index)
/// Derived from per-source rate × warmup window + safety margin.
#[allow(dead_code)]
const RING_SPECS: &[(&str, usize, f64, bool)] = &[
    // Oracle / sigma — 300s OracleProfile + SigmaEma warmup
    ("binance", 300, 300.0, false),
    ("brti", 100, 300.0, false),
    ("rtds_chainlink", 100, 300.0, false),
    ("rtds_binance", 100, 300.0, false),
    ("chainlink", 32, 300.0, false),           // slow HTTP poll
    ("deribit_iv", 32, 300.0, false),          // REST fallback
    ("deribit_iv_computed", 32, 300.0, false), // REST fallback
    ("deribit_ws", 1500, 300.0, false),        // 20 instruments × 5/s
    // Perp context — 60s moderate history
    ("hyperliquid", 64, 60.0, false),
    ("hyperliquid_trades", 300, 60.0, false),
    ("hyperliquid_l2", 64, 60.0, false),
    // Venue — 30s recency buffer, ticker-indexed
    ("kalshi_book", 180, 30.0, true),
    ("kalshi_delta", 180, 30.0, true),
    ("kalshi_ticker", 180, 30.0, true),
    ("poly_book", 300, 30.0, true),
    ("poly_bbo", 300, 30.0, true),
    ("poly_price", 2400, 30.0, true), // ~80/s burst
    ("poly_trade", 600, 30.0, true),
    ("poly_resolved", 32, 30.0, true), // minimum 32 slots
];

impl RingSet {
    /// Allocate all 19 rings. Called once at startup before feeds spawn.
    /// Total allocation: ~1 MB (19 rings × per-source capacity × 152 bytes).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            binance: SourceRing::new(300, 300.0, false),
            brti: SourceRing::new(100, 300.0, false),
            rtds_chainlink: SourceRing::new(100, 300.0, false),
            rtds_binance: SourceRing::new(100, 300.0, false),
            chainlink: SourceRing::new(32, 300.0, false),
            deribit_iv: SourceRing::new(32, 300.0, false),
            deribit_iv_computed: SourceRing::new(32, 300.0, false),
            deribit_ws: SourceRing::new(1500, 300.0, false),
            hyperliquid: SourceRing::new(64, 60.0, false),
            hyperliquid_trades: SourceRing::new(300, 60.0, false),
            hyperliquid_l2: SourceRing::new(64, 60.0, false),
            kalshi_book: SourceRing::new(180, 30.0, true),
            kalshi_delta: SourceRing::new(180, 30.0, true),
            kalshi_ticker: SourceRing::new(180, 30.0, true),
            poly_book: SourceRing::new(300, 30.0, true),
            poly_bbo: SourceRing::new(300, 30.0, true),
            poly_price: SourceRing::new(2400, 30.0, true),
            poly_trade: SourceRing::new(600, 30.0, true),
            poly_resolved: SourceRing::new(32, 30.0, true),
        })
    }

    /// Drain all rings and collect pending entries for the flusher.
    ///
    /// Returns tuples of `(source: &'static str, entry: RingEntry, ticker: Option<String>)`
    /// for every entry written since the last drain. The flusher converts these
    /// to `FeedRow`s and bulk-inserts into SQLite. `ticker` is `Some` for venue
    /// ring entries (kalshi_*, poly_*) and `None` for oracle/sigma/perp rings.
    pub fn drain_all(&self) -> Vec<(&'static str, RingEntry, Option<String>)> {
        let mut out = Vec::new();
        out.extend(self.binance.flush_drain("binance"));
        out.extend(self.brti.flush_drain("brti"));
        out.extend(self.rtds_chainlink.flush_drain("rtds_chainlink"));
        out.extend(self.rtds_binance.flush_drain("rtds_binance"));
        out.extend(self.chainlink.flush_drain("chainlink"));
        out.extend(self.deribit_iv.flush_drain("deribit_iv"));
        out.extend(self.deribit_iv_computed.flush_drain("deribit_iv_computed"));
        out.extend(self.deribit_ws.flush_drain("deribit_ws"));
        out.extend(self.hyperliquid.flush_drain("hyperliquid"));
        out.extend(self.hyperliquid_trades.flush_drain("hyperliquid_trades"));
        out.extend(self.hyperliquid_l2.flush_drain("hyperliquid_l2"));
        out.extend(self.kalshi_book.flush_drain("kalshi_book"));
        out.extend(self.kalshi_delta.flush_drain("kalshi_delta"));
        out.extend(self.kalshi_ticker.flush_drain("kalshi_ticker"));
        out.extend(self.poly_book.flush_drain("poly_book"));
        out.extend(self.poly_bbo.flush_drain("poly_bbo"));
        out.extend(self.poly_price.flush_drain("poly_price"));
        out.extend(self.poly_trade.flush_drain("poly_trade"));
        out.extend(self.poly_resolved.flush_drain("poly_resolved"));
        out
    }

    /// Total entries pending flush across all rings.
    pub fn total_pending_flush(&self) -> usize {
        self.binance.pending_flush()
            + self.brti.pending_flush()
            + self.rtds_chainlink.pending_flush()
            + self.rtds_binance.pending_flush()
            + self.chainlink.pending_flush()
            + self.deribit_iv.pending_flush()
            + self.deribit_iv_computed.pending_flush()
            + self.deribit_ws.pending_flush()
            + self.hyperliquid.pending_flush()
            + self.hyperliquid_trades.pending_flush()
            + self.hyperliquid_l2.pending_flush()
            + self.kalshi_book.pending_flush()
            + self.kalshi_delta.pending_flush()
            + self.kalshi_ticker.pending_flush()
            + self.poly_book.pending_flush()
            + self.poly_bbo.pending_flush()
            + self.poly_price.pending_flush()
            + self.poly_trade.pending_flush()
            + self.poly_resolved.pending_flush()
    }
}

impl Default for RingSet {
    fn default() -> Self {
        // delegate to new() but without Arc wrapping for Default impl
        Self {
            binance: SourceRing::new(300, 300.0, false),
            brti: SourceRing::new(100, 300.0, false),
            rtds_chainlink: SourceRing::new(100, 300.0, false),
            rtds_binance: SourceRing::new(100, 300.0, false),
            chainlink: SourceRing::new(32, 300.0, false),
            deribit_iv: SourceRing::new(32, 300.0, false),
            deribit_iv_computed: SourceRing::new(32, 300.0, false),
            deribit_ws: SourceRing::new(1500, 300.0, false),
            hyperliquid: SourceRing::new(64, 60.0, false),
            hyperliquid_trades: SourceRing::new(300, 60.0, false),
            hyperliquid_l2: SourceRing::new(64, 60.0, false),
            kalshi_book: SourceRing::new(180, 30.0, true),
            kalshi_delta: SourceRing::new(180, 30.0, true),
            kalshi_ticker: SourceRing::new(180, 30.0, true),
            poly_book: SourceRing::new(300, 30.0, true),
            poly_bbo: SourceRing::new(300, 30.0, true),
            poly_price: SourceRing::new(2400, 30.0, true),
            poly_trade: SourceRing::new(600, 30.0, true),
            poly_resolved: SourceRing::new(32, 30.0, true),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- RingEntry -----------------------------------------------------------

    #[test]
    fn entry_size_is_160_bytes() {
        assert_eq!(std::mem::size_of::<RingEntry>(), 160);
    }

    #[test]
    fn entry_default_is_zero() {
        let e = RingEntry::default();
        assert_eq!(e.seq, 0);
        assert_eq!(e.ts, 0.0);
        assert_eq!(e.value, 0.0);
        assert_eq!(e.meta_len, 0);
        assert_eq!(e.meta, [0u8; META_CAP]);
    }

    #[test]
    fn entry_meta_str_empty() {
        let e = RingEntry::default();
        assert!(e.meta_str().is_none());
    }

    #[test]
    fn entry_meta_str_decode() {
        let mut e = RingEntry::default();
        let s = b"{\"v\":1.0}";
        e.meta[..s.len()].copy_from_slice(s);
        e.meta_len = s.len() as u32;
        assert_eq!(e.meta_str(), Some("{\"v\":1.0}"));
    }

    #[test]
    fn entry_is_fresh_new() {
        let now = 1_700_000_100.0_f64;
        let e = RingEntry {
            ts: now - 10.0,
            value: 1.0,
            ..Default::default()
        };
        assert!(e.is_fresh(now, 300.0));
    }

    #[test]
    fn entry_is_fresh_stale() {
        let now = 1_700_000_100.0_f64;
        let e = RingEntry {
            ts: now - 400.0,
            value: 1.0,
            ..Default::default()
        };
        assert!(!e.is_fresh(now, 300.0));
    }

    #[test]
    fn entry_is_fresh_zero_ts() {
        let e = RingEntry::default(); // ts = 0.0
        assert!(!e.is_fresh(1_700_000_000.0, 300.0));
    }

    // -- SourceRing basic ----------------------------------------------------

    #[test]
    fn empty_ring_head_is_none() {
        let ring = SourceRing::new(16, 300.0, false);
        assert!(ring.head().is_none());
    }

    #[test]
    fn write_and_head_roundtrip() {
        let ring = SourceRing::new(16, 300.0, false);
        ring.write(1_700_000_000.0, 95123.45, b"{\"q\":\"0.001\"}", None);
        let e = ring.head().unwrap();
        assert_eq!(e.seq, 0); // first write, write_idx was 0
        assert_eq!(e.ts, 1_700_000_000.0);
        assert_eq!(e.value, 95123.45);
        assert_eq!(e.meta_str(), Some("{\"q\":\"0.001\"}"));
    }

    #[test]
    fn seq_increments_with_writes() {
        let ring = SourceRing::new(16, 300.0, false);
        ring.write(1.0, 1.0, b"", None);
        ring.write(2.0, 2.0, b"", None);
        ring.write(3.0, 3.0, b"", None);
        let e = ring.head().unwrap();
        assert_eq!(e.seq, 2); // third write, write_idx was 2
    }

    #[test]
    fn seq_survives_ring_wrap() {
        // capacity = 4, write 6 entries — seq should be 5 for the last entry
        let ring = SourceRing::new(4, 300.0, false);
        for i in 0..6u64 {
            ring.write(i as f64, i as f64, b"", None);
        }
        let e = ring.head().unwrap();
        assert_eq!(e.seq, 5); // 6th write, write_idx was 5
        assert_eq!(e.ts, 5.0);
    }

    #[test]
    fn head_returns_latest_after_multiple_writes() {
        let ring = SourceRing::new(16, 300.0, false);
        ring.write(1.0, 1.0, b"a", None);
        ring.write(2.0, 2.0, b"b", None);
        ring.write(3.0, 3.0, b"c", None);
        let e = ring.head().unwrap();
        assert_eq!(e.ts, 3.0);
        assert_eq!(e.value, 3.0);
    }

    #[test]
    fn ring_wraps_correctly_oldest_evicted() {
        // capacity = 4, write 5 entries — first should be evicted
        let ring = SourceRing::new(4, 300.0, false);
        for i in 0..5_u64 {
            ring.write(i as f64, i as f64, b"", None);
        }
        // head should be the 5th entry (ts=4.0)
        assert_eq!(ring.head().unwrap().ts, 4.0);
        assert_eq!(ring.write_count(), 5);
    }

    #[test]
    fn write_count_monotonically_increases() {
        let ring = SourceRing::new(16, 300.0, false);
        assert_eq!(ring.write_count(), 0);
        ring.write(1.0, 1.0, b"", None);
        assert_eq!(ring.write_count(), 1);
        ring.write(2.0, 2.0, b"", None);
        assert_eq!(ring.write_count(), 2);
    }

    #[test]
    fn meta_truncated_at_meta_cap() {
        let ring = SourceRing::new(16, 300.0, false);
        let big_meta = vec![b'x'; META_CAP + 50];
        ring.write(1.0, 1.0, &big_meta, None);
        let e = ring.head().unwrap();
        assert_eq!(e.meta_len as usize, META_CAP);
        // All bytes within META_CAP should be 'x'
        assert!(e.meta.iter().all(|&b| b == b'x'));
    }

    #[test]
    fn meta_remainder_zeroed_after_shorter_write() {
        let ring = SourceRing::new(16, 300.0, false);
        // Write a long entry first
        ring.write(1.0, 1.0, &[b'A'; 100], None);
        // Then a shorter one into the same slot would only happen after wrap;
        // but we can write two entries and check the second one's meta is clean
        ring.write(2.0, 2.0, b"short", None);
        let e = ring.head().unwrap();
        assert_eq!(e.meta_len, 5);
        assert_eq!(&e.meta[..5], b"short");
        // Remainder should be zeros, not leftover from previous entry
        assert!(e.meta[5..].iter().all(|&b| b == 0));
    }

    // -- Ticker index --------------------------------------------------------

    #[test]
    fn no_ticker_index_returns_none() {
        let ring = SourceRing::new(16, 300.0, false); // no index
        assert!(ring.get_by_ticker("FOO", 1_700_000_000.0).is_none());
    }

    #[test]
    fn ticker_index_write_and_get() {
        let ring = SourceRing::new(16, 300.0, true);
        let now = 1_700_000_000.0_f64;
        ring.write(now, 0.48, b"{\"best_ask\":0.50}", Some("KXBTCD-T70000"));
        let e = ring.get_by_ticker("KXBTCD-T70000", now + 1.0).unwrap();
        assert_eq!(e.value, 0.48);
        assert_eq!(e.meta_str(), Some("{\"best_ask\":0.50}"));
    }

    #[test]
    fn ticker_index_unknown_ticker_returns_none() {
        let ring = SourceRing::new(16, 300.0, true);
        assert!(ring.get_by_ticker("NOTEXIST", 1_700_000_000.0).is_none());
    }

    #[test]
    fn ticker_index_stale_returns_none() {
        let ring = SourceRing::new(16, 30.0, true); // 30s window
        let old_ts = 1_700_000_000.0_f64;
        ring.write(old_ts, 0.48, b"", Some("KXBTCD-T70000"));
        // Query 60s later — entry is outside 30s window
        let now = old_ts + 60.0;
        assert!(ring.get_by_ticker("KXBTCD-T70000", now).is_none());
    }

    #[test]
    fn ticker_index_lapped_returns_none_aba_guard() {
        // capacity = 4, write 5 entries for same ticker — ring laps
        let ring = SourceRing::new(4, 300.0, true);
        let now = 1_700_000_000.0_f64;
        ring.write(now, 0.48, b"", Some("KXBTCD-T70000"));
        // Overwrite with different tickers to lap the first slot
        for i in 1..5u64 {
            ring.write(now + i as f64, i as f64, b"", Some("OTHER"));
        }
        // The indexed slot for KXBTCD-T70000 has been lapped.
        // ABA guard: entry.seq (now written by OTHER at write_idx=4) != mono_idx (0).
        assert!(ring.get_by_ticker("KXBTCD-T70000", now + 5.0).is_none());
    }

    #[test]
    fn ticker_index_multiple_markets_independent() {
        let ring = SourceRing::new(64, 300.0, true);
        let now = 1_700_000_000.0_f64;
        ring.write(now, 0.40, b"a", Some("MARKET-A"));
        ring.write(now, 0.60, b"b", Some("MARKET-B"));
        ring.write(now, 0.80, b"c", Some("MARKET-C"));

        let a = ring.get_by_ticker("MARKET-A", now + 1.0).unwrap();
        let b = ring.get_by_ticker("MARKET-B", now + 1.0).unwrap();
        let c = ring.get_by_ticker("MARKET-C", now + 1.0).unwrap();
        assert_eq!(a.value, 0.40);
        assert_eq!(b.value, 0.60);
        assert_eq!(c.value, 0.80);
    }

    #[test]
    fn ticker_index_updated_on_each_write() {
        let ring = SourceRing::new(16, 300.0, true);
        let now = 1_700_000_000.0_f64;
        ring.write(now, 0.40, b"", Some("KXBTCD-T70000"));
        ring.write(now + 1.0, 0.42, b"", Some("KXBTCD-T70000"));
        ring.write(now + 2.0, 0.45, b"", Some("KXBTCD-T70000"));
        // Should return the latest
        let e = ring.get_by_ticker("KXBTCD-T70000", now + 3.0).unwrap();
        assert_eq!(e.value, 0.45);
        assert_eq!(e.ts, now + 2.0);
    }

    // -- Flush drain ---------------------------------------------------------

    #[test]
    fn flush_drain_empty_ring_returns_empty() {
        let ring = SourceRing::new(16, 300.0, false);
        assert!(ring.flush_drain("binance").is_empty());
    }

    #[test]
    fn flush_drain_returns_all_entries() {
        let ring = SourceRing::new(16, 300.0, false);
        ring.write(1.0, 10.0, b"", None);
        ring.write(2.0, 20.0, b"", None);
        ring.write(3.0, 30.0, b"", None);
        let drained = ring.flush_drain("binance");
        assert_eq!(drained.len(), 3);
        assert!(drained.iter().all(|(src, _, _)| *src == "binance"));
        assert_eq!(drained[0].1.value, 10.0);
        assert_eq!(drained[1].1.value, 20.0);
        assert_eq!(drained[2].1.value, 30.0);
    }

    #[test]
    fn flush_drain_respects_cursor() {
        let ring = SourceRing::new(16, 300.0, false);
        ring.write(1.0, 1.0, b"", None);
        ring.write(2.0, 2.0, b"", None);
        let first = ring.flush_drain("binance");
        assert_eq!(first.len(), 2);

        ring.write(3.0, 3.0, b"", None);
        let second = ring.flush_drain("binance");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].1.value, 3.0);

        // Third drain with nothing new
        let third = ring.flush_drain("binance");
        assert!(third.is_empty());
    }

    #[test]
    fn flush_drain_lapped_advances_cursor() {
        // capacity = 4, write 6 entries — flusher drains after all 6
        let ring = SourceRing::new(4, 300.0, false);
        for i in 1..=6u64 {
            ring.write(i as f64, i as f64, b"", None);
        }
        // Flusher hasn't drained yet (cursor=0). Ring has lapped.
        // flush_drain should advance cursor to write_idx - capacity (= 2)
        // and return the 4 available entries (entries 3..=6).
        let drained = ring.flush_drain("binance");
        assert_eq!(drained.len(), 4);
        // Entries 1 and 2 were missed
        assert_eq!(drained[0].1.ts, 3.0);
        assert_eq!(drained[3].1.ts, 6.0);
    }

    #[test]
    fn pending_flush_counts_correctly() {
        let ring = SourceRing::new(16, 300.0, false);
        assert_eq!(ring.pending_flush(), 0);
        ring.write(1.0, 1.0, b"", None);
        ring.write(2.0, 2.0, b"", None);
        assert_eq!(ring.pending_flush(), 2);
        ring.flush_drain("binance");
        assert_eq!(ring.pending_flush(), 0);
    }

    // -- RingSet -------------------------------------------------------------

    #[test]
    fn ringset_new_allocates_all_19_rings() {
        let rings = RingSet::new();
        // Spot check capacities
        assert_eq!(rings.binance.capacity, 300);
        assert_eq!(rings.deribit_ws.capacity, 1500);
        assert_eq!(rings.poly_price.capacity, 2400);
        assert_eq!(rings.poly_resolved.capacity, 32);
    }

    #[test]
    fn ringset_drain_all_collects_from_all_rings() {
        let rings = RingSet::new();
        rings.binance.write(1.0, 95000.0, b"", None);
        rings.brti.write(2.0, 94990.0, b"", None);
        rings
            .kalshi_ticker
            .write(3.0, 0.48, b"", Some("KXBTCD-T70000"));

        let drained = rings.drain_all();
        assert_eq!(drained.len(), 3);

        let sources: Vec<&str> = drained.iter().map(|(s, _, _)| *s).collect();
        assert!(sources.contains(&"binance"));
        assert!(sources.contains(&"brti"));
        assert!(sources.contains(&"kalshi_ticker"));
    }

    #[test]
    fn ringset_total_pending_flush() {
        let rings = RingSet::new();
        assert_eq!(rings.total_pending_flush(), 0);
        rings.binance.write(1.0, 1.0, b"", None);
        rings.binance.write(2.0, 2.0, b"", None);
        rings.deribit_ws.write(3.0, 50.0, b"", None);
        assert_eq!(rings.total_pending_flush(), 3);
    }

    #[test]
    fn ringset_approximate_memory_footprint() {
        // Total slots across all 19 rings
        let total_slots: usize = RING_SPECS.iter().map(|(_, cap, _, _)| cap).sum();
        let total_bytes = total_slots * std::mem::size_of::<RingEntry>();
        // Should be well under 2 MB
        assert!(
            total_bytes < 2 * 1024 * 1024,
            "ring memory {total_bytes} bytes exceeds 2 MB"
        );
        // Should be at least 800 KB (our estimate was ~1 MB)
        assert!(
            total_bytes > 800 * 1024,
            "ring memory {total_bytes} bytes seems too small — check capacities"
        );
    }

    // -- Thread-safety -------------------------------------------------------

    #[test]
    fn write_from_one_thread_read_from_another() {
        let ring = Arc::new(SourceRing::new(64, 300.0, false));
        let writer = ring.clone();
        let reader = ring.clone();

        let handle = std::thread::spawn(move || {
            for i in 0..100u64 {
                writer.write(i as f64, i as f64, b"", None);
            }
        });
        handle.join().unwrap();

        // After writer finishes, reader should see the last entry
        let e = reader.head().unwrap();
        assert_eq!(e.ts, 99.0);
        assert_eq!(e.value, 99.0);
        assert_eq!(reader.write_count(), 100);
    }

    #[test]
    fn ringset_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RingSet>();
        assert_send_sync::<Arc<RingSet>>();
    }
}
