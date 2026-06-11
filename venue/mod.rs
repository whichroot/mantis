//! Venue infrastructure — trait and shared types for exchange clients.
//!
//! Each venue implements [`VenueClient`]. The main loop calls `sync_markets`
//! on a timer to discover new markets. Real-time data comes from WS feeds
//! (kalshi_ws, polymarket_ws) which write BBO to the ring buffer.

pub mod kalshi;
pub mod polymarket;

// Re-export shared DB types used by venue clients.
pub use crate::db::MarketRow;

// ---------------------------------------------------------------------------
// VenueClient trait
// ---------------------------------------------------------------------------

/// A prediction-market venue. Handles auth, rate-limits, and API specifics.
pub trait VenueClient: Send + 'static {
    /// Short identifier for this venue, e.g. "kalshi" or "polymarket".
    fn name(&self) -> &'static str;

    /// Discover and upsert active markets into the DB.
    ///
    /// Takes `db_path` rather than `&Connection` so implementations own their
    /// connection. Owned `rusqlite::Connection` is `Send`; `&Connection` is not
    /// (Connection is `!Sync`), which would make the future `!Send` and block
    /// `tokio::spawn`.
    fn sync_markets<'a>(
        &'a self,
        db_path: &'a str,
    ) -> impl std::future::Future<Output = anyhow::Result<usize>> + Send + 'a;
}
