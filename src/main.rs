use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use mantis_hybrid::book::{self, BookEvent, BOOK_CHANNEL_CAP};
use mantis_hybrid::db::{self, ResolutionRow};
use mantis_hybrid::debug;
use mantis_hybrid::feed::{
    self, Feed, FeedRow, LiveState,
    binance::BinanceFeed,
    brti::BrtiFeed,
    chainlink::ChainlinkFeed,
    deribit_ws::DeribitWsFeed,
    hyperliquid::HyperliquidFeed,
    kalshi_ws::KalshiWsFeed,
    polymarket_ws::PolymarketWsFeed,
    rtds::RtdsFeed,
};
use mantis_hybrid::ring::RingSet;
use mantis_hybrid::venue::VenueClient;
use mantis_hybrid::venue::kalshi::{
    KalshiClient, check_resolution as kalshi_check_resolution,
};
use mantis_hybrid::venue::polymarket::{
    PolymarketClient, check_resolution as poly_check_resolution,
};
use mantis_hybrid::ws_relay;

// ---------------------------------------------------------------------------
// Feed flusher — drains ring buffers to local SQLite
// ---------------------------------------------------------------------------

/// Drains feed rows from the ring buffers and inserts them into local SQLite.
/// Opens its own Connection (rusqlite::Connection is !Send).
/// Runs until cancellation, then does one final drain.
async fn feed_flusher(
    db_path: String,
    rings: Arc<RingSet>,
    state: Arc<LiveState>,
    stop: CancellationToken,
) {
    let conn = match db::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[flusher] DB open error: {e}");
            return;
        }
    };

    loop {
        let timed_out = tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(5)) => true,
            () = stop.cancelled() => false,
        };

        // Drain all pending ring entries and convert to FeedRows
        let entries = rings.drain_all();
        if !entries.is_empty() {
            let batch: Vec<FeedRow> = entries
                .into_iter()
                .map(|(source, entry, ticker)| FeedRow {
                    ts: entry.ts,
                    source,
                    value: entry.value,
                    meta: entry.meta_str().map(|s| s.to_owned()),
                    ticker,
                })
                .collect();
            let n = batch.len();
            match db::insert_feeds(&conn, &batch) {
                Ok(_) => {
                    state
                        .feed_inserts
                        .fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("[flusher] insert error ({n} rows): {e}");
                    state.inc_errors();
                }
            }
        }

        if !timed_out {
            // Cancellation — one final drain before exit
            let final_entries = rings.drain_all();
            if !final_entries.is_empty() {
                let final_batch: Vec<FeedRow> = final_entries
                    .into_iter()
                    .map(|(source, entry, ticker)| FeedRow {
                        ts: entry.ts,
                        source,
                        value: entry.value,
                        meta: entry.meta_str().map(|s| s.to_owned()),
                        ticker,
                    })
                    .collect();
                let _ = db::insert_feeds(&conn, &final_batch);
            }
            break;
        }
    }

    eprintln!("[flusher] shutdown complete");
}

// ---------------------------------------------------------------------------
// Sigma updater — polls Deribit every 300s for implied volatility
// ---------------------------------------------------------------------------

/// Fetches sigma_1s from Deribit options, updates LiveState.
/// Writes deribit FeedRows to rings so they get flushed to DB.
async fn sigma_updater(
    state: Arc<LiveState>,
    rings: Arc<RingSet>,
    stop: CancellationToken,
) {
    let http = match reqwest::Client::builder()
        .user_agent("mantis-hybrid/0.1")
        .timeout(Duration::from_secs(20))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[sigma] HTTP client error: {e}");
            return;
        }
    };

    let interval = Duration::from_secs(300);

    loop {
        // Wait for at least one Binance tick before fetching sigma
        if state.binance_count.load(Ordering::Relaxed) == 0 {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
                () = stop.cancelled() => break,
            }
            continue;
        }

        let spot = state.binance_price.load();
        if let Some(sigma) = mantis_hybrid::feed::deribit::fetch_sigma(&http, spot, &rings).await {
            state.sigma_1s.store(sigma);
            state.sigma_ts.store(feed::wall_clock());
            eprintln!("[sigma] sigma_1s = {sigma:.2e}  (spot=${spot:.0})");
        }

        let cancelled = tokio::select! {
            () = tokio::time::sleep(interval) => false,
            () = stop.cancelled() => true,
        };
        if cancelled {
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Progress printer
// ---------------------------------------------------------------------------

async fn progress_printer(state: Arc<LiveState>, stop: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // skip first immediate tick

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            () = stop.cancelled() => break,
        }

        let bn = state.binance_count.load(Ordering::Relaxed);
        let brti = state.brti_count.load(Ordering::Relaxed);
        let cl = state.chainlink_count.load(Ordering::Relaxed);
        let hl = state.hl_count.load(Ordering::Relaxed);
        let ins = state.feed_inserts.load(Ordering::Relaxed);
        let errs = state.errors.load(Ordering::Relaxed);
        let spot = state.binance_price.load();
        let sigma = state.sigma_1s.load();

        eprintln!(
            "[status] spot=${spot:.0}  sigma={sigma:.2e}  \
             ticks: bn={bn} brti={brti} cl={cl} hl={hl}  \
             flushed={ins}  errors={errs}"
        );
    }
}

// ---------------------------------------------------------------------------
// Binance spot HTTP fetch (for sigma bootstrap)
// ---------------------------------------------------------------------------

/// One-shot HTTP fetch of current BTC/USDT spot from Binance REST API.
/// Used for sigma bootstrap before WebSocket feeds start.
async fn fetch_binance_spot_http(client: &reqwest::Client) -> Option<f64> {
    let url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT";
    let resp: serde_json::Value = client.get(url).send().await.ok()?.json().await.ok()?;
    resp.get("price")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|&p| p > 0.0 && p.is_finite())
}



// ---------------------------------------------------------------------------
// Resolution loop
// ---------------------------------------------------------------------------

/// Checks resolution candidates every 60 seconds and writes resolution records.
///
/// Opens its own Connection (rusqlite::Connection is !Send).
/// Periodically re-runs market discovery so new short-term markets (5m, 15m)
/// are found as Polymarket creates them.
///
/// Runs every 300s — enough to catch each 5m market before it expires.
/// Discovery functions are idempotent (upsert with COALESCE).
/// Passes current Binance spot to Polymarket discovery so btc-updown-*
/// markets receive a fallback strike (reference price at discovery time).
async fn discovery_loop(
    db_path: String,
    state: Arc<LiveState>,
    stop: CancellationToken,
) {
    let kalshi = KalshiClient::default();
    let poly = PolymarketClient::default();
    let interval = Duration::from_secs(300);

    loop {
        let cancelled = tokio::select! {
            () = tokio::time::sleep(interval) => false,
            () = stop.cancelled() => true,
        };
        if cancelled {
            break;
        }

        match kalshi.sync_markets(&db_path).await {
            Ok(n) => eprintln!("[discovery_loop] Kalshi: {n} markets upserted"),
            Err(e) => eprintln!("[discovery_loop] Kalshi error: {e}"),
        }

        let spot = state.binance_price.load();
        let fallback = if spot > 0.0 { Some(spot) } else { None };
        match poly.sync_markets_with_strike(&db_path, fallback).await {
            Ok(n) => eprintln!("[discovery_loop] Polymarket: {n} markets upserted"),
            Err(e) => eprintln!("[discovery_loop] Polymarket error: {e}"),
        }
    }
}

async fn resolution_loop(
    db_path: String,
    state: Arc<LiveState>,
    stop: CancellationToken,
) {
    let conn = match db::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[resolution] DB open error: {e}");
            return;
        }
    };

    let http = match reqwest::Client::builder()
        .user_agent("mantis-hybrid/0.1")
        .timeout(Duration::from_secs(15))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[resolution] HTTP client error: {e}");
            return;
        }
    };

    let interval = Duration::from_secs(60);

    loop {
        let cancelled = tokio::select! {
            () = tokio::time::sleep(interval) => false,
            () = stop.cancelled() => true,
        };
        if cancelled {
            break;
        }

        let candidates = match db::resolution_candidates(&conn) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[resolution] resolution_candidates error: {e}");
                state.inc_errors();
                continue;
            }
        };

        if candidates.is_empty() {
            continue;
        }

        eprintln!("[resolution] checking {} candidates", candidates.len());

        for market in &candidates {
            let (outcome, oracle_value_at_close, close_time_str, resolution_time_str) =
                if market.venue == "kalshi" {
                    tokio::time::sleep(Duration::from_millis(500)).await;

                    match kalshi_check_resolution(&http, &market.ticker).await {
                        Some(info) => (
                            info.outcome,
                            info.oracle_value_at_close,
                            info.close_time_str,
                            info.resolution_time_str,
                        ),
                        None => continue,
                    }
                } else if market.venue == "polymarket" {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    match poly_check_resolution(&http, &market.ticker).await {
                        Some(info) => (
                            info.outcome,
                            info.oracle_value_at_close,
                            info.close_time_str,
                            info.resolution_time_str,
                        ),
                        None => continue,
                    }
                } else {
                    continue;
                };

            // Parse close and resolution times
            let close_time = close_time_str
                .as_deref()
                .and_then(db::parse_iso_to_unix);
            let resolution_time = resolution_time_str
                .as_deref()
                .and_then(db::parse_iso_to_unix);

            // Compute resolution lag
            let lag_s = match (close_time, resolution_time) {
                (Some(ct), Some(rt)) => Some(rt - ct),
                _ => None,
            };

            // Get nearest Binance spot at close time
            let binance_at_close = close_time
                .and_then(|ct| db::binance_at_close(&conn, ct).ok().flatten());

            // Compute displacement: binance_at_close − oracle_value_at_close
            let displacement_at_close = match (binance_at_close, oracle_value_at_close) {
                (Some(b), Some(o)) => Some(b - o),
                _ => None,
            };

            let row = ResolutionRow {
                market_id: market.id,
                close_time,
                resolution_time,
                resolution_lag_s: lag_s,
                outcome: outcome.clone(),
                oracle_value_at_close,
                binance_at_close,
                displacement_at_close,
                meta: None,
            };

            match db::insert_resolution(&conn, &row) {
                Ok(true) => {
                    eprintln!(
                        "[resolution] resolved market {} ({}) → {:?}",
                        market.ticker, market.venue, outcome
                    );
                }
                Ok(false) => {} // already recorded
                Err(e) => {
                    eprintln!("[resolution] insert_resolution error for {}: {e}", market.ticker);
                    state.inc_errors();
                }
            }
        }
    }

    eprintln!("[resolution] shutdown complete");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SOPS key loading — decrypt credentials at startup
// ---------------------------------------------------------------------------

/// Decrypt SOPS-encrypted credentials and set env vars.
/// Falls back gracefully if sops/keys are unavailable.
fn load_sops_keys() {
    let age_key = "keys/age-key.txt";
    if !std::path::Path::new(age_key).exists() {
        eprintln!("[keys] age-key.txt not found — skipping SOPS decrypt");
        eprintln!("       (set KALSHI_API_KEY_ID and KALSHI_API_KEY_PRIVATE manually if needed)");
        return;
    }

    // Kalshi API key ID
    if std::env::var("KALSHI_API_KEY_ID").is_err() {
        match sops_decrypt(age_key, "keys/encrypted/kalshiid.enc") {
            Some(v) => {
                let v = v.trim().to_string();
                eprintln!("[keys] KALSHI_API_KEY_ID loaded ({} chars)", v.len());
                // SAFETY: called before any threads are spawned (single-threaded init).
                unsafe { std::env::set_var("KALSHI_API_KEY_ID", &v) };
            }
            None => eprintln!("[keys] kalshiid.enc decrypt failed — Kalshi WS will use REST fallback"),
        }
    } else {
        eprintln!("[keys] KALSHI_API_KEY_ID already set via env");
    }

    // Kalshi RSA private key
    if std::env::var("KALSHI_API_KEY_PRIVATE").is_err() {
        match sops_decrypt(age_key, "keys/encrypted/bcn-read.enc") {
            Some(v) => {
                eprintln!("[keys] KALSHI_API_KEY_PRIVATE loaded ({} bytes)", v.len());
                // SAFETY: called before any threads are spawned (single-threaded init).
                unsafe { std::env::set_var("KALSHI_API_KEY_PRIVATE", &v) };
            }
            None => eprintln!("[keys] bcn-read.enc decrypt failed — Kalshi WS will use REST fallback"),
        }
    } else {
        eprintln!("[keys] KALSHI_API_KEY_PRIVATE already set via env");
    }

    // WS relay shared secret
    if std::env::var("MANTIS_WS_SECRET").is_err() {
        match sops_decrypt(age_key, "keys/encrypted/ws-auth.enc") {
            Some(v) => {
                let v = v.trim().to_string();
                eprintln!("[keys] MANTIS_WS_SECRET loaded ({} chars)", v.len());
                // SAFETY: called before any threads are spawned (single-threaded init).
                unsafe { std::env::set_var("MANTIS_WS_SECRET", &v) };
            }
            None => eprintln!("[keys] ws-auth.enc decrypt failed — relay will use empty secret"),
        }
    } else {
        eprintln!("[keys] MANTIS_WS_SECRET already set via env");
    }
}

/// Run `sops --decrypt` on an encrypted file, return contents as String.
fn sops_decrypt(age_key_path: &str, enc_path: &str) -> Option<String> {
    if !std::path::Path::new(enc_path).exists() {
        return None;
    }
    let output = std::process::Command::new("sops")
        .args(["--decrypt", "--input-type", "binary", "--output-type", "binary", enc_path])
        .env("SOPS_AGE_KEY_FILE", age_key_path)
        .output()
        .ok()?;
    if output.status.success() {
        String::from_utf8(output.stdout).ok()
    } else {
        None
    }
}

#[tokio::main]
async fn main() {
    let db_path = std::env::var("MANTIS_DB").unwrap_or_else(|_| "data/beacon.db".to_string());

    eprintln!("{}", "=".repeat(60));
    eprintln!("  mantis-hybrid — live data collector + encrypted relay");
    eprintln!("  DB: {db_path}");
    eprintln!("{}", "=".repeat(60));
    eprintln!();

    // ── 0. NTP sync ─────────────────────────────────────────────────────
    feed::ntp_sync().await;
    eprintln!();

    // ── 0b. Load SOPS-encrypted credentials ─────────────────────────────
    load_sops_keys();
    eprintln!();

    // ── 1. Open / init local DB ─────────────────────────────────────────
    if let Some(parent) = std::path::Path::new(&db_path).parent()
        && !parent.as_os_str().is_empty()
    {
        let _ = std::fs::create_dir_all(parent);
    }

    eprintln!("[init] Opening database...");
    let conn = match db::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[FATAL] DB open failed: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("       {db_path}");
    eprintln!();

    let state: Arc<LiveState> = Arc::new(LiveState::default());
    let stop = CancellationToken::new();

    // Ctrl-C handler
    {
        let stop2 = stop.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                eprintln!("\n[ctrl+c] Stopping...");
                stop2.cancel();
            }
        });
    }

    // ── 2. Initial sigma bootstrap ──────────────────────────────────────
    eprintln!("[init] Fetching initial sigma_1s from Deribit...");
    let http_init = reqwest::Client::builder()
        .user_agent("mantis-hybrid/0.1")
        .timeout(Duration::from_secs(15))
        .build()
        .expect("reqwest::Client build");

    let initial_spot = match fetch_binance_spot_http(&http_init).await {
        Some(s) => s,
        None => {
            eprintln!("       [WARN] Binance spot unavailable — sigma bootstrap skipped");
            0.0
        }
    };

    // Allocate ring set before any feeds start
    let rings = RingSet::new();

    if initial_spot > 0.0 {
        if let Some(sigma) =
            mantis_hybrid::feed::deribit::fetch_sigma(&http_init, initial_spot, &rings).await
        {
            state.sigma_1s.store(sigma);
            state.sigma_ts.store(feed::wall_clock());
            eprintln!("       sigma_1s = {sigma:.2e}  (spot=${initial_spot:.0})");
        } else {
            eprintln!("       [WARN] Deribit IV unavailable — sigma will update on first tick");
        }
    }
    // Flush init rows from rings to DB
    {
        let init_entries = rings.drain_all();
        if !init_entries.is_empty() {
            let init_rows: Vec<FeedRow> = init_entries
                .into_iter()
                .map(|(source, entry, ticker)| FeedRow {
                    ts: entry.ts,
                    source,
                    value: entry.value,
                    meta: entry.meta_str().map(|s| s.to_owned()),
                    ticker,
                })
                .collect();
            let _ = db::insert_feeds(&conn, &init_rows);
        }
    }
    eprintln!();

    // ── 3. Market discovery ─────────────────────────────────────────────
    eprintln!("[init] Running market discovery...");
    {
        let kalshi = KalshiClient::default();
        let poly = PolymarketClient::default();

        match kalshi.sync_markets(&db_path).await {
            Ok(n) => eprintln!("[discovery] Kalshi: {n} markets upserted"),
            Err(e) => eprintln!("[discovery] Kalshi sync error: {e}"),
        }

        match poly.sync_markets(&db_path).await {
            Ok(n) => eprintln!("[discovery] Polymarket: {n} markets upserted"),
            Err(e) => eprintln!("[discovery] Polymarket sync error: {e}"),
        }
    }
    eprintln!();

    // ── 3b. Query discovered markets for WS feed subscriptions ──────────
    let (kalshi_tickers, poly_token_ids) = match db::open(&db_path) {
        Ok(q_conn) => {
            // Kalshi: active market tickers
            let kt: Vec<String> = db::active_markets(&q_conn)
                .unwrap_or_default()
                .iter()
                .filter(|m| m.venue == "kalshi")
                .map(|m| m.ticker.clone())
                .collect();

            // Polymarket: active markets with token_ids
            let pt: Vec<String> = db::active_markets(&q_conn)
                .unwrap_or_default()
                .iter()
                .filter(|m| m.venue == "polymarket")
                .filter_map(|m| m.token_id.clone())
                .collect();

            eprintln!(
                "[init] WS subscriptions: {} Kalshi tickers, {} Polymarket token_ids",
                kt.len(),
                pt.len()
            );
            (kt, pt)
        }
        Err(_) => (Vec::new(), Vec::new()),
    };
    eprintln!();

    // Drop the init-phase DB connection before spawning tasks.
    // Each task opens its own Connection (rusqlite is !Send).
    drop(conn);

    // ── 4. Spawn concurrent tasks ───────────────────────────────────────
    eprintln!("[run] Spawning feeds + relay...");
    eprintln!();

    let mut task_handles = Vec::new();

    // Spawn each feed individually (Feed trait uses impl Future → !dyn-compatible)
    macro_rules! spawn_feed {
        ($feed:expr) => {{
            let name = $feed.name();
            let r = Arc::clone(&rings);
            let st = Arc::clone(&state);
            let s = stop.clone();
            let h = tokio::spawn(async move {
                Box::new($feed).run(r, st, s).await;
                eprintln!("[feed] {name} exited");
            });
            task_handles.push(h);
        }};
    }
    spawn_feed!(BinanceFeed::new());
    spawn_feed!(BrtiFeed::new());
    spawn_feed!(ChainlinkFeed::new());
    spawn_feed!(RtdsFeed::new());
    spawn_feed!(HyperliquidFeed::new());

    // WS-first venue feeds (REST fallback via snapshot_loop)
    spawn_feed!(DeribitWsFeed::new());

    // Book side channel — full L2 depth to cold path
    let (book_tx, book_rx) = mpsc::channel::<BookEvent>(BOOK_CHANNEL_CAP);

    let poly_ws = PolymarketWsFeed::new(poly_token_ids, db_path.clone()).with_book_tx(book_tx.clone());
    spawn_feed!(poly_ws);
    let kalshi_ws = KalshiWsFeed::new(kalshi_tickers).with_book_tx(book_tx);
    spawn_feed!(kalshi_ws);

    // Feed flusher — drains rings to local SQLite
    {
        let path = db_path.clone();
        let r = Arc::clone(&rings);
        let st = Arc::clone(&state);
        let s = stop.clone();
        let h = tokio::spawn(feed_flusher(path, r, st, s));
        task_handles.push(h);
    }

    // Book flusher — drains L2 book channel to book_snapshots table
    {
        let path = db_path.clone();
        let st = Arc::clone(&state);
        let s = stop.clone();
        let h = tokio::spawn(book::book_flusher(path, book_rx, st, s));
        task_handles.push(h);
    }

    // Sigma updater — polls Deribit every 300s, rows flow through rings
    {
        let st = Arc::clone(&state);
        let r = Arc::clone(&rings);
        let s = stop.clone();
        let h = tokio::spawn(sigma_updater(st, r, s));
        task_handles.push(h);
    }

    // Progress printer
    {
        let st = Arc::clone(&state);
        let s = stop.clone();
        let h = tokio::spawn(progress_printer(st, s));
        task_handles.push(h);
    }

    // Discovery loop — re-runs market discovery every 300s
    // Catches new 5m/15m Polymarket markets as they're created.
    {
        let path = db_path.clone();
        let st = Arc::clone(&state);
        let s = stop.clone();
        let h = tokio::spawn(discovery_loop(path, st, s));
        task_handles.push(h);
    }

    // Resolution loop — checks for resolved markets every 60s
    {
        let path = db_path.clone();
        let st = Arc::clone(&state);
        let s = stop.clone();
        let h = tokio::spawn(resolution_loop(path, st, s));
        task_handles.push(h);
    }

    // WS relay — ships rows to mantis-archive via encrypted WebSocket
    {
        let relay_config = Arc::new(ws_relay::RelayConfig {
            url: std::env::var("MANTIS_WS_URL")
                .unwrap_or_else(|_| "wss://localhost:8787/ws".to_string()),
            shared_secret: std::env::var("MANTIS_WS_SECRET")
                .unwrap_or_default()
                .into_bytes(),
        });
        let s = stop.clone();
        let path = db_path.clone();
        let h = tokio::spawn(ws_relay::run_relay(relay_config, path, s));
        task_handles.push(h);
    }

    // ── 4b. Paper trader (optional, gated behind --paper) ───────────────
    let paper = std::env::args().any(|a| a == "--paper");
    if paper {
        eprintln!("[init] Paper trader enabled");
        let path = db_path.clone();
        let r = Arc::clone(&rings);
        let st = Arc::clone(&state);
        let s = stop.clone();
        let h = tokio::spawn(debug::run_paper_trader(path, r, st, s));
        task_handles.push(h);
    }

    // ── 5. Wait for all tasks ───────────────────────────────────────────
    for h in task_handles {
        let _ = h.await;
    }

    // ── 6. Shutdown summary ─────────────────────────────────────────────
    let bn = state.binance_count.load(Ordering::Relaxed);
    let brti = state.brti_count.load(Ordering::Relaxed);
    let cl = state.chainlink_count.load(Ordering::Relaxed);
    let hl = state.hl_count.load(Ordering::Relaxed);
    let ins = state.feed_inserts.load(Ordering::Relaxed);
    let errs = state.errors.load(Ordering::Relaxed);

    eprintln!();
    eprintln!("{}", "=".repeat(60));
    eprintln!("  Shutdown summary");
    eprintln!("  Binance: {bn}  BRTI: {brti}  Chainlink: {cl}  Hyperliquid: {hl}");
    eprintln!("  Flushed: {ins}  Errors: {errs}");
    eprintln!("{}", "=".repeat(60));
}


