//! Encrypted WebSocket relay — ships feed data to mantis-archive.
//!
//! Topology: hybrid → CF Worker (WSS) → archive (TimescaleDB).
//! CF Worker is an auth gateway — it verifies the HMAC handshake then
//! passes encrypted frames through opaque. The archive decrypts.
//!
//! Protocol:
//! 1. WSS connect to CF Worker.
//! 2. Server sends 32-byte random nonce.
//! 3. Client sends HMAC-SHA256(shared_secret, nonce).
//! 4. Server verifies. Connection authenticated.
//! 5. Both derive session_key = HKDF-SHA256(shared_secret, nonce, "mantis-ws-v1").
//! 6. All subsequent frames: 12-byte GCM nonce ‖ ciphertext ‖ 16-byte tag.
//!    Plaintext = JSON row.
//!
//! The relay reads from local SQLite (the primary store), encrypts, and
//! sends. On disconnect, it reconnects with backoff. Collection is never
//! blocked by the relay.

use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, AeadCore};
use futures_util::{SinkExt, StreamExt};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio_util::sync::CancellationToken;

use crate::feed::Backoff;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// HKDF info string — binds the derived key to this protocol version.
const HKDF_INFO: &[u8] = b"mantis-ws-v1";

/// Expected nonce length from the server (bytes).
const NONCE_LEN: usize = 32;

/// HMAC-SHA256 output length (bytes). Used in handshake validation.
#[cfg(test)]
const HMAC_LEN: usize = 32;

/// Relay poll interval: how often to check for un-shipped rows (seconds).
const POLL_INTERVAL_SECS: f64 = 5.0;

/// Maximum rows to ship per batch. Reserved for the shipping loop.
#[allow(dead_code)]
const BATCH_SIZE: usize = 100;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the WS relay.
#[derive(Clone)]
pub struct RelayConfig {
    /// WSS URL of the CF Worker endpoint.
    pub url: String,
    /// Pre-shared secret (decrypted from SOPS at startup).
    pub shared_secret: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Session crypto — derived per connection
// ---------------------------------------------------------------------------

/// A live encrypted session. Created after successful HMAC handshake.
pub struct Session {
    cipher: Aes256Gcm,
}

impl Session {
    /// Derive a session from the shared secret and the handshake nonce.
    ///
    /// session_key = HKDF-SHA256(ikm=shared_secret, salt=nonce, info="mantis-ws-v1")
    fn derive(shared_secret: &[u8], nonce: &[u8]) -> anyhow::Result<Self> {
        let hk = Hkdf::<Sha256>::new(Some(nonce), shared_secret);
        let mut key_bytes = [0u8; 32];
        hk.expand(HKDF_INFO, &mut key_bytes)
            .map_err(|e| anyhow::anyhow!("HKDF expand failed: {e}"))?;
        let cipher = Aes256Gcm::new_from_slice(&key_bytes)
            .map_err(|e| anyhow::anyhow!("AES-256-GCM key init failed: {e}"))?;
        Ok(Self { cipher })
    }

    /// Encrypt a plaintext message for transmission.
    ///
    /// Returns: 12-byte GCM nonce ‖ ciphertext ‖ 16-byte tag.
    pub fn encrypt(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| anyhow::anyhow!("AES-GCM encrypt failed: {e}"))?;
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a received frame.
    ///
    /// Input: 12-byte GCM nonce ‖ ciphertext ‖ 16-byte tag.
    pub fn decrypt(&self, frame: &[u8]) -> anyhow::Result<Vec<u8>> {
        if frame.len() < 12 + 16 {
            anyhow::bail!("frame too short: {} bytes", frame.len());
        }
        let (nonce_bytes, ciphertext) = frame.split_at(12);
        let nonce = aes_gcm::Nonce::from_slice(nonce_bytes);
        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow::anyhow!("AES-GCM decrypt failed: {e}"))?;
        Ok(plaintext)
    }
}

// ---------------------------------------------------------------------------
// HMAC handshake
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256(shared_secret, nonce) for the challenge-response.
pub fn compute_hmac(shared_secret: &[u8], nonce: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(shared_secret)
        .expect("HMAC accepts any key length");
    mac.update(nonce);
    mac.finalize().into_bytes().to_vec()
}

/// Verify an HMAC-SHA256 response against the expected value.
pub fn verify_hmac(shared_secret: &[u8], nonce: &[u8], response: &[u8]) -> bool {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(shared_secret)
        .expect("HMAC accepts any key length");
    mac.update(nonce);
    mac.verify_slice(response).is_ok()
}

// ---------------------------------------------------------------------------
// Relay task
// ---------------------------------------------------------------------------

/// Run the WS relay. Spawned as an async task in main.rs.
///
/// Connects to the CF Worker, authenticates via HMAC, derives a session key,
/// and ships feed rows from local SQLite. Reconnects with backoff on failure.
/// Never blocks the collection loop.
pub async fn run_relay(
    config: Arc<RelayConfig>,
    db_path: String,
    stop: CancellationToken,
) {
    // Open own DB connection for reading un-shipped rows.
    let conn = match crate::db::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ws_relay] DB open error: {e}");
            return;
        }
    };

    let mut feed_watermark: i64 = 0;
    let mut book_watermark: i64 = 0;
    let mut backoff = Backoff::new(2.0, 60.0);

    loop {
        if stop.is_cancelled() {
            eprintln!("[ws_relay] shutting down");
            break;
        }

        // ── Connect ──────────────────────────────────────────────────────
        let ws = match tokio_tungstenite::connect_async(&config.url).await {
            Ok((stream, _)) => {
                backoff.reset();
                eprintln!("[ws_relay] connected to {}", config.url);
                stream
            }
            Err(e) => {
                eprintln!("[ws_relay] connect error: {e}");
                backoff.wait(&stop).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        // ── HMAC handshake ───────────────────────────────────────────────
        let nonce = match recv_nonce(&mut read, &stop).await {
            Some(n) => n,
            None => {
                eprintln!("[ws_relay] handshake failed: no nonce received");
                backoff.wait(&stop).await;
                continue;
            }
        };

        let hmac_response = compute_hmac(&config.shared_secret, &nonce);
        if let Err(e) = write
            .send(tokio_tungstenite::tungstenite::Message::Binary(
                hmac_response.into(),
            ))
            .await
        {
            eprintln!("[ws_relay] handshake send error: {e}");
            backoff.wait(&stop).await;
            continue;
        }

        match recv_ack(&mut read, &stop).await {
            Some(true) => {}
            _ => {
                eprintln!("[ws_relay] handshake rejected by server");
                backoff.wait(&stop).await;
                continue;
            }
        }

        let session = match Session::derive(&config.shared_secret, &nonce) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[ws_relay] session key derivation failed: {e}");
                backoff.wait(&stop).await;
                continue;
            }
        };

        eprintln!("[ws_relay] authenticated, session established");

        // ── Ship loop ────────────────────────────────────────────────────
        // Reads un-shipped rows from local SQLite, encrypts, sends.
        // Advances watermark only after successful send.
        let mut ship_interval = tokio::time::interval(
            std::time::Duration::from_secs_f64(POLL_INTERVAL_SECS),
        );
        ship_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut alive = true;
        while alive {
            tokio::select! {
                _ = ship_interval.tick() => {
                    // ── Ship feed rows ───────────────────────────────
                    match crate::db::unshipped_feeds(&conn, feed_watermark, BATCH_SIZE) {
                        Ok(rows) if !rows.is_empty() => {
                            let max_id = rows.last().unwrap().0;
                            let mut batch = Vec::with_capacity(rows.len());
                            for (_, row) in &rows {
                                batch.push(serde_json::json!({
                                    "type": "feed",
                                    "ts": row.ts,
                                    "source": row.source,
                                    "value": row.value,
                                    "meta": row.meta.as_deref()
                                        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
                                }));
                            }
                            let payload = serde_json::json!({
                                "batch": "feeds",
                                "count": batch.len(),
                                "rows": batch,
                            });
                            match send_encrypted(&session, &mut write, &payload).await {
                                Ok(()) => {
                                    feed_watermark = max_id;
                                }
                                Err(e) => {
                                    eprintln!("[ws_relay] feed send error: {e}");
                                    alive = false;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[ws_relay] unshipped_feeds error: {e}");
                        }
                        _ => {}
                    }

                    // ── Ship book snapshots ──────────────────────────
                    if alive {
                        match crate::db::unshipped_books(&conn, book_watermark, BATCH_SIZE) {
                            Ok(rows) if !rows.is_empty() => {
                                let max_id = rows.last().unwrap().0;
                                let mut batch = Vec::with_capacity(rows.len());
                                for (_, ticker, row) in &rows {
                                    batch.push(serde_json::json!({
                                        "type": "book",
                                        "ts": row.ts,
                                        "ticker": ticker,
                                        "venue": row.venue,
                                        "best_bid": row.best_bid,
                                        "best_ask": row.best_ask,
                                        "spread": row.spread,
                                        "bid_depth": row.bid_depth,
                                        "ask_depth": row.ask_depth,
                                        "levels": row.levels.as_deref()
                                            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok()),
                                    }));
                                }
                                let payload = serde_json::json!({
                                    "batch": "books",
                                    "count": batch.len(),
                                    "rows": batch,
                                });
                                match send_encrypted(&session, &mut write, &payload).await {
                                    Ok(()) => {
                                        book_watermark = max_id;
                                    }
                                    Err(e) => {
                                        eprintln!("[ws_relay] book send error: {e}");
                                        alive = false;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[ws_relay] unshipped_books error: {e}");
                            }
                            _ => {}
                        }
                    }
                }
                msg = read.next() => {
                    match msg {
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data))) => {
                            // Server ACK — decrypt and log
                            match session.decrypt(&data) {
                                Ok(plaintext) => {
                                    if let Ok(ack) = serde_json::from_slice::<serde_json::Value>(&plaintext)
                                        && let Some(acked) = ack.get("acked").and_then(|v| v.as_u64())
                                    {
                                        eprintln!("[ws_relay] server acked {acked} rows");
                                    }
                                }
                                Err(e) => {
                                    eprintln!("[ws_relay] decrypt error: {e}");
                                    alive = false;
                                }
                            }
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                            eprintln!("[ws_relay] server closed connection");
                            alive = false;
                        }
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) |
                        Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {}
                        Some(Err(e)) => {
                            eprintln!("[ws_relay] ws error: {e}");
                            alive = false;
                        }
                        None => {
                            eprintln!("[ws_relay] stream ended");
                            alive = false;
                        }
                        _ => {}
                    }
                }
                () = stop.cancelled() => {
                    alive = false;
                }
            }
        }

        if !stop.is_cancelled() {
            eprintln!(
                "[ws_relay] disconnected, reconnecting in {}s",
                backoff.current as u64
            );
            backoff.wait(&stop).await;
        }
    }
}

/// Encrypt a JSON payload and send it as a binary WS frame.
async fn send_encrypted<S>(
    session: &Session,
    write: &mut S,
    payload: &serde_json::Value,
) -> anyhow::Result<()>
where
    S: SinkExt<tokio_tungstenite::tungstenite::Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let plaintext = payload.to_string();
    let frame = session.encrypt(plaintext.as_bytes())?;
    write
        .send(tokio_tungstenite::tungstenite::Message::Binary(frame.into()))
        .await
        .map_err(|e| anyhow::anyhow!("ws send: {e}"))
}

// ---------------------------------------------------------------------------
// Handshake helpers
// ---------------------------------------------------------------------------

/// Receive the server's nonce (first binary frame after connect).
async fn recv_nonce<S>(read: &mut S, stop: &CancellationToken) -> Option<Vec<u8>>
where
    S: StreamExt<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let msg = tokio::select! {
        m = tokio::time::timeout(std::time::Duration::from_secs(10), read.next()) => m,
        () = stop.cancelled() => return None,
    };

    match msg {
        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(data)))) => {
            if data.len() == NONCE_LEN {
                Some(data.to_vec())
            } else {
                eprintln!(
                    "[ws_relay] nonce wrong length: expected {NONCE_LEN}, got {}",
                    data.len()
                );
                None
            }
        }
        _ => None,
    }
}

/// Receive server ACK after HMAC response. Any non-close message = success.
async fn recv_ack<S>(read: &mut S, stop: &CancellationToken) -> Option<bool>
where
    S: StreamExt<Item = Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
{
    let msg = tokio::select! {
        m = tokio::time::timeout(std::time::Duration::from_secs(10), read.next()) => m,
        () = stop.cancelled() => return None,
    };

    match msg {
        Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))) => Some(false),
        Ok(Some(Ok(_))) => Some(true),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- HMAC (4 tests) ---------------------------------------------------

    #[test]
    fn hmac_deterministic() {
        let secret = b"test-secret-32-bytes-minimum!!!!";
        let nonce = b"random-nonce-32-bytes-exactly!!!!";
        let h1 = compute_hmac(secret, nonce);
        let h2 = compute_hmac(secret, nonce);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), HMAC_LEN);
    }

    #[test]
    fn hmac_different_nonce_different_output() {
        let secret = b"test-secret-32-bytes-minimum!!!!";
        let h1 = compute_hmac(secret, b"nonce-a-32-bytes-exactly!!!!!!!!");
        let h2 = compute_hmac(secret, b"nonce-b-32-bytes-exactly!!!!!!!!");
        assert_ne!(h1, h2);
    }

    #[test]
    fn hmac_verify_correct() {
        let secret = b"test-secret-32-bytes-minimum!!!!";
        let nonce = b"random-nonce-32-bytes-exactly!!!!";
        let response = compute_hmac(secret, nonce);
        assert!(verify_hmac(secret, nonce, &response));
    }

    #[test]
    fn hmac_verify_wrong_secret() {
        let secret = b"test-secret-32-bytes-minimum!!!!";
        let wrong = b"wrong-secret-32-bytes-minimum!!!";
        let nonce = b"random-nonce-32-bytes-exactly!!!!";
        let response = compute_hmac(secret, nonce);
        assert!(!verify_hmac(wrong, nonce, &response));
    }

    // -- Session key derivation (3 tests) ---------------------------------

    #[test]
    fn session_derive_succeeds() {
        let secret = b"shared-secret-for-test-purposes!";
        let nonce = b"session-nonce-32-bytes-exactly!!";
        let session = Session::derive(secret, nonce);
        assert!(session.is_ok());
    }

    #[test]
    fn session_different_nonce_different_key() {
        let secret = b"shared-secret-for-test-purposes!";
        let s1 = Session::derive(secret, b"nonce-a").unwrap();
        let s2 = Session::derive(secret, b"nonce-b").unwrap();
        // Encrypt the same plaintext — ciphertexts should differ
        let pt = b"test plaintext";
        let c1 = s1.encrypt(pt).unwrap();
        let c2 = s2.encrypt(pt).unwrap();
        // Different keys + different random GCM nonces → different ciphertexts
        assert_ne!(c1, c2);
    }

    #[test]
    fn session_different_secret_different_key() {
        let nonce = b"same-nonce-for-both-sessions!!!!";
        let s1 = Session::derive(b"secret-a-32-bytes-for-testing!!", nonce).unwrap();
        let s2 = Session::derive(b"secret-b-32-bytes-for-testing!!", nonce).unwrap();
        let pt = b"test";
        // s2 cannot decrypt what s1 encrypted
        let ct = s1.encrypt(pt).unwrap();
        assert!(s2.decrypt(&ct).is_err());
    }

    // -- Encrypt/decrypt roundtrip (5 tests) ------------------------------

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        let plaintext = b"hello mantis-archive";
        let encrypted = session.encrypt(plaintext).unwrap();
        let decrypted = session.decrypt(&encrypted).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn encrypt_decrypt_json_row() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        let row = serde_json::json!({
            "ts": 1710000000.12,
            "source": "binance",
            "value": 95123.45,
            "meta": {"trade_id": 123456789}
        });
        let plaintext = row.to_string();
        let encrypted = session.encrypt(plaintext.as_bytes()).unwrap();
        let decrypted = session.decrypt(&encrypted).unwrap();
        let recovered: serde_json::Value = serde_json::from_slice(&decrypted).unwrap();
        assert_eq!(recovered, row);
    }

    #[test]
    fn encrypt_decrypt_empty() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        let encrypted = session.encrypt(b"").unwrap();
        let decrypted = session.decrypt(&encrypted).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn decrypt_tampered_frame_fails() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        let mut encrypted = session.encrypt(b"secret data").unwrap();
        // Flip a byte in the ciphertext
        let mid = encrypted.len() / 2;
        encrypted[mid] ^= 0xFF;
        assert!(session.decrypt(&encrypted).is_err());
    }

    #[test]
    fn decrypt_short_frame_fails() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        // Less than 12 (nonce) + 16 (tag) = 28 bytes
        let short = vec![0u8; 20];
        assert!(session.decrypt(&short).is_err());
    }

    // -- Frame format (2 tests) -------------------------------------------

    #[test]
    fn encrypted_frame_has_nonce_prefix() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        let encrypted = session.encrypt(b"data").unwrap();
        // Frame = 12-byte GCM nonce + ciphertext + 16-byte tag
        // For 4 bytes of plaintext: 12 + 4 + 16 = 32 bytes minimum
        assert!(encrypted.len() >= 12 + 16);
    }

    #[test]
    fn each_encrypt_produces_unique_nonce() {
        let session = Session::derive(b"key-material", b"nonce").unwrap();
        let e1 = session.encrypt(b"same").unwrap();
        let e2 = session.encrypt(b"same").unwrap();
        // First 12 bytes (GCM nonce) should differ
        assert_ne!(&e1[..12], &e2[..12]);
        // Entire ciphertext differs too
        assert_ne!(e1, e2);
    }

    // -- Cross-session isolation (1 test) ---------------------------------

    #[test]
    fn cross_session_decrypt_fails() {
        let s1 = Session::derive(b"key", b"nonce-1").unwrap();
        let s2 = Session::derive(b"key", b"nonce-2").unwrap();
        let encrypted = s1.encrypt(b"session 1 data").unwrap();
        // s2 has a different session key — decryption must fail
        assert!(s2.decrypt(&encrypted).is_err());
    }

    // -- Handshake integration (2 tests) ----------------------------------

    #[test]
    fn full_handshake_flow() {
        // Simulate the complete handshake: server generates nonce, client
        // responds with HMAC, server verifies, both derive same session key.
        let shared_secret = b"production-grade-secret-key-here!";
        let server_nonce = b"server-generated-random-nonce!!!";

        // Client computes HMAC
        let client_response = compute_hmac(shared_secret, server_nonce);

        // Server verifies
        assert!(verify_hmac(shared_secret, server_nonce, &client_response));

        // Both derive session key — encrypt/decrypt must be symmetric
        let client_session = Session::derive(shared_secret, server_nonce).unwrap();
        let server_session = Session::derive(shared_secret, server_nonce).unwrap();

        let msg = b"test message from client";
        let encrypted = client_session.encrypt(msg).unwrap();
        let decrypted = server_session.decrypt(&encrypted).unwrap();
        assert_eq!(&decrypted, msg);

        // And the reverse direction
        let reply = b"ack from server";
        let encrypted_reply = server_session.encrypt(reply).unwrap();
        let decrypted_reply = client_session.decrypt(&encrypted_reply).unwrap();
        assert_eq!(&decrypted_reply, reply);
    }

    #[test]
    fn handshake_wrong_secret_no_decrypt() {
        let real_secret = b"real-secret-for-auth!!!!!!!!!!!!!";
        let fake_secret = b"attacker-secret-no-auth!!!!!!!!!";
        let nonce = b"server-nonce-32-bytes!!!!!!!!!!!!";

        // Attacker authenticates with wrong secret — server rejects
        let fake_hmac = compute_hmac(fake_secret, nonce);
        assert!(!verify_hmac(real_secret, nonce, &fake_hmac));

        // Even if attacker derives a session from their secret, they can't
        // decrypt messages encrypted with the real session key.
        let real_session = Session::derive(real_secret, nonce).unwrap();
        let fake_session = Session::derive(fake_secret, nonce).unwrap();

        let encrypted = real_session.encrypt(b"sensitive feed data").unwrap();
        assert!(fake_session.decrypt(&encrypted).is_err());
    }
}
