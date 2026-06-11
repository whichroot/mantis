# Mantis

Live data collector and encrypted relay for BTC binary prediction markets.
Ingests real-time feeds from 19 sources across 8 WebSocket connections,
computes the Omega scalar field, and evaluates whether money can be made
on Kalshi and Polymarket up/down binaries.

A policy network trained on a tensor is separate architecture and required to provide RiskConfig weights.

## Architecture

```
Feeds (8 WS + REST fallback, 19 sources)
    │ write (lock-free, zero-alloc)
    ▼
Per-source ring buffers (~1 MB total, pre-allocated)
    │ head() / get_by_ticker()      │ flush_drain()
    ▼                                ▼
Kernel (hot path)              Flusher → SQLite → WS Relay → Archive
```

The kernel reads rings directly; no SQLite in the hot path.

### Data sources

| Source | Transport | Data |
|--------|-----------|------|
| Binance | WebSocket | BTC/USDT spot (aggTrade) |
| Deribit | WebSocket + REST fallback | Mark IV, greeks, mark price |
| BRTI | WebSocket | CF Benchmarks 60s trailing avg (Kalshi oracle) |
| Chainlink | HTTP (REST) | DON consensus price (Polymarket oracle) |
| RTDS | WebSocket | Chainlink + Binance via Polymarket live data |
| Hyperliquid | WebSocket | Perp ctx, L2 book, trades |
| Kalshi | WebSocket + REST | Orderbook delta, ticker, market discovery |
| Polymarket | WebSocket + REST | CLOB book, BBO, price changes, trades, resolution |

### Kernel

Four files, clean dependency chain:

```
math ← risk ← batch
       normalizer ← (reads rings, produces typed inputs for risk/math)
```

- **`math.rs`** -- 25 parameter-free physics formulas. Pure `f64 → f64`.
- **`risk.rs`** -- `RiskConfig` (12 tunable knobs) + domain types + `omega_at`.
- **`batch.rs`** -- Sequential batch operations for throughput.
- **`normalizer.rs`** -- Type boundary. `RingEntry → MarketInputs`.

### Venue modules

Kalshi (`venue/kalshi.rs`) and Polymarket (`venue/polymarket.rs`) are
independent modules. They share no trait, no types, no interface.
`main.rs` calls the kernel with numbers the venue modules provide.

### Project layout

```
src/
  main.rs          Startup, feed flusher, progress loop
  lib.rs           Crate root
  ring.rs          Lock-free ring buffers (19 per-source rings)
  db.rs            SQLite schema + insert/query (rusqlite only)
  book.rs          L2 book side channel (mpsc → SQLite)
  ws_relay.rs      Encrypted WS relay (AES-256-GCM + HMAC auth)
  normalizer.rs    Ring → typed kernel inputs
  debug.rs         Debug utilities
  kernel/
    math.rs        Physics formulas (zero dependencies)
    risk.rs        Risk config + omega computation
    batch.rs       Batch kernel operations
feed/
  binance.rs       Binance aggTrade WS
  brti.rs          BRTI CF Benchmarks WS
  chainlink.rs     Chainlink HTTP poller
  deribit.rs       Deribit REST sigma fallback
  deribit_ws.rs    Deribit real-time WS
  hyperliquid.rs   Hyperliquid perp WS (3 channels)
  kalshi_ws.rs     Kalshi WS (RSA-PSS auth)
  polymarket_ws.rs Polymarket CLOB WS
  rtds.rs          RTDS live data WS
venue/
  kalshi.rs        Kalshi discovery, resolution, book parsing
  polymarket.rs    Polymarket discovery, resolution, book parsing
keys/
  encrypted/       SOPS-encrypted credentials
    kalshiid.enc   Kalshi API key ID
    bcn-read.enc   Kalshi RSA private key
    ws-auth.enc    WS relay shared secret
  age-key.txt      age decryption key
training/          Python training pipeline
data/              Local SQLite database
```

## Prerequisites

- **Rust** (edition 2024)
- **SOPS** (`brew install sops` / `apt install sops`)
- **age** (`brew install age` / `apt install age`)

## Secrets management with SOPS + age

All API credentials are encrypted at rest using
[SOPS](https://github.com/getsops/sops) with
[age](https://github.com/FiloSottile/age) as the backend.
No plaintext keys are ever committed to the repository.

### How it works

1. An **age keypair** provides the encryption identity. The public key is
   stored in `.sops.yaml`.

2. **Encrypted files** live in `keys/encrypted/` and are committed to the
   repo. They can only be decrypted with the matching age private key.

3. At **startup**, `main.rs` calls `load_sops_keys()` which shells out to
   `sops --decrypt` for each encrypted file, using `SOPS_AGE_KEY_FILE`
   to point at the age private key. Decrypted values are set as
   environment variables before any threads are spawned.

4. If `keys/age-key.txt` is missing or decryption fails, the system
   **degrades gracefully** -- Kalshi WS falls back to REST, the relay
   uses an empty secret. No crash, no panic.

### Encrypted credentials

| File | Env var | Purpose |
|------|---------|---------|
| `kalshiid.enc` | `KALSHI_API_KEY_ID` | Kalshi API key identifier |
| `bcn-read.enc` | `KALSHI_API_KEY_PRIVATE` | Kalshi RSA private key (RSA-PSS auth) |
| `ws-auth.enc` | `MANTIS_WS_SECRET` | WS relay pre-shared secret |

### Setting up keys for a new deployment

1. **Generate an age keypair** (one-time):

   ```sh
   age-keygen -o keys/age-key.txt
   ```

   This creates a file containing both the public and private key.
   The public key is printed to stderr -- note it for step 2.

2. **Configure SOPS** -- create or update `.sops.yaml` at the repo root:

   ```yaml
   creation_rules:
     - path_regex: .*
       age: <your-age-public-key>
   ```

3. **Encrypt a credential**:

   ```sh
   # From a file:
   sops --encrypt --input-type binary --output-type binary \
     --age <your-age-public-key> \
     plaintext-key.pem > keys/encrypted/bcn-read.enc

   # From stdin:
   echo -n "your-api-key-id" | \
     sops --encrypt --input-type binary --output-type binary \
     --age <your-age-public-key> /dev/stdin > keys/encrypted/kalshiid.enc
   ```

4. **Verify decryption works**:

   ```sh
   SOPS_AGE_KEY_FILE=keys/age-key.txt \
     sops --decrypt --input-type binary --output-type binary \
     keys/encrypted/kalshiid.enc
   ```

5. **Commit the encrypted files** (`keys/encrypted/*.enc`).
   Never commit `keys/age-key.txt` or any `*.dec` / `*.key` file.

### Rotating keys

To re-encrypt with a new age identity:

```sh
# Generate new keypair
age-keygen -o keys/age-key-new.txt

# Update .sops.yaml with the new public key, then:
for f in keys/encrypted/*.enc; do
  SOPS_AGE_KEY_FILE=keys/age-key.txt sops updatekeys "$f"
done

# Replace the old key
mv keys/age-key-new.txt keys/age-key.txt
```

### Manual override

If SOPS is unavailable, set the environment variables directly:

```sh
export KALSHI_API_KEY_ID="your-key-id"
export KALSHI_API_KEY_PRIVATE="$(cat /path/to/private-key.pem)"
export MANTIS_WS_SECRET="your-shared-secret"
cargo run
```

The startup code checks for existing env vars before attempting
SOPS decryption and skips files that are already set.

## Build and test

```sh
cargo test && cargo clippy
```

559 tests must pass with zero warnings before every commit.

## Running

```sh
# Default (uses data/beacon.db):
cargo run

# Custom database path:
MANTIS_DB=/path/to/beacon.db cargo run
```

The system performs NTP sync, decrypts credentials, initializes SQLite,
starts all feed WebSocket connections, and begins collecting data.
Progress is printed to stderr every 30 seconds.

## Design constraints

- **Rust only** (except `training/` which is Python).
- **rusqlite only** -- no external database drivers.
- **Data leaves via encrypted WebSocket only** -- no direct external DB writes.
- **No derived values stored** -- everything computable is computed at point of use.
- **NaN firewall** at every boundary. No module trusts upstream.
- **WS-first, REST fallback** -- never fabricate data.
- **The system has paper trading only (debug.rs), not live execution. No HTTP calls to any venue API.
- **To actually execute orders on Polymarket you would need:
    - Polymarket CLOB API keys (API key, API secret, API passphrase)
    - An Ethereum wallet with USDC on Polygon for signing and funding orders
    - The CLOB order endpoint (POST https://clob.polymarket.com/order) which requires HMAC-signed requests
