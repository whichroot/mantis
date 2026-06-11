//! Pure kernel — 43 functions, 12 free parameters, zero I/O, zero state.
//!
//! Three files, one-way dependency: math ← risk ← batch.
//!
//! - `math`: 23 parameter-free physics formulas. f64 → f64.
//! - `risk`: RiskConfig (12 knobs) + domain types + 14 parameterized functions.
//! - `batch`: 6 sequential batch operations for throughput.

pub mod batch;
pub mod math;
pub mod risk;
