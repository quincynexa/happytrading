//! Postgres-backed persistence for hyperfun.
//!
//! See `docs/superpowers/specs/2026-04-14-postgres-storage-design.md`.

pub mod config;
pub mod ops;
pub mod handle;
pub mod writer;
pub mod loader;
pub mod rollup;

// Re-export core types used across the API
pub use hyperfun_core::{Candle, Position, TradeEvent, TradeRecord};
