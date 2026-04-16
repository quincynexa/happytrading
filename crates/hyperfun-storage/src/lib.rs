//! Postgres-backed persistence for hyperfun.
//!
//! See `docs/superpowers/specs/2026-04-14-postgres-storage-design.md`.

pub mod config;
pub mod ops;
pub mod handle;
pub mod journal;
pub mod writer;
pub mod loader;
pub mod rollup;

pub use config::parse_database_url_redacted;
pub use handle::StorageHandle;
pub use journal::JournalWriter;
pub use ops::{BarScoreRecord, WriteOp};
pub use writer::{spawn_reconnect_task, spawn_writer_task, StorageWriter};
pub use loader::{CandleCoverage, CooldownState, load_candles_bulk, load_cooldowns, load_positions, max_close_times};
pub use rollup::{rollup_daily_stats, run_retention, spawn_daily_task};

pub use hyperfun_core::{Candle, Position, TradeEvent, TradeRecord};
pub use hyperfun_core::config::StorageConfig;
