use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::Result;
use serde::Serialize;
use tracing;

use crate::ops::BarScoreRecord;
use hyperfun_core::{Candle, Position, TradeRecord};

#[derive(Serialize)]
pub struct PositionJournalEntry<'a> {
    pub event: &'a str, // 'open' | 'close' | 'flip_close' | 'flip_open' | 'stop_close'
    pub position: Option<&'a Position>,
    pub ts: i64,
}

#[derive(Serialize)]
pub struct CooldownJournalEntry<'a> {
    pub symbol: &'a str,
    pub cooldown_until_ts: i64,
    pub cooldown_bars: u32,
    pub updated_at: i64,
}

#[derive(Serialize)]
pub struct MarketSnapshotJournalEntry<'a> {
    pub ts: i64,
    pub symbol: Option<&'a str>,
    pub data_type: &'a str,
    pub payload: &'a serde_json::Value,
}

pub struct JournalWriter {
    scores: BufWriter<File>,
    trades: BufWriter<File>,
    positions: BufWriter<File>,
    cooldowns: BufWriter<File>,
    market_snapshots: BufWriter<File>,
    candles: BufWriter<File>,
}

impl JournalWriter {
    pub fn new(dir: &str) -> Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            scores: BufWriter::new(open_append(Path::new(dir).join("scores.jsonl"))?),
            trades: BufWriter::new(open_append(Path::new(dir).join("trades.jsonl"))?),
            positions: BufWriter::new(open_append(Path::new(dir).join("positions.jsonl"))?),
            cooldowns: BufWriter::new(open_append(Path::new(dir).join("cooldowns.jsonl"))?),
            market_snapshots: BufWriter::new(open_append(Path::new(dir).join("market_snapshots.jsonl"))?),
            candles: BufWriter::new(open_append(Path::new(dir).join("candles.jsonl"))?),
        })
    }

    pub fn write_score(&mut self, record: &BarScoreRecord) {
        write_line(&mut self.scores, record);
    }

    pub fn write_trade(&mut self, record: &TradeRecord) {
        write_line(&mut self.trades, record);
    }

    pub fn write_position(&mut self, event: &str, position: Option<&Position>, ts: i64) {
        write_line(&mut self.positions, &PositionJournalEntry { event, position, ts });
    }

    pub fn write_cooldown(&mut self, symbol: &str, cooldown_until_ts: i64, cooldown_bars: u32, updated_at: i64) {
        write_line(&mut self.cooldowns, &CooldownJournalEntry { symbol, cooldown_until_ts, cooldown_bars, updated_at });
    }

    pub fn write_market_snapshot(&mut self, data_type: &str, symbol: Option<&str>, ts: i64, payload: &serde_json::Value) {
        write_line(&mut self.market_snapshots, &MarketSnapshotJournalEntry { ts, symbol, data_type, payload });
    }

    pub fn write_candle(&mut self, candle: &Candle) {
        write_line(&mut self.candles, candle);
    }
}

fn write_line<T: Serialize, W: Write>(w: &mut W, value: &T) {
    match serde_json::to_string(value) {
        Ok(line) => {
            if let Err(e) = writeln!(w, "{}", line) {
                tracing::error!(error = %e, "JSONL write failed");
            } else if let Err(e) = w.flush() {
                tracing::error!(error = %e, "JSONL flush failed");
            }
        }
        Err(e) => tracing::error!(error = %e, "JSONL serialization failed"),
    }
}

fn open_append(path: impl AsRef<Path>) -> Result<File> {
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}
