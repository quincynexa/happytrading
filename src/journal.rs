use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::Result;
use serde::Serialize;

/// JSONL record written on every bar-close evaluation.
#[derive(Serialize)]
pub struct BarScoreRecord {
    pub ts: i64,
    pub symbol: String,
    pub open_time: i64,
    pub close_price: f64,
    pub composite: f64,
    pub trend: Option<f64>,
    pub momentum: Option<f64>,
    pub volatility: Option<f64>,
    pub hl_native: Option<f64>,
    pub funding: Option<f64>,
    pub action: String,
}

/// JSONL record written on every trade event (open or close).
#[derive(Serialize)]
pub struct TradeRecord {
    pub ts: i64,
    pub symbol: String,
    pub event: String,
    pub direction: String,
    pub price: f64,
    pub fill_price: f64,
    pub composite: f64,
    pub atr: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

pub struct JournalWriter {
    scores: BufWriter<File>,
    trades: BufWriter<File>,
}

impl JournalWriter {
    pub fn new(dir: &str) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let scores = open_append(Path::new(dir).join("scores.jsonl"))?;
        let trades = open_append(Path::new(dir).join("trades.jsonl"))?;
        Ok(Self {
            scores: BufWriter::new(scores),
            trades: BufWriter::new(trades),
        })
    }

    pub fn write_score(&mut self, record: &BarScoreRecord) {
        if let Ok(line) = serde_json::to_string(record) {
            let _ = writeln!(self.scores, "{}", line);
            let _ = self.scores.flush();
        }
    }

    pub fn write_trade(&mut self, record: &TradeRecord) {
        if let Ok(line) = serde_json::to_string(record) {
            let _ = writeln!(self.trades, "{}", line);
            let _ = self.trades.flush();
        }
    }
}

fn open_append(path: impl AsRef<Path>) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?)
}
