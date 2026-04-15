use hyperfun_core::{Candle, Position, TradeRecord};
use serde::{Deserialize, Serialize};

/// Full score snapshot from one bar-close evaluation.
#[derive(Serialize, Deserialize, Debug, Clone)]
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

/// Operations consumed by the writer task. Each variant maps to exactly one
/// SQL transaction (for stateful variants) or one INSERT (non-stateful).
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Bar-close score row (non-stateful, ON CONFLICT DO NOTHING).
    BarScore(BarScoreRecord),

    /// Open a new position. Transaction: INSERT trades + INSERT positions.
    OpenTrade {
        trade: TradeRecord,
        position: Position,
    },

    /// Close a position. Transaction: INSERT trades + DELETE positions + UPSERT cooldowns.
    CloseTrade {
        trade: TradeRecord,
        symbol: String,
        cooldown_until: i64,
        cooldown_bars: u32,
    },

    /// Flip direction. Transaction: INSERT close trade + DELETE old position
    /// + INSERT open trade + INSERT new position + UPSERT cooldowns.
    FlipTrade {
        close_trade: TradeRecord,
        open_trade: TradeRecord,
        new_position: Position,
        cooldown_until: i64,
        cooldown_bars: u32,
    },

    /// Raw market data snapshot.
    MarketSnapshot {
        data_type: String,
        symbol: Option<String>,
        ts: i64,
        payload: serde_json::Value,
    },

    /// Closed candle, upserted into candle_cache.
    CandleClose(Candle),
}
