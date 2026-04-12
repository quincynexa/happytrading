use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Long,
    Short,
}

impl Direction {
    pub fn opposite(&self) -> Self {
        match self {
            Direction::Long => Direction::Short,
            Direction::Short => Direction::Long,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub symbol: String,
    pub interval: String,
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub num_trades: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingData {
    pub symbol: String,
    pub funding_rate: f64,
    pub predicted_rate: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OIData {
    pub symbol: String,
    pub open_interest: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HlpData {
    pub symbol: String,
    pub position_size: f64,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationData {
    pub symbol: String,
    pub direction: Direction,
    pub size: f64,
    pub price: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhaleData {
    pub address: String,
    pub symbol: String,
    pub position_size: f64,
    pub entry_price: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketData {
    CandleUpdate(Candle),
    Funding(FundingData),
    OpenInterest(OIData),
    HlpPosition(HlpData),
    Liquidation(LiquidationData),
    WhalePosition(WhaleData),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignal {
    pub symbol: String,
    pub direction: Direction,
    pub strength: f64,
    pub composite_score: f64,
    pub factor_scores: Vec<(String, f64)>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SignalAction {
    Open(Direction),
    Close,
    Hold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub direction: Direction,
    pub size_usd: f64,
    pub entry_price: f64,
    pub entry_time: i64,
    pub stop_loss: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub fees_paid: f64,
    pub funding_paid: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_opposite() {
        assert_eq!(Direction::Long.opposite(), Direction::Short);
        assert_eq!(Direction::Short.opposite(), Direction::Long);
    }

    #[test]
    fn direction_roundtrip_json() {
        let d = Direction::Long;
        let json = serde_json::to_string(&d).unwrap();
        let d2: Direction = serde_json::from_str(&json).unwrap();
        assert_eq!(d, d2);
    }

    #[test]
    fn candle_serialization_roundtrip() {
        let candle = Candle {
            symbol: "BTC".to_string(),
            interval: "15m".to_string(),
            open_time: 1_700_000_000,
            close_time: 1_700_000_900,
            open: 40000.0,
            high: 40500.0,
            low: 39900.0,
            close: 40250.0,
            volume: 1234.5,
            num_trades: 9876,
        };
        let json = serde_json::to_string(&candle).unwrap();
        let c2: Candle = serde_json::from_str(&json).unwrap();
        assert_eq!(c2.symbol, "BTC");
        assert_eq!(c2.interval, "15m");
        assert_eq!(c2.open_time, 1_700_000_000);
        assert_eq!(c2.close, 40250.0);
        assert_eq!(c2.num_trades, 9876);
    }
}
