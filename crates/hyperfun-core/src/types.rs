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

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Long => write!(f, "Long"),
            Direction::Short => write!(f, "Short"),
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

impl Position {
    /// Create a new position.
    pub fn new(
        symbol: impl Into<String>,
        direction: Direction,
        size_usd: f64,
        entry_price: f64,
        stop_loss: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            direction,
            size_usd,
            entry_price,
            entry_time: timestamp,
            stop_loss,
            unrealized_pnl: 0.0,
            realized_pnl: 0.0,
            fees_paid: 0.0,
            funding_paid: 0.0,
        }
    }

    /// Update unrealized PnL based on the current mark price.
    pub fn update_pnl(&mut self, mark_price: f64) {
        let qty = self.size_usd / self.entry_price;
        let raw = match self.direction {
            Direction::Long => qty * (mark_price - self.entry_price),
            Direction::Short => qty * (self.entry_price - mark_price),
        };
        self.unrealized_pnl = raw - self.fees_paid - self.funding_paid;
    }

    /// Close the position at `exit_price`, record the fee, and return realized PnL.
    pub fn close(&mut self, exit_price: f64, fee: f64) -> f64 {
        let qty = self.size_usd / self.entry_price;
        let gross = match self.direction {
            Direction::Long => qty * (exit_price - self.entry_price),
            Direction::Short => qty * (self.entry_price - exit_price),
        };
        self.fees_paid += fee;
        let pnl = gross - self.fees_paid - self.funding_paid;
        self.realized_pnl = pnl;
        self.unrealized_pnl = 0.0;
        pnl
    }

    /// Return `true` when the current price has breached the stop-loss level.
    pub fn should_stop_loss(&self, current_price: f64) -> bool {
        match self.direction {
            Direction::Long => current_price <= self.stop_loss,
            Direction::Short => current_price >= self.stop_loss,
        }
    }
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

    // ── Position tests ──────────────────────────────────────────────────────

    #[test]
    fn position_long_unrealized_pnl() {
        // $1 000 long @ $50 000, mark moves to $51 000
        // qty = 1000 / 50000 = 0.02 BTC
        // pnl = 0.02 * (51000 - 50000) = $20
        let mut pos = Position::new("BTC", Direction::Long, 1000.0, 50_000.0, 49_000.0, 0);
        pos.update_pnl(51_000.0);
        assert!((pos.unrealized_pnl - 20.0).abs() < 1e-9);
    }

    #[test]
    fn position_short_unrealized_pnl() {
        // $1 000 short @ $50 000, mark moves to $49 000
        // qty = 0.02 BTC
        // pnl = 0.02 * (50000 - 49000) = $20
        let mut pos = Position::new("BTC", Direction::Short, 1000.0, 50_000.0, 51_000.0, 0);
        pos.update_pnl(49_000.0);
        assert!((pos.unrealized_pnl - 20.0).abs() < 1e-9);
    }

    #[test]
    fn position_stop_loss_trigger_long() {
        // Long @ $50 000, stop @ $49 000 — price $48 500 should trigger
        let pos = Position::new("BTC", Direction::Long, 1000.0, 50_000.0, 49_000.0, 0);
        assert!(pos.should_stop_loss(48_500.0));
        assert!(!pos.should_stop_loss(49_500.0));
    }

    #[test]
    fn position_close_long_with_fees() {
        // Long $1 000 @ $50 000, exit @ $51 000
        // qty = 0.02 BTC
        // gross pnl = 0.02 * (51 000 - 50 000) = $20
        // entry fee already paid before close call: $0.35
        // exit fee passed to close: $0.35
        // net = 20 - 0.35 - 0.35 = $19.30
        let mut pos = Position::new("BTC", Direction::Long, 1000.0, 50_000.0, 49_000.0, 0);
        pos.fees_paid = 0.35; // entry fee
        let pnl = pos.close(51_000.0, 0.35); // exit fee
        assert!((pnl - 19.30).abs() < 1e-9);
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
