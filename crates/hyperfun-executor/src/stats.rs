/// Running statistics for a paper (or live) strategy session.
#[derive(Debug, Clone)]
pub struct RunningStats {
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub total_pnl: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub max_drawdown: f64,
    pub peak_equity: f64,
    pub current_equity: f64,
}

impl RunningStats {
    /// Create a new stats tracker with the given starting equity.
    pub fn new(initial_equity: f64) -> Self {
        Self {
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            total_pnl: 0.0,
            gross_profit: 0.0,
            gross_loss: 0.0,
            max_drawdown: 0.0,
            peak_equity: initial_equity,
            current_equity: initial_equity,
        }
    }

    /// Record a completed trade and update all running statistics.
    pub fn record_trade(&mut self, pnl: f64) {
        self.total_trades += 1;
        self.total_pnl += pnl;
        self.current_equity += pnl;

        if pnl > 0.0 {
            self.winning_trades += 1;
            self.gross_profit += pnl;
        } else {
            self.losing_trades += 1;
            self.gross_loss += pnl.abs();
        }

        // Track peak equity and max drawdown.
        if self.current_equity > self.peak_equity {
            self.peak_equity = self.current_equity;
        }
        let drawdown = if self.peak_equity > 0.0 {
            (self.peak_equity - self.current_equity) / self.peak_equity
        } else {
            0.0
        };
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
    }

    /// Fraction of trades that were winners (0.0 – 1.0).
    /// Returns 0.0 when no trades have been recorded.
    pub fn win_rate(&self) -> f64 {
        if self.total_trades == 0 {
            return 0.0;
        }
        self.winning_trades as f64 / self.total_trades as f64
    }

    /// Ratio of gross profit to gross loss.
    /// Returns `f64::INFINITY` when there have been no losing trades.
    pub fn profit_factor(&self) -> f64 {
        if self.gross_loss == 0.0 {
            return f64::INFINITY;
        }
        self.gross_profit / self.gross_loss
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_stats_three_trades() {
        let mut stats = RunningStats::new(10_000.0);
        stats.record_trade(100.0);
        stats.record_trade(-50.0);
        stats.record_trade(200.0);

        assert_eq!(stats.total_trades, 3);
        assert_eq!(stats.winning_trades, 2);
        assert_eq!(stats.losing_trades, 1);
        assert!((stats.total_pnl - 250.0).abs() < 1e-9);
        // win rate ≈ 66.67 %
        assert!((stats.win_rate() - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn drawdown_calculation() {
        let mut stats = RunningStats::new(10_000.0);
        stats.record_trade(1_000.0); // equity → 11 000, peak 11 000
        stats.record_trade(-2_000.0); // equity → 9 000

        // drawdown = (11000 - 9000) / 11000
        let expected = 2_000.0 / 11_000.0;
        assert!((stats.max_drawdown - expected).abs() < 1e-9);
    }

    #[test]
    fn profit_factor() {
        let mut stats = RunningStats::new(10_000.0);
        stats.record_trade(300.0);
        stats.record_trade(-100.0);

        assert!((stats.profit_factor() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn profit_factor_no_losses() {
        let mut stats = RunningStats::new(10_000.0);
        stats.record_trade(100.0);

        assert_eq!(stats.profit_factor(), f64::INFINITY);
    }
}
