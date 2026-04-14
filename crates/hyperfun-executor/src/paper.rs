use std::collections::HashMap;

use hyperfun_core::{Direction, Position, SignalAction};
use tracing::info;

use crate::stats::RunningStats;

/// A paper-trading executor that simulates order fills with configurable
/// slippage and fees.
pub struct PaperExecutor {
    positions: HashMap<String, Position>,
    stats: RunningStats,
    slippage_pct: f64,
    fee_pct: f64,
    position_size_usd: f64,
    atr_stop_multiplier: f64,
    trailing_activation_atr: f64,
    trailing_distance_atr: f64,
}

impl PaperExecutor {
    /// Create a new paper executor.
    ///
    /// * `slippage_pct` – one-way slippage as a percentage of price (e.g. `0.05` = 5 bp)
    /// * `fee_pct`      – one-way trading fee as a percentage of notional (e.g. `0.035`)
    /// * `position_size_usd` – fixed notional in USD for every trade
    /// * `atr_stop_multiplier` – how many ATRs away the stop-loss is set
    /// * `trailing_activation_atr` – profit in ATR units to activate trailing stop
    /// * `trailing_distance_atr` – trailing stop distance in ATR units
    pub fn new(
        slippage_pct: f64,
        fee_pct: f64,
        position_size_usd: f64,
        atr_stop_multiplier: f64,
        trailing_activation_atr: f64,
        trailing_distance_atr: f64,
    ) -> Self {
        Self {
            positions: HashMap::new(),
            stats: RunningStats::new(0.0),
            slippage_pct,
            fee_pct,
            position_size_usd,
            atr_stop_multiplier,
            trailing_activation_atr,
            trailing_distance_atr,
        }
    }

    // ── fill-price helpers ────────────────────────────────────────────────

    fn entry_fill(&self, price: f64, direction: Direction) -> f64 {
        let slip = price * self.slippage_pct / 100.0;
        match direction {
            Direction::Long => price + slip,
            Direction::Short => price - slip,
        }
    }

    fn exit_fill(&self, price: f64, direction: Direction) -> f64 {
        let slip = price * self.slippage_pct / 100.0;
        match direction {
            Direction::Long => price - slip,
            Direction::Short => price + slip,
        }
    }

    fn fee(&self) -> f64 {
        self.position_size_usd * self.fee_pct / 100.0
    }

    fn stop_loss_price(&self, fill: f64, direction: Direction, atr: f64) -> f64 {
        let offset = atr * self.atr_stop_multiplier;
        match direction {
            Direction::Long => fill - offset,
            Direction::Short => fill + offset,
        }
    }

    // ── core operations ───────────────────────────────────────────────────

    fn open_position(&mut self, direction: Direction, symbol: &str, price: f64, atr: f64, ts: i64) {
        let fill = self.entry_fill(price, direction);
        let entry_fee = self.fee();
        let stop = self.stop_loss_price(fill, direction, atr);

        let mut pos = Position::new(symbol, direction, self.position_size_usd, fill, stop, ts);
        pos.fees_paid = entry_fee;

        info!(
            symbol,
            direction = ?direction,
            fill_price = fill,
            stop_loss = stop,
            entry_fee,
            "paper: opened position"
        );

        self.positions.insert(symbol.to_string(), pos);
    }

    // ── public API ────────────────────────────────────────────────────────

    /// Process a signal action for the given symbol.
    /// Returns a `TradeEvent` describing what happened.
    pub fn execute_signal(
        &mut self,
        action: SignalAction,
        symbol: &str,
        current_price: f64,
        atr: f64,
        timestamp: i64,
    ) -> hyperfun_core::TradeEvent {
        use hyperfun_core::{TradeEvent, TradeRecord};

        match action {
            SignalAction::Open(dir) => {
                let existing_dir = self.positions.get(symbol).map(|p| p.direction);

                let dir_str = match dir {
                    Direction::Long => "Long",
                    Direction::Short => "Short",
                };

                match existing_dir {
                    Some(ed) if ed != dir => {
                        // Flip: close existing, open new
                        let close_dir_str = match ed {
                            Direction::Long => "Long",
                            Direction::Short => "Short",
                        };
                        let close_fill = self.exit_fill(current_price, ed);
                        let exit_fee = self.fee();
                        let close_pnl = {
                            let pos = self.positions.get_mut(symbol).unwrap();
                            pos.close(close_fill, exit_fee)
                        };
                        self.stats.record_trade(close_pnl);
                        self.positions.remove(symbol);

                        let close_trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "close".into(),
                            direction: close_dir_str.into(),
                            price: current_price,
                            fill_price: close_fill,
                            composite: 0.0,
                            atr,
                            stop_loss: None,
                            pnl: Some(close_pnl),
                            reason: Some("direction_flip".into()),
                        };

                        self.open_position(dir, symbol, current_price, atr, timestamp);
                        let new_pos = self.positions.get(symbol).unwrap().clone();
                        let open_trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "open".into(),
                            direction: dir_str.into(),
                            price: current_price,
                            fill_price: new_pos.entry_price,
                            composite: 0.0,
                            atr,
                            stop_loss: Some(new_pos.stop_loss),
                            pnl: None,
                            reason: None,
                        };

                        TradeEvent::Flipped {
                            close_trade,
                            open_trade,
                            new_position: new_pos,
                            pnl: close_pnl,
                        }
                    }
                    Some(_) => TradeEvent::None, // same direction, already open
                    None => {
                        self.open_position(dir, symbol, current_price, atr, timestamp);
                        let pos = self.positions.get(symbol).unwrap().clone();
                        let trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "open".into(),
                            direction: dir_str.into(),
                            price: current_price,
                            fill_price: pos.entry_price,
                            composite: 0.0,
                            atr,
                            stop_loss: Some(pos.stop_loss),
                            pnl: None,
                            reason: None,
                        };
                        TradeEvent::Opened {
                            position: pos,
                            trade,
                        }
                    }
                }
            }
            SignalAction::Close => {
                let existing_dir = self.positions.get(symbol).map(|p| p.direction);
                match existing_dir {
                    Some(dir) => {
                        let dir_str = match dir {
                            Direction::Long => "Long",
                            Direction::Short => "Short",
                        };
                        let close_fill = self.exit_fill(current_price, dir);
                        let exit_fee = self.fee();
                        let pnl = {
                            let pos = self.positions.get_mut(symbol).unwrap();
                            pos.close(close_fill, exit_fee)
                        };
                        self.stats.record_trade(pnl);
                        self.positions.remove(symbol);

                        let trade = TradeRecord {
                            ts: timestamp,
                            symbol: symbol.to_string(),
                            event: "close".into(),
                            direction: dir_str.into(),
                            price: current_price,
                            fill_price: close_fill,
                            composite: 0.0,
                            atr,
                            stop_loss: None,
                            pnl: Some(pnl),
                            reason: Some("signal".into()),
                        };
                        TradeEvent::Closed { trade, pnl }
                    }
                    None => TradeEvent::None,
                }
            }
            SignalAction::Hold => TradeEvent::None,
        }
    }

    /// Update trailing stop and check whether the stop-loss has been breached.
    /// Returns the realized `TradeEvent` if a stop was triggered.
    pub fn check_stop_losses(
        &mut self,
        symbol: &str,
        current_price: f64,
        atr: f64,
    ) -> Option<hyperfun_core::TradeEvent> {
        use hyperfun_core::{TradeEvent, TradeRecord};

        // Update trailing stop before checking
        if let Some(pos) = self.positions.get_mut(symbol) {
            pos.update_trailing_stop(
                current_price,
                atr,
                self.trailing_activation_atr,
                self.trailing_distance_atr,
            );
        }

        let (triggered, direction) = self
            .positions
            .get(symbol)
            .map(|p| (p.should_stop_loss(current_price), p.direction))
            .unwrap_or((false, Direction::Long));

        if !triggered {
            return None;
        }

        let dir_str = match direction {
            Direction::Long => "Long",
            Direction::Short => "Short",
        };
        let close_fill = self.exit_fill(current_price, direction);
        let exit_fee = self.fee();
        let pnl = {
            let pos = self.positions.get_mut(symbol).unwrap();
            pos.close(close_fill, exit_fee)
        };
        self.stats.record_trade(pnl);

        let was_profitable = {
            let pos = self.positions.get(symbol).unwrap();
            let profit = match direction {
                Direction::Long => current_price - pos.entry_price,
                Direction::Short => pos.entry_price - current_price,
            };
            profit > 0.0
        };
        self.positions.remove(symbol);

        let reason = if was_profitable { "trailing_stop" } else { "stop_loss" };

        let trade = TradeRecord {
            ts: 0, // caller fills in actual timestamp if needed
            symbol: symbol.to_string(),
            event: "close".into(),
            direction: dir_str.into(),
            price: current_price,
            fill_price: close_fill,
            composite: 0.0,
            atr,
            stop_loss: None,
            pnl: Some(pnl),
            reason: Some(reason.into()),
        };
        Some(TradeEvent::StopLossTriggered { trade, pnl })
    }

    /// Update the unrealized PnL for a position without closing it.
    pub fn update_unrealized_pnl(&mut self, symbol: &str, mark_price: f64) {
        if let Some(pos) = self.positions.get_mut(symbol) {
            pos.update_pnl(mark_price);
        }
    }

    /// Access the running statistics.
    pub fn stats(&self) -> &RunningStats {
        &self.stats
    }

    /// Whether an open position exists for `symbol`.
    pub fn has_position(&self, symbol: &str) -> bool {
        self.positions.contains_key(symbol)
    }

    /// Return a reference to the open position for `symbol`, if any.
    pub fn get_position(&self, symbol: &str) -> Option<&Position> {
        self.positions.get(symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::{Direction, SignalAction};

    fn make_executor() -> PaperExecutor {
        // slippage 0.05%, fee 0.035%, size $1 000, 2x ATR stop, trailing 1.5/1.5
        PaperExecutor::new(0.05, 0.035, 1_000.0, 2.0, 1.5, 1.5)
    }

    #[test]
    fn open_and_close_long() {
        let mut ex = make_executor();

        // Open a long at $50 000, ATR = $500 (stop = fill - 1000)
        let _ = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        assert!(ex.has_position("BTC"));

        let pos = ex.get_position("BTC").unwrap();
        assert_eq!(pos.direction, Direction::Long);
        // fill = 50000 * 1.0005 = 50025
        assert!((pos.entry_price - 50_025.0).abs() < 1e-6);

        // Close at $51 000 (profit)
        let _ = ex.execute_signal(SignalAction::Close, "BTC", 51_000.0, 500.0, 1);
        assert!(!ex.has_position("BTC"));
        assert_eq!(ex.stats().total_trades, 1);
        assert_eq!(ex.stats().winning_trades, 1);
        assert!(ex.stats().total_pnl > 0.0);
    }

    #[test]
    fn direction_flip_closes_long_opens_short() {
        let mut ex = make_executor();

        // Open long
        let _ = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        assert!(ex.has_position("BTC"));
        assert_eq!(ex.get_position("BTC").unwrap().direction, Direction::Long);

        // Flip to short — should close the long first, then open short
        let _ = ex.execute_signal(SignalAction::Open(Direction::Short), "BTC", 50_500.0, 500.0, 1);
        assert!(ex.has_position("BTC"));
        assert_eq!(ex.get_position("BTC").unwrap().direction, Direction::Short);
        // The long close was recorded as a trade
        assert_eq!(ex.stats().total_trades, 1);
    }

    #[test]
    fn stop_loss_closes_position() {
        let mut ex = make_executor();

        // Open long at $50 000, ATR = $500 → stop at 50025 - 1000 = 49025
        let _ = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        assert!(ex.has_position("BTC"));

        let stop = ex.get_position("BTC").unwrap().stop_loss;

        // Price above stop: position still open
        let _ = ex.check_stop_losses("BTC", stop + 100.0, 500.0);
        assert!(ex.has_position("BTC"));

        // Price at/below stop: position closes
        let _ = ex.check_stop_losses("BTC", stop - 1.0, 500.0);
        assert!(!ex.has_position("BTC"));
        assert_eq!(ex.stats().total_trades, 1);
    }

    #[test]
    fn trailing_stop_locks_profit() {
        let mut ex = make_executor();

        // Open long at $50 000, ATR = $500
        // Initial stop = 50025 - 1000 = 49025
        let _ = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        let initial_stop = ex.get_position("BTC").unwrap().stop_loss;

        // Price moves to $50 800 — profit = 1.55 ATR (> 1.5 activation)
        // Trailing stop should activate: 50800 - 1.5*500 = 50050
        let _ = ex.check_stop_losses("BTC", 50_800.0, 500.0);
        assert!(ex.has_position("BTC"));
        let new_stop = ex.get_position("BTC").unwrap().stop_loss;
        assert!(new_stop > initial_stop, "trailing stop should have moved up: {} > {}", new_stop, initial_stop);

        // Price continues to $51 500 — trailing moves up further
        let _ = ex.check_stop_losses("BTC", 51_500.0, 500.0);
        assert!(ex.has_position("BTC"));
        let higher_stop = ex.get_position("BTC").unwrap().stop_loss;
        assert!(higher_stop > new_stop);

        // Price drops back to trailing stop level — should trigger
        let _ = ex.check_stop_losses("BTC", higher_stop - 1.0, 500.0);
        assert!(!ex.has_position("BTC"));
        // It was profitable — trailing stop, not initial stop
        assert!(ex.stats().total_pnl > 0.0);
    }

    #[test]
    fn execute_signal_open_returns_opened_event() {
        use hyperfun_core::TradeEvent;
        let mut ex = make_executor();
        let ev = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        match ev {
            TradeEvent::Opened { position, trade } => {
                assert_eq!(position.direction, Direction::Long);
                assert_eq!(trade.event, "open");
                assert_eq!(trade.direction, "Long");
            }
            other => panic!("expected TradeEvent::Opened, got {:?}", other),
        }
    }

    #[test]
    fn execute_signal_flip_returns_flipped_event() {
        use hyperfun_core::TradeEvent;
        let mut ex = make_executor();
        let _ = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        let ev = ex.execute_signal(SignalAction::Open(Direction::Short), "BTC", 50_500.0, 500.0, 1);
        match ev {
            TradeEvent::Flipped { close_trade, open_trade, new_position, .. } => {
                assert_eq!(close_trade.event, "close");
                assert_eq!(close_trade.direction, "Long");
                assert_eq!(open_trade.event, "open");
                assert_eq!(open_trade.direction, "Short");
                assert_eq!(new_position.direction, Direction::Short);
            }
            other => panic!("expected TradeEvent::Flipped, got {:?}", other),
        }
    }

    #[test]
    fn check_stop_losses_returns_stop_event() {
        use hyperfun_core::TradeEvent;
        let mut ex = make_executor();
        let _ = ex.execute_signal(SignalAction::Open(Direction::Long), "BTC", 50_000.0, 500.0, 0);
        let stop = ex.get_position("BTC").unwrap().stop_loss;
        let ev = ex.check_stop_losses("BTC", stop - 1.0, 500.0);
        match ev {
            Some(TradeEvent::StopLossTriggered { trade, pnl }) => {
                assert_eq!(trade.event, "close");
                assert!(pnl < 0.0);
            }
            other => panic!("expected StopLossTriggered, got {:?}", other),
        }
    }
}
