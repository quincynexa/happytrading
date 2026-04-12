use hyperfun_core::{Candle, CandleIndicator};
use std::collections::VecDeque;
use ta::indicators::AverageTrueRange;
use ta::Next;

pub struct Atr {
    atr: AverageTrueRange,
    period: usize,
    buffer: VecDeque<f64>,
    atr_val: f64,
    count: usize,
}

impl Atr {
    pub fn new(period: usize) -> Self {
        Self {
            atr: AverageTrueRange::new(period).unwrap(),
            period,
            buffer: VecDeque::with_capacity(period),
            atr_val: 0.0,
            count: 0,
        }
    }

    /// Returns the current ATR value (used by paper executor for stop loss calculations).
    pub fn atr_value(&self) -> f64 {
        self.atr_val
    }
}

impl Default for Atr {
    fn default() -> Self {
        Self::new(14)
    }
}

impl CandleIndicator for Atr {
    fn name(&self) -> &str {
        "atr"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;

        // Ensure OHLCV values satisfy DataItem constraints (high >= open/close, low <= open/close).
        let high = candle.high.max(candle.open).max(candle.close);
        let low = candle.low.min(candle.open).min(candle.close).max(0.0);
        let volume = candle.volume.max(0.0);

        let data_item = ta::DataItem::builder()
            .high(high)
            .low(low)
            .close(candle.close)
            .open(candle.open)
            .volume(volume)
            .build()
            .unwrap();

        self.atr_val = self.atr.next(&data_item);

        if self.buffer.len() == self.period {
            self.buffer.pop_front();
        }
        self.buffer.push_back(self.atr_val);
    }

    fn value(&self) -> f64 {
        self.atr_val
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        let n = self.buffer.len() as f64;
        if n == 0.0 {
            return 0.0;
        }
        let avg_atr: f64 = self.buffer.iter().sum::<f64>() / n;
        if avg_atr == 0.0 {
            return 0.0;
        }
        // Score: current ATR relative to average ATR
        // +1 = highly expanding volatility, -1 = very contracting
        ((self.atr_val / avg_atr - 1.0) / 0.5).clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.count >= self.period
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::Candle;

    fn make_candle(open: f64, high: f64, low: f64, close: f64) -> Candle {
        Candle {
            symbol: "BTC".to_string(),
            interval: "1m".to_string(),
            open_time: 0,
            close_time: 0,
            open,
            high,
            low,
            close,
            volume: 1000.0,
            num_trades: 100,
        }
    }

    #[test]
    fn not_ready_until_period() {
        let mut atr = Atr::new(14);
        for i in 0..13 {
            let base = 100.0 + i as f64;
            atr.update(&make_candle(base, base + 1.0, base - 1.0, base));
            assert!(!atr.ready(), "should not be ready at count {}", i + 1);
        }
        let base = 114.0;
        atr.update(&make_candle(base, base + 1.0, base - 1.0, base));
        assert!(atr.ready(), "should be ready at count 14");
    }

    #[test]
    fn score_in_range_after_50_candles() {
        let mut atr = Atr::new(14);
        for i in 0..50 {
            let base = 100.0 + i as f64;
            let volatility = 2.0 + (i as f64 * 0.1).sin(); // always >= 1.0
            // close is base, so high >= close and low <= close always hold
            atr.update(&make_candle(base, base + volatility, base - volatility, base));
        }
        assert!(atr.ready());
        let s = atr.score();
        assert!(s >= -1.0 && s <= 1.0, "score {} out of [-1, 1]", s);
    }

    #[test]
    fn score_zero_when_not_ready() {
        let mut atr = Atr::new(14);
        for _ in 0..5 {
            atr.update(&make_candle(100.0, 101.0, 99.0, 100.0));
        }
        assert_eq!(atr.score(), 0.0);
    }

    #[test]
    fn atr_value_accessor_returns_current_atr() {
        let mut atr = Atr::new(14);
        for i in 0..20 {
            let base = 100.0 + i as f64;
            atr.update(&make_candle(base, base + 2.0, base - 2.0, base));
        }
        assert!(atr.ready());
        assert!(atr.atr_value() > 0.0, "ATR value should be positive");
        assert_eq!(atr.atr_value(), atr.value());
    }

    #[test]
    fn expanding_volatility_positive_score() {
        let mut atr = Atr::new(14);
        // Start with low volatility
        for i in 0..30 {
            let base = 100.0 + i as f64;
            atr.update(&make_candle(base, base + 0.5, base - 0.5, base));
        }
        // Then spike volatility significantly
        for i in 30..50 {
            let base = 100.0 + i as f64;
            atr.update(&make_candle(base, base + 10.0, base - 10.0, base));
        }
        assert!(atr.ready());
        let s = atr.score();
        assert!(s > 0.0, "expanding volatility should give positive score, got {}", s);
    }
}
