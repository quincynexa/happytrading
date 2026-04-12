use hyperfun_core::{Candle, CandleIndicator};
use ta::indicators::RelativeStrengthIndex;
use ta::Next;

pub struct Rsi {
    rsi: RelativeStrengthIndex,
    period: usize,
    overbought: f64,
    oversold: f64,
    rsi_val: f64,
    count: usize,
}

impl Rsi {
    pub fn new(period: usize, overbought: f64, oversold: f64) -> Self {
        Self {
            rsi: RelativeStrengthIndex::new(period).unwrap(),
            period,
            overbought,
            oversold,
            rsi_val: 50.0,
            count: 0,
        }
    }
}

impl Default for Rsi {
    fn default() -> Self {
        Self::new(14, 70.0, 30.0)
    }
}

impl CandleIndicator for Rsi {
    fn name(&self) -> &str {
        "rsi"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;
        self.rsi_val = self.rsi.next(candle.close);
    }

    fn value(&self) -> f64 {
        self.rsi_val
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        let midpoint = (self.overbought + self.oversold) / 2.0;
        let half_range = (self.overbought - self.oversold) / 2.0;
        ((self.rsi_val - midpoint) / half_range).clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.count >= self.period
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::Candle;

    fn make_candle(close: f64) -> Candle {
        Candle {
            symbol: "BTC".to_string(),
            interval: "1m".to_string(),
            open_time: 0,
            close_time: 0,
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1000.0,
            num_trades: 100,
        }
    }

    #[test]
    fn not_ready_until_period() {
        let mut rsi = Rsi::new(14, 70.0, 30.0);
        for i in 0..13 {
            rsi.update(&make_candle(100.0 + i as f64));
            assert!(!rsi.ready(), "should not be ready at count {}", i + 1);
        }
        rsi.update(&make_candle(114.0));
        assert!(rsi.ready(), "should be ready at count 14");
    }

    #[test]
    fn score_in_range_after_50_candles() {
        let mut rsi = Rsi::new(14, 70.0, 30.0);
        for i in 0..50 {
            rsi.update(&make_candle(100.0 + (i as f64 * 0.5).sin() * 10.0));
        }
        assert!(rsi.ready());
        let s = rsi.score();
        assert!(s >= -1.0 && s <= 1.0, "score {} out of [-1, 1]", s);
    }

    #[test]
    fn score_zero_when_not_ready() {
        let mut rsi = Rsi::new(14, 70.0, 30.0);
        for _ in 0..5 {
            rsi.update(&make_candle(100.0));
        }
        assert_eq!(rsi.score(), 0.0);
    }

    #[test]
    fn rising_market_positive_score() {
        let mut rsi = Rsi::new(14, 70.0, 30.0);
        for i in 0..50 {
            rsi.update(&make_candle(100.0 + i as f64 * 2.0));
        }
        assert!(rsi.ready());
        let s = rsi.score();
        assert!(s > 0.0, "strongly rising market should give positive score, got {}", s);
    }
}
