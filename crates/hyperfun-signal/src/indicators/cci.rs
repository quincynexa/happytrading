use hyperfun_core::{Candle, CandleIndicator};
use std::collections::VecDeque;

pub struct Cci {
    period: usize,
    buffer: VecDeque<f64>,
    cci_val: f64,
    count: usize,
}

impl Cci {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            buffer: VecDeque::with_capacity(period),
            cci_val: 0.0,
            count: 0,
        }
    }

    fn compute_cci(&self) -> f64 {
        let n = self.buffer.len() as f64;
        let sma: f64 = self.buffer.iter().sum::<f64>() / n;
        let mad: f64 = self.buffer.iter().map(|tp| (tp - sma).abs()).sum::<f64>() / n;
        let tp = *self.buffer.back().unwrap_or(&0.0);
        if mad == 0.0 {
            return 0.0;
        }
        (tp - sma) / (0.015 * mad)
    }
}

impl Default for Cci {
    fn default() -> Self {
        Self::new(20)
    }
}

impl CandleIndicator for Cci {
    fn name(&self) -> &str {
        "cci"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;
        let tp = (candle.high + candle.low + candle.close) / 3.0;
        if self.buffer.len() == self.period {
            self.buffer.pop_front();
        }
        self.buffer.push_back(tp);
        if self.ready() {
            self.cci_val = self.compute_cci();
        }
    }

    fn value(&self) -> f64 {
        self.cci_val
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        (self.cci_val / 200.0).clamp(-1.0, 1.0)
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
        let mut cci = Cci::new(20);
        for i in 0..19 {
            let base = 100.0 + i as f64;
            cci.update(&make_candle(base, base + 1.0, base - 1.0, base));
            assert!(!cci.ready(), "should not be ready at count {}", i + 1);
        }
        let base = 120.0;
        cci.update(&make_candle(base, base + 1.0, base - 1.0, base));
        assert!(cci.ready(), "should be ready at count 20");
    }

    #[test]
    fn score_in_range_after_50_candles() {
        let mut cci = Cci::new(20);
        for i in 0..50 {
            let base = 100.0 + (i as f64 * 0.3).sin() * 15.0;
            cci.update(&make_candle(base, base + 2.0, base - 2.0, base + 0.5));
        }
        assert!(cci.ready());
        let s = cci.score();
        assert!(s >= -1.0 && s <= 1.0, "score {} out of [-1, 1]", s);
    }

    #[test]
    fn score_zero_when_not_ready() {
        let mut cci = Cci::new(20);
        for _ in 0..5 {
            cci.update(&make_candle(100.0, 101.0, 99.0, 100.0));
        }
        assert_eq!(cci.score(), 0.0);
    }

    #[test]
    fn flat_market_gives_zero_score() {
        let mut cci = Cci::new(20);
        // All identical candles → MAD = 0 → CCI = 0 → score = 0
        for _ in 0..50 {
            cci.update(&make_candle(100.0, 101.0, 99.0, 100.0));
        }
        assert!(cci.ready());
        assert_eq!(cci.score(), 0.0);
    }
}
