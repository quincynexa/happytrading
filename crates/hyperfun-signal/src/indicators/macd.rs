use hyperfun_core::{Candle, CandleIndicator};
use ta::indicators::MovingAverageConvergenceDivergence;
use ta::Next;

pub struct MacdHistogram {
    macd: MovingAverageConvergenceDivergence,
    #[allow(dead_code)]
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
    histogram: f64,
    max_abs_histogram: f64,
    count: usize,
}

impl MacdHistogram {
    pub fn new(fast_period: usize, slow_period: usize, signal_period: usize) -> Self {
        Self {
            macd: MovingAverageConvergenceDivergence::new(fast_period, slow_period, signal_period)
                .unwrap(),
            fast_period,
            slow_period,
            signal_period,
            histogram: 0.0,
            max_abs_histogram: 0.0,
            count: 0,
        }
    }
}

impl CandleIndicator for MacdHistogram {
    fn name(&self) -> &str {
        "macd_histogram"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;
        let output = self.macd.next(candle.close);
        self.histogram = output.histogram;
        let abs_h = self.histogram.abs();
        if abs_h > self.max_abs_histogram {
            self.max_abs_histogram = abs_h;
        }
    }

    fn value(&self) -> f64 {
        self.histogram
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        if self.max_abs_histogram == 0.0 {
            return 0.0;
        }
        (self.histogram / self.max_abs_histogram).clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.count >= self.slow_period + self.signal_period
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
            high: close,
            low: close,
            close,
            volume: 1000.0,
            num_trades: 100,
        }
    }

    #[test]
    fn not_ready_until_slow_plus_signal() {
        let mut ind = MacdHistogram::new(12, 26, 9);
        // ready threshold is 26 + 9 = 35
        for i in 0..34 {
            ind.update(&make_candle(100.0 + i as f64));
            assert!(!ind.ready(), "should not be ready at count {}", i + 1);
        }
        ind.update(&make_candle(135.0));
        assert!(ind.ready(), "should be ready at count 35");
    }

    #[test]
    fn score_in_range_after_100_candles() {
        let mut ind = MacdHistogram::new(12, 26, 9);
        for i in 0..100 {
            ind.update(&make_candle(100.0 + i as f64 * 0.5));
        }
        assert!(ind.ready());
        let s = ind.score();
        assert!(s >= -1.0 && s <= 1.0, "score {} out of [-1, 1]", s);
    }

    #[test]
    fn score_zero_when_not_ready() {
        let mut ind = MacdHistogram::new(12, 26, 9);
        for _ in 0..10 {
            ind.update(&make_candle(100.0));
        }
        assert_eq!(ind.score(), 0.0);
    }
}
