use hyperfun_core::{Candle, CandleIndicator};
use ta::indicators::ExponentialMovingAverage;
use ta::Next;

pub struct EmaCrossover {
    short_ema: ExponentialMovingAverage,
    long_ema: ExponentialMovingAverage,
    #[allow(dead_code)]
    short_period: usize,
    long_period: usize,
    short_val: f64,
    long_val: f64,
    count: usize,
}

impl EmaCrossover {
    pub fn new(short_period: usize, long_period: usize) -> Self {
        Self {
            short_ema: ExponentialMovingAverage::new(short_period).unwrap(),
            long_ema: ExponentialMovingAverage::new(long_period).unwrap(),
            short_period,
            long_period,
            short_val: 0.0,
            long_val: 0.0,
            count: 0,
        }
    }
}

impl CandleIndicator for EmaCrossover {
    fn name(&self) -> &str {
        "ema_crossover"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;
        self.short_val = self.short_ema.next(candle.close);
        self.long_val = self.long_ema.next(candle.close);
    }

    fn value(&self) -> f64 {
        self.short_val - self.long_val
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        if self.long_val == 0.0 {
            return 0.0;
        }
        let normalized = (self.short_val - self.long_val) / self.long_val;
        // scale: 2% difference = full signal (1.0), so divide by 0.02
        let scaled = normalized / 0.02;
        scaled.clamp(-1.0, 1.0)
    }

    fn ready(&self) -> bool {
        self.count >= self.long_period
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
    fn not_ready_until_long_period() {
        let mut ema = EmaCrossover::new(3, 10);
        for i in 0..9 {
            ema.update(&make_candle(100.0 + i as f64));
            assert!(!ema.ready(), "should not be ready at count {}", i + 1);
        }
        ema.update(&make_candle(110.0));
        assert!(ema.ready(), "should be ready at count 10");
    }

    #[test]
    fn score_is_zero_when_not_ready() {
        let mut ema = EmaCrossover::new(3, 10);
        for _ in 0..5 {
            ema.update(&make_candle(100.0));
        }
        assert_eq!(ema.score(), 0.0);
    }

    #[test]
    fn score_in_range_for_rising_prices() {
        let mut ema = EmaCrossover::new(5, 20);
        // Feed rising prices so short EMA > long EMA
        for i in 0..40 {
            ema.update(&make_candle(100.0 + i as f64 * 2.0));
        }
        assert!(ema.ready());
        let s = ema.score();
        assert!(s > 0.0, "rising prices should give positive score, got {}", s);
        assert!(s <= 1.0, "score should be <= 1.0, got {}", s);
    }

    #[test]
    fn score_in_range_for_falling_prices() {
        let mut ema = EmaCrossover::new(5, 20);
        // Feed falling prices so short EMA < long EMA
        for i in 0..40 {
            ema.update(&make_candle(1000.0 - i as f64 * 2.0));
        }
        assert!(ema.ready());
        let s = ema.score();
        assert!(s < 0.0, "falling prices should give negative score, got {}", s);
        assert!(s >= -1.0, "score should be >= -1.0, got {}", s);
    }
}
