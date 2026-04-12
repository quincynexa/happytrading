use hyperfun_core::{Candle, CandleIndicator};
use ta::indicators::BollingerBands;
use ta::Next;

pub struct BollingerBandsIndicator {
    bb: BollingerBands,
    period: usize,
    percent_b: f64,
    count: usize,
}

impl BollingerBandsIndicator {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            bb: BollingerBands::new(period, multiplier).unwrap(),
            period,
            percent_b: 0.5,
            count: 0,
        }
    }
}

impl Default for BollingerBandsIndicator {
    fn default() -> Self {
        Self::new(20, 2.0)
    }
}

impl CandleIndicator for BollingerBandsIndicator {
    fn name(&self) -> &str {
        "bollinger_bands"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;
        let output = self.bb.next(candle.close);
        let band_width = output.upper - output.lower;
        if band_width == 0.0 {
            self.percent_b = 0.5;
        } else {
            self.percent_b = (candle.close - output.lower) / band_width;
        }
    }

    fn value(&self) -> f64 {
        self.percent_b
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        (self.percent_b * 2.0 - 1.0).clamp(-1.0, 1.0)
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
        let mut bb = BollingerBandsIndicator::new(20, 2.0);
        for i in 0..19 {
            bb.update(&make_candle(100.0 + i as f64));
            assert!(!bb.ready(), "should not be ready at count {}", i + 1);
        }
        bb.update(&make_candle(120.0));
        assert!(bb.ready(), "should be ready at count 20");
    }

    #[test]
    fn score_in_range_after_50_candles() {
        let mut bb = BollingerBandsIndicator::new(20, 2.0);
        for i in 0..50 {
            bb.update(&make_candle(100.0 + (i as f64 * 0.4).sin() * 5.0));
        }
        assert!(bb.ready());
        let s = bb.score();
        assert!(s >= -1.0 && s <= 1.0, "score {} out of [-1, 1]", s);
    }

    #[test]
    fn score_zero_when_not_ready() {
        let mut bb = BollingerBandsIndicator::new(20, 2.0);
        for _ in 0..5 {
            bb.update(&make_candle(100.0));
        }
        assert_eq!(bb.score(), 0.0);
    }

    #[test]
    fn rising_market_positive_score() {
        let mut bb = BollingerBandsIndicator::new(20, 2.0);
        for i in 0..50 {
            bb.update(&make_candle(100.0 + i as f64 * 2.0));
        }
        assert!(bb.ready());
        let s = bb.score();
        assert!(s > 0.0, "rising market (near upper band) should give positive score, got {}", s);
    }

    #[test]
    fn falling_market_negative_score() {
        let mut bb = BollingerBandsIndicator::new(20, 2.0);
        for i in 0..50 {
            bb.update(&make_candle(1000.0 - i as f64 * 2.0));
        }
        assert!(bb.ready());
        let s = bb.score();
        assert!(s < 0.0, "falling market (near lower band) should give negative score, got {}", s);
    }
}
