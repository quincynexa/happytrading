use hyperfun_core::{Candle, CandleIndicator};
use ta::indicators::AverageTrueRange;
use ta::Next;

pub struct Supertrend {
    atr: AverageTrueRange,
    period: usize,
    multiplier: f64,
    count: usize,
    // Band state
    upper_band: f64,
    lower_band: f64,
    // Previous close for direction flipping logic
    prev_close: f64,
    // 1.0 = uptrend, -1.0 = downtrend
    direction: f64,
    score_val: f64,
}

impl Supertrend {
    pub fn new(period: usize, multiplier: f64) -> Self {
        Self {
            atr: AverageTrueRange::new(period).unwrap(),
            period,
            multiplier,
            count: 0,
            upper_band: f64::MAX,
            lower_band: f64::MIN,
            prev_close: 0.0,
            direction: 1.0, // start assuming uptrend
            score_val: 0.0,
        }
    }
}

impl CandleIndicator for Supertrend {
    fn name(&self) -> &str {
        "supertrend"
    }

    fn update(&mut self, candle: &Candle) {
        self.count += 1;

        // Defensive clamping: ensure OHLCV values satisfy DataItem constraints
        // (high >= open/close, low <= open/close). Same pattern as ATR indicator.
        let high = candle.high.max(candle.open).max(candle.close);
        let low = candle.low.min(candle.open).min(candle.close).max(0.0);
        let volume = candle.volume.max(0.0);

        let data_item = match ta::DataItem::builder()
            .high(high)
            .low(low)
            .close(candle.close)
            .open(candle.open)
            .volume(volume)
            .build()
        {
            Ok(item) => item,
            Err(_) => {
                // Malformed candle data — skip this update entirely.
                self.count -= 1;
                return;
            }
        };

        let atr_val = self.atr.next(&data_item);

        let hl2 = (candle.high + candle.low) / 2.0;
        let basic_upper = hl2 + self.multiplier * atr_val;
        let basic_lower = hl2 - self.multiplier * atr_val;

        if self.count == 1 {
            // Initialize bands on first candle
            self.upper_band = basic_upper;
            self.lower_band = basic_lower;
            self.prev_close = candle.close;
            self.score_val = self.direction;
            return;
        }

        // Bands only tighten (standard Supertrend logic)
        let new_upper = if basic_upper < self.upper_band || self.prev_close > self.upper_band {
            basic_upper
        } else {
            self.upper_band
        };

        let new_lower = if basic_lower > self.lower_band || self.prev_close < self.lower_band {
            basic_lower
        } else {
            self.lower_band
        };

        self.upper_band = new_upper;
        self.lower_band = new_lower;

        // Flip direction when price crosses bands
        let new_direction = if self.direction > 0.0 {
            // Currently in uptrend - flip to downtrend if price crosses below lower band
            if candle.close < self.lower_band {
                -1.0
            } else {
                1.0
            }
        } else {
            // Currently in downtrend - flip to uptrend if price crosses above upper band
            if candle.close > self.upper_band {
                1.0
            } else {
                -1.0
            }
        };

        self.direction = new_direction;
        self.prev_close = candle.close;
        self.score_val = self.direction;
    }

    fn value(&self) -> f64 {
        self.direction
    }

    fn score(&self) -> f64 {
        if !self.ready() {
            return 0.0;
        }
        self.score_val
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
        let mut st = Supertrend::new(10, 3.0);
        for i in 0..9 {
            let base = 100.0 + i as f64;
            st.update(&make_candle(base, base + 1.0, base - 1.0, base + 0.5));
            assert!(!st.ready(), "should not be ready at count {}", i + 1);
        }
        let base = 110.0;
        st.update(&make_candle(base, base + 1.0, base - 1.0, base + 0.5));
        assert!(st.ready(), "should be ready at count 10");
    }

    #[test]
    fn rising_market_gives_positive_score() {
        let mut st = Supertrend::new(7, 3.0);
        // Strongly rising market: each candle higher than the last
        for i in 0..50 {
            let base = 100.0 + i as f64 * 5.0;
            st.update(&make_candle(base, base + 2.0, base - 0.5, base + 1.5));
        }
        assert!(st.ready());
        assert_eq!(
            st.score(),
            1.0,
            "strongly rising market should give score +1.0"
        );
    }

    #[test]
    fn falling_market_gives_negative_score() {
        let mut st = Supertrend::new(7, 3.0);
        // Strongly falling market
        for i in 0..50 {
            let base = 1000.0 - i as f64 * 5.0;
            st.update(&make_candle(base, base + 0.5, base - 2.0, base - 1.5));
        }
        assert!(st.ready());
        assert_eq!(
            st.score(),
            -1.0,
            "strongly falling market should give score -1.0"
        );
    }

    #[test]
    fn score_zero_when_not_ready() {
        let mut st = Supertrend::new(14, 3.0);
        for _ in 0..5 {
            st.update(&make_candle(100.0, 101.0, 99.0, 100.5));
        }
        assert_eq!(st.score(), 0.0);
    }

    #[test]
    fn malformed_candle_does_not_panic() {
        let mut st = Supertrend::new(7, 3.0);
        // high < low — previously would have panicked
        st.update(&make_candle(100.0, 95.0, 105.0, 100.0));
        // Negative prices
        st.update(&make_candle(-10.0, -5.0, -15.0, -10.0));
        // Should not panic and should not have advanced count
        // (both candles are clamped/handled gracefully)
        assert!(!st.ready());
    }
}
