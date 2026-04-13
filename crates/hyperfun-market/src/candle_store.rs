use hyperfun_core::Candle;
use std::collections::HashMap;
use std::collections::VecDeque;

pub struct CandleStore {
    capacity: usize,
    store: HashMap<(String, String), VecDeque<Candle>>,
    stale: bool,
}

impl CandleStore {
    pub fn new(capacity: usize) -> Self {
        Self { capacity, store: HashMap::new(), stale: false }
    }

    pub fn push(&mut self, candle: Candle) {
        let key = (candle.symbol.clone(), candle.interval.clone());
        let buf = self.store.entry(key).or_insert_with(VecDeque::new);
        if buf.len() >= self.capacity { buf.pop_front(); }
        buf.push_back(candle);
    }

    pub fn get_last_n(&self, symbol: &str, interval: &str, n: usize) -> Vec<&Candle> {
        self.store
            .get(&(symbol.to_string(), interval.to_string()))
            .map(|buf| {
                let start = buf.len().saturating_sub(n);
                buf.iter().skip(start).collect()
            })
            .unwrap_or_default()
    }

    pub fn last(&self, symbol: &str, interval: &str) -> Option<&Candle> {
        self.store.get(&(symbol.to_string(), interval.to_string())).and_then(|buf| buf.back())
    }

    pub fn len(&self, symbol: &str, interval: &str) -> usize {
        self.store.get(&(symbol.to_string(), interval.to_string())).map(|buf| buf.len()).unwrap_or(0)
    }

    pub fn set_stale(&mut self, stale: bool) { self.stale = stale; }
    pub fn is_stale(&self) -> bool { self.stale }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperfun_core::Candle;

    fn make_candle(symbol: &str, interval: &str, close: f64) -> Candle {
        Candle {
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            open_time: 0,
            close_time: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close,
            volume: 0.0,
            num_trades: 0,
        }
    }

    #[test]
    fn test_push_and_get() {
        let mut store = CandleStore::new(500);
        store.push(make_candle("BTC", "1m", 100.0));
        store.push(make_candle("BTC", "1m", 200.0));
        assert_eq!(store.len("BTC", "1m"), 2);
        assert_eq!(store.last("BTC", "1m").unwrap().close, 200.0);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let mut store = CandleStore::new(3);
        for i in 1..=5u64 {
            store.push(make_candle("BTC", "1m", i as f64));
        }
        assert_eq!(store.len("BTC", "1m"), 3);
        let candles = store.get_last_n("BTC", "1m", 3);
        assert_eq!(candles[0].close, 3.0);
        assert_eq!(candles[1].close, 4.0);
        assert_eq!(candles[2].close, 5.0);
    }

    #[test]
    fn test_separate_symbols() {
        let mut store = CandleStore::new(500);
        store.push(make_candle("BTC", "1m", 50000.0));
        store.push(make_candle("ETH", "1m", 3000.0));
        assert_eq!(store.len("BTC", "1m"), 1);
        assert_eq!(store.len("ETH", "1m"), 1);
        assert_eq!(store.last("BTC", "1m").unwrap().close, 50000.0);
        assert_eq!(store.last("ETH", "1m").unwrap().close, 3000.0);
    }

    #[test]
    fn test_stale_flag() {
        let mut store = CandleStore::new(500);
        assert!(!store.is_stale());
        store.set_stale(true);
        assert!(store.is_stale());
    }

    #[test]
    fn test_get_last_n_partial() {
        let mut store = CandleStore::new(500);
        store.push(make_candle("BTC", "1m", 42.0));
        let candles = store.get_last_n("BTC", "1m", 50);
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].close, 42.0);
    }
}
