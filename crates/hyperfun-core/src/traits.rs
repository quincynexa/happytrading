use crate::types::{Candle, MarketData};

pub trait CandleIndicator: Send + Sync {
    fn name(&self) -> &str;
    fn update(&mut self, candle: &Candle);
    fn value(&self) -> f64;
    fn score(&self) -> f64;
    fn ready(&self) -> bool;
}

pub trait HlSignalProvider: Send + Sync {
    fn name(&self) -> &str;
    fn update(&mut self, data: &MarketData);
    fn score(&self) -> f64;
    fn ready(&self) -> bool;
}
