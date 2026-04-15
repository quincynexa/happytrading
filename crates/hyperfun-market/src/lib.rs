// hyperfun-market: Hyperliquid market data ingestion (REST + WebSocket)

pub mod candle_store;
pub mod rest;
pub mod ws;

use anyhow::Result;
use hyperfun_core::config::AppConfig;

use crate::candle_store::CandleStore;
use crate::rest::HlRestClient;

/// High-level engine that wraps REST client, WS config, and the CandleStore.
pub struct MarketDataEngine {
    config: AppConfig,
    candle_store: CandleStore,
    rest_client: HlRestClient,
    ws_url: String,
}

impl MarketDataEngine {
    /// Initialise all components from the application config.
    pub fn new(config: &AppConfig) -> Self {
        let rest_client = HlRestClient::new(&config.hyperliquid.rest_url);
        let ws_url = config.hyperliquid.ws_url.clone();
        let candle_store = CandleStore::new(500);

        Self {
            config: config.clone(),
            candle_store,
            rest_client,
            ws_url,
        }
    }

    /// Backfill with optional per-key start overrides.
    /// `start_overrides` is a map of (symbol, interval) -> start_ms from cache.
    /// If the override implies cache has enough coverage, fetch only the gap.
    /// Otherwise fall through to the full 500-bar fetch.
    pub async fn backfill_incremental(
        &mut self,
        start_overrides: &std::collections::HashMap<(String, String), i64>,
    ) -> Result<()> {
        self.candle_store.set_stale(true);

        let symbols = &self.config.symbols.watchlist;
        let timeframes = [
            self.config.timeframes.trend.clone(),
            self.config.timeframes.entry.clone(),
        ];

        let now_ms = chrono::Utc::now().timestamp_millis();
        let full_lookback: i64 = 500 * 4 * 3600 * 1000;
        let full_start = now_ms - full_lookback;

        let mut all_succeeded = true;
        for symbol in symbols {
            for tf in &timeframes {
                let key = (symbol.clone(), tf.clone());
                let start_ms = match start_overrides.get(&key) {
                    Some(&override_start) => override_start + 1,
                    None => full_start,
                };

                match self
                    .rest_client
                    .fetch_candles(symbol, tf, start_ms, now_ms)
                    .await
                {
                    Ok(candles) => {
                        let count = candles.len();
                        for candle in candles {
                            self.candle_store.push(candle);
                        }
                        tracing::info!(symbol = %symbol, timeframe = %tf, count, start_ms, "backfill_incremental complete");
                    }
                    Err(e) => {
                        tracing::warn!(symbol = %symbol, timeframe = %tf, error = %e, "backfill_incremental failed");
                        all_succeeded = false;
                    }
                }
            }
        }

        if all_succeeded {
            self.candle_store.set_stale(false);
        } else {
            tracing::warn!("partial backfill_incremental failure — candle store remains stale");
        }
        Ok(())
    }

    /// Fetch the last ~500 candles per symbol per timeframe from REST and load
    /// them into the CandleStore. Logs the count per symbol.
    /// Marks the candle store as stale during the backfill and clears it after.
    pub async fn backfill(&mut self) -> Result<()> {
        self.backfill_incremental(&std::collections::HashMap::new()).await
    }

    pub fn candle_store(&self) -> &CandleStore {
        &self.candle_store
    }

    pub fn candle_store_mut(&mut self) -> &mut CandleStore {
        &mut self.candle_store
    }

    pub fn rest_client(&self) -> &HlRestClient {
        &self.rest_client
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }
}
