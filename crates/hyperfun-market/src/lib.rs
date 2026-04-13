// hyperfun-market: Hyperliquid market data ingestion (REST + WebSocket)

pub mod candle_store;
pub mod rest;
pub mod ws;

use anyhow::Result;
use hyperfun_core::config::AppConfig;
use tracing::info;

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

    /// Fetch the last ~500 candles per symbol per timeframe from REST and load
    /// them into the CandleStore. Logs the count per symbol.
    /// Marks the candle store as stale during the backfill and clears it after.
    pub async fn backfill(&mut self) -> Result<()> {
        self.candle_store.set_stale(true);

        let symbols = &self.config.symbols.watchlist;
        let timeframes = [
            self.config.timeframes.trend.clone(),
            self.config.timeframes.entry.clone(),
        ];

        // Use a generous time window: 500 candles of the largest timeframe.
        // For simplicity we use a very wide window and let the server return
        // at most 500 candles.
        let now_ms = chrono::Utc::now().timestamp_millis();
        // 500 candles * 4 h * 3600 s * 1000 ms — works for all supported intervals
        let lookback_ms: i64 = 500 * 4 * 3600 * 1000;
        let start_ms = now_ms - lookback_ms;

        let mut all_succeeded = true;

        for symbol in symbols {
            for tf in &timeframes {
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
                        info!(symbol = %symbol, timeframe = %tf, count, "backfill complete");
                    }
                    Err(e) => {
                        tracing::warn!(symbol = %symbol, timeframe = %tf, error = %e, "backfill failed");
                        all_succeeded = false;
                    }
                }
            }
        }

        if all_succeeded {
            self.candle_store.set_stale(false);
        } else {
            tracing::warn!("partial backfill failure — candle store remains stale");
        }
        Ok(())
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
