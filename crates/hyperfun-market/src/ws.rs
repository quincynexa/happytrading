// WebSocket client for Hyperliquid's candle stream.
// Connects, subscribes, parses candle messages, and sends them over an mpsc channel.
// Auto-reconnects with exponential backoff on close or error.

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use hyperfun_core::Candle;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::rest::parse_candle_from_value;

/// WebSocket client for Hyperliquid candle streams.
pub struct HlWsClient {
    url: String,
}

impl HlWsClient {
    /// Create a new client that will connect to `url`.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }

    /// Return the configured WebSocket URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Connect, subscribe to the given symbols/intervals, and stream parsed `Candle`s
    /// into `tx`. Reconnects automatically with exponential backoff.
    pub async fn run(
        &self,
        symbols: Vec<String>,
        intervals: Vec<String>,
        tx: mpsc::Sender<Candle>,
    ) {
        let mut backoff_secs: u64 = 1;

        loop {
            match self.connect_and_stream(&symbols, &intervals, &tx).await {
                Ok(()) => {
                    // Clean close — reconnect immediately (server may have closed gracefully).
                    info!("WS connection closed cleanly; reconnecting...");
                }
                Err(e) => {
                    warn!("WS error: {e}; reconnecting in {backoff_secs}s");
                }
            }

            sleep(Duration::from_secs(backoff_secs)).await;

            // Exponential backoff capped at 30 s.
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }

    /// One connection attempt: connect → subscribe → read loop.
    /// Returns `Ok(())` on a clean close frame or `Err` on any error.
    async fn connect_and_stream(
        &self,
        symbols: &[String],
        intervals: &[String],
        tx: &mpsc::Sender<Candle>,
    ) -> Result<()> {
        info!("Connecting to {}", self.url);
        let (ws_stream, _) = connect_async(&self.url).await?;
        let (mut write, mut read) = ws_stream.split();

        // Subscribe to every (symbol, interval) combination.
        for coin in symbols {
            for interval in intervals {
                let sub = json!({
                    "method": "subscribe",
                    "subscription": {
                        "type": "candle",
                        "coin": coin,
                        "interval": interval,
                    }
                });
                write.send(Message::Text(sub.to_string().into())).await?;
                info!("Subscribed to candle stream: {coin} {interval}");
            }
        }

        // Read loop.
        while let Some(msg) = read.next().await {
            match msg? {
                Message::Text(text) => {
                    if let Err(e) = Self::handle_text(&text, tx).await {
                        warn!("Failed to handle WS message: {e}");
                    }
                }
                Message::Ping(payload) => {
                    write.send(Message::Pong(payload)).await?;
                }
                Message::Close(_) => {
                    info!("Received close frame");
                    return Ok(());
                }
                // Binary / Pong / Frame — ignore.
                _ => {}
            }
        }

        Ok(())
    }

    /// Parse a raw text message and, if it is a candle event, forward the `Candle` to `tx`.
    async fn handle_text(text: &str, tx: &mpsc::Sender<Candle>) -> Result<()> {
        let v: Value = serde_json::from_str(text)?;

        let channel = v["channel"].as_str().unwrap_or("");
        if channel != "candle" {
            return Ok(());
        }

        let data = &v["data"];
        if let Some(candle) = parse_candle_from_value(data) {
            if tx.send(candle).await.is_err() {
                error!("Candle receiver dropped; stopping WS client");
                anyhow::bail!("channel closed");
            }
        } else {
            warn!("Could not parse candle from data: {data}");
        }

        Ok(())
    }
}
