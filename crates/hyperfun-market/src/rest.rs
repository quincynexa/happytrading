// REST client for Hyperliquid's info endpoint.
// All requests are POST to {base_url}/info with JSON bodies.

use anyhow::{anyhow, Result};
use hyperfun_core::{Candle, FundingData, HlpData, OIData};
use serde_json::{json, Value};

pub struct HlRestClient {
    client: reqwest::Client,
    base_url: String,
}

impl HlRestClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }

    async fn post(&self, body: Value) -> Result<Value> {
        let url = format!("{}/info", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// Fetch OHLCV candles for `coin` with `interval` between `start_time` and `end_time` (ms).
    pub async fn fetch_candles(
        &self,
        coin: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<Candle>> {
        let body = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_time,
                "endTime": end_time,
            }
        });
        let resp = self.post(body).await?;
        let arr = resp.as_array().ok_or_else(|| anyhow!("expected array"))?;
        let candles = arr
            .iter()
            .filter_map(parse_candle_from_value)
            .collect();
        Ok(candles)
    }

    /// Fetch predicted funding rates for all coins.
    pub async fn fetch_predicted_fundings(&self) -> Result<Vec<FundingData>> {
        let body = json!({ "type": "predictedFundings" });
        let resp = self.post(body).await?;
        let arr = resp.as_array().ok_or_else(|| anyhow!("expected array"))?;

        // Response: [[coin, [[venue, {fundingRate, nextFundingTime}], ...]], ...]
        let mut result = Vec::new();
        for item in arr {
            let pair = item.as_array().ok_or_else(|| anyhow!("expected [coin, venues]"))?;
            if pair.len() < 2 {
                continue;
            }
            let coin = pair[0].as_str().unwrap_or("").to_string();
            let venues = pair[1].as_array().ok_or_else(|| anyhow!("expected venues array"))?;
            for venue_pair in venues {
                let vp = venue_pair.as_array().ok_or_else(|| anyhow!("expected [venue, data]"))?;
                if vp.len() < 2 {
                    continue;
                }
                let data = &vp[1];
                let funding_rate = data["fundingRate"]
                    .as_str()
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let next_funding_time = data["nextFundingTime"]
                    .as_i64()
                    .unwrap_or(0);
                result.push(FundingData {
                    symbol: coin.clone(),
                    funding_rate,
                    predicted_rate: funding_rate,
                    timestamp: next_funding_time,
                });
            }
        }
        Ok(result)
    }

    /// Fetch open interest data and daily notional volume for all coins.
    /// Returns `(oi_data, [(symbol, day_ntl_vlm)])`
    pub async fn fetch_meta_and_asset_ctxs(
        &self,
    ) -> Result<(Vec<OIData>, Vec<(String, f64)>)> {
        let body = json!({ "type": "metaAndAssetCtxs" });
        let resp = self.post(body).await?;
        let arr = resp.as_array().ok_or_else(|| anyhow!("expected array"))?;
        if arr.len() < 2 {
            return Err(anyhow!("metaAndAssetCtxs: expected [meta, assetCtxs]"));
        }

        let meta = &arr[0];
        let asset_ctxs = arr[1].as_array().ok_or_else(|| anyhow!("expected assetCtxs array"))?;
        let universe = meta["universe"]
            .as_array()
            .ok_or_else(|| anyhow!("universe missing"))?;

        let now = 0i64; // no live clock in unit-testable code; callers can adjust if needed
        let mut oi_data = Vec::new();
        let mut vlm_data = Vec::new();

        for (i, ctx) in asset_ctxs.iter().enumerate() {
            let name = universe
                .get(i)
                .and_then(|u| u["name"].as_str())
                .unwrap_or("")
                .to_string();

            let open_interest = ctx["openInterest"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let day_ntl_vlm = ctx["dayNtlVlm"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            oi_data.push(OIData {
                symbol: name.clone(),
                open_interest,
                timestamp: now,
            });
            vlm_data.push((name, day_ntl_vlm));
        }

        Ok((oi_data, vlm_data))
    }

    /// Fetch HLP positions for the given `address`.
    pub async fn fetch_clearinghouse_state(&self, address: &str) -> Result<Vec<HlpData>> {
        let body = json!({
            "type": "clearinghouseState",
            "user": address,
        });
        let resp = self.post(body).await?;

        let asset_positions = resp["assetPositions"]
            .as_array()
            .ok_or_else(|| anyhow!("assetPositions missing"))?;

        let now = 0i64;
        let mut result = Vec::new();
        for ap in asset_positions {
            let pos = &ap["position"];
            let coin = pos["coin"].as_str().unwrap_or("").to_string();
            let position_size = pos["szi"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let entry_price = pos["entryPx"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let unrealized_pnl = pos["unrealizedPnl"]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            result.push(HlpData {
                symbol: coin,
                position_size,
                entry_price,
                unrealized_pnl,
                timestamp: now,
            });
        }
        Ok(result)
    }
}

/// Parse a single HL candle JSON object into a `Candle`.
/// Returns `None` if any required field is absent or unparseable.
pub(crate) fn parse_candle_from_value(v: &Value) -> Option<Candle> {
    // t / T are millisecond timestamps (numbers); s = symbol; i = interval
    // o, c, h, l, v are string-encoded floats; n is a number
    let open_time = v["t"].as_i64()?;
    let close_time = v["T"].as_i64()?;
    let symbol = v["s"].as_str()?.to_string();
    let interval = v["i"].as_str()?.to_string();
    let open = v["o"].as_str()?.parse::<f64>().ok()?;
    let close = v["c"].as_str()?.parse::<f64>().ok()?;
    let high = v["h"].as_str()?.parse::<f64>().ok()?;
    let low = v["l"].as_str()?.parse::<f64>().ok()?;
    let volume = v["v"].as_str()?.parse::<f64>().ok()?;
    let num_trades = v["n"].as_u64()?;

    Some(Candle {
        symbol,
        interval,
        open_time,
        close_time,
        open,
        high,
        low,
        close,
        volume,
        num_trades,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_candle_json() -> Value {
        json!({
            "t": 1700000000000i64,
            "T": 1700000900000i64,
            "s": "BTC",
            "i": "15m",
            "o": "29295.0",
            "c": "29500.0",
            "h": "29600.0",
            "l": "29200.0",
            "v": "123.456",
            "n": 4321u64
        })
    }

    #[test]
    fn test_parse_candle_valid() {
        let v = sample_candle_json();
        let candle = parse_candle_from_value(&v).expect("should parse");
        assert_eq!(candle.symbol, "BTC");
        assert_eq!(candle.interval, "15m");
        assert_eq!(candle.open_time, 1700000000000);
        assert_eq!(candle.close_time, 1700000900000);
        assert!((candle.open - 29295.0).abs() < 1e-9);
        assert!((candle.close - 29500.0).abs() < 1e-9);
        assert!((candle.high - 29600.0).abs() < 1e-9);
        assert!((candle.low - 29200.0).abs() < 1e-9);
        assert!((candle.volume - 123.456).abs() < 1e-9);
        assert_eq!(candle.num_trades, 4321);
    }

    #[test]
    fn test_parse_candle_missing_field() {
        // Missing "v" (volume) field — should return None
        let v = json!({
            "t": 1700000000000i64,
            "T": 1700000900000i64,
            "s": "BTC",
            "i": "15m",
            "o": "29295.0",
            "c": "29500.0",
            "h": "29600.0",
            "l": "29200.0",
            // "v" intentionally omitted
            "n": 4321u64
        });
        assert!(parse_candle_from_value(&v).is_none());
    }
}
