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
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("failed to build HTTP client"),
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
                if let Some(fd) = parse_funding_from_value(&coin, venue_pair) {
                    result.push(fd);
                }
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

        let now = chrono::Utc::now().timestamp_millis();
        let mut oi_data = Vec::new();
        let mut vlm_data = Vec::new();

        for (i, ctx) in asset_ctxs.iter().enumerate() {
            let name = universe
                .get(i)
                .and_then(|u| u["name"].as_str())
                .unwrap_or("");

            if let Some((oi, vlm)) = parse_asset_ctx(name, ctx, now) {
                oi_data.push(oi);
                vlm_data.push(vlm);
            }
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

        let now = chrono::Utc::now().timestamp_millis();
        let mut result = Vec::new();
        for ap in asset_positions {
            if let Some(hlp) = parse_position(ap, now) {
                result.push(hlp);
            }
        }
        Ok(result)
    }
}

/// Parse a single HL candle JSON object into a `Candle`.
/// Returns `None` if any required field is absent or unparseable,
/// or if float values are non-finite / logically invalid (high < low, close <= 0).
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

    // Validate: all floats must be finite
    if !open.is_finite() || !close.is_finite() || !high.is_finite()
        || !low.is_finite() || !volume.is_finite()
    {
        return None;
    }
    // Validate: high >= low and close > 0
    if high < low || close <= 0.0 {
        return None;
    }

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

/// Parse a single element from the predictedFundings venue pair array.
/// Input: `[venue, {fundingRate, nextFundingTime}]`
pub(crate) fn parse_funding_from_value(coin: &str, venue_pair: &Value) -> Option<FundingData> {
    let vp = venue_pair.as_array()?;
    if vp.len() < 2 {
        return None;
    }
    let data = &vp[1];
    let funding_rate = data["fundingRate"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())?;
    let next_funding_time = data["nextFundingTime"].as_i64()?;
    Some(FundingData {
        symbol: coin.to_string(),
        funding_rate,
        predicted_rate: funding_rate,
        timestamp: next_funding_time,
    })
}

/// Parse one asset context from metaAndAssetCtxs response.
/// Returns `(OIData, (symbol, day_ntl_vlm))` or None if fields are missing.
pub(crate) fn parse_asset_ctx(name: &str, ctx: &Value, now: i64) -> Option<(OIData, (String, f64))> {
    let open_interest = ctx["openInterest"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())?;
    let day_ntl_vlm = ctx["dayNtlVlm"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())?;
    Some((
        OIData {
            symbol: name.to_string(),
            open_interest,
            timestamp: now,
        },
        (name.to_string(), day_ntl_vlm),
    ))
}

/// Parse one position from clearinghouseState assetPositions array.
pub(crate) fn parse_position(ap: &Value, now: i64) -> Option<HlpData> {
    let pos = &ap["position"];
    let coin = pos["coin"].as_str()?.to_string();
    let position_size = pos["szi"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())?;
    let entry_price = pos["entryPx"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())?;
    let unrealized_pnl = pos["unrealizedPnl"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())?;
    Some(HlpData {
        symbol: coin,
        position_size,
        entry_price,
        unrealized_pnl,
        timestamp: now,
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

    #[test]
    fn test_parse_candle_high_less_than_low() {
        let v = json!({
            "t": 1700000000000i64,
            "T": 1700000900000i64,
            "s": "BTC",
            "i": "15m",
            "o": "29295.0",
            "c": "29500.0",
            "h": "29100.0",  // high < low
            "l": "29200.0",
            "v": "123.456",
            "n": 4321u64
        });
        assert!(parse_candle_from_value(&v).is_none());
    }

    #[test]
    fn test_parse_candle_close_zero() {
        let v = json!({
            "t": 1700000000000i64,
            "T": 1700000900000i64,
            "s": "BTC",
            "i": "15m",
            "o": "0.0",
            "c": "0.0",      // close <= 0
            "h": "1.0",
            "l": "0.0",
            "v": "0.0",
            "n": 0u64
        });
        assert!(parse_candle_from_value(&v).is_none());
    }

    #[test]
    fn test_parse_candle_infinity() {
        let v = json!({
            "t": 1700000000000i64,
            "T": 1700000900000i64,
            "s": "BTC",
            "i": "15m",
            "o": "inf",
            "c": "29500.0",
            "h": "29600.0",
            "l": "29200.0",
            "v": "123.456",
            "n": 4321u64
        });
        assert!(parse_candle_from_value(&v).is_none());
    }

    // ── parse_funding_from_value tests ──────────────────────────────────

    #[test]
    fn test_parse_funding_valid() {
        let venue_pair = json!(["Hyperliquid", {
            "fundingRate": "0.0001",
            "nextFundingTime": 1700001000000i64
        }]);
        let fd = parse_funding_from_value("BTC", &venue_pair).expect("should parse");
        assert_eq!(fd.symbol, "BTC");
        assert!((fd.funding_rate - 0.0001).abs() < 1e-12);
        assert_eq!(fd.predicted_rate, fd.funding_rate);
        assert_eq!(fd.timestamp, 1700001000000);
    }

    #[test]
    fn test_parse_funding_missing_rate() {
        let venue_pair = json!(["Hyperliquid", {
            "nextFundingTime": 1700001000000i64
        }]);
        assert!(parse_funding_from_value("BTC", &venue_pair).is_none());
    }

    #[test]
    fn test_parse_funding_malformed_not_array() {
        let venue_pair = json!("garbage");
        assert!(parse_funding_from_value("BTC", &venue_pair).is_none());
    }

    #[test]
    fn test_parse_funding_short_array() {
        let venue_pair = json!(["only_one"]);
        assert!(parse_funding_from_value("BTC", &venue_pair).is_none());
    }

    // ── parse_asset_ctx tests ───────────────────────────────────────────

    #[test]
    fn test_parse_asset_ctx_valid() {
        let ctx = json!({
            "openInterest": "12345.67",
            "dayNtlVlm": "98765432.10"
        });
        let now = 1700000000000i64;
        let (oi, vlm) = parse_asset_ctx("ETH", &ctx, now).expect("should parse");
        assert_eq!(oi.symbol, "ETH");
        assert!((oi.open_interest - 12345.67).abs() < 1e-9);
        assert_eq!(oi.timestamp, now);
        assert_eq!(vlm.0, "ETH");
        assert!((vlm.1 - 98765432.10).abs() < 1e-2);
    }

    #[test]
    fn test_parse_asset_ctx_missing_oi() {
        let ctx = json!({
            "dayNtlVlm": "1000.0"
        });
        assert!(parse_asset_ctx("ETH", &ctx, 0).is_none());
    }

    #[test]
    fn test_parse_asset_ctx_missing_vlm() {
        let ctx = json!({
            "openInterest": "1000.0"
        });
        assert!(parse_asset_ctx("ETH", &ctx, 0).is_none());
    }

    // ── parse_position tests ────────────────────────────────────────────

    #[test]
    fn test_parse_position_valid() {
        let ap = json!({
            "position": {
                "coin": "BTC",
                "szi": "-1.5",
                "entryPx": "50000.0",
                "unrealizedPnl": "-200.5"
            }
        });
        let now = 1700000000000i64;
        let hlp = parse_position(&ap, now).expect("should parse");
        assert_eq!(hlp.symbol, "BTC");
        assert!((hlp.position_size - (-1.5)).abs() < 1e-9);
        assert!((hlp.entry_price - 50000.0).abs() < 1e-9);
        assert!((hlp.unrealized_pnl - (-200.5)).abs() < 1e-9);
        assert_eq!(hlp.timestamp, now);
    }

    #[test]
    fn test_parse_position_missing_coin() {
        let ap = json!({
            "position": {
                "szi": "1.0",
                "entryPx": "50000.0",
                "unrealizedPnl": "0.0"
            }
        });
        assert!(parse_position(&ap, 0).is_none());
    }

    #[test]
    fn test_parse_position_missing_szi() {
        let ap = json!({
            "position": {
                "coin": "BTC",
                "entryPx": "50000.0",
                "unrealizedPnl": "0.0"
            }
        });
        assert!(parse_position(&ap, 0).is_none());
    }

    #[test]
    fn test_parse_position_no_position_key() {
        let ap = json!({ "other": "stuff" });
        assert!(parse_position(&ap, 0).is_none());
    }
}
