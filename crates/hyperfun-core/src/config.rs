use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub symbols: SymbolsConfig,
    pub timeframes: TimeframesConfig,
    pub indicators: IndicatorsConfig,
    pub signal: SignalConfig,
    pub paper: PaperConfig,
    pub hyperliquid: HyperliquidConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeneralConfig {
    pub mode: String,
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolsConfig {
    pub watchlist: Vec<String>,
    pub min_daily_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeframesConfig {
    pub trend: String,
    pub entry: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorsConfig {
    pub trend: TrendConfig,
    pub momentum: MomentumConfig,
    pub volatility: VolatilityConfig,
    pub hl_native: HlNativeConfig,
    pub funding: FundingConfig,
    pub mtf: MtfConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendConfig {
    pub ema_short: u32,
    pub ema_long: u32,
    pub macd_fast: u32,
    pub macd_slow: u32,
    pub macd_signal: u32,
    pub supertrend_period: u32,
    pub supertrend_multiplier: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MomentumConfig {
    pub rsi_period: u32,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub cci_period: u32,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolatilityConfig {
    pub atr_period: u32,
    pub bollinger_period: u32,
    pub bollinger_std: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HlNativeConfig {
    pub hlp_vault_address: String,
    pub whale_addresses: Vec<String>,
    pub liquidation_lookback_secs: u64,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingConfig {
    pub funding_extreme_threshold: f64,
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MtfConfig {
    pub weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    pub open_threshold: f64,
    pub close_threshold: f64,
    pub cooldown_bars: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperConfig {
    pub simulated_slippage_pct: f64,
    pub simulated_fee_pct: f64,
    pub position_size_usd: f64,
    pub atr_stop_multiplier: f64,
    pub trailing_stop_activation_atr: f64,
    pub trailing_stop_distance_atr: f64,
    pub summary_interval_mins: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidConfig {
    pub ws_url: String,
    pub rest_url: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, config::ConfigError> {
        let cfg = config::Config::builder()
            .add_source(config::File::with_name("config/default"))
            .add_source(config::Environment::with_prefix("HYPERFUN").separator("__"))
            .build()?;
        cfg.try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_default_config() {
        // Change to repo root so config/default.toml can be found.
        // In a workspace, tests run from the crate directory; use CARGO_MANIFEST_DIR
        // to navigate to the workspace root.
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root = std::path::Path::new(manifest_dir)
            .parent()  // crates/
            .unwrap()
            .parent()  // workspace root
            .unwrap();

        let cfg_path = workspace_root.join("config/default");
        let cfg = config::Config::builder()
            .add_source(config::File::with_name(cfg_path.to_str().unwrap()))
            .build()
            .expect("failed to build config");

        let app: AppConfig = cfg.try_deserialize().expect("failed to deserialize AppConfig");

        assert_eq!(app.general.mode, "paper");
        assert_eq!(app.general.log_level, "info");
        assert!(app.symbols.watchlist.contains(&"BTC".to_string()));
        assert_eq!(app.timeframes.trend, "4h");
        assert_eq!(app.timeframes.entry, "15m");
        assert!((app.indicators.trend.weight - 0.25).abs() < f64::EPSILON);
        assert_eq!(app.indicators.trend.ema_short, 20);
        assert_eq!(app.indicators.trend.ema_long, 50);
        assert_eq!(app.indicators.momentum.rsi_period, 14);
        assert!((app.signal.open_threshold - 0.35).abs() < f64::EPSILON);
        assert!((app.signal.close_threshold - 0.12).abs() < f64::EPSILON);
        assert_eq!(app.paper.position_size_usd, 1000.0);
        assert_eq!(app.hyperliquid.ws_url, "wss://api.hyperliquid.xyz/ws");
    }
}
