use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn};

use hyperfun_core::config::AppConfig;
use hyperfun_core::{Candle, CandleIndicator, HlSignalProvider, SignalAction};
use hyperfun_executor::PaperExecutor;
use hyperfun_market::ws::HlWsClient;
use hyperfun_market::MarketDataEngine;
use hyperfun_signal::aggregator::SignalAggregator;
use hyperfun_signal::factors::FactorGroup;
use hyperfun_signal::hl_signals::funding::FundingSignal;
use hyperfun_signal::hl_signals::hlp_inventory::HlpInventorySignal;
use hyperfun_signal::hl_signals::liquidation::LiquidationSignal;
use hyperfun_signal::hl_signals::whale_flow::WhaleFlowSignal;
use hyperfun_signal::indicators::atr::Atr;
use hyperfun_signal::indicators::bollinger::BollingerBandsIndicator;
use hyperfun_signal::indicators::cci::Cci;
use hyperfun_signal::indicators::ema::EmaCrossover;
use hyperfun_signal::indicators::macd::MacdHistogram;
use hyperfun_signal::indicators::rsi::Rsi;
use hyperfun_signal::indicators::supertrend::Supertrend;

#[tokio::main]
async fn main() -> Result<()> {
    // ── 1. Load config ───────────────────────────────────────────────────
    let config = AppConfig::load().expect("failed to load config/default.toml");

    // ── 2. Init tracing (JSON format, env filter from config) ────────────
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(format!("hyperfun={}", config.general.log_level))
    });
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(env_filter)
        .init();

    info!(mode = %config.general.mode, "hyperfun starting");

    // ── 3. Create MarketDataEngine and backfill ──────────────────────────
    let mut engine = MarketDataEngine::new(&config);
    engine.backfill().await?;

    // ── 4. Create factor groups ──────────────────────────────────────────
    let tc = &config.indicators.trend;
    let mut trend_group = FactorGroup::new("trend", tc.weight);
    trend_group.add_indicator(Box::new(EmaCrossover::new(
        tc.ema_short as usize,
        tc.ema_long as usize,
    )));
    trend_group.add_indicator(Box::new(MacdHistogram::new(
        tc.macd_fast as usize,
        tc.macd_slow as usize,
        tc.macd_signal as usize,
    )));
    trend_group.add_indicator(Box::new(Supertrend::new(
        tc.supertrend_period as usize,
        tc.supertrend_multiplier,
    )));

    let mc = &config.indicators.momentum;
    let mut momentum_group = FactorGroup::new("momentum", mc.weight);
    momentum_group.add_indicator(Box::new(Rsi::new(
        mc.rsi_period as usize,
        mc.rsi_overbought,
        mc.rsi_oversold,
    )));
    momentum_group.add_indicator(Box::new(Cci::new(mc.cci_period as usize)));

    let vc = &config.indicators.volatility;
    let mut volatility_group = FactorGroup::new("volatility", vc.weight);
    // We keep a separate ATR for stop-loss calculations; the one inside the
    // volatility group is used purely for scoring.
    volatility_group.add_indicator(Box::new(Atr::new(vc.atr_period as usize)));
    volatility_group.add_indicator(Box::new(BollingerBandsIndicator::new(
        vc.bollinger_period as usize,
        vc.bollinger_std,
    )));

    // Per-symbol standalone ATR used by the executor for stop-loss distance.
    // (keyed by symbol)
    let mut atr_for_stop: std::collections::HashMap<String, Atr> =
        std::collections::HashMap::new();
    for sym in &config.symbols.watchlist {
        atr_for_stop.insert(sym.clone(), Atr::new(vc.atr_period as usize));
    }

    // ── 5. Create HL-native signals ──────────────────────────────────────
    // NOTE: these signals won't receive data until REST polling tasks are
    // spawned (not implemented yet). They'll return ready() = false and be
    // excluded from scoring. That's OK for the initial version.
    let hlp_signal = HlpInventorySignal::new();
    let liquidation_signal = LiquidationSignal::new(
        config.indicators.hl_native.liquidation_lookback_secs as i64,
    );
    let whale_signal = WhaleFlowSignal::new();
    let funding_signal = FundingSignal::new(
        config.indicators.funding.funding_extreme_threshold,
    );

    // ── 6. Create SignalAggregator ───────────────────────────────────────
    let mut aggregator = SignalAggregator::new(
        config.signal.open_threshold,
        config.signal.close_threshold,
    );

    // ── 7. Create PaperExecutor ──────────────────────────────────────────
    let mut executor = PaperExecutor::new(
        config.paper.simulated_slippage_pct,
        config.paper.simulated_fee_pct,
        config.paper.position_size_usd,
        config.paper.atr_stop_multiplier,
    );

    // ── 8. Warm up indicators with backfilled candle data ────────────────
    let entry_tf = &config.timeframes.entry;
    for symbol in &config.symbols.watchlist {
        let candles = engine
            .candle_store()
            .get_last_n(symbol, entry_tf, 500);

        let count = candles.len();
        for candle in candles {
            trend_group.update_all(candle);
            momentum_group.update_all(candle);
            volatility_group.update_all(candle);
            if let Some(atr) = atr_for_stop.get_mut(symbol) {
                atr.update(candle);
            }
        }

        info!(
            symbol = %symbol,
            candles_warmed = count,
            trend_ready = trend_group.ready(),
            momentum_ready = momentum_group.ready(),
            volatility_ready = volatility_group.ready(),
            "indicators warmed up"
        );
    }

    // ── 9. Spawn WS candle stream task ───────────────────────────────────
    let (candle_tx, mut candle_rx) = mpsc::channel::<Candle>(256);
    let ws_client = HlWsClient::new(engine.ws_url());
    let ws_symbols = config.symbols.watchlist.clone();
    let ws_intervals = vec![
        config.timeframes.trend.clone(),
        config.timeframes.entry.clone(),
    ];

    tokio::spawn(async move {
        ws_client.run(ws_symbols, ws_intervals, candle_tx).await;
    });

    info!("WS candle stream spawned — entering main loop");

    // ── 10. Main loop ────────────────────────────────────────────────────
    let summary_interval_ms =
        (config.paper.summary_interval_mins as i64) * 60 * 1000;
    let mut last_summary_ts: i64 = 0;

    let hl_native_weight = config.indicators.hl_native.weight;
    let funding_weight = config.indicators.funding.weight;

    while let Some(candle) = candle_rx.recv().await {
        let symbol = candle.symbol.clone();
        let interval = candle.interval.clone();
        let price = candle.close;
        let timestamp = candle.close_time;

        // Store in CandleStore
        engine.candle_store_mut().push(candle.clone());

        // Only process entry-timeframe candles for signal decisions
        if interval != *entry_tf {
            continue;
        }

        // Update indicators
        trend_group.update_all(&candle);
        momentum_group.update_all(&candle);
        volatility_group.update_all(&candle);
        if let Some(atr) = atr_for_stop.get_mut(&symbol) {
            atr.update(&candle);
        }

        // Check stop losses first
        executor.check_stop_losses(&symbol, price);
        if !executor.has_position(&symbol) {
            aggregator.clear_position(&symbol);
        }

        // Compute composite score
        // Build the factor_scores slice for the aggregator.
        // HL-native signals are included but will return None when not ready,
        // which causes them to be excluded from the weighted average.
        let hl_native_score = if hlp_signal.ready()
            || liquidation_signal.ready()
            || whale_signal.ready()
        {
            // Average of whichever HL-native signals are ready
            let mut sum = 0.0f64;
            let mut n = 0u32;
            if hlp_signal.ready() {
                sum += hlp_signal.score();
                n += 1;
            }
            if liquidation_signal.ready() {
                sum += liquidation_signal.score();
                n += 1;
            }
            if whale_signal.ready() {
                sum += whale_signal.score();
                n += 1;
            }
            Some(sum / n as f64)
        } else {
            None
        };

        let funding_score = if funding_signal.ready() {
            Some(funding_signal.score())
        } else {
            None
        };

        let factor_scores: Vec<(&str, f64, Option<f64>)> = vec![
            ("trend", trend_group.weight, trend_group.score()),
            ("momentum", momentum_group.weight, momentum_group.score()),
            ("volatility", volatility_group.weight, volatility_group.score()),
            ("hl_native", hl_native_weight, hl_native_score),
            ("funding", funding_weight, funding_score),
        ];

        let (composite_score, details) = aggregator.compute_score(&factor_scores);

        // Decide action
        let action = aggregator.decide(&symbol, composite_score);

        // Get current ATR value for stop-loss sizing
        let current_atr = atr_for_stop
            .get(&symbol)
            .map(|a| a.atr_value())
            .unwrap_or(0.0);

        // Execute via paper executor
        match action {
            SignalAction::Open(dir) => {
                info!(
                    symbol = %symbol,
                    direction = ?dir,
                    composite = composite_score,
                    details = ?details,
                    "signal: OPEN"
                );
                executor.execute_signal(action, &symbol, price, current_atr, timestamp);
                aggregator.set_position(&symbol, dir);
            }
            SignalAction::Close => {
                info!(
                    symbol = %symbol,
                    composite = composite_score,
                    "signal: CLOSE"
                );
                executor.execute_signal(action, &symbol, price, current_atr, timestamp);
                aggregator.clear_position(&symbol);
            }
            SignalAction::Hold => {
                // Update unrealized PnL
                executor.update_unrealized_pnl(&symbol, price);
            }
        }

        // Log stats periodically
        if timestamp - last_summary_ts >= summary_interval_ms {
            let stats = executor.stats();
            info!(
                total_trades = stats.total_trades,
                winning = stats.winning_trades,
                losing = stats.losing_trades,
                total_pnl = format!("{:.2}", stats.total_pnl),
                win_rate = format!("{:.1}%", stats.win_rate() * 100.0),
                max_drawdown = format!("{:.2}%", stats.max_drawdown * 100.0),
                profit_factor = format!("{:.2}", stats.profit_factor()),
                "periodic summary"
            );
            last_summary_ts = timestamp;
        }
    }

    warn!("candle stream ended — shutting down");
    Ok(())
}
