use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use hyperfun_core::config::AppConfig;
use hyperfun_core::{Candle, CandleIndicator, Direction, HlSignalProvider, MarketData, SignalAction, TradeEvent};
use hyperfun_executor::PaperExecutor;
use hyperfun_market::rest::HlRestClient;
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
use hyperfun_storage::{
    parse_database_url_redacted, spawn_reconnect_task, spawn_writer_task,
    BarScoreRecord, JournalWriter, StorageHandle, WriteOp,
};
use sqlx::postgres::PgPoolOptions;

/// Per-symbol state: independent indicator instances so that candles from
/// different symbols never pollute each other's EMA / RSI / etc.
struct SymbolState {
    trend_group: FactorGroup,
    momentum_group: FactorGroup,
    volatility_group: FactorGroup,
    atr_for_stop: Atr,
    hlp_signal: HlpInventorySignal,
    liquidation_signal: LiquidationSignal,
    whale_signal: WhaleFlowSignal,
    funding_signal: FundingSignal,
    /// Whether whale addresses are configured — when false, whale_signal
    /// is excluded from HL-native readiness so it doesn't block scoring.
    whale_configured: bool,
}

impl SymbolState {
    fn new(config: &AppConfig) -> Self {
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
        volatility_group.add_indicator(Box::new(Atr::new(vc.atr_period as usize)));
        volatility_group.add_indicator(Box::new(BollingerBandsIndicator::new(
            vc.bollinger_period as usize,
            vc.bollinger_std,
        )));

        let atr_for_stop = Atr::new(vc.atr_period as usize);

        let hlp_signal = HlpInventorySignal::new();
        let liquidation_signal = LiquidationSignal::new(
            config.indicators.hl_native.liquidation_lookback_secs as i64,
        );
        let whale_signal = WhaleFlowSignal::new();
        let funding_signal = FundingSignal::new(
            config.indicators.funding.funding_extreme_threshold,
        );

        let whale_configured = !config.indicators.hl_native.whale_addresses.is_empty();

        Self {
            trend_group,
            momentum_group,
            volatility_group,
            atr_for_stop,
            hlp_signal,
            liquidation_signal,
            whale_signal,
            funding_signal,
            whale_configured,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Load config
    let config = AppConfig::load().expect("failed to load config/default.toml");

    // 2. Init tracing
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(format!("hyperfun={}", config.general.log_level))
    });
    tracing_subscriber::fmt().json().with_env_filter(env_filter).init();
    info!(mode = %config.general.mode, "hyperfun starting");

    // 3. Connect to Postgres (fail-open on failure)
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL env var not set")?;
    let (connect_opts, redacted) = parse_database_url_redacted(&database_url)?;
    info!(url = %redacted, "connecting to Postgres");

    let pool_opt: Option<sqlx::PgPool> = match PgPoolOptions::new()
        .max_connections(config.storage.pool_size)
        .connect_with(connect_opts.clone()).await
    {
        Ok(p) => {
            info!("Postgres pool connected");
            sqlx::migrate!("crates/hyperfun-storage/migrations")
                .run(&p).await
                .context("migrations failed")?;
            info!("migrations applied");
            Some(p)
        }
        Err(e) => {
            warn!(error = %e, "Postgres connection failed — starting in JSONL fallback mode");
            None
        }
    };
    let pool = Arc::new(RwLock::new(pool_opt));

    // 4. Spawn writer + reconnect tasks
    let journal = JournalWriter::new("data")?;
    let (storage_handle, _writer_join) = spawn_writer_task(pool.clone(), journal, 1024);
    let _reconnect_join = spawn_reconnect_task(
        pool.clone(), connect_opts, config.storage.pool_size, config.storage.reconnect_secs,
    );

    // 5. MarketDataEngine, with cache-aware backfill
    let mut engine = MarketDataEngine::new(&config);

    let mut start_overrides: HashMap<(String, String), i64> = HashMap::new();
    if let Some(pool_ref) = pool.read().await.as_ref() {
        match hyperfun_storage::load_candles_bulk(pool_ref).await {
            Ok(candles) => {
                let count = candles.len();
                for c in candles {
                    engine.candle_store_mut().push(c);
                }
                info!(count, "loaded candles from candle_cache");
            }
            Err(e) => warn!(error = %e, "candle_cache load failed; proceeding with empty cache"),
        }

        match hyperfun_storage::max_close_times(pool_ref).await {
            Ok(covs) => {
                for cov in covs {
                    let expected_count: i64 = 500;
                    let coverage_ok = cov.count >= (expected_count * 95 / 100) && cov.count >= 100;
                    if coverage_ok {
                        if let Some(max_ct) = cov.max_close_time {
                            start_overrides.insert((cov.symbol.clone(), cov.interval.clone()), max_ct);
                        }
                    }
                }
            }
            Err(e) => warn!(error = %e, "max_close_times query failed"),
        }
    }

    engine.backfill_incremental(&start_overrides).await?;

    // 6. Per-symbol state + warm indicators
    let mut symbol_states: HashMap<String, SymbolState> = HashMap::new();
    for sym in &config.symbols.watchlist {
        symbol_states.insert(sym.clone(), SymbolState::new(&config));
    }
    let entry_tf = config.timeframes.entry.clone();
    for symbol in &config.symbols.watchlist {
        let candles = engine.candle_store().get_last_n(symbol, &entry_tf, 500);
        let count = candles.len();
        if let Some(state) = symbol_states.get_mut(symbol) {
            for candle in candles {
                state.trend_group.update_all(candle);
                state.momentum_group.update_all(candle);
                state.volatility_group.update_all(candle);
                state.atr_for_stop.update(candle);
            }
            info!(
                symbol = %symbol,
                candles_warmed = count,
                trend_ready = state.trend_group.ready(),
                momentum_ready = state.momentum_group.ready(),
                volatility_ready = state.volatility_group.ready(),
                "indicators warmed up"
            );
        }
    }

    // 7. Create aggregator and executor
    let entry_interval_ms = parse_interval_ms(&config.timeframes.entry);
    let mut aggregator = SignalAggregator::new(
        config.signal.open_threshold,
        config.signal.close_threshold,
        config.signal.cooldown_bars,
        entry_interval_ms,
    );
    let mut executor = PaperExecutor::new(
        config.paper.simulated_slippage_pct,
        config.paper.simulated_fee_pct,
        config.paper.position_size_usd,
        config.paper.atr_stop_multiplier,
        config.paper.trailing_stop_activation_atr,
        config.paper.trailing_stop_distance_atr,
    );

    // 8. Restore positions + cooldowns (fatal on failure)
    if let Some(pool_ref) = pool.read().await.as_ref() {
        let positions = hyperfun_storage::load_positions(pool_ref).await
            .context("FATAL: could not load positions — refusing to start")?;
        for pos in positions {
            let symbol = pos.symbol.clone();
            let direction = pos.direction;
            executor.restore_position(pos);
            aggregator.set_position(&symbol, direction);
            info!(symbol = %symbol, direction = ?direction, "restored position from DB");
        }

        let cooldowns = hyperfun_storage::load_cooldowns(pool_ref).await
            .context("FATAL: could not load cooldowns — refusing to start")?;
        for c in cooldowns {
            aggregator.restore_cooldown(&c.symbol, c.cooldown_until_ts, c.cooldown_bars);
            info!(symbol = %c.symbol, until_ts = c.cooldown_until_ts, bars = c.cooldown_bars, "restored cooldown");
        }
    } else {
        warn!("pool unavailable at startup — positions and cooldowns not restored (JSONL mode)");
    }

    run_main_loop(&mut engine, symbol_states, aggregator, executor, storage_handle, &config).await
}

fn parse_interval_ms(interval: &str) -> i64 {
    if interval.is_empty() {
        return 60_000;
    }
    let (num_str, unit) = interval.split_at(interval.len().saturating_sub(1));
    let n: i64 = num_str.parse().unwrap_or(0);
    let unit_ms: i64 = match unit {
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => 60_000,
    };
    n * unit_ms
}

async fn run_main_loop(
    engine: &mut MarketDataEngine,
    mut symbol_states: HashMap<String, SymbolState>,
    mut aggregator: SignalAggregator,
    mut executor: PaperExecutor,
    storage_handle: StorageHandle,
    config: &AppConfig,
) -> Result<()> {
    let entry_tf = config.timeframes.entry.clone();
    let trend_tf = config.timeframes.trend.clone();
    let hl_native_weight = config.indicators.hl_native.weight;
    let funding_weight = config.indicators.funding.weight;

    let (md_tx, mut md_rx) = mpsc::channel::<MarketData>(256);
    spawn_rest_pollers(&md_tx, config);
    drop(md_tx);

    let (candle_tx, mut candle_rx) = mpsc::channel::<Candle>(256);
    let ws_client = HlWsClient::new(engine.ws_url());
    let ws_symbols = config.symbols.watchlist.clone();
    let ws_intervals = vec![config.timeframes.trend.clone(), config.timeframes.entry.clone()];
    tokio::spawn(async move {
        ws_client.run(ws_symbols, ws_intervals, candle_tx).await;
    });

    info!("WS candle stream and REST pollers spawned — entering main loop");

    let mut last_candle: HashMap<(String, String), Candle> = HashMap::new();
    let summary_interval_ms = (config.paper.summary_interval_mins as i64) * 60 * 1000;
    let mut last_summary_ts: i64 = 0;
    let mut bar_close_count: u64 = 0;
    let mut mid_bar_count: u64 = 0;
    let mut signal_count: u64 = 0;

    loop {
        tokio::select! {
            Some(candle) = candle_rx.recv() => {
                let symbol = candle.symbol.clone();
                let interval = candle.interval.clone();
                let bar_key = (symbol.clone(), interval.clone());

                let prev = last_candle.get(&bar_key).cloned();
                let closed_bar = match prev {
                    Some(ref pc) if candle.open_time != pc.open_time => Some(pc.clone()),
                    _ => None,
                };
                last_candle.insert(bar_key, candle.clone());

                if let Some(ref closed) = closed_bar {
                    engine.candle_store_mut().push(closed.clone());
                    let _ = storage_handle.try_send(WriteOp::CandleClose(closed.clone()));
                }

                if engine.candle_store().is_stale() { continue; }
                if interval != entry_tf { continue; }

                let closed = match closed_bar {
                    Some(c) => c,
                    None => {
                        mid_bar_count += 1;
                        executor.update_unrealized_pnl(&symbol, candle.close);
                        debug!(
                            symbol = %symbol,
                            open_time = candle.open_time,
                            price = candle.close,
                            mid_bar_count,
                            "mid-bar tick"
                        );
                        continue;
                    }
                };
                let closed_price = closed.close;
                let closed_ts = closed.close_time;
                bar_close_count += 1;

                let state = match symbol_states.get_mut(&symbol) {
                    Some(s) => s,
                    None => continue,
                };

                // Stop-loss check on bar-close
                let current_atr_for_stop = state.atr_for_stop.atr_value();
                if let Some(stop_event) = executor.check_stop_losses(&symbol, closed_price, current_atr_for_stop, closed_ts) {
                    handle_trade_event(stop_event, &mut aggregator, &storage_handle, closed_ts, &symbol).await;
                }

                // Update indicators with closed bar
                state.trend_group.update_all(&closed);
                state.momentum_group.update_all(&closed);
                state.volatility_group.update_all(&closed);
                state.atr_for_stop.update(&closed);

                // Compute composite score
                let hl_native_score = {
                    let hlp_ready = state.hlp_signal.ready();
                    let whale_ready = !state.whale_configured || state.whale_signal.ready();
                    if hlp_ready && whale_ready {
                        let mut sum = state.hlp_signal.score();
                        let mut count = 1.0_f64;
                        if state.whale_configured {
                            sum += state.whale_signal.score();
                            count += 1.0;
                        }
                        Some(sum / count)
                    } else { None }
                };
                let funding_score = if state.funding_signal.ready() { Some(state.funding_signal.score()) } else { None };
                let trend_score = state.trend_group.score();
                let momentum_score = state.momentum_group.score();
                let volatility_score = state.volatility_group.score();

                let factor_scores: Vec<(&str, f64, Option<f64>)> = vec![
                    ("trend", state.trend_group.weight, trend_score),
                    ("momentum", state.momentum_group.weight, momentum_score),
                    ("volatility", state.volatility_group.weight, volatility_score),
                    ("hl_native", hl_native_weight, hl_native_score),
                    ("funding", funding_weight, funding_score),
                ];
                let (composite_score, details) = aggregator.compute_score(&factor_scores);

                info!(
                    symbol = %symbol, bar_close_count,
                    open_time = closed.open_time, close_price = closed_price,
                    composite = format!("{:.4}", composite_score),
                    trend = format!("{:?}", trend_score),
                    momentum = format!("{:?}", momentum_score),
                    volatility = format!("{:?}", volatility_score),
                    hl_native = format!("{:?}", hl_native_score),
                    funding = format!("{:?}", funding_score),
                    "bar closed — scores evaluated"
                );

                let trend_direction = engine.candle_store().last(&symbol, &trend_tf)
                    .map(|c| if c.close > c.open { Direction::Long } else { Direction::Short });
                let action = aggregator.decide(&symbol, composite_score, closed_ts);
                let action = match (action, trend_direction) {
                    (SignalAction::Open(Direction::Long), Some(Direction::Short)) => {
                        info!(symbol = %symbol, "MTF filter: suppressed Long signal (trend is Short)");
                        SignalAction::Hold
                    }
                    (SignalAction::Open(Direction::Short), Some(Direction::Long)) => {
                        info!(symbol = %symbol, "MTF filter: suppressed Short signal (trend is Long)");
                        SignalAction::Hold
                    }
                    (a, _) => a,
                };

                let action_label = match action {
                    SignalAction::Open(Direction::Long) => "open_long",
                    SignalAction::Open(Direction::Short) => "open_short",
                    SignalAction::Close => "close",
                    SignalAction::Hold => "hold",
                };
                let _ = storage_handle.try_send(WriteOp::BarScore(BarScoreRecord {
                    ts: closed_ts,
                    symbol: symbol.clone(),
                    open_time: closed.open_time,
                    close_price: closed_price,
                    composite: composite_score,
                    trend: trend_score,
                    momentum: momentum_score,
                    volatility: volatility_score,
                    hl_native: hl_native_score,
                    funding: funding_score,
                    action: action_label.into(),
                }));

                let current_atr = state.atr_for_stop.atr_value();
                let event = executor.execute_signal(action, &symbol, closed_price, current_atr, closed_ts);
                if !matches!(event, TradeEvent::None) {
                    signal_count += 1;
                    info!(
                        symbol = %symbol,
                        composite = format!("{:.4}", composite_score),
                        details = ?details,
                        signal_count,
                        "signal executed"
                    );
                }
                handle_trade_event(event, &mut aggregator, &storage_handle, closed_ts, &symbol).await;

                executor.update_unrealized_pnl(&symbol, closed_price);

                if closed_ts - last_summary_ts >= summary_interval_ms {
                    let stats = executor.stats();
                    info!(
                        total_trades = stats.total_trades,
                        winning = stats.winning_trades,
                        losing = stats.losing_trades,
                        total_pnl = format!("{:.2}", stats.total_pnl),
                        win_rate = format!("{:.1}%", stats.win_rate() * 100.0),
                        max_drawdown = format!("{:.2}%", stats.max_drawdown * 100.0),
                        profit_factor = format!("{:.2}", stats.profit_factor()),
                        bar_close_count, mid_bar_count, signal_count,
                        "periodic summary"
                    );
                    last_summary_ts = closed_ts;
                }
            }
            Some(market_data) = md_rx.recv() => {
                dispatch_market_data(&market_data, &mut symbol_states, &storage_handle).await;
            }
            else => break,
        }
    }

    warn!("all streams ended — shutting down");
    Ok(())
}

async fn handle_trade_event(
    event: TradeEvent,
    aggregator: &mut SignalAggregator,
    storage: &StorageHandle,
    closed_ts: i64,
    symbol: &str,
) {
    let cooldown_bars = aggregator.cooldown_bars();
    match event {
        TradeEvent::None => {}
        TradeEvent::Opened { position, mut trade } => {
            trade.ts = closed_ts;
            aggregator.set_position(symbol, position.direction);
            let _ = storage.send_with_timeout(WriteOp::OpenTrade { trade, position }).await;
        }
        TradeEvent::Closed { mut trade, .. } => {
            trade.ts = closed_ts;
            let until = aggregator.clear_position(symbol, closed_ts).unwrap_or(closed_ts);
            let _ = storage.send_with_timeout(WriteOp::CloseTrade {
                trade,
                symbol: symbol.to_string(),
                cooldown_until: until,
                cooldown_bars,
            }).await;
        }
        TradeEvent::Flipped { mut close_trade, mut open_trade, new_position, .. } => {
            close_trade.ts = closed_ts;
            open_trade.ts = closed_ts;
            let until = aggregator.clear_position(symbol, closed_ts).unwrap_or(closed_ts);
            aggregator.set_position(symbol, new_position.direction);
            let _ = storage.send_with_timeout(WriteOp::FlipTrade {
                close_trade,
                open_trade,
                new_position,
                cooldown_until: until,
                cooldown_bars,
            }).await;
        }
        TradeEvent::StopLossTriggered { mut trade, .. } => {
            trade.ts = closed_ts;
            let until = aggregator.clear_position(symbol, closed_ts).unwrap_or(closed_ts);
            let _ = storage.send_with_timeout(WriteOp::CloseTrade {
                trade,
                symbol: symbol.to_string(),
                cooldown_until: until,
                cooldown_bars,
            }).await;
        }
    }
}

async fn dispatch_market_data(
    market_data: &MarketData,
    symbol_states: &mut HashMap<String, SymbolState>,
    storage: &StorageHandle,
) {
    match market_data {
        MarketData::Funding(f) => {
            if let Some(state) = symbol_states.get_mut(&f.symbol) {
                state.funding_signal.update(market_data);
            }
            if let Ok(payload) = serde_json::to_value(f) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "funding".into(),
                    symbol: Some(f.symbol.clone()),
                    ts: f.timestamp,
                    payload,
                });
            }
        }
        MarketData::HlpPosition(h) => {
            if let Some(state) = symbol_states.get_mut(&h.symbol) {
                state.hlp_signal.update(market_data);
            }
            if let Ok(payload) = serde_json::to_value(h) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "hlp".into(),
                    symbol: Some(h.symbol.clone()),
                    ts: h.timestamp,
                    payload,
                });
            }
        }
        MarketData::WhalePosition(w) => {
            if let Some(state) = symbol_states.get_mut(&w.symbol) {
                state.whale_signal.update(market_data);
            }
            if let Ok(payload) = serde_json::to_value(w) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "whale".into(),
                    symbol: Some(w.symbol.clone()),
                    ts: w.timestamp,
                    payload,
                });
            }
        }
        MarketData::Liquidation(l) => {
            if let Some(state) = symbol_states.get_mut(&l.symbol) {
                state.liquidation_signal.update(market_data);
            }
        }
        MarketData::OpenInterest(o) => {
            if let Ok(payload) = serde_json::to_value(o) {
                let _ = storage.try_send(WriteOp::MarketSnapshot {
                    data_type: "oi".into(),
                    symbol: Some(o.symbol.clone()),
                    ts: o.timestamp,
                    payload,
                });
            }
        }
        MarketData::CandleUpdate(_) => {}
    }
}

fn spawn_rest_pollers(md_tx: &mpsc::Sender<MarketData>, config: &AppConfig) {
    {
        let md_tx = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let symbols: std::collections::HashSet<String> = config.symbols.watchlist.iter().cloned().collect();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                match client.fetch_predicted_fundings().await {
                    Ok(fs) => {
                        for f in fs {
                            if symbols.contains(&f.symbol) {
                                let _ = md_tx.send(MarketData::Funding(f)).await;
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "funding poll failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }
    {
        let md_tx = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let hlp_address = config.indicators.hl_native.hlp_vault_address.clone();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                match client.fetch_clearinghouse_state(&hlp_address).await {
                    Ok(ps) => {
                        for p in ps {
                            let _ = md_tx.send(MarketData::HlpPosition(p)).await;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "HLP poll failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });
    }
    if !config.indicators.hl_native.whale_addresses.is_empty() {
        let md_tx = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let whale_addresses = config.indicators.hl_native.whale_addresses.clone();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                for address in &whale_addresses {
                    match client.fetch_clearinghouse_state(address).await {
                        Ok(ps) => {
                            for p in ps {
                                let whale = hyperfun_core::WhaleData {
                                    address: address.clone(),
                                    symbol: p.symbol,
                                    position_size: p.position_size,
                                    entry_price: p.entry_price,
                                    timestamp: p.timestamp,
                                };
                                let _ = md_tx.send(MarketData::WhalePosition(whale)).await;
                            }
                        }
                        Err(e) => tracing::warn!(address = %address, error = %e, "whale poll failed"),
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }
}
