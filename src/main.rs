mod journal;

use std::collections::HashMap;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use hyperfun_core::config::AppConfig;
use hyperfun_core::{Candle, CandleIndicator, Direction, HlSignalProvider, MarketData, SignalAction};
use journal::{BarScoreRecord, JournalWriter, TradeRecord};
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
    /// is excluded from hl_native readiness so it doesn't block scoring.
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
        // V1 limitation: LiquidationSignal has no data source (requires WS subscription
        // for liquidation events, not implemented yet). Excluded from HL-native group
        // readiness check. Will always report ready()=false and score()=0.0.
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

    // ── 4. Create per-symbol state (independent indicator instances) ─────
    let mut symbol_states: HashMap<String, SymbolState> = HashMap::new();
    for sym in &config.symbols.watchlist {
        symbol_states.insert(sym.clone(), SymbolState::new(&config));
    }

    // ── 5. Create SignalAggregator ───────────────────────────────────────
    let mut aggregator = SignalAggregator::new(
        config.signal.open_threshold,
        config.signal.close_threshold,
    );

    // ── 6. Create PaperExecutor ──────────────────────────────────────────
    let mut executor = PaperExecutor::new(
        config.paper.simulated_slippage_pct,
        config.paper.simulated_fee_pct,
        config.paper.position_size_usd,
        config.paper.atr_stop_multiplier,
    );

    // ── 6b. Create JournalWriter (JSONL output to data/) ──────────────
    let mut journal = JournalWriter::new("data")?;
    info!("journal writer initialized — data/scores.jsonl + data/trades.jsonl");

    // ── 7. Warm up indicators with backfilled candle data ────────────────
    let entry_tf = &config.timeframes.entry;
    for symbol in &config.symbols.watchlist {
        let candles = engine
            .candle_store()
            .get_last_n(symbol, entry_tf, 500);

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

    // ── 8. Spawn REST polling tasks for HL-native data ───────────────────
    let (md_tx, mut md_rx) = mpsc::channel::<MarketData>(256);

    // Funding + OI poller (every 60s)
    {
        let md_tx_funding = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let symbols: std::collections::HashSet<String> =
            config.symbols.watchlist.iter().cloned().collect();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                match client.fetch_predicted_fundings().await {
                    Ok(fundings) => {
                        for f in fundings {
                            if symbols.contains(&f.symbol) {
                                let _ = md_tx_funding.send(MarketData::Funding(f)).await;
                            }
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "funding poll failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }

    // HLP vault poller (every 30s)
    {
        let md_tx_hlp = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let hlp_address = config.indicators.hl_native.hlp_vault_address.clone();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                match client.fetch_clearinghouse_state(&hlp_address).await {
                    Ok(positions) => {
                        for p in positions {
                            let _ = md_tx_hlp.send(MarketData::HlpPosition(p)).await;
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "HLP poll failed"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        });
    }

    // Whale position poller (every 60s per spec) — polls each configured whale address
    if !config.indicators.hl_native.whale_addresses.is_empty() {
        let md_tx_whale = md_tx.clone();
        let rest_url = config.hyperliquid.rest_url.clone();
        let whale_addresses = config.indicators.hl_native.whale_addresses.clone();
        tokio::spawn(async move {
            let client = HlRestClient::new(&rest_url);
            loop {
                for address in &whale_addresses {
                    match client.fetch_clearinghouse_state(address).await {
                        Ok(positions) => {
                            for p in positions {
                                // Convert HlpData into WhaleData for the whale signal
                                let whale = hyperfun_core::WhaleData {
                                    address: address.clone(),
                                    symbol: p.symbol,
                                    position_size: p.position_size,
                                    entry_price: p.entry_price,
                                    timestamp: p.timestamp,
                                };
                                let _ = md_tx_whale.send(MarketData::WhalePosition(whale)).await;
                            }
                        }
                        Err(e) => tracing::warn!(address = %address, error = %e, "whale poll failed"),
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    }

    // NOTE: Liquidation events require a separate WS subscription or parsing
    // from the trade stream. No REST endpoint exists for live liquidation events.
    // The LiquidationSignal will remain not-ready until that is implemented.

    // Drop the original sender so the channel can close when all spawned tasks end
    drop(md_tx);

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

    info!("WS candle stream and REST pollers spawned — entering main loop");

    // ── 10. Main loop (select over candle stream + market data channel) ──
    let summary_interval_ms =
        (config.paper.summary_interval_mins as i64) * 60 * 1000;
    let mut last_summary_ts: i64 = 0;

    let hl_native_weight = config.indicators.hl_native.weight;
    let funding_weight = config.indicators.funding.weight;
    let trend_tf = config.timeframes.trend.clone();

    // Bar-close detection: track last open_time per (symbol, interval).
    // When open_time changes, the previous bar has closed and a new one started.
    let mut last_open_time: HashMap<(String, String), i64> = HashMap::new();

    // Observability counters
    let mut mid_bar_count: u64 = 0;
    let mut bar_close_count: u64 = 0;
    let mut signal_count: u64 = 0;

    loop {
        tokio::select! {
            Some(candle) = candle_rx.recv() => {
                let symbol = candle.symbol.clone();
                let interval = candle.interval.clone();
                let price = candle.close;
                let timestamp = candle.close_time;

                // ── Bar-close detection ─────────────────────────────────────
                // Hyperliquid WS pushes mid-bar updates on every trade.
                // A bar is "closed" when the open_time changes for the same
                // (symbol, interval) — the new open_time means a new bar started,
                // so the previous bar is final.
                let bar_key = (symbol.clone(), interval.clone());
                let prev_open_time = last_open_time.get(&bar_key).copied();
                let is_bar_close = match prev_open_time {
                    Some(prev) => candle.open_time != prev,
                    None => false, // First candle for this key — no closed bar yet
                };
                last_open_time.insert(bar_key, candle.open_time);

                // Store in CandleStore (always, even mid-bar — needed for stop-loss checks)
                engine.candle_store_mut().push(candle.clone());

                // Skip signal generation while candle store is stale (e.g. during backfill)
                if engine.candle_store().is_stale() {
                    continue;
                }

                // Only process entry-timeframe candles for signal decisions
                if interval != *entry_tf {
                    continue;
                }

                // ── Mid-bar: only update unrealized PnL, skip everything else ──
                // Stop-loss is checked on bar-close only to avoid being
                // stopped out by intra-bar wicks/noise.
                if !is_bar_close {
                    mid_bar_count += 1;
                    executor.update_unrealized_pnl(&symbol, price);
                    debug!(
                        symbol = %symbol,
                        open_time = candle.open_time,
                        price = price,
                        mid_bar_count,
                        "mid-bar tick"
                    );
                    continue;
                }

                bar_close_count += 1;

                // Check stop losses on bar-close price (not mid-bar wicks)
                let stop_dir = executor.get_position(&symbol).map(|p| p.direction);
                if let Some(stop_pnl) = executor.check_stop_losses(&symbol, price) {
                    let dir_str = match stop_dir {
                        Some(Direction::Long) => "Long",
                        Some(Direction::Short) => "Short",
                        None => "Unknown",
                    };
                    journal.write_trade(&TradeRecord {
                        ts: timestamp,
                        symbol: symbol.clone(),
                        event: "close".into(),
                        direction: dir_str.into(),
                        price,
                        fill_price: price,
                        composite: 0.0,
                        atr: 0.0,
                        stop_loss: None,
                        pnl: Some(stop_pnl),
                        reason: Some("stop_loss".into()),
                    });
                }
                if !executor.has_position(&symbol) {
                    aggregator.clear_position(&symbol);
                }

                // Look up this symbol's state
                let state = match symbol_states.get_mut(&symbol) {
                    Some(s) => s,
                    None => continue, // unknown symbol, skip
                };

                // Update indicators with this symbol's closed bar
                state.trend_group.update_all(&candle);
                state.momentum_group.update_all(&candle);
                state.volatility_group.update_all(&candle);
                state.atr_for_stop.update(&candle);

                // Compute composite score using this symbol's indicators
                // hl_native readiness: HLP is always required; whale is only
                // required when whale_addresses are configured. Liquidation
                // excluded (no data source in V1).
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
                    } else {
                        None
                    }
                };

                let funding_score = if state.funding_signal.ready() {
                    Some(state.funding_signal.score())
                } else {
                    None
                };

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

                // ── Observability: log every bar-close evaluation ───────────
                info!(
                    symbol = %symbol,
                    bar_close_count,
                    open_time = candle.open_time,
                    close_price = price,
                    composite = format!("{:.4}", composite_score),
                    trend = format!("{:?}", trend_score),
                    momentum = format!("{:?}", momentum_score),
                    volatility = format!("{:?}", volatility_score),
                    hl_native = format!("{:?}", hl_native_score),
                    funding = format!("{:?}", funding_score),
                    "bar closed — scores evaluated"
                );

                // MTF filter: check trend-TF direction from the last closed
                // higher-timeframe candle. If the signal opposes the trend, suppress it.
                let trend_direction = engine.candle_store()
                    .last(&symbol, &trend_tf)
                    .map(|c| {
                        if c.close > c.open { Direction::Long } else { Direction::Short }
                    });

                // Decide action
                let action = aggregator.decide(&symbol, composite_score);

                // Apply MTF filter: suppress signals that oppose the higher-TF trend
                let action = match (action, trend_direction) {
                    (SignalAction::Open(Direction::Long), Some(Direction::Short)) => {
                        info!(symbol = %symbol, "MTF filter: suppressed Long signal (trend is Short)");
                        SignalAction::Hold
                    }
                    (SignalAction::Open(Direction::Short), Some(Direction::Long)) => {
                        info!(symbol = %symbol, "MTF filter: suppressed Short signal (trend is Long)");
                        SignalAction::Hold
                    }
                    (action, _) => action,
                };

                // Get current ATR value for stop-loss sizing
                let current_atr = state.atr_for_stop.atr_value();

                // Determine action label for the score record
                let action_label = match action {
                    SignalAction::Open(Direction::Long) => "open_long",
                    SignalAction::Open(Direction::Short) => "open_short",
                    SignalAction::Close => "close",
                    SignalAction::Hold => "hold",
                };

                // ── Journal: write bar-close score record ───────────────────
                journal.write_score(&BarScoreRecord {
                    ts: timestamp,
                    symbol: symbol.clone(),
                    open_time: candle.open_time,
                    close_price: price,
                    composite: composite_score,
                    trend: trend_score,
                    momentum: momentum_score,
                    volatility: volatility_score,
                    hl_native: hl_native_score,
                    funding: funding_score,
                    action: action_label.into(),
                });

                // Execute via paper executor
                match action {
                    SignalAction::Open(dir) => {
                        signal_count += 1;
                        let dir_str = match dir {
                            Direction::Long => "Long",
                            Direction::Short => "Short",
                        };
                        info!(
                            symbol = %symbol,
                            direction = ?dir,
                            composite = format!("{:.4}", composite_score),
                            details = ?details,
                            signal_count,
                            "signal: OPEN"
                        );
                        let (close_pnl, fill) = executor.execute_signal(action, &symbol, price, current_atr, timestamp);

                        // Journal: record flip-close if there was one
                        if let Some(pnl) = close_pnl {
                            journal.write_trade(&TradeRecord {
                                ts: timestamp,
                                symbol: symbol.clone(),
                                event: "close".into(),
                                direction: dir.opposite().to_string(),
                                price,
                                fill_price: price,
                                composite: composite_score,
                                atr: current_atr,
                                stop_loss: None,
                                pnl: Some(pnl),
                                reason: Some("direction_flip".into()),
                            });
                        }

                        // Journal: record open
                        let stop = executor.get_position(&symbol).map(|p| p.stop_loss);
                        journal.write_trade(&TradeRecord {
                            ts: timestamp,
                            symbol: symbol.clone(),
                            event: "open".into(),
                            direction: dir_str.into(),
                            price,
                            fill_price: fill.unwrap_or(price),
                            composite: composite_score,
                            atr: current_atr,
                            stop_loss: stop,
                            pnl: None,
                            reason: None,
                        });

                        aggregator.set_position(&symbol, dir);
                    }
                    SignalAction::Close => {
                        signal_count += 1;
                        let close_dir = executor.get_position(&symbol).map(|p| p.direction);
                        let dir_str = match close_dir {
                            Some(Direction::Long) => "Long",
                            Some(Direction::Short) => "Short",
                            None => "Unknown",
                        };
                        info!(
                            symbol = %symbol,
                            composite = format!("{:.4}", composite_score),
                            signal_count,
                            "signal: CLOSE"
                        );
                        let (pnl, _) = executor.execute_signal(action, &symbol, price, current_atr, timestamp);

                        // Journal: record close
                        journal.write_trade(&TradeRecord {
                            ts: timestamp,
                            symbol: symbol.clone(),
                            event: "close".into(),
                            direction: dir_str.into(),
                            price,
                            fill_price: price,
                            composite: composite_score,
                            atr: current_atr,
                            stop_loss: None,
                            pnl,
                            reason: Some("signal".into()),
                        });

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
                        bar_close_count,
                        mid_bar_count,
                        signal_count,
                        "periodic summary"
                    );
                    last_summary_ts = timestamp;
                }
            }
            Some(market_data) = md_rx.recv() => {
                // Route HL-native market data to the correct symbol's signal providers
                match &market_data {
                    MarketData::Funding(f) => {
                        if let Some(state) = symbol_states.get_mut(&f.symbol) {
                            state.funding_signal.update(&market_data);
                            debug!(symbol = %f.symbol, rate = f.predicted_rate, "funding update");
                        }
                    }
                    MarketData::HlpPosition(h) => {
                        if let Some(state) = symbol_states.get_mut(&h.symbol) {
                            state.hlp_signal.update(&market_data);
                            debug!(symbol = %h.symbol, size = h.position_size, "HLP position update");
                        }
                    }
                    MarketData::Liquidation(l) => {
                        if let Some(state) = symbol_states.get_mut(&l.symbol) {
                            state.liquidation_signal.update(&market_data);
                        }
                    }
                    MarketData::WhalePosition(w) => {
                        if let Some(state) = symbol_states.get_mut(&w.symbol) {
                            state.whale_signal.update(&market_data);
                            debug!(symbol = %w.symbol, address = %w.address, size = w.position_size, "whale update");
                        }
                    }
                    MarketData::OpenInterest(_) | MarketData::CandleUpdate(_) => {
                        // Not routed to signal providers currently
                    }
                }
            }
            else => {
                break;
            }
        }
    }

    warn!("all streams ended — shutting down");
    Ok(())
}
