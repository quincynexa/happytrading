use std::sync::Arc;
use anyhow::Result;
use sqlx::PgPool;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

use crate::journal::JournalWriter;
use crate::ops::WriteOp;

pub struct StorageWriter {
    rx: mpsc::Receiver<WriteOp>,
    pool: Arc<RwLock<Option<PgPool>>>,
    journal: JournalWriter,
}

impl StorageWriter {
    pub fn new(
        rx: mpsc::Receiver<WriteOp>,
        pool: Arc<RwLock<Option<PgPool>>>,
        journal: JournalWriter,
    ) -> Self {
        Self { rx, pool, journal }
    }

    /// Main loop — consume ops until the channel closes.
    pub async fn run(mut self) {
        info!("StorageWriter task started");
        while let Some(op) = self.rx.recv().await {
            self.handle(op).await;
        }
        info!("StorageWriter task exiting (channel closed)");
    }

    async fn handle(&mut self, op: WriteOp) {
        let pool_opt = self.pool.read().await.clone();
        if let Some(pool) = pool_opt {
            match self.write_db(&pool, &op).await {
                Ok(()) => return,
                Err(e) => {
                    warn!(error = %e, op = %op_kind(&op), "DB write failed; flipping to JSONL fallback");
                    *self.pool.write().await = None;
                }
            }
        }
        self.write_journal(&op);
    }

    async fn write_db(&self, pool: &PgPool, op: &WriteOp) -> Result<()> {
        match op {
            WriteOp::BarScore(r) => {
                sqlx::query(
                    "INSERT INTO bar_scores (ts, symbol, open_time, close_price, composite, trend, momentum, volatility, hl_native, funding, action) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
                     ON CONFLICT (symbol, open_time) DO NOTHING"
                )
                .bind(r.ts).bind(&r.symbol).bind(r.open_time).bind(r.close_price).bind(r.composite)
                .bind(r.trend).bind(r.momentum).bind(r.volatility).bind(r.hl_native).bind(r.funding)
                .bind(&r.action)
                .execute(pool).await?;
            }
            WriteOp::MarketSnapshot { data_type, symbol, ts, payload } => {
                sqlx::query(
                    "INSERT INTO market_data_snapshots (ts, symbol, data_type, payload) \
                     VALUES ($1,$2,$3,$4)"
                )
                .bind(ts).bind(symbol).bind(data_type).bind(payload)
                .execute(pool).await?;
            }
            WriteOp::CandleClose(c) => {
                sqlx::query(
                    "INSERT INTO candle_cache (symbol, interval, open_time, close_time, open, high, low, close, volume, num_trades) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
                     ON CONFLICT (symbol, interval, open_time) DO UPDATE SET \
                     close_time = EXCLUDED.close_time, open = EXCLUDED.open, high = EXCLUDED.high, \
                     low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, \
                     num_trades = EXCLUDED.num_trades"
                )
                .bind(&c.symbol).bind(&c.interval).bind(c.open_time).bind(c.close_time)
                .bind(c.open).bind(c.high).bind(c.low).bind(c.close).bind(c.volume)
                .bind(c.num_trades as i64)
                .execute(pool).await?;
            }
            WriteOp::OpenTrade { .. } | WriteOp::CloseTrade { .. } | WriteOp::FlipTrade { .. } => {
                self.write_stateful(pool, op).await?;
            }
        }
        Ok(())
    }

    async fn write_stateful(&self, pool: &PgPool, op: &WriteOp) -> Result<()> {
        let mut tx = pool.begin().await?;
        match op {
            WriteOp::OpenTrade { trade, position } => {
                insert_trade(&mut tx, trade).await?;
                insert_position(&mut tx, position).await?;
            }
            WriteOp::CloseTrade { trade, symbol, cooldown_until, cooldown_bars } => {
                insert_trade(&mut tx, trade).await?;
                sqlx::query("DELETE FROM positions WHERE symbol = $1").bind(symbol).execute(&mut *tx).await?;
                upsert_cooldown(&mut tx, symbol, *cooldown_until, *cooldown_bars, trade.ts).await?;
            }
            WriteOp::FlipTrade { close_trade, open_trade, new_position, cooldown_until, cooldown_bars } => {
                insert_trade(&mut tx, close_trade).await?;
                sqlx::query("DELETE FROM positions WHERE symbol = $1")
                    .bind(&close_trade.symbol).execute(&mut *tx).await?;
                insert_trade(&mut tx, open_trade).await?;
                insert_position(&mut tx, new_position).await?;
                upsert_cooldown(&mut tx, &close_trade.symbol, *cooldown_until, *cooldown_bars, close_trade.ts).await?;
            }
            _ => unreachable!("non-stateful op passed to write_stateful"),
        }
        tx.commit().await?;
        Ok(())
    }

    fn write_journal(&mut self, op: &WriteOp) {
        match op {
            WriteOp::BarScore(r) => self.journal.write_score(r),
            WriteOp::OpenTrade { trade, position } => {
                self.journal.write_trade(trade);
                self.journal.write_position("open", Some(position), trade.ts);
            }
            WriteOp::CloseTrade { trade, cooldown_until, cooldown_bars, symbol } => {
                self.journal.write_trade(trade);
                self.journal.write_position("close", None, trade.ts);
                self.journal.write_cooldown(symbol, *cooldown_until, *cooldown_bars, trade.ts);
            }
            WriteOp::FlipTrade { close_trade, open_trade, new_position, cooldown_until, cooldown_bars } => {
                self.journal.write_trade(close_trade);
                self.journal.write_position("flip_close", None, close_trade.ts);
                self.journal.write_trade(open_trade);
                self.journal.write_position("flip_open", Some(new_position), open_trade.ts);
                self.journal.write_cooldown(&close_trade.symbol, *cooldown_until, *cooldown_bars, close_trade.ts);
            }
            WriteOp::MarketSnapshot { data_type, symbol, ts, payload } => {
                self.journal.write_market_snapshot(data_type, symbol.as_deref(), *ts, payload);
            }
            WriteOp::CandleClose(c) => self.journal.write_candle(c),
        }
    }
}

async fn insert_trade(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, t: &hyperfun_core::TradeRecord) -> Result<()> {
    sqlx::query(
        "INSERT INTO trades (ts, symbol, event, direction, price, fill_price, composite, atr, stop_loss, pnl, reason) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
         ON CONFLICT (symbol, ts, event) DO NOTHING"
    )
    .bind(t.ts).bind(&t.symbol).bind(&t.event).bind(&t.direction)
    .bind(t.price).bind(t.fill_price).bind(t.composite).bind(t.atr)
    .bind(t.stop_loss).bind(t.pnl).bind(&t.reason)
    .execute(&mut **tx).await?;
    Ok(())
}

async fn insert_position(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, p: &hyperfun_core::Position) -> Result<()> {
    let dir = match p.direction {
        hyperfun_core::Direction::Long => "Long",
        hyperfun_core::Direction::Short => "Short",
    };
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query(
        "INSERT INTO positions (symbol, direction, size_usd, entry_price, entry_time, stop_loss, extreme_price, fees_paid, funding_paid, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
         ON CONFLICT (symbol) DO UPDATE SET \
         direction = EXCLUDED.direction, size_usd = EXCLUDED.size_usd, entry_price = EXCLUDED.entry_price, \
         entry_time = EXCLUDED.entry_time, stop_loss = EXCLUDED.stop_loss, extreme_price = EXCLUDED.extreme_price, \
         fees_paid = EXCLUDED.fees_paid, funding_paid = EXCLUDED.funding_paid, updated_at = EXCLUDED.updated_at"
    )
    .bind(&p.symbol).bind(dir).bind(p.size_usd).bind(p.entry_price).bind(p.entry_time)
    .bind(p.stop_loss).bind(p.extreme_price).bind(p.fees_paid).bind(p.funding_paid).bind(now)
    .execute(&mut **tx).await?;
    Ok(())
}

async fn upsert_cooldown(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, symbol: &str, until_ts: i64, bars: u32, updated_at: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO cooldowns (symbol, cooldown_until_ts, cooldown_bars, updated_at) \
         VALUES ($1,$2,$3,$4) \
         ON CONFLICT (symbol) DO UPDATE SET \
         cooldown_until_ts = EXCLUDED.cooldown_until_ts, \
         cooldown_bars = EXCLUDED.cooldown_bars, \
         updated_at = EXCLUDED.updated_at"
    )
    .bind(symbol).bind(until_ts).bind(bars as i32).bind(updated_at)
    .execute(&mut **tx).await?;
    Ok(())
}

fn op_kind(op: &WriteOp) -> &'static str {
    match op {
        WriteOp::BarScore(_) => "BarScore",
        WriteOp::OpenTrade { .. } => "OpenTrade",
        WriteOp::CloseTrade { .. } => "CloseTrade",
        WriteOp::FlipTrade { .. } => "FlipTrade",
        WriteOp::MarketSnapshot { .. } => "MarketSnapshot",
        WriteOp::CandleClose(_) => "CandleClose",
    }
}

/// Spawn the writer task. Returns the handle that the main loop should use.
pub fn spawn_writer_task(
    pool: Arc<RwLock<Option<PgPool>>>,
    journal: JournalWriter,
    channel_capacity: usize,
) -> (crate::StorageHandle, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(channel_capacity);
    let writer = StorageWriter::new(rx, pool, journal);
    let join = tokio::spawn(async move { writer.run().await });
    (crate::StorageHandle::new(tx), join)
}

/// Background task: if pool is None, try to reconnect every `reconnect_secs`.
pub fn spawn_reconnect_task(
    pool: Arc<RwLock<Option<PgPool>>>,
    connect_opts: sqlx::postgres::PgConnectOptions,
    pool_size: u32,
    reconnect_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(reconnect_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // skip immediate first tick
        loop {
            interval.tick().await;
            let is_none = pool.read().await.is_none();
            if !is_none { continue; }

            match sqlx::postgres::PgPoolOptions::new()
                .max_connections(pool_size)
                .connect_with(connect_opts.clone()).await
            {
                Ok(new_pool) => {
                    match sqlx::query("SELECT 1").execute(&new_pool).await {
                        Ok(_) => {
                            info!("DB pool reconnected");
                            *pool.write().await = Some(new_pool);
                        }
                        Err(e) => warn!(error = %e, "reconnect probe failed"),
                    }
                }
                Err(e) => warn!(error = %e, "reconnect attempt failed"),
            }
        }
    })
}
