//! Startup bulk loaders for candle_cache, positions, cooldowns.

use anyhow::Result;
use sqlx::PgPool;
use hyperfun_core::{Candle, Direction, Position};

#[derive(sqlx::FromRow)]
struct CandleRow {
    symbol: String,
    interval: String,
    open_time: i64,
    close_time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    num_trades: i64,
}

/// Load all candles into memory. Returns them grouped and ordered by
/// (symbol, interval, open_time). The caller partitions into the
/// `CandleStore` using the existing `push` API.
pub async fn load_candles_bulk(pool: &PgPool) -> Result<Vec<Candle>> {
    let rows: Vec<CandleRow> = sqlx::query_as(
        "SELECT symbol, interval, open_time, close_time, open, high, low, close, volume, num_trades \
         FROM candle_cache ORDER BY symbol, interval, open_time"
    )
    .fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| Candle {
        symbol: r.symbol,
        interval: r.interval,
        open_time: r.open_time,
        close_time: r.close_time,
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
        volume: r.volume,
        num_trades: r.num_trades as u64,
    }).collect())
}

#[derive(sqlx::FromRow)]
struct MaxCtRow {
    symbol: String,
    interval: String,
    max_ct: Option<i64>,
    cnt: i64,
}

pub struct CandleCoverage {
    pub symbol: String,
    pub interval: String,
    pub max_close_time: Option<i64>,
    pub count: i64,
}

/// Grouped metadata query for incremental backfill decisions.
pub async fn max_close_times(pool: &PgPool) -> Result<Vec<CandleCoverage>> {
    let rows: Vec<MaxCtRow> = sqlx::query_as(
        "SELECT symbol, interval, MAX(close_time) AS max_ct, COUNT(*) AS cnt \
         FROM candle_cache GROUP BY symbol, interval"
    )
    .fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| CandleCoverage {
        symbol: r.symbol, interval: r.interval,
        max_close_time: r.max_ct, count: r.cnt,
    }).collect())
}

#[derive(sqlx::FromRow)]
struct PositionRow {
    symbol: String,
    direction: String,
    size_usd: f64,
    entry_price: f64,
    entry_time: i64,
    stop_loss: f64,
    extreme_price: f64,
    fees_paid: f64,
    funding_paid: f64,
    #[allow(dead_code)]
    updated_at: i64,
}

/// Load all positions. Failure (DB or any row deserialize) is fatal — caller
/// should propagate and refuse to start.
pub async fn load_positions(pool: &PgPool) -> Result<Vec<Position>> {
    let rows: Vec<PositionRow> = sqlx::query_as(
        "SELECT symbol, direction, size_usd, entry_price, entry_time, stop_loss, \
         extreme_price, fees_paid, funding_paid, updated_at FROM positions"
    )
    .fetch_all(pool).await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let direction = match r.direction.as_str() {
            "Long" => Direction::Long,
            "Short" => Direction::Short,
            other => anyhow::bail!("invalid direction in positions row: {}", other),
        };
        if !r.entry_price.is_finite() || !r.stop_loss.is_finite() || !r.extreme_price.is_finite() {
            anyhow::bail!("non-finite float in positions row for {}", r.symbol);
        }
        let mut p = Position::new(
            r.symbol, direction, r.size_usd, r.entry_price, r.stop_loss, r.entry_time,
        );
        p.extreme_price = r.extreme_price;
        p.fees_paid = r.fees_paid;
        p.funding_paid = r.funding_paid;
        out.push(p);
    }
    Ok(out)
}

#[derive(sqlx::FromRow)]
struct CooldownRow {
    symbol: String,
    cooldown_until_ts: i64,
    cooldown_bars: i32,
}

pub struct CooldownState {
    pub symbol: String,
    pub cooldown_until_ts: i64,
    pub cooldown_bars: u32,
}

pub async fn load_cooldowns(pool: &PgPool) -> Result<Vec<CooldownState>> {
    let rows: Vec<CooldownRow> = sqlx::query_as(
        "SELECT symbol, cooldown_until_ts, cooldown_bars FROM cooldowns"
    )
    .fetch_all(pool).await?;

    Ok(rows.into_iter().map(|r| CooldownState {
        symbol: r.symbol,
        cooldown_until_ts: r.cooldown_until_ts,
        cooldown_bars: r.cooldown_bars as u32,
    }).collect())
}
