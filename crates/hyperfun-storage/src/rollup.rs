//! Daily stats rollup and batched retention pruning.

use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Compute daily_stats for the previous UTC day from the trades table.
pub async fn rollup_daily_stats(pool: &PgPool) -> Result<u64> {
    let rows = sqlx::query(
        r#"
        INSERT INTO daily_stats (date, symbol, total_trades, winning_trades, losing_trades,
                                 total_pnl, gross_profit, gross_loss, max_drawdown)
        SELECT
          (to_timestamp(ts/1000.0) AT TIME ZONE 'UTC')::date AS date,
          symbol,
          COUNT(*) AS total_trades,
          COUNT(*) FILTER (WHERE pnl > 0) AS winning_trades,
          COUNT(*) FILTER (WHERE pnl < 0) AS losing_trades,
          COALESCE(SUM(pnl), 0) AS total_pnl,
          COALESCE(SUM(pnl) FILTER (WHERE pnl > 0), 0) AS gross_profit,
          COALESCE(SUM(-pnl) FILTER (WHERE pnl < 0), 0) AS gross_loss,
          0.0 AS max_drawdown  -- placeholder; full DD requires equity curve, done offline
        FROM trades
        WHERE event = 'close' AND pnl IS NOT NULL
          AND ts >= (EXTRACT(EPOCH FROM (now() - INTERVAL '1 day')::date)::bigint * 1000)
          AND ts <  (EXTRACT(EPOCH FROM (now())::date)::bigint * 1000)
        GROUP BY 1, symbol
        ON CONFLICT (date, symbol) DO UPDATE SET
          total_trades   = EXCLUDED.total_trades,
          winning_trades = EXCLUDED.winning_trades,
          losing_trades  = EXCLUDED.losing_trades,
          total_pnl      = EXCLUDED.total_pnl,
          gross_profit   = EXCLUDED.gross_profit,
          gross_loss     = EXCLUDED.gross_loss
        "#,
    )
    .execute(pool)
    .await?;
    Ok(rows.rows_affected())
}

/// Batched DELETE for a single retention target to avoid long-running transactions.
async fn prune_batched(pool: &PgPool, table: &str, where_clause: &str, batch: i64) -> Result<u64> {
    let mut total = 0u64;
    loop {
        let sql = format!(
            "DELETE FROM {table} WHERE ctid IN (SELECT ctid FROM {table} WHERE {where_clause} LIMIT {batch})"
        );
        let result = sqlx::query(&sql).execute(pool).await?;
        let n = result.rows_affected();
        total += n;
        if n == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(total)
}

/// Delete old rows: bar_scores >90d, market_data_snapshots >30d, candle_cache >365d.
pub async fn run_retention(pool: &PgPool) -> Result<()> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let cutoff_90d = now_ms - 90 * 86_400_000;
    let cutoff_30d = now_ms - 30 * 86_400_000;
    let cutoff_365d = now_ms - 365 * 86_400_000;

    let n1 = prune_batched(pool, "bar_scores", &format!("ts < {cutoff_90d}"), 10_000).await?;
    let n2 = prune_batched(
        pool,
        "market_data_snapshots",
        &format!("ts < {cutoff_30d}"),
        10_000,
    )
    .await?;
    let n3 = prune_batched(
        pool,
        "candle_cache",
        &format!("close_time < {cutoff_365d}"),
        10_000,
    )
    .await?;
    info!(bar_scores = n1, mds = n2, candles = n3, "retention pruning complete");
    Ok(())
}

/// Background task: once a day at UTC 00:05 run rollup, at 00:10 run retention.
pub fn spawn_daily_task(pool: Arc<RwLock<Option<PgPool>>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // Sleep until the next 00:05 UTC
            let now = chrono::Utc::now();
            let next_0005 = (now.date_naive().and_hms_opt(0, 5, 0).unwrap()
                + chrono::Duration::days(1))
            .and_utc();
            let sleep_dur = (next_0005 - now)
                .to_std()
                .unwrap_or(Duration::from_secs(60));
            tokio::time::sleep(sleep_dur).await;

            let pool_opt = pool.read().await.clone();
            if let Some(p) = pool_opt {
                match rollup_daily_stats(&p).await {
                    Ok(n) => info!(rows = n, "daily_stats rollup complete"),
                    Err(e) => warn!(error = %e, "daily_stats rollup failed"),
                }
                tokio::time::sleep(Duration::from_secs(5 * 60)).await; // wait until ~00:10
                if let Err(e) = run_retention(&p).await {
                    warn!(error = %e, "retention pruning failed");
                }
            } else {
                warn!("daily task skipped: pool unavailable");
            }
        }
    })
}
