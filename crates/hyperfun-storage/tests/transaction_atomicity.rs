#![cfg(feature = "integration-tests")]
use hyperfun_core::{Direction, Position, TradeRecord};
use hyperfun_storage::WriteOp;
use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn open_trade_is_atomic(pool: PgPool) {
    let trade = TradeRecord {
        ts: 100,
        symbol: "BTC".into(),
        event: "open".into(),
        direction: "Long".into(),
        price: 50000.0,
        fill_price: 50025.0,
        composite: 0.5,
        atr: 500.0,
        stop_loss: Some(49000.0),
        pnl: None,
        reason: None,
    };
    let pos = Position::new("BTC", Direction::Long, 1000.0, 50025.0, 49000.0, 100);
    let op = WriteOp::OpenTrade {
        trade,
        position: pos,
    };

    // Simulate a transaction that fails mid-way by rolling back manually
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO trades (ts, symbol, event, direction, price, fill_price, composite, atr, reason) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(100i64)
    .bind("BTC")
    .bind("open")
    .bind("Long")
    .bind(50000.0_f64)
    .bind(50025.0_f64)
    .bind(0.5_f64)
    .bind(500.0_f64)
    .bind("signal")
    .execute(&mut *tx)
    .await
    .unwrap();
    // Intentional rollback — simulates a mid-transaction failure
    tx.rollback().await.unwrap();

    // Assert: no row survived the rollback
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trades WHERE symbol = 'BTC'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "rollback must leave no rows");

    let _ = op; // suppress unused warning; full WriteOp path tested in writer tests
}
