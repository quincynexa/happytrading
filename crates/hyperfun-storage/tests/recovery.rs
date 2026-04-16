#![cfg(feature = "integration-tests")]
use hyperfun_core::Direction;
use hyperfun_storage::{load_cooldowns, load_positions};
use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn positions_roundtrip(pool: PgPool) {
    sqlx::query(
        "INSERT INTO positions (symbol, direction, size_usd, entry_price, entry_time, \
         stop_loss, extreme_price, fees_paid, funding_paid, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
    )
    .bind("BTC")
    .bind("Long")
    .bind(1000.0_f64)
    .bind(50025.0_f64)
    .bind(100i64)
    .bind(49000.0_f64)
    .bind(51000.0_f64)
    .bind(0.35_f64)
    .bind(0.0_f64)
    .bind(0i64)
    .execute(&pool)
    .await
    .unwrap();

    let positions = load_positions(&pool).await.unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].symbol, "BTC");
    assert_eq!(positions[0].direction, Direction::Long);
    assert!((positions[0].entry_price - 50025.0).abs() < 1e-9);
    assert!((positions[0].extreme_price - 51000.0).abs() < 1e-9);
}

#[sqlx::test(migrations = "./migrations")]
async fn cooldowns_roundtrip(pool: PgPool) {
    sqlx::query(
        "INSERT INTO cooldowns (symbol, cooldown_until_ts, cooldown_bars, updated_at) \
         VALUES ($1,$2,$3,$4)",
    )
    .bind("BTC")
    .bind(5_000_000i64)
    .bind(3i32)
    .bind(1_000_000i64)
    .execute(&pool)
    .await
    .unwrap();

    let cds = load_cooldowns(&pool).await.unwrap();
    assert_eq!(cds.len(), 1);
    assert_eq!(cds[0].symbol, "BTC");
    assert_eq!(cds[0].cooldown_until_ts, 5_000_000);
    assert_eq!(cds[0].cooldown_bars, 3);
}

#[sqlx::test(migrations = "./migrations")]
async fn load_positions_fails_on_invalid_direction(pool: PgPool) {
    // Bypass CHECK constraint to inject a bad row
    sqlx::query("ALTER TABLE positions DROP CONSTRAINT positions_direction_check")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO positions (symbol, direction, size_usd, entry_price, entry_time, \
         stop_loss, extreme_price, fees_paid, funding_paid, updated_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
    )
    .bind("BTC")
    .bind("Invalid")
    .bind(1000.0_f64)
    .bind(50025.0_f64)
    .bind(0i64)
    .bind(49000.0_f64)
    .bind(51000.0_f64)
    .bind(0.0_f64)
    .bind(0.0_f64)
    .bind(0i64)
    .execute(&pool)
    .await
    .unwrap();

    let result = load_positions(&pool).await;
    assert!(result.is_err(), "invalid direction must be fatal");
}
