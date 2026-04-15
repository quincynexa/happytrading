#![cfg(feature = "integration-tests")]
use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn migrations_create_all_tables(pool: PgPool) {
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
    )
    .fetch_all(&pool)
    .await
    .expect("query tables");

    let names: Vec<String> = tables.into_iter().map(|(t,)| t).collect();
    for expected in [
        "bar_scores", "candle_cache", "cooldowns", "daily_stats",
        "market_data_snapshots", "positions", "trades",
    ] {
        assert!(names.contains(&expected.to_string()), "missing table {}: {:?}", expected, names);
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_are_idempotent(pool: PgPool) {
    // sqlx::test runs migrations once. Re-running sqlx::migrate should be a no-op.
    sqlx::migrate!("./migrations").run(&pool).await.expect("re-run migrations");
}
