//! Storage config and secure URL parsing.

use anyhow::{anyhow, Result};
use sqlx::postgres::PgConnectOptions;
use std::str::FromStr;

/// Parse a DATABASE_URL into sqlx connect options, returning a redacted
/// string representation safe for logging (password removed).
pub fn parse_database_url_redacted(url: &str) -> Result<(PgConnectOptions, String)> {
    let opts = PgConnectOptions::from_str(url)
        .map_err(|e| anyhow!("invalid DATABASE_URL: {}", e))?;

    // Build a redacted URL from opts, without exposing password
    let host = opts.get_host();
    let port = opts.get_port();
    let username = opts.get_username();
    let database = opts.get_database().unwrap_or("");
    let redacted = format!(
        "postgres://{}:***@{}:{}/{}",
        username, host, port, database
    );
    Ok((opts, redacted))
}
