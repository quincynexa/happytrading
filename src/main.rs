use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("hyperfun=info".parse()?),
        )
        .init();

    tracing::info!("hyperfun starting");
    Ok(())
}
