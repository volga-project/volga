use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    volga::longrun::cli::run(std::env::args().skip(1)).await
}
