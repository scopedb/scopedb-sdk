mod common;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;

    let result = client.statement("SELECT 1".to_string()).execute().await?;

    let rows = result.into_values()?;
    println!("rows: {rows:?}");
    Ok(())
}
