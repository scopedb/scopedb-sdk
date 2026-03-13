mod common;

use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = common::client()?;

    let stream = client
        .ingest_stream(
            r#"
            SELECT
                $0["ts"]::timestamp as ts,
                $0["name"]::string as name,
                $0
            INSERT INTO public.events (ts, name, raw)
            "#,
        )
        .batch_bytes(1024)
        .flush_interval(Duration::from_millis(250))
        .build();

    stream
        .send(&serde_json::json!({
            "ts": "2026-03-13T12:00:00Z",
            "name": "alpha",
            "extra": {"source": "example"},
        }))
        .await?;
    stream
        .send(&serde_json::json!({
            "ts": "2026-03-13T12:00:01Z",
            "name": "beta",
            "extra": {"source": "example"},
        }))
        .await?;

    let flushed = stream.flush().await?;
    println!("flush result: {flushed:?}");

    let final_flush = stream.shutdown().await?;
    println!("shutdown result: {final_flush:?}");

    Ok(())
}
