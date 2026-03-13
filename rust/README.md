# ScopeDB SDK for Rust

This crate provides a Rust client for ScopeDB.

## Installation

```toml
[dependencies]
scopedb-client = "0.2"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Create a Client

```rust
use scopedb_client::Client;

let client = Client::new("http://127.0.0.1:6543", reqwest::Client::new())?;
# Ok::<(), scopedb_client::Error>(())
```

## Run a Statement

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", reqwest::Client::new())?;
let result = client
    .statement("SELECT 1".to_string())
    .execute()
    .await?;

let rows = result.into_values()?;
println!("{rows:?}");
# Ok(())
# }
```

## Table Helper

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", reqwest::Client::new())?;
let table = client.table("events").with_schema("public");
println!("identifier = {}", table.identifier());

let schema = table.table_schema().await?;
println!("fields = {}", schema.fields().len());
# Ok(())
# }
```

## Batched JSON Ingest

```rust
# async fn demo() -> Result<(), scopedb_client::Error> {
# let client = scopedb_client::Client::new("http://127.0.0.1:6543", reqwest::Client::new())?;
let batcher = client
    .json_batcher(
        r#"
        SELECT
            $0["ts"]::timestamp as ts,
            $0["name"]::string as name
        INSERT INTO public.events (ts, name)
        "#,
    )
    .build();

batcher
    .send(&serde_json::json!({
        "ts": "2026-03-13T12:00:00Z",
        "name": "scopedb",
    }))
    .await?;

batcher.flush().await?;
batcher.shutdown().await?;
# Ok(())
# }
```

## Examples

See runnable examples under [`examples/`](examples/):

- `cargo run --example statement`
- `cargo run --example table`
- `cargo run --example batch`
