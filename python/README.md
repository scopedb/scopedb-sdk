# ScopeDB Python SDK

A basic Python SDK for ScopeDB, modeled after the Go and Rust implementations.

## Installation

```bash
uv add scopedb-sdk
```

## Usage

### Connecting and Querying

```python
import asyncio
from scopedb import Client

async def main():
    async with Client("http://localhost:8080") as client:
        # Execute a query
        result = await client.query("FROM users LIMIT 10")
        print(result)

if __name__ == "__main__":
    asyncio.run(main())
```

### Ingestion (Cables)

The SDK supports the "Cable" concept for efficient batch ingestion.

```python
import asyncio
from scopedb import Client

async def main():
    async with Client("http://localhost:8080") as client:
        # Create a RawCable for the 'users' table
        cable = client.create_raw_cable("users", batch_size=1000)
        
        # Append rows
        await cable.append({"id": 1, "name": "Alice"})
        await cable.append({"id": 2, "name": "Bob"})
        
        # Ensure all data is sent
        await cable.flush()

if __name__ == "__main__":
    asyncio.run(main())
```

## Development

This project is managed with `uv`.

```bash
# Run tests
uv run pytest

# Format and lint
uv run ruff format src tests
uv run ruff check src tests
```
