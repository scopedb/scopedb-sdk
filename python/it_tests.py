import asyncio
import os
import sys
import argparse
from typing import Optional

# Ensure we can import from src if running from the python directory
sys.path.append(os.path.abspath("src"))

from scopedb import Client

async def run_test(url: str, token: Optional[str] = None):
    print(f"Connecting to {url}...")
    try:
        async with Client(url, token=token) as client:
            print("Submitting statement: SELECT 1")
            stmt = client.statement("SELECT 1")

            print("Executing and waiting for result...")
            result_set = await stmt.execute()

            print(f"Total rows: {result_set.total_rows}")
            values = result_set.to_values()
            print(f"Result: {values}")

            print("Testing manual polling with StatementHandle...")
            handle = await client.statement("SELECT 2").submit()
            print(f"Statement ID: {handle.id}")

            while True:
                status = handle.status
                if status and status.is_terminated():
                    break
                print(f"Current status: {status}")
                await handle.fetch_once()
                await asyncio.sleep(0.5)

            final_rs = handle.result_set()
            if final_rs:
                print(f"Final manual fetch result: {final_rs.to_values()}")

            print("Integration test passed successfully!")
    except Exception as e:
        print(f"Error during integration test: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ScopeDB Python SDK Integration Test")
    parser.add_argument("url", help="ScopeDB endpoint URL (e.g., https://trial.ap-southeast-1.aliyun.scopedb.cloud)")
    parser.add_argument("--token", help="ScopeDB auth token", default=os.getenv("SCOPEDB_TOKEN"))

    args = parser.parse_args()
    asyncio.run(run_test(args.url, args.token))
