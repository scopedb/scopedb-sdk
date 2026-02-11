import asyncio
from typing import List, Dict, Any, TYPE_CHECKING
from .errors import ScopeDBError

if TYPE_CHECKING:
    from .client import Client


class RawCable:
    """
    A cable for ingesting raw data (dictionaries) into a ScopeDB table.
    """

    def __init__(self, client: "Client", table: str, batch_size: int = 1000):
        self._client = client
        self._table = table
        self._batch_size = batch_size
        self._buffer: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def append(self, row: Dict[str, Any]) -> None:
        """
        Append a row to the buffer. Auto-flushes if batch size is reached.
        """
        async with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self._batch_size:
                await self._flush_unsafe()

    async def flush(self) -> None:
        """
        Flush any remaining rows in the buffer.
        """
        async with self._lock:
            if self._buffer:
                await self._flush_unsafe()

    async def _flush_unsafe(self) -> None:
        try:
            # Assuming client has a generic ingest method
            await self._client._ingest_batch(self._table, self._buffer)
            self._buffer = []
        except Exception as e:
            raise ScopeDBError(
                f"Failed to flush cable for table {self._table}: {e}"
            ) from e
