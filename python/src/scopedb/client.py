import httpx
from typing import Optional, Any, List, Dict
from uuid import UUID

from .errors import ConnectionError, QueryError, ScopeDBError
from .cable import RawCable
from .statement import Statement, StatementHandle
from .models import ResultFormat


class Client:
    """
    ScopeDB Client for Python.
    """

    def __init__(self, dsn: str, token: Optional[str] = None):
        self.base_url = dsn.rstrip("/")
        self.token = token
        self._client: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        """
        Initialize the HTTP client.
        """
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        self._client = httpx.AsyncClient(
            base_url=self.base_url, headers=headers, timeout=30.0
        )
        # Optional: Ping server to verify connection
        try:
            resp = await self._client.get("/health")
            resp.raise_for_status()
        except httpx.HTTPError:
            # We don't raise here strictly, but it's good practice.
            # For a basic SDK, we might just log or ignore if lazy connect.
            pass

    async def close(self) -> None:
        """
        Close the HTTP client.
        """
        if self._client:
            await self._client.aclose()
            self._client = None

    def statement(self, stmt: str) -> Statement:
        """
        Create a new statement.
        """
        return Statement(self, stmt)

    async def submit_statement_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method to submit a statement request.
        """
        if not self._client:
            await self.connect()
        assert self._client is not None

        try:
            resp = await self._client.post("/v1/statements", json=payload)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise QueryError(f"Statement submission failed: {e.response.text}") from e
        except httpx.HTTPError as e:
            raise ConnectionError(f"Network error during statement submission: {e}") from e

    async def fetch_statement_result(self, statement_id: UUID, format: ResultFormat) -> Dict[str, Any]:
        """
        Internal method to fetch statement results.
        """
        if not self._client:
            await self.connect()
        assert self._client is not None

        try:
            resp = await self._client.get(
                f"/v1/statements/{statement_id}",
                params={"format": format.value}
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise QueryError(f"Fetch statement result failed: {e.response.text}") from e
        except httpx.HTTPError as e:
            raise ConnectionError(f"Network error during fetch statement result: {e}") from e

    async def cancel_statement(self, statement_id: UUID) -> Dict[str, Any]:
        """
        Internal method to cancel a statement.
        """
        if not self._client:
            await self.connect()
        assert self._client is not None

        try:
            # Empty body POST
            resp = await self._client.post(f"/v1/statements/{statement_id}/cancel", content=b"")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise QueryError(f"Cancel statement failed: {e.response.text}") from e
        except httpx.HTTPError as e:
            raise ConnectionError(f"Network error during cancel statement: {e}") from e

    async def query(self, sql: str) -> Dict[str, Any]:
        """
        Execute a SQL query.
        """
        if not self._client:
            await self.connect()

        assert self._client is not None

        try:
            resp = await self._client.post("/v1/query", json={"query": sql})
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise QueryError(f"Query failed: {e.response.text}") from e
        except httpx.HTTPError as e:
            raise ConnectionError(f"Network error during query: {e}") from e

    def create_raw_cable(self, table: str, batch_size: int = 1000) -> RawCable:
        """
        Create a cable for ingesting raw data.
        """
        return RawCable(self, table, batch_size)

    async def _ingest_batch(self, table: str, rows: List[Dict[str, Any]]) -> None:
        """
        Internal method to send a batch of rows.
        """
        if not self._client:
            await self.connect()

        assert self._client is not None

        try:
            # Payload structure is a guess based on typical batch APIs
            payload = {"table": table, "data": rows}
            resp = await self._client.post("/v1/ingest", json=payload)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            raise ConnectionError(f"Ingest failed: {e}") from e

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
