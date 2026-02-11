import asyncio
import time
from typing import Optional, TYPE_CHECKING
from uuid import UUID

from .models import (
    StatementStatus,
    StatementProgress,
    ResultFormat,
    ResultSetMetadata,
    FieldSchema,
    DataType,
)
from .result import ResultSet
from .errors import ScopeDBError

if TYPE_CHECKING:
    from .client import Client


class StatementHandle:
    """
    A handle to a statement that has been submitted to ScopeDB.
    """

    def __init__(
        self,
        client: "Client",
        statement_id: UUID,
        format: ResultFormat = ResultFormat.JSON,
        initial_response: Optional[dict] = None,
    ):
        self._client = client
        self.id = statement_id
        self.format = format
        self._last_response = initial_response

    @property
    def status(self) -> Optional[StatementStatus]:
        if not self._last_response:
            return None
        status_str = self._last_response.get("status")
        return StatementStatus(status_str) if status_str else None

    @property
    def progress(self) -> Optional[StatementProgress]:
        if not self._last_response:
            return None
        prog_data = self._last_response.get("progress")
        if not prog_data:
            return None
        return StatementProgress(**prog_data)

    def result_set(self) -> Optional[ResultSet]:
        if not self._last_response:
            return None

        rs_data = self._last_response.get("result_set")
        if not rs_data:
            return None

        meta_data = rs_data.get("metadata", {})
        fields_data = meta_data.get("fields", [])
        fields = []
        for f in fields_data:
            name = f.get("name")
            # Handle potential casing variations in API response
            dtype_str = f.get("data_Type") or f.get("data_type") or f.get("dataType")
            if name and dtype_str:
                fields.append(FieldSchema(name, DataType(dtype_str)))

        metadata = ResultSetMetadata(fields=fields, num_rows=meta_data.get("num_rows", 0))

        return ResultSet(
            metadata=metadata,
            format=ResultFormat(rs_data.get("format", self.format)),
            rows_raw=rs_data.get("rows", []),
        )

    async def fetch_once(self) -> None:
        """
        Fetches the result set of the statement once.
        """
        if self.status and self.status.is_terminated():
            return

        resp = await self._client.fetch_statement_result(self.id, self.format)
        self._last_response = resp

        msg = resp.get("message")
        if msg:
            # In Go: if resp.Message != nil { return &Error{Message: *resp.Message} }
            # Here we might raise or just let the user check status.
            # Go's FetchOnce returns error if Message is present.
            raise ScopeDBError(f"Statement failed: {msg}")

    async def fetch(self) -> ResultSet:
        """
        Fetches the result set of the statement until it is finished, failed or cancelled.
        """
        tick = 0.005  # 5ms
        max_tick = 1.0  # 1s

        while True:
            # Check if we already have a result or error in the last response
            if self._last_response:
                rs = self.result_set()
                if rs:
                    return rs

                msg = self._last_response.get("message")
                if msg:
                    raise ScopeDBError(f"Statement failed: {msg}")

                # If terminated but no result and no message? (e.g. cancelled without message?)
                if self.status and self.status.is_terminated():
                     raise ScopeDBError(f"Statement terminated with status: {self.status}")

            await asyncio.sleep(tick)
            if tick < max_tick:
                tick = min(tick * 2, max_tick)

            await self.fetch_once()

    async def cancel(self) -> StatementStatus:
        """
        Cancels the statement if it is running or pending.
        """
        if self.status and self.status.is_terminated():
            return self.status

        resp = await self._client.cancel_statement(self.id)

        # Update internal state with cancel response
        # The cancel response structure in Go: { Status, Message }
        # It doesn't give full statement response, but we should update our status.
        new_status = StatementStatus(resp["status"])

        # We need to mimic a statement response update or just update status manually?
        # Ideally we fetch once more or update local state partially.
        # For simplicity, let's just update the status in a mock response wrapper if needed,
        # but simpler is just returning the status and letting next fetch handle full state.
        # However, Go updates h.resp.Status = resp.Status.
        if self._last_response:
            self._last_response["status"] = new_status
            if "message" in resp:
                self._last_response["message"] = resp["message"]

        return new_status


class Statement:
    """
    Represents a statement to be executed on ScopeDB.
    """

    def __init__(self, client: "Client", stmt: str):
        self._client = client
        self.stmt = stmt
        self.id: Optional[UUID] = None
        self.exec_timeout: Optional[str] = None
        self.result_format = ResultFormat.JSON

    async def submit(self) -> StatementHandle:
        """
        Submits the statement to ScopeDB for execution.
        """
        req = {
            "statement": self.stmt,
            "format": self.result_format,
        }
        if self.id:
            req["statement_id"] = str(self.id)
        if self.exec_timeout:
            req["exec_timeout"] = self.exec_timeout

        resp = await self._client.submit_statement_request(req)

        return StatementHandle(
            client=self._client,
            statement_id=UUID(resp["statement_id"]),
            format=self.result_format,
            initial_response=resp
        )

    async def execute(self) -> ResultSet:
        """
        Submits the statement to ScopeDB for execution and waits for its completion.
        """
        handle = await self.submit()
        return await handle.fetch()
