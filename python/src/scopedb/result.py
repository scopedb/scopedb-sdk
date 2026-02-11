import json
from typing import Any, List, Optional
from datetime import datetime, timedelta
from .models import DataType, FieldSchema, ResultFormat, ResultSetMetadata

class ResultSet:
    """
    Stores the result of a statement execution.
    """
    def __init__(
        self,
        metadata: ResultSetMetadata,
        format: ResultFormat,
        rows_raw: Any,  # This is the raw JSON data (list of lists of strings/nulls)
    ):
        self.metadata = metadata
        self.format = format
        self._rows_raw = rows_raw
        self._parsed_rows: Optional[List[List[Any]]] = None

    @property
    def total_rows(self) -> int:
        return self.metadata.num_rows

    @property
    def schema(self) -> List[FieldSchema]:
        return self.metadata.fields

    def to_values(self) -> List[List[Any]]:
        """
        Reads the result set and returns the rows as a 2D list of values.
        """
        if self._parsed_rows is not None:
            return self._parsed_rows

        if self.format != ResultFormat.JSON:
            raise ValueError(f"Unexpected result set format: {self.format}")

        # rows_raw should be List[List[Optional[str]]] based on Go implementation logic
        # but since we receive parsed JSON from httpx, it might be List[List[Optional[str]]] already.
        # The Go code does json.Unmarshal(rs.Rows, &rows) where rows is [][]*string.

        parsed_data = []
        for row in self._rows_raw:
            if len(row) != len(self.schema):
                raise ValueError("Schema length does not match record length")

            parsed_row = []
            for i, val_str in enumerate(row):
                field_type = self.schema[i].type
                parsed_row.append(self._convert_value(val_str, field_type))
            parsed_data.append(parsed_row)

        self._parsed_rows = parsed_data
        return parsed_data

    def _convert_value(self, v: Optional[str], typ: DataType) -> Any:
        if v is None:
            return None

        if typ == DataType.STRING:
            return v
        elif typ == DataType.INT:
            return int(v)
        elif typ == DataType.UINT:
            return int(v)  # Python ints are arbitrary precision
        elif typ == DataType.FLOAT:
            return float(v)
        elif typ == DataType.BOOLEAN:
            # Go's strconv.ParseBool handles 1, t, T, TRUE, true, True, etc.
            # Assuming standard JSON bool or string "true"/"false" from server.
            # If server sends actual boolean in JSON, v is bool. If server sends string representation, v is str.
            # Go SDK expects *string in Unmarshal, so likely server sends strings for everything in JSON lines format?
            # Wait, Go struct definition: Rows json.RawMessage `json:"rows"`.
            # And `var rows [][]*string`.
            # So yes, everything is a string or null in the JSON array.
            return v.lower() == "true"
        elif typ == DataType.TIMESTAMP:
            # RFC3339Nano
            try:
                # Python 3.11+ has fromisoformat support for Z, but standard lib might need help for older versions or nano.
                # Simplest is fromisoformat if format is standard.
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                return v # Return as string if parse fails
        elif typ == DataType.INTERVAL:
            # Go duration string (e.g., "1h2m"). Python doesn't parse this natively easily.
            # For now, return as string or implementing a simple parser if critical.
            # Keeping as string for parity with "hard to parse" types unless we add a library.
            return v
        elif typ in (DataType.ARRAY, DataType.OBJECT, DataType.ANY):
            return v

        return v
