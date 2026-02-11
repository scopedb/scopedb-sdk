from enum import Enum
from dataclasses import dataclass
from typing import Optional

class ResultFormat(str, Enum):
    JSON = "json"

class StatementStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"

    def is_finished(self) -> bool:
        return self == StatementStatus.FINISHED

    def is_terminated(self) -> bool:
        return self in (
            StatementStatus.FINISHED,
            StatementStatus.FAILED,
            StatementStatus.CANCELLED,
        )

class DataType(str, Enum):
    STRING = "string"
    INT = "int"
    UINT = "uint"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    INTERVAL = "interval"
    ARRAY = "array"
    OBJECT = "object"
    ANY = "any"

@dataclass
class StatementProgress:
    total_percentage: float
    nanos_from_submitted: int
    nanos_from_started: int
    total_stages: int
    total_partitions: int
    total_rows: int
    total_compressed_bytes: int
    total_uncompressed_bytes: int
    scanned_stages: int
    scanned_partitions: int
    scanned_rows: int
    scanned_compressed_bytes: int
    scanned_uncompressed_bytes: int

@dataclass
class FieldSchema:
    name: str
    type: DataType

@dataclass
class ResultSetMetadata:
    fields: list[FieldSchema]
    num_rows: int
