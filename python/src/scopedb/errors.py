class ScopeDBError(Exception):
    """Base exception for ScopeDB SDK."""

    pass


class ConnectionError(ScopeDBError):
    """Raised when connection fails."""

    pass


class QueryError(ScopeDBError):
    """Raised when a query fails."""

    pass
