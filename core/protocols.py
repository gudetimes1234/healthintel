"""Protocol definitions for dependency injection."""

from typing import Protocol, Any, Iterator
from contextlib import contextmanager


class HttpClient(Protocol):
    """Protocol for HTTP client implementations."""

    def get(self, url: str, params: dict[str, Any] | None = None,
            timeout: int = 30) -> dict[str, Any]:
        """Make a GET request and return JSON response."""
        ...


class DatabaseSession(Protocol):
    """Protocol for database session operations."""

    def query(self, model: Any) -> Any:
        """Query a model."""
        ...

    def add(self, instance: Any) -> None:
        """Add an instance to the session."""
        ...

    def commit(self) -> None:
        """Commit the transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the transaction."""
        ...


class DatabaseSessionFactory(Protocol):
    """Protocol for database session factory."""

    @contextmanager
    def get_session(self) -> Iterator[DatabaseSession]:
        """Get a database session context manager."""
        ...


class Logger(Protocol):
    """Protocol for logger implementations."""

    def info(self, msg: str, *args: Any) -> None: ...
    def warning(self, msg: str, *args: Any) -> None: ...
    def error(self, msg: str, *args: Any) -> None: ...
    def debug(self, msg: str, *args: Any) -> None: ...
