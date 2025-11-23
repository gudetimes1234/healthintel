"""Dependency injection container."""

import os
import logging
import requests
from typing import Any, Iterator
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .config import Config
from .protocols import HttpClient, DatabaseSessionFactory


class RequestsHttpClient:
    """HTTP client implementation using requests library."""

    def __init__(self, retries: int = 3, retry_delay: int = 60):
        self.retries = retries
        self.retry_delay = retry_delay

    def get(self, url: str, params: dict[str, Any] | None = None,
            timeout: int = 30) -> dict[str, Any]:
        """Make a GET request with retry logic."""
        import time

        last_error = None
        for attempt in range(self.retries):
            try:
                response = requests.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                last_error = e
                if attempt < self.retries - 1:
                    time.sleep(self.retry_delay)

        raise last_error


class SQLAlchemySessionFactory:
    """Database session factory using SQLAlchemy."""

    def __init__(self, database_url: str | None = None, pool_pre_ping: bool = True):
        if database_url is None:
            database_url = os.getenv('DATABASE_URL')
            if not database_url:
                raise ValueError("DATABASE_URL environment variable not set")

        self.engine = create_engine(database_url, pool_pre_ping=pool_pre_ping)
        self._session_maker = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self) -> Iterator[Session]:
        """Get a database session with automatic commit/rollback."""
        session = self._session_maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def init_tables(self, base: Any) -> None:
        """Create all tables from SQLAlchemy base."""
        base.metadata.create_all(self.engine)


class Container:
    """Dependency injection container for managing application dependencies."""

    def __init__(self, config: Config | None = None):
        self._config = config or Config()
        self._instances: dict[str, Any] = {}
        self._factories: dict[str, Any] = {}

        # Register default implementations
        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register default dependency implementations."""
        global_config = self._config.get_global_config()

        # Logger factory
        def create_logger(name: str) -> logging.Logger:
            log_config = global_config.get('logging', {})
            logging.basicConfig(
                level=getattr(logging, log_config.get('level', 'INFO')),
                format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
            return logging.getLogger(name)

        self._factories['logger'] = create_logger

        # HTTP client (singleton)
        self._factories['http_client'] = lambda: RequestsHttpClient()

        # Database session factory (singleton)
        db_config = global_config.get('database', {})
        self._factories['db_session_factory'] = lambda: SQLAlchemySessionFactory(
            pool_pre_ping=db_config.get('pool_pre_ping', True)
        )

    def get_logger(self, name: str) -> logging.Logger:
        """Get a logger instance for the given name."""
        return self._factories['logger'](name)

    def get_http_client(self) -> HttpClient:
        """Get the HTTP client instance."""
        if 'http_client' not in self._instances:
            self._instances['http_client'] = self._factories['http_client']()
        return self._instances['http_client']

    def get_db_session_factory(self) -> SQLAlchemySessionFactory:
        """Get the database session factory."""
        if 'db_session_factory' not in self._instances:
            self._instances['db_session_factory'] = self._factories['db_session_factory']()
        return self._instances['db_session_factory']

    def get_config(self) -> Config:
        """Get the configuration instance."""
        return self._config

    # Methods for testing - allow overriding dependencies
    def set_http_client(self, client: HttpClient) -> None:
        """Override the HTTP client (useful for testing)."""
        self._instances['http_client'] = client

    def set_db_session_factory(self, factory: DatabaseSessionFactory) -> None:
        """Override the database session factory (useful for testing)."""
        self._instances['db_session_factory'] = factory
