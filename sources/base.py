"""Base class for all data sources with dependency injection."""

from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
from datetime import datetime

from core.container import Container
from core.protocols import HttpClient, Logger
from core.container import SQLAlchemySessionFactory


class BaseDataSource(ABC):
    """Abstract base class for data sources with dependency injection."""

    # Subclasses must define these
    name: str
    description: str

    def __init__(self, container: Container):
        """Initialize with dependency container."""
        self.container = container
        self.config = container.get_config().get_source_config(self.name)
        self.logger = container.get_logger(f"sources.{self.name}")
        self.http_client = container.get_http_client()
        self.db_factory = container.get_db_session_factory()

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """Extract raw data from the source API.

        Returns:
            List of raw records from the API.
        """
        ...

    @abstractmethod
    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw data into normalized format.

        Args:
            raw_data: Raw records from extract phase.

        Returns:
            List of transformed records ready for validation.
        """
        ...

    @abstractmethod
    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate data quality.

        Args:
            data: Transformed records.

        Returns:
            List of valid records.
        """
        ...

    @abstractmethod
    def load(self, data: list[dict[str, Any]]) -> dict[str, int]:
        """Load data into the database.

        Args:
            data: Validated records.

        Returns:
            Summary dict with 'inserted', 'updated', 'total' counts.
        """
        ...

    def run(self) -> dict[str, Any]:
        """Execute the full ETL pipeline.

        Returns:
            Summary of the ETL run including counts and timing.
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Starting {self.description} ETL Pipeline")
        self.logger.info("=" * 60)

        start_time = datetime.utcnow()

        try:
            # Extract
            raw_data = self.extract()
            self.logger.info(f"Extracted {len(raw_data)} records")

            # Transform
            transformed_data = self.transform(raw_data)
            self.logger.info(f"Transformed {len(transformed_data)} records")

            # Validate
            valid_data = self.validate(transformed_data)
            self.logger.info(f"Validated {len(valid_data)} records")

            # Load
            result = self.load(valid_data)

            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            self.logger.info("=" * 60)
            self.logger.info(f"ETL Pipeline completed in {duration:.2f}s")
            self.logger.info(f"Summary: {result}")
            self.logger.info("=" * 60)

            return {
                'source': self.name,
                'success': True,
                'duration_seconds': duration,
                **result
            }

        except Exception as e:
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            return {
                'source': self.name,
                'success': False,
                'error': str(e)
            }

    def is_enabled(self) -> bool:
        """Check if this source is enabled in configuration."""
        return self.config.get('enabled', False)
