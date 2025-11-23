"""Configuration loader for data sources."""

import os
import yaml
from pathlib import Path
from typing import Any


class Config:
    """Configuration manager that loads from YAML files."""

    def __init__(self, config_path: str | None = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "sources.yaml"

        self._config_path = Path(config_path)
        self._config: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """Load configuration from YAML file."""
        if not self._config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self._config_path}")

        with open(self._config_path) as f:
            self._config = yaml.safe_load(f)

    def get_source_config(self, source_name: str) -> dict[str, Any]:
        """Get configuration for a specific data source."""
        sources = self._config.get('sources', {})
        if source_name not in sources:
            raise KeyError(f"Source '{source_name}' not found in configuration")
        return sources[source_name]

    def get_enabled_sources(self) -> list[str]:
        """Get list of enabled data source names."""
        sources = self._config.get('sources', {})
        return [name for name, cfg in sources.items() if cfg.get('enabled', False)]

    def get_global_config(self) -> dict[str, Any]:
        """Get global configuration settings."""
        return self._config.get('global', {})

    def get(self, key: str, default: Any = None) -> Any:
        """Get a top-level config value."""
        return self._config.get(key, default)
