"""Registry for auto-discovering and managing data sources."""

from typing import Type
from .base import BaseDataSource
from core.container import Container


class SourceRegistry:
    """Registry for data source classes."""

    def __init__(self):
        self._sources: dict[str, Type[BaseDataSource]] = {}

    def register(self, source_class: Type[BaseDataSource]) -> Type[BaseDataSource]:
        """Register a data source class. Can be used as a decorator.

        Args:
            source_class: The data source class to register.

        Returns:
            The same class (for decorator usage).
        """
        self._sources[source_class.name] = source_class
        return source_class

    def get(self, name: str) -> Type[BaseDataSource] | None:
        """Get a data source class by name."""
        return self._sources.get(name)

    def get_all(self) -> dict[str, Type[BaseDataSource]]:
        """Get all registered source classes."""
        return self._sources.copy()

    def create_source(self, name: str, container: Container) -> BaseDataSource:
        """Create an instance of a data source.

        Args:
            name: Name of the data source.
            container: Dependency injection container.

        Returns:
            Instantiated data source.

        Raises:
            KeyError: If source name is not registered.
        """
        source_class = self._sources.get(name)
        if source_class is None:
            raise KeyError(f"Data source '{name}' not found in registry")
        return source_class(container)

    def create_enabled_sources(self, container: Container) -> list[BaseDataSource]:
        """Create instances of all enabled data sources.

        Args:
            container: Dependency injection container.

        Returns:
            List of instantiated enabled data sources.
        """
        enabled_names = container.get_config().get_enabled_sources()
        sources = []

        for name in enabled_names:
            if name in self._sources:
                sources.append(self.create_source(name, container))

        return sources


# Global registry instance
_registry = SourceRegistry()


def get_registry() -> SourceRegistry:
    """Get the global source registry."""
    return _registry


def register(source_class: Type[BaseDataSource]) -> Type[BaseDataSource]:
    """Decorator to register a data source class."""
    return _registry.register(source_class)
