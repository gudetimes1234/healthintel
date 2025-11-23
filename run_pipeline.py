#!/usr/bin/env python3
"""Unified ETL pipeline runner with dependency injection."""

import argparse
import sys
from typing import Any

from core.container import Container
from core.config import Config
from sources.registry import get_registry
from database import Base

# Import sources to register them
import sources.flu_surveillance
import sources.covid_hospitalizations


def run_all_sources(container: Container) -> list[dict[str, Any]]:
    """Run ETL for all enabled sources."""
    registry = get_registry()
    sources = registry.create_enabled_sources(container)

    if not sources:
        print("No enabled sources found in configuration")
        return []

    results = []
    for source in sources:
        print(f"\nRunning {source.name}...")
        result = source.run()
        results.append(result)

    return results


def run_single_source(container: Container, source_name: str) -> dict[str, Any]:
    """Run ETL for a single source."""
    registry = get_registry()

    try:
        source = registry.create_source(source_name, container)
        return source.run()
    except KeyError as e:
        print(f"Error: {e}")
        print(f"Available sources: {list(registry.get_all().keys())}")
        sys.exit(1)


def list_sources(container: Container) -> None:
    """List all available sources and their status."""
    registry = get_registry()
    config = container.get_config()

    print("\nAvailable Data Sources:")
    print("-" * 50)

    for name, source_class in registry.get_all().items():
        try:
            source_config = config.get_source_config(name)
            enabled = source_config.get('enabled', False)
            status = "enabled" if enabled else "disabled"
            desc = source_config.get('description', source_class.description)
        except KeyError:
            status = "not configured"
            desc = source_class.description

        print(f"  {name}: {desc}")
        print(f"    Status: {status}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Run ETL pipelines for health data sources"
    )
    parser.add_argument(
        '--source', '-s',
        help="Run specific source (default: all enabled sources)"
    )
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help="List available sources"
    )
    parser.add_argument(
        '--config', '-c',
        help="Path to configuration file"
    )

    args = parser.parse_args()

    # Initialize container
    config = Config(args.config) if args.config else Config()
    container = Container(config)

    # Initialize database tables
    db_factory = container.get_db_session_factory()
    db_factory.init_tables(Base)

    if args.list:
        list_sources(container)
        return

    # Run pipelines
    if args.source:
        result = run_single_source(container, args.source)
        results = [result]
    else:
        results = run_all_sources(container)

    # Print summary
    print("\n" + "=" * 60)
    print("ETL Pipeline Summary")
    print("=" * 60)

    for result in results:
        status = "SUCCESS" if result.get('success') else "FAILED"
        print(f"\n{result['source']}: {status}")

        if result.get('success'):
            print(f"  Inserted: {result.get('inserted', 0)}")
            print(f"  Updated: {result.get('updated', 0)}")
            print(f"  Total: {result.get('total', 0)}")
            print(f"  Duration: {result.get('duration_seconds', 0):.2f}s")
        else:
            print(f"  Error: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main()
