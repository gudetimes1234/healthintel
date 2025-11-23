"""Tenant context management for multi-tenant data isolation."""

from contextvars import ContextVar
from typing import Any, Iterator
from contextlib import contextmanager

# Context variable to hold the current tenant ID
_current_tenant: ContextVar[str | None] = ContextVar('current_tenant', default=None)


def get_current_tenant() -> str | None:
    """Get the current tenant ID from context."""
    return _current_tenant.get()


def set_current_tenant(tenant_id: str | None) -> None:
    """Set the current tenant ID in context."""
    _current_tenant.set(tenant_id)


@contextmanager
def tenant_context(tenant_id: str) -> Iterator[str]:
    """Context manager for tenant-scoped operations.

    Usage:
        with tenant_context('acme_corp'):
            # All operations here are scoped to acme_corp
            data = query_tenant_data()
    """
    token = _current_tenant.set(tenant_id)
    try:
        yield tenant_id
    finally:
        _current_tenant.reset(token)


class TenantAwareSessionFactory:
    """Database session factory that automatically applies tenant context.

    Wraps a regular session factory and sets PostgreSQL session variables
    for row-level security policies.
    """

    def __init__(self, base_factory: Any):
        self._base_factory = base_factory

    @contextmanager
    def get_session(self) -> Iterator[Any]:
        """Get a tenant-aware database session."""
        with self._base_factory.get_session() as session:
            tenant_id = get_current_tenant()

            if tenant_id:
                # Set PostgreSQL session variable for RLS policies
                session.execute(
                    f"SET app.current_tenant = '{tenant_id}'"
                )

            yield session

    def init_tables(self, base: Any) -> None:
        """Create all tables from SQLAlchemy base."""
        self._base_factory.init_tables(base)


def setup_row_level_security(session: Any) -> None:
    """Set up PostgreSQL row-level security policies.

    Run this once during database initialization to enable
    tenant isolation at the database level.
    """
    # Enable RLS on tenant_observations table
    session.execute("""
        ALTER TABLE tenant_observations ENABLE ROW LEVEL SECURITY;
    """)

    # Create policy that restricts access based on tenant_id
    session.execute("""
        CREATE POLICY tenant_isolation_policy ON tenant_observations
            USING (tenant_id = current_setting('app.current_tenant', true));
    """)

    # Allow service accounts to bypass RLS for admin operations
    session.execute("""
        CREATE POLICY service_account_bypass ON tenant_observations
            FOR ALL
            TO service_account
            USING (true);
    """)

    session.commit()
