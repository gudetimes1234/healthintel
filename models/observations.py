"""Unified observation models for public and tenant data."""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime,
    Index, UniqueConstraint, ForeignKey
)

from .base import Base, TimestampMixin


class PublicObservation(Base, TimestampMixin):
    """Public health observations accessible to all tenants.

    This table stores data from public sources like CDC, NHSN, etc.
    All organizations can query this data.
    """
    __tablename__ = 'public_observations'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Time dimension
    date = Column(Date, nullable=False)

    # Geography dimension
    geo_type = Column(String(20), nullable=False)   # nation, state, hhs_region, county
    geo_value = Column(String(50), nullable=False)  # us, ca, hhs1, 06037

    # Signal identification
    source = Column(String(50), nullable=False)     # nhsn, fluview, nwss
    signal = Column(String(100), nullable=False)    # covid_hosp, ili_pct, rsv_hosp

    # Observation values
    value = Column(Float)
    stderr = Column(Float)                          # Standard error (if available)
    sample_size = Column(Integer)                   # Sample size (if available)

    __table_args__ = (
        # Prevent duplicate observations
        UniqueConstraint('date', 'geo_type', 'geo_value', 'source', 'signal',
                        name='uq_public_obs'),
        # Optimize common query patterns
        Index('idx_pub_signal_geo_date', 'signal', 'geo_type', 'geo_value', 'date'),
        Index('idx_pub_source_date', 'source', 'date'),
        Index('idx_pub_geo_date', 'geo_type', 'geo_value', 'date'),
    )

    def __repr__(self):
        return f"<PublicObservation({self.source}/{self.signal} {self.geo_value} {self.date}: {self.value})>"


class TenantObservation(Base, TimestampMixin):
    """Private observations belonging to specific tenants.

    This table stores organization-specific data that should only
    be accessible to that organization. Row-level security should
    be applied based on tenant_id.
    """
    __tablename__ = 'tenant_observations'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Tenant isolation
    tenant_id = Column(String(50), nullable=False, index=True)

    # Time dimension
    date = Column(Date, nullable=False)

    # Geography dimension
    geo_type = Column(String(20), nullable=False)
    geo_value = Column(String(50), nullable=False)

    # Signal identification
    source = Column(String(100), nullable=False)    # upload, ehr_integration, custom_api
    signal = Column(String(100), nullable=False)

    # Observation values
    value = Column(Float)
    stderr = Column(Float)
    sample_size = Column(Integer)

    # Optional metadata
    metadata_json = Column(String(1000))            # Additional context as JSON

    __table_args__ = (
        # Prevent duplicate observations per tenant
        UniqueConstraint('tenant_id', 'date', 'geo_type', 'geo_value', 'source', 'signal',
                        name='uq_tenant_obs'),
        # Optimize tenant-specific queries
        Index('idx_tenant_signal_date', 'tenant_id', 'signal', 'geo_type', 'geo_value', 'date'),
        Index('idx_tenant_source', 'tenant_id', 'source', 'date'),
    )

    def __repr__(self):
        return f"<TenantObservation({self.tenant_id}/{self.source}/{self.signal} {self.date}: {self.value})>"
