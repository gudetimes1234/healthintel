"""Dimension tables for geography and signals."""

from sqlalchemy import Column, Integer, String, Float, Boolean, Text

from .base import Base, TimestampMixin


class GeoLocation(Base, TimestampMixin):
    """Geographic location dimension table.

    Provides consistent geography metadata across all observations.
    """
    __tablename__ = 'dim_geo_locations'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Geographic identifiers
    geo_type = Column(String(20), nullable=False)   # nation, state, hhs_region, county
    geo_value = Column(String(50), nullable=False)  # us, ca, hhs1, 06037

    # Display information
    name = Column(String(100), nullable=False)      # United States, California, HHS Region 1
    abbreviation = Column(String(10))               # US, CA

    # Hierarchy
    parent_geo_type = Column(String(20))            # state's parent is nation
    parent_geo_value = Column(String(50))           # ca's parent is us

    # Metadata
    population = Column(Integer)                    # For per-capita calculations
    latitude = Column(Float)
    longitude = Column(Float)
    fips_code = Column(String(10))                  # Federal code

    def __repr__(self):
        return f"<GeoLocation({self.geo_type}/{self.geo_value}: {self.name})>"


class SignalDefinition(Base, TimestampMixin):
    """Signal/metric definition dimension table.

    Provides metadata about each signal type for display and analysis.
    """
    __tablename__ = 'dim_signals'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Signal identification
    source = Column(String(50), nullable=False)     # nhsn, fluview
    signal = Column(String(100), nullable=False)    # covid_hosp, ili_pct

    # Display information
    display_name = Column(String(200), nullable=False)  # "COVID-19 Hospital Admissions"
    description = Column(Text)

    # Categorization
    category = Column(String(50))                   # respiratory, hospitalization, mortality
    subcategory = Column(String(50))                # covid, flu, rsv

    # Data characteristics
    unit = Column(String(50))                       # count, percent, rate_per_100k
    value_type = Column(String(20))                 # integer, float, percentage
    aggregation_type = Column(String(20))           # sum, average, rate

    # Time characteristics
    typical_lag_days = Column(Integer)              # How delayed is this data?
    update_frequency = Column(String(20))           # daily, weekly

    # Display settings
    format_string = Column(String(50))              # "{:.1f}%", "{:,.0f}"
    color_scale = Column(String(50))                # Reds, Blues, Viridis

    # Status
    is_active = Column(Boolean, default=True)
    is_public = Column(Boolean, default=True)       # Can tenants see this?

    def __repr__(self):
        return f"<SignalDefinition({self.source}/{self.signal}: {self.display_name})>"
