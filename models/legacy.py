"""Legacy models for backward compatibility.

These models match the original database tables and can be used
for migration or alongside the new unified schema.
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Date,
    Index, UniqueConstraint
)

from .base import Base


class CDCFluData(Base):
    """Original CDC flu data table (legacy)."""
    __tablename__ = 'cdc_flu_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    week_ending = Column(Date, nullable=False)
    season = Column(String(20), nullable=False)
    region = Column(String(100), nullable=False)
    percent_positive = Column(Float, nullable=False)
    total_specimens = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_region_week', 'region', 'week_ending'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_season', 'season'),
        UniqueConstraint('season', 'region', 'week_ending', name='uq_season_region_week'),
    )


class CovidData(Base):
    """Original COVID data table (legacy)."""
    __tablename__ = 'covid_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    geo_type = Column(String(20), nullable=False)
    geo_value = Column(String(50), nullable=False)
    confirmed_cases = Column(Float, nullable=True)
    deaths = Column(Float, nullable=True)
    confirmed_7day_avg = Column(Float, nullable=True)
    deaths_7day_avg = Column(Float, nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_covid_geo_date', 'geo_type', 'geo_value', 'date'),
        Index('idx_covid_date', 'date'),
        Index('idx_covid_timestamp', 'timestamp'),
        UniqueConstraint('date', 'geo_type', 'geo_value', name='uq_date_geo'),
    )
