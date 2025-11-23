import os
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Date, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

Base = declarative_base()

class CDCFluData(Base):
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
    
    def __repr__(self):
        return f"<CDCFluData(region={self.region}, week_ending={self.week_ending}, percent_positive={self.percent_positive})>"

def get_engine():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    return create_engine(database_url, pool_pre_ping=True)

def get_session_maker():
    engine = get_engine()
    return sessionmaker(bind=engine)

@contextmanager
def get_db_session():
    SessionLocal = get_session_maker()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("Database tables created successfully")

if __name__ == "__main__":
    init_db()
