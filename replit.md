# Public Health Intelligence Platform

## Overview

A real-time public health intelligence platform that automates the collection, processing, and visualization of CDC influenza surveillance data. The system fetches data from the CMU Delphi Epidata API every 6 hours, stores it in PostgreSQL, and presents interactive visualizations through a Streamlit dashboard. The platform provides geographic breakdowns, trend analysis, and key health metrics for public health monitoring.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Data Pipeline Architecture

**Problem**: Need automated, reliable data collection from CDC sources with proper error handling and scheduling.

**Solution**: Dual-mode ETL pipeline supporting both Prefect-orchestrated and standalone execution.

- **Prefect Flow Mode** (`prefect_flows.py`): Task-based workflow with retry logic (3 retries, 60s delay), task caching, and scheduled deployments (6-hour intervals)
- **Simple ETL Mode** (`simple_etl.py`): Lightweight alternative for environments without Prefect server, same extraction and transformation logic

**Rationale**: The dual approach provides flexibility - Prefect for production orchestration with monitoring/observability, simple mode for development and resource-constrained environments.

**Data Flow**:
1. Extract: Fetch data from CDC Delphi Epidata API for 11 regions (national + 10 HHS regions)
2. Transform: Parse JSON responses, normalize epiweek formats to dates, map region codes
3. Validate: Data quality checks on percent ranges, specimen counts, required fields
4. Load: Upsert to PostgreSQL with conflict resolution on unique constraint

### Frontend Architecture

**Problem**: Need real-time data visualization accessible to non-technical users.

**Solution**: Streamlit-based dashboard with server-side caching and Plotly visualizations.

**Key Design Decisions**:
- **Caching Strategy**: `@st.cache_data(ttl=3600)` for database queries to reduce load and improve performance
- **Wide Layout**: `layout="wide"` to maximize chart real estate for trend visualization
- **Session Management**: Context managers for database connections to prevent connection leaks

**Components**:
- Interactive filters (date range, region selection)
- Time-series line charts for trend analysis
- Geographic breakdowns via bar charts
- Metric cards for key statistics
- Paginated data tables for record inspection

### Data Storage Architecture

**Problem**: Need reliable storage with fast queries for time-series and geographic data.

**Solution**: PostgreSQL with SQLAlchemy ORM and optimized indexing strategy.

**Schema Design** (`CDCFluData` model):
- Primary key: Auto-incrementing integer ID
- Core fields: `week_ending` (Date), `season` (String), `region` (String), `percent_positive` (Float), `total_specimens` (Integer)
- Audit field: `timestamp` (DateTime with UTC default)

**Indexing Strategy**:
- Composite index on `(region, week_ending)` - optimizes geographic + temporal queries
- Single indexes on `timestamp` (recent data queries) and `season` (seasonal filtering)
- Unique constraint on `(season, region, week_ending)` - prevents duplicate data, enables upsert logic

**Connection Management**:
- Engine with `pool_pre_ping=True` to handle stale connections
- Context managers for automatic session cleanup and transaction management
- Environment-based configuration via `DATABASE_URL`

**Pros**: 
- ACID compliance for data integrity
- Efficient querying with proper indexes
- Built-in date/time handling
- Familiar SQL ecosystem

**Cons**:
- Requires PostgreSQL server (handled by Replit environment)
- More complex than document stores for simple use cases

### Workflow Orchestration

**Problem**: Need scheduled, monitored data pipelines with retry logic and observability.

**Solution**: Prefect 3.x for modern workflow orchestration.

**Architecture**:
- Task decorators with configurable retries and caching (`task_input_hash`)
- Flow composition for complex ETL workflows
- Deployment configuration for scheduling (`interval=timedelta(hours=6)`)
- Tag-based organization (`cdc`, `flu`, `health-intel`)

**Alternative Considered**: Apache Airflow (mentioned in original requirements)

**Why Prefect**:
- Simpler setup without complex folder structures (dags/, plugins/)
- Native Python API without custom operators
- Better developer experience with dynamic workflows
- Modern architecture with API-first design

## External Dependencies

### Third-Party APIs

**CMU Delphi Epidata API** (`https://api.delphi.cmu.edu/epidata/fluview/`)
- Purpose: Source of CDC influenza surveillance data
- Authentication: None (public API)
- Data Format: JSON responses with epidata arrays
- Rate Limits: None documented, using 30s timeout + retry logic
- Regions Supported: National ('nat') + 10 HHS regions ('hhs1' - 'hhs10')
- Epiweek Format: YYYYWW format (e.g., 202440 for week 40 of 2024)

### Database

**PostgreSQL**
- Connection: Via `DATABASE_URL` environment variable
- ORM: SQLAlchemy 2.x
- Migration Strategy: Declarative Base with programmatic table creation
- Connection Pooling: Enabled with pre-ping for connection health checks

### Python Libraries

**Core Framework**:
- `streamlit`: Web dashboard framework for data apps
- `prefect`: Workflow orchestration and scheduling
- `sqlalchemy`: Database ORM and connection management

**Data Processing**:
- `pandas`: Data manipulation and analysis
- `requests`: HTTP client for API calls

**Visualization**:
- `plotly`: Interactive charting library (express and graph_objects modules)

**Utilities**:
- `logging`: Application logging and debugging
- `contextlib`: Context manager utilities for resource management
- `datetime`: Date/time manipulation for epiweek calculations

### Environment Configuration

**Required Environment Variables**:
- `DATABASE_URL`: PostgreSQL connection string (automatically set in Replit)

**Configuration Files**:
- `.streamlit/config.toml`: Streamlit server configuration (theme, port settings)

### Deployment Environment

**Replit Specifics**:
- Automatic PostgreSQL provisioning
- Nix-based package management
- Environment variable injection
- Web hosting for Streamlit application