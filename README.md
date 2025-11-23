# Public Health Intelligence Platform

A real-time public health intelligence platform that fetches, processes, and visualizes CDC influenza surveillance data using Prefect workflow orchestration and Streamlit dashboards.

## Features

- **Automated Data Pipeline**: Prefect-based ETL workflow that runs every 6 hours
- **Real-time CDC Data**: Fetches influenza surveillance data via CMU Delphi Epidata API
- **Interactive Dashboard**: Beautiful Streamlit UI with charts, filters, and metrics
- **PostgreSQL Storage**: Reliable data storage with proper indexing
- **Data Quality Validation**: Built-in validation checks and error handling
- **Responsive Design**: Clean, professional interface with geographic breakdowns

## Tech Stack

- **Workflow Orchestration**: Prefect 3.x
- **Web Dashboard**: Streamlit
- **Database**: PostgreSQL
- **Data Processing**: Pandas, SQLAlchemy
- **Visualization**: Plotly
- **Language**: Python 3.11+

## Project Structure

```
health-intel-platform/
├── app.py                          # Streamlit dashboard application
├── database.py                     # Database models and connection management
├── prefect_flows.py                # Prefect ETL pipeline tasks and flows
├── run_etl.py                      # Script to manually run ETL pipeline
├── setup_prefect_deployment.py    # Script to set up scheduled deployments
├── README.md                       # This file
└── .streamlit/
    └── config.toml                 # Streamlit configuration
```

## Getting Started

### Prerequisites

- Python 3.11 or higher
- PostgreSQL database (automatically configured in Replit)
- Internet connection for CDC API access

### Installation

All dependencies are already installed in this Replit environment:
- prefect
- streamlit
- pandas
- plotly
- requests
- psycopg2-binary
- sqlalchemy

### Initial Setup

1. **Initialize the Database**

   The database tables will be created automatically when you run the ETL pipeline for the first time.

2. **Run the ETL Pipeline**

   Before viewing the dashboard, you need to fetch some data:

   ```bash
   python run_etl.py
   ```

   This will:
   - Extract data from CDC's FluView API
   - Transform and clean the data
   - Validate data quality
   - Load data into PostgreSQL

3. **View the Dashboard**

   The Streamlit dashboard is already running and accessible through the Webview panel. It will display:
   - Latest flu surveillance metrics
   - Interactive trend charts
   - Geographic breakdowns
   - Data tables with download options

## Usage

### Manual ETL Execution

To manually fetch and update data:

```bash
python run_etl.py
```

### Scheduled ETL Execution

To set up automatic data fetching every 6 hours:

```bash
python setup_prefect_deployment.py
```

This will start a Prefect deployment that runs the ETL pipeline every 6 hours automatically.

**Note**: Press `Ctrl+C` to stop the scheduled execution.

### Viewing the Dashboard

The Streamlit dashboard runs on port 5000 and provides:

#### Sidebar Filters
- **Region Selection**: Filter by specific geographic regions or view all
- **Date Range**: Select custom date ranges for analysis
- **Metric Selection**: Toggle between % Positive and Total Specimens

#### Main Dashboard
- **Key Metrics Cards**: Latest statistics and week-over-week changes
- **Trend Charts**: Interactive line charts showing flu trends over time
- **Geographic Breakdown**: Bar charts showing regional comparisons
- **Data Table**: Recent records with CSV download capability

## ETL Pipeline Details

### Extract Task
- Fetches data from CMU Delphi Epidata API for CDC FluView
- Implements retry logic (3 retries with 60-second delays)
- Timeout protection (30 seconds)
- Retrieves data for National and 10 HHS Regions
- Covers current flu season (Week 40 of previous year through current week)

### Transform Task
- Parses JSON responses from Delphi Epidata API
- Normalizes data into consistent schema
- Maps Delphi API fields to database fields:
  - `epiweek` (YYYYWW) → `week_ending` (converted to date)
  - Computed from epiweek → `season` (e.g., "2024-25")
  - `region` (nat, hhs1-hhs10) → `region` (friendly names)
  - `ili` (ILI percentage) → `percent_positive`
  - `num_patients` → `total_specimens`

### Validate Task
- Checks for null/missing values
- Validates data types
- Ensures value ranges:
  - Percent positive: 0-100
  - Total specimens: >= 0
- Raises alerts if error rate exceeds 50%

### Load Task
- Implements upsert logic (insert or update)
- Prevents duplicate records
- Creates database indexes for performance
- Logs detailed statistics

## Database Schema

### Table: `cdc_flu_data`

| Column | Type | Description |
|--------|------|-------------|
| id | Integer | Primary key (auto-increment) |
| week_ending | Date | Week ending date |
| season | String(20) | Flu season (e.g., "2024-25") |
| region | String(100) | Geographic region |
| percent_positive | Float | ILI percentage (Influenza-like Illness) |
| total_specimens | Integer | Total patient visits |
| timestamp | DateTime | Record creation timestamp |

### Indexes & Constraints
- `idx_region_week`: Composite index on (region, week_ending)
- `idx_timestamp`: Index on timestamp
- `idx_season`: Index on season
- `uq_season_region_week`: Unique constraint on (season, region, week_ending)

## Data Source

**CDC FluView via Delphi Epidata API**
- Source: Carnegie Mellon University Delphi Research Group Epidata API
- URL: https://api.delphi.cmu.edu/epidata/fluview/
- Update Frequency: Weekly
- Coverage: National and HHS Regional U.S. data (National + 10 HHS Regions)
- Documentation: https://cmu-delphi.github.io/delphi-epidata/api/fluview.html

## Error Handling

The platform includes comprehensive error handling:

- **Network Errors**: Automatic retries with exponential backoff
- **Data Quality Issues**: Validation with detailed logging
- **Database Errors**: Transaction rollback and error reporting
- **API Failures**: Graceful degradation with user notifications

## Logging

All pipeline operations are logged with:
- Timestamp
- Log level (INFO, WARNING, ERROR)
- Component name
- Detailed messages

View logs during ETL execution to monitor progress and diagnose issues.

## Future Enhancements

Potential additions for the next phase:
- Additional CDC data sources (COVID-19, hospitalizations, mortality)
- Alerting system for data anomalies
- Predictive analytics and trend forecasting
- User-configurable dashboard widgets
- Data export in multiple formats (Excel, JSON)
- Historical comparison features
- Email/SMS notifications for significant changes

## Troubleshooting

### No data showing in dashboard
- Run `python run_etl.py` to fetch initial data
- Check logs for any error messages
- Verify database connection (DATABASE_URL environment variable)

### ETL pipeline fails
- Check internet connectivity
- Verify CDC API is accessible
- Review error logs for specific issues
- Ensure PostgreSQL database is running

### Dashboard not loading
- Verify Streamlit is running on port 5000
- Check for Python errors in the console
- Ensure all dependencies are installed

## Support

For issues or questions:
1. Check the error logs
2. Review the troubleshooting section
3. Verify all prerequisites are met

## License

This project is built for public health monitoring and educational purposes.

---

**Built with ❤️ for public health intelligence**
