#!/usr/bin/env python3
"""
Script to set up Prefect deployment for scheduled CDC Flu data fetching.
This creates a deployment that runs every 6 hours.
"""

from prefect import serve
from prefect_flows import cdc_flu_etl_flow
from datetime import timedelta

if __name__ == "__main__":
    print("Setting up Prefect deployment for CDC Flu ETL Pipeline")
    print("This will schedule the pipeline to run every 6 hours")
    
    cdc_deployment = cdc_flu_etl_flow.to_deployment(
        name="cdc-flu-6-hour-schedule",
        interval=timedelta(hours=6),
        tags=["cdc", "flu", "health-intel"],
        description="Fetches CDC flu surveillance data every 6 hours"
    )
    
    print("\nStarting Prefect server...")
    print("The ETL pipeline will run every 6 hours automatically.")
    print("Press Ctrl+C to stop the scheduler.")
    print("\nNote: In production, you would use 'prefect deploy' and run a Prefect server separately.")
    
    serve(cdc_deployment)
