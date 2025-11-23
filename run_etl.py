#!/usr/bin/env python3
"""
Script to manually run the CDC Flu ETL pipeline.
This can be used to populate the database initially or refresh data on demand.
"""

import logging
from simple_etl import run_etl_pipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == "__main__":
    print("=" * 60)
    print("Running CDC Flu Data ETL Pipeline")
    print("=" * 60)
    
    try:
        result = run_etl_pipeline()
        print("\n" + "=" * 60)
        print("ETL Pipeline completed successfully!")
        print(f"Results: {result}")
        print("=" * 60)
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"ETL Pipeline failed: {str(e)}")
        print("=" * 60)
        raise
