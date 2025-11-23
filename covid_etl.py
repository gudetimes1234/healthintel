#!/usr/bin/env python3
"""
COVID-19 ETL script that fetches data from CMU Delphi COVIDcast API.
Collects confirmed cases and deaths data at state and national level.
"""

import os
import requests
from datetime import datetime, timedelta
from database import CovidData, get_db_session, init_db
from sqlalchemy import and_
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def date_to_covidcast_format(date_obj):
    """Convert datetime to COVIDcast YYYYMMDD format."""
    return int(date_obj.strftime('%Y%m%d'))

def epiweek_to_date(epiweek):
    """Convert CDC epiweek (YYYYWW) to week ending date."""
    year = int(str(epiweek)[:4])
    week = int(str(epiweek)[4:])
    
    jan_4 = datetime(year, 1, 4)
    week_1_start = jan_4 - timedelta(days=jan_4.weekday())
    
    week_start = week_1_start + timedelta(weeks=week-1)
    week_ending = week_start + timedelta(days=6)
    
    return week_ending.date()

def covidcast_date_to_datetime(date_int):
    """Convert COVIDcast YYYYMMDD integer or epiweek to date object."""
    date_str = str(date_int)
    
    # Check if it's epiweek format (YYYYWW, 6 digits) or date format (YYYYMMDD, 8 digits)
    if len(date_str) == 6:
        # It's an epiweek, convert to week ending date
        return epiweek_to_date(date_int)
    else:
        # It's a YYYYMMDD date
        return datetime.strptime(date_str, '%Y%m%d').date()

def extract_covid_data():
    """Extract COVID-19 hospitalization data from Delphi COVIDcast API (NHSN source)."""
    logger.info("Starting COVID-19 data extraction from Delphi COVIDcast API (NHSN)")
    
    try:
        base_url = "https://api.delphi.cmu.edu/epidata/covidcast/"
        
        # Fetch last 12 weeks of data (NHSN reports weekly)
        # NHSN data became mandatory on Nov 1, 2024
        end_week = datetime.now().isocalendar()
        end_epiweek = end_week[0] * 100 + end_week[1]
        
        # Start from 12 weeks ago
        start_date = datetime.now() - timedelta(weeks=12)
        start_week = start_date.isocalendar()
        start_epiweek = start_week[0] * 100 + start_week[1]
        
        # Data source: NHSN (National Healthcare Safety Network)
        # Using confirmed admissions data which is actively maintained
        signals = [
            ('nhsn', 'confirmed_admissions_covid_ew'),  # COVID-19 confirmed hospital admissions (weekly)
        ]
        
        all_data = {}
        
        # Fetch state-level data
        for data_source, signal in signals:
            params = {
                'data_source': data_source,
                'signals': signal,
                'time_type': 'week',
                'geo_type': 'state',
                'time_values': f"{start_epiweek}-{end_epiweek}",
                'geo_values': '*'
            }
            
            logger.info(f"Fetching {signal} for states (epiweeks {start_epiweek}-{end_epiweek})...")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('result') == 1 and 'epidata' in result:
                logger.info(f"Received {len(result['epidata'])} state records")
                for record in result['epidata']:
                    key = (record['time_value'], record['geo_type'], record['geo_value'])
                    if key not in all_data:
                        all_data[key] = {
                            'date': record['time_value'],
                            'geo_type': record['geo_type'],
                            'geo_value': record['geo_value']
                        }
                    
                    # Store COVID hospitalization admissions
                    all_data[key]['confirmed_7day_avg'] = record.get('value')
            else:
                logger.warning(f"No results for {signal}: {result.get('message', 'Unknown error')}")
        
        # Fetch national-level data
        for data_source, signal in signals:
            params = {
                'data_source': data_source,
                'signals': signal,
                'time_type': 'week',
                'geo_type': 'nation',
                'time_values': f"{start_epiweek}-{end_epiweek}",
                'geo_values': 'us'
            }
            
            logger.info(f"Fetching {signal} for national level (epiweeks {start_epiweek}-{end_epiweek})...")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('result') == 1 and 'epidata' in result:
                logger.info(f"Received {len(result['epidata'])} national records")
                for record in result['epidata']:
                    key = (record['time_value'], record['geo_type'], record['geo_value'])
                    if key not in all_data:
                        all_data[key] = {
                            'date': record['time_value'],
                            'geo_type': record['geo_type'],
                            'geo_value': record['geo_value']
                        }
                    
                    # Store COVID hospitalization admissions
                    all_data[key]['confirmed_7day_avg'] = record.get('value')
            else:
                logger.warning(f"No results for {signal}: {result.get('message', 'Unknown error')}")
        
        logger.info(f"Successfully extracted {len(all_data)} unique date-location combinations")
        
        return list(all_data.values())
        
    except requests.RequestException as e:
        logger.error(f"Failed to extract data from COVIDcast API: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {str(e)}")
        raise

def validate_covid_data(data):
    """Validate COVID-19 data quality."""
    logger.info("Validating COVID-19 data")
    
    valid_records = []
    invalid_count = 0
    
    for record in data:
        try:
            # Check required fields
            if not all(k in record for k in ['date', 'geo_type', 'geo_value']):
                logger.warning(f"Missing required fields in record: {record}")
                invalid_count += 1
                continue
            
            # At least one metric should be present
            has_data = any([
                record.get('confirmed_cases') is not None,
                record.get('deaths') is not None,
                record.get('confirmed_7day_avg') is not None,
                record.get('deaths_7day_avg') is not None
            ])
            
            if not has_data:
                logger.warning(f"No metrics present in record: {record}")
                invalid_count += 1
                continue
            
            # Validate numeric ranges (negative values can occur due to data corrections)
            # but extremely large values might indicate errors
            for field in ['confirmed_cases', 'deaths', 'confirmed_7day_avg', 'deaths_7day_avg']:
                if field in record and record[field] is not None:
                    if abs(record[field]) > 1000000:  # Sanity check
                        logger.warning(f"Suspiciously large value for {field}: {record[field]}")
            
            valid_records.append(record)
            
        except Exception as e:
            logger.error(f"Error validating record {record}: {str(e)}")
            invalid_count += 1
    
    logger.info(f"Validation complete: {len(valid_records)} valid, {invalid_count} invalid")
    
    return valid_records

def load_covid_data(data):
    """Load COVID-19 data into PostgreSQL database."""
    logger.info("Loading COVID-19 data into database")
    
    try:
        with get_db_session() as session:
            loaded_count = 0
            updated_count = 0
            
            for record in data:
                date_obj = covidcast_date_to_datetime(record['date'])
                
                # Check if record exists
                existing = session.query(CovidData).filter(
                    and_(
                        CovidData.date == date_obj,
                        CovidData.geo_type == record['geo_type'],
                        CovidData.geo_value == record['geo_value']
                    )
                ).first()
                
                if existing:
                    # Update existing record
                    existing.confirmed_cases = record.get('confirmed_cases')
                    existing.deaths = record.get('deaths')
                    existing.confirmed_7day_avg = record.get('confirmed_7day_avg')
                    existing.deaths_7day_avg = record.get('deaths_7day_avg')
                    existing.timestamp = datetime.utcnow()
                    updated_count += 1
                else:
                    # Insert new record
                    covid_record = CovidData(
                        date=date_obj,
                        geo_type=record['geo_type'],
                        geo_value=record['geo_value'],
                        confirmed_cases=record.get('confirmed_cases'),
                        deaths=record.get('deaths'),
                        confirmed_7day_avg=record.get('confirmed_7day_avg'),
                        deaths_7day_avg=record.get('deaths_7day_avg'),
                        timestamp=datetime.utcnow()
                    )
                    session.add(covid_record)
                    loaded_count += 1
            
            session.commit()
            logger.info(f"Data load complete: {loaded_count} new records, {updated_count} updated")
            
            return loaded_count + updated_count
            
    except Exception as e:
        logger.error(f"Error loading data into database: {str(e)}")
        raise

def run_covid_etl():
    """Run the complete COVID-19 ETL pipeline."""
    logger.info("=" * 60)
    logger.info("Starting COVID-19 ETL Pipeline")
    logger.info("=" * 60)
    
    try:
        # Initialize database (creates tables if they don't exist)
        init_db()
        
        # Extract
        raw_data = extract_covid_data()
        
        # Validate
        validated_data = validate_covid_data(raw_data)
        
        # Load
        records_loaded = load_covid_data(validated_data)
        
        logger.info("=" * 60)
        logger.info(f"COVID-19 ETL Pipeline completed successfully!")
        logger.info(f"Total records processed: {records_loaded}")
        logger.info("=" * 60)
        
        return records_loaded
        
    except Exception as e:
        logger.error(f"COVID-19 ETL Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_covid_etl()
