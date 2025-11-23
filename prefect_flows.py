import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from database import CDCFluData, get_db_session, init_db
from sqlalchemy import and_
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task(name="Extract CDC Flu Data", retries=3, retry_delay_seconds=60)
def extract_cdc_data():
    """
    Extract influenza surveillance data from CDC API via Delphi Epidata.
    This uses the CMU Delphi Epidata API for ILI surveillance data.
    """
    logger.info("Starting data extraction from CDC Delphi Epidata API")
    
    try:
        url = "https://api.delphi.cmu.edu/epidata/fluview/"
        
        regions = ['nat', 'hhs1', 'hhs2', 'hhs3', 'hhs4', 'hhs5', 
                   'hhs6', 'hhs7', 'hhs8', 'hhs9', 'hhs10']
        
        current_year = datetime.now().year
        current_week = datetime.now().isocalendar()[1]
        
        start_epiweek = f"{current_year - 1}{40:02d}"
        end_epiweek = f"{current_year}{current_week:02d}"
        
        all_data = []
        
        for region in regions:
            params = {
                'regions': region,
                'epiweeks': f"{start_epiweek}-{end_epiweek}"
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get('result') == 1 and 'epidata' in result:
                all_data.extend(result['epidata'])
        
        logger.info(f"Successfully extracted {len(all_data)} records from CDC Delphi API")
        
        return all_data
        
    except requests.RequestException as e:
        logger.error(f"Failed to extract data from CDC API: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {str(e)}")
        raise

def epiweek_to_date(epiweek):
    """Convert CDC epiweek (YYYYWW) to week ending date."""
    year = int(str(epiweek)[:4])
    week = int(str(epiweek)[4:])
    
    jan_4 = datetime(year, 1, 4)
    week_1_start = jan_4 - timedelta(days=jan_4.weekday())
    
    week_start = week_1_start + timedelta(weeks=week-1)
    week_ending = week_start + timedelta(days=6)
    
    return week_ending.date()

def get_season_from_epiweek(epiweek):
    """Determine flu season from epiweek."""
    year = int(str(epiweek)[:4])
    week = int(str(epiweek)[4:])
    
    if week >= 40:
        return f"{year}-{str(year+1)[2:]}"
    else:
        return f"{year-1}-{str(year)[2:]}"

@task(name="Transform CDC Flu Data")
def transform_cdc_data(raw_data):
    """
    Transform and normalize raw CDC data from Delphi Epidata format.
    Maps epiweeks to dates, regions to friendly names, and ILI metrics to our schema.
    """
    logger.info(f"Starting transformation of {len(raw_data)} records")
    
    try:
        transformed_records = []
        
        region_name_map = {
            'nat': 'National',
            'hhs1': 'HHS Region 1',
            'hhs2': 'HHS Region 2',
            'hhs3': 'HHS Region 3',
            'hhs4': 'HHS Region 4',
            'hhs5': 'HHS Region 5',
            'hhs6': 'HHS Region 6',
            'hhs7': 'HHS Region 7',
            'hhs8': 'HHS Region 8',
            'hhs9': 'HHS Region 9',
            'hhs10': 'HHS Region 10'
        }
        
        for record in raw_data:
            try:
                epiweek = record.get('epiweek')
                if not epiweek:
                    continue
                
                week_ending = epiweek_to_date(epiweek)
                season = get_season_from_epiweek(epiweek)
                
                region_code = record.get('region', 'nat')
                region = region_name_map.get(region_code, region_code)
                
                percent_positive = float(record.get('ili', 0) or 0)
                total_specimens = int(record.get('num_patients', 0) or 0)
                
                transformed_record = {
                    'week_ending': week_ending,
                    'season': season,
                    'region': region,
                    'percent_positive': percent_positive,
                    'total_specimens': total_specimens,
                    'timestamp': datetime.utcnow()
                }
                
                transformed_records.append(transformed_record)
                
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Skipping malformed record: {str(e)}")
                continue
        
        logger.info(f"Successfully transformed {len(transformed_records)} records")
        return transformed_records
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise

@task(name="Validate CDC Flu Data")
def validate_cdc_data(transformed_data):
    """
    Validate data quality with checks for nulls, data types, and value ranges.
    Raises alerts if data quality issues are found.
    """
    logger.info(f"Starting validation of {len(transformed_data)} records")
    
    validation_errors = []
    valid_records = []
    
    for i, record in enumerate(transformed_data):
        errors = []
        
        if record.get('week_ending') is None:
            errors.append(f"Record {i}: Missing week_ending")
        
        if not record.get('season'):
            errors.append(f"Record {i}: Missing season")
        
        if not record.get('region'):
            errors.append(f"Record {i}: Missing region")
        
        percent_positive = record.get('percent_positive', -1)
        if percent_positive < 0 or percent_positive > 100:
            errors.append(f"Record {i}: Invalid percent_positive ({percent_positive}), must be 0-100")
        
        total_specimens = record.get('total_specimens', -1)
        if total_specimens < 0:
            errors.append(f"Record {i}: Invalid total_specimens ({total_specimens}), must be >= 0")
        
        if errors:
            validation_errors.extend(errors)
        else:
            valid_records.append(record)
    
    if validation_errors:
        logger.warning(f"Found {len(validation_errors)} validation issues")
        for error in validation_errors[:10]:
            logger.warning(error)
        if len(validation_errors) > 10:
            logger.warning(f"... and {len(validation_errors) - 10} more validation issues")
    
    error_rate = len(validation_errors) / len(transformed_data) if transformed_data else 0
    if error_rate > 0.5:
        raise ValueError(f"Data quality check failed: {error_rate*100:.1f}% of records have errors")
    
    logger.info(f"Validation complete: {len(valid_records)} valid records, {len(validation_errors)} errors")
    return valid_records

@task(name="Load CDC Flu Data to PostgreSQL")
def load_to_database(validated_data):
    """
    Load validated data into PostgreSQL database.
    Implements upsert logic to avoid duplicates.
    """
    logger.info(f"Starting database load for {len(validated_data)} records")
    
    try:
        init_db()
        
        with get_db_session() as session:
            inserted_count = 0
            updated_count = 0
            
            for record in validated_data:
                existing = session.query(CDCFluData).filter(
                    and_(
                        CDCFluData.week_ending == record['week_ending'],
                        CDCFluData.region == record['region'],
                        CDCFluData.season == record['season']
                    )
                ).first()
                
                if existing:
                    existing.percent_positive = record['percent_positive']
                    existing.total_specimens = record['total_specimens']
                    existing.timestamp = record['timestamp']
                    updated_count += 1
                else:
                    flu_record = CDCFluData(**record)
                    session.add(flu_record)
                    inserted_count += 1
            
            session.commit()
            logger.info(f"Database load complete: {inserted_count} inserted, {updated_count} updated")
            
            return {
                'inserted': inserted_count,
                'updated': updated_count,
                'total': len(validated_data)
            }
            
    except Exception as e:
        logger.error(f"Error loading data to database: {str(e)}")
        raise

@flow(name="CDC Flu Data ETL Pipeline", log_prints=True)
def cdc_flu_etl_flow():
    """
    Main ETL flow for CDC flu surveillance data.
    Runs every 6 hours to fetch and process the latest data.
    """
    logger.info("="*50)
    logger.info("Starting CDC Flu Data ETL Pipeline")
    logger.info("="*50)
    
    try:
        raw_data = extract_cdc_data()
        
        transformed_data = transform_cdc_data(raw_data)
        
        validated_data = validate_cdc_data(transformed_data)
        
        result = load_to_database(validated_data)
        
        logger.info("="*50)
        logger.info(f"ETL Pipeline completed successfully")
        logger.info(f"Summary: {result}")
        logger.info("="*50)
        
        return result
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    cdc_flu_etl_flow()
