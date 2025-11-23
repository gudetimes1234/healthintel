"""COVID-19 hospitalizations data source - CDC Delphi COVIDcast API."""

from datetime import datetime, timedelta
from typing import Any
from sqlalchemy import and_

from .base import BaseDataSource
from .registry import register
from database import CovidData


def epiweek_to_date(epiweek: int) -> datetime.date:
    """Convert CDC epiweek (YYYYWW) to week ending date."""
    year = int(str(epiweek)[:4])
    week = int(str(epiweek)[4:])

    jan_4 = datetime(year, 1, 4)
    week_1_start = jan_4 - timedelta(days=jan_4.weekday())

    week_start = week_1_start + timedelta(weeks=week - 1)
    week_ending = week_start + timedelta(days=6)

    return week_ending.date()


def covidcast_date_to_datetime(date_int: int) -> datetime.date:
    """Convert COVIDcast date integer to date object."""
    date_str = str(date_int)

    if len(date_str) == 6:
        # Epiweek format (YYYYWW)
        return epiweek_to_date(date_int)
    else:
        # YYYYMMDD format
        return datetime.strptime(date_str, '%Y%m%d').date()


@register
class CovidHospitalizationsSource(BaseDataSource):
    """COVID-19 hospitalizations data source from NHSN."""

    name = "covid_hospitalizations"
    description = "COVID-19 Hospital Admissions (NHSN data)"

    def extract(self) -> list[dict[str, Any]]:
        """Extract COVID-19 hospitalization data from COVIDcast API."""
        self.logger.info("Starting COVID-19 data extraction from COVIDcast API")

        api_config = self.config.get('api', {})
        url = api_config.get('base_url')
        timeout = api_config.get('timeout', 30)

        data_source = self.config.get('data_source', 'nhsn')
        signal = self.config.get('signal', 'confirmed_admissions_covid_ew')
        time_type = self.config.get('time_type', 'week')
        lookback_weeks = self.config.get('lookback_weeks', 12)
        geo_levels = self.config.get('geo_levels', ['state', 'nation'])

        # Calculate epiweek range
        end_week = datetime.now().isocalendar()
        end_epiweek = end_week[0] * 100 + end_week[1]

        start_date = datetime.now() - timedelta(weeks=lookback_weeks)
        start_week = start_date.isocalendar()
        start_epiweek = start_week[0] * 100 + start_week[1]

        all_data = {}

        # Fetch for each geographic level
        for geo_type in geo_levels:
            geo_values = 'us' if geo_type == 'nation' else '*'

            params = {
                'data_source': data_source,
                'signals': signal,
                'time_type': time_type,
                'geo_type': geo_type,
                'time_values': f"{start_epiweek}-{end_epiweek}",
                'geo_values': geo_values
            }

            self.logger.info(f"Fetching {signal} for {geo_type} (epiweeks {start_epiweek}-{end_epiweek})")
            result = self.http_client.get(url, params=params, timeout=timeout)

            if result.get('result') == 1 and 'epidata' in result:
                self.logger.info(f"Received {len(result['epidata'])} {geo_type} records")

                for record in result['epidata']:
                    key = (record['time_value'], record['geo_type'], record['geo_value'])

                    if key not in all_data:
                        all_data[key] = {
                            'date': record['time_value'],
                            'geo_type': record['geo_type'],
                            'geo_value': record['geo_value']
                        }

                    all_data[key]['confirmed_7day_avg'] = record.get('value')
            else:
                self.logger.warning(f"No results for {geo_type}: {result.get('message', 'Unknown error')}")

        self.logger.info(f"Extracted {len(all_data)} unique date-location combinations")
        return list(all_data.values())

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw data (minimal transformation needed)."""
        # Data is already in correct format from extract
        return raw_data

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate COVID-19 data quality."""
        self.logger.info(f"Validating {len(data)} records")

        validation_config = self.config.get('validation', {})
        max_value = validation_config.get('max_value', 1000000)

        valid_records = []
        invalid_count = 0

        for record in data:
            try:
                # Check required fields
                if not all(k in record for k in ['date', 'geo_type', 'geo_value']):
                    self.logger.warning(f"Missing required fields: {record}")
                    invalid_count += 1
                    continue

                # Check at least one metric present
                has_data = any([
                    record.get('confirmed_cases') is not None,
                    record.get('deaths') is not None,
                    record.get('confirmed_7day_avg') is not None,
                    record.get('deaths_7day_avg') is not None
                ])

                if not has_data:
                    self.logger.warning(f"No metrics in record: {record}")
                    invalid_count += 1
                    continue

                # Validate value ranges
                for field in ['confirmed_7day_avg', 'confirmed_cases', 'deaths', 'deaths_7day_avg']:
                    if field in record and record[field] is not None:
                        if abs(record[field]) > max_value:
                            self.logger.warning(f"Suspiciously large {field}: {record[field]}")

                valid_records.append(record)

            except Exception as e:
                self.logger.error(f"Error validating record: {e}")
                invalid_count += 1

        self.logger.info(f"Validation complete: {len(valid_records)} valid, {invalid_count} invalid")
        return valid_records

    def load(self, data: list[dict[str, Any]]) -> dict[str, int]:
        """Load validated data into database."""
        self.logger.info(f"Loading {len(data)} records to database")

        inserted = 0
        updated = 0

        with self.db_factory.get_session() as session:
            for record in data:
                date_obj = covidcast_date_to_datetime(record['date'])

                existing = session.query(CovidData).filter(
                    and_(
                        CovidData.date == date_obj,
                        CovidData.geo_type == record['geo_type'],
                        CovidData.geo_value == record['geo_value']
                    )
                ).first()

                if existing:
                    existing.confirmed_cases = record.get('confirmed_cases')
                    existing.deaths = record.get('deaths')
                    existing.confirmed_7day_avg = record.get('confirmed_7day_avg')
                    existing.deaths_7day_avg = record.get('deaths_7day_avg')
                    existing.timestamp = datetime.utcnow()
                    updated += 1
                else:
                    session.add(CovidData(
                        date=date_obj,
                        geo_type=record['geo_type'],
                        geo_value=record['geo_value'],
                        confirmed_cases=record.get('confirmed_cases'),
                        deaths=record.get('deaths'),
                        confirmed_7day_avg=record.get('confirmed_7day_avg'),
                        deaths_7day_avg=record.get('deaths_7day_avg'),
                        timestamp=datetime.utcnow()
                    ))
                    inserted += 1

        return {'inserted': inserted, 'updated': updated, 'total': len(data)}
