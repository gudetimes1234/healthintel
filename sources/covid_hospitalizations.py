"""COVID-19 hospitalizations data source - CDC Delphi COVIDcast API."""

from datetime import datetime, timedelta
from typing import Any
from sqlalchemy import and_

from .base import BaseDataSource
from .registry import register
from models import PublicObservation


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
        return epiweek_to_date(date_int)
    else:
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

            self.logger.info(f"Fetching {signal} for {geo_type}")
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

                    all_data[key]['value'] = record.get('value')
                    all_data[key]['stderr'] = record.get('stderr')
            else:
                self.logger.warning(f"No results for {geo_type}: {result.get('message', 'Unknown')}")

        self.logger.info(f"Extracted {len(all_data)} unique date-location combinations")
        return list(all_data.values())

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw data into unified observation format."""
        self.logger.info(f"Transforming {len(raw_data)} records")

        transformed = []

        for record in raw_data:
            try:
                date_obj = covidcast_date_to_datetime(record['date'])

                transformed.append({
                    'date': date_obj,
                    'geo_type': record['geo_type'],
                    'geo_value': record['geo_value'],
                    'source': 'nhsn',
                    'signal': 'covid_hosp',
                    'value': record.get('value'),
                    'stderr': record.get('stderr'),
                })

            except (ValueError, KeyError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e}")

        return transformed

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate COVID-19 data quality."""
        self.logger.info(f"Validating {len(data)} records")

        validation_config = self.config.get('validation', {})
        max_value = validation_config.get('max_value', 1000000)

        valid_records = []
        invalid_count = 0

        for record in data:
            try:
                if not all(k in record for k in ['date', 'geo_type', 'geo_value', 'signal']):
                    self.logger.warning(f"Missing required fields: {record}")
                    invalid_count += 1
                    continue

                if record.get('value') is None:
                    self.logger.warning(f"No value in record: {record}")
                    invalid_count += 1
                    continue

                if abs(record['value']) > max_value:
                    self.logger.warning(f"Suspiciously large value: {record['value']}")

                valid_records.append(record)

            except Exception as e:
                self.logger.error(f"Error validating record: {e}")
                invalid_count += 1

        self.logger.info(f"Validation complete: {len(valid_records)} valid, {invalid_count} invalid")
        return valid_records

    def load(self, data: list[dict[str, Any]]) -> dict[str, int]:
        """Load validated data into unified observations table."""
        self.logger.info(f"Loading {len(data)} records to database")

        inserted = 0
        updated = 0

        with self.db_factory.get_session() as session:
            for record in data:
                existing = session.query(PublicObservation).filter(
                    and_(
                        PublicObservation.date == record['date'],
                        PublicObservation.geo_type == record['geo_type'],
                        PublicObservation.geo_value == record['geo_value'],
                        PublicObservation.source == record['source'],
                        PublicObservation.signal == record['signal'],
                    )
                ).first()

                if existing:
                    existing.value = record['value']
                    existing.stderr = record.get('stderr')
                    existing.updated_at = datetime.utcnow()
                    updated += 1
                else:
                    obs = PublicObservation(
                        date=record['date'],
                        geo_type=record['geo_type'],
                        geo_value=record['geo_value'],
                        source=record['source'],
                        signal=record['signal'],
                        value=record['value'],
                        stderr=record.get('stderr'),
                    )
                    session.add(obs)
                    inserted += 1

        return {'inserted': inserted, 'updated': updated, 'total': len(data)}
