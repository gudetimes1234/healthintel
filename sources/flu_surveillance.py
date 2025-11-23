"""Flu surveillance data source - CDC Delphi Epidata API."""

from datetime import datetime, timedelta
from typing import Any
from sqlalchemy import and_

from .base import BaseDataSource
from .registry import register
from database import CDCFluData


def epiweek_to_date(epiweek: int) -> datetime.date:
    """Convert CDC epiweek (YYYYWW) to week ending date."""
    year = int(str(epiweek)[:4])
    week = int(str(epiweek)[4:])

    jan_4 = datetime(year, 1, 4)
    week_1_start = jan_4 - timedelta(days=jan_4.weekday())

    week_start = week_1_start + timedelta(weeks=week - 1)
    week_ending = week_start + timedelta(days=6)

    return week_ending.date()


def get_season_from_epiweek(epiweek: int) -> str:
    """Determine flu season from epiweek."""
    year = int(str(epiweek)[:4])
    week = int(str(epiweek)[4:])

    if week >= 40:
        return f"{year}-{str(year + 1)[2:]}"
    else:
        return f"{year - 1}-{str(year)[2:]}"


@register
class FluSurveillanceSource(BaseDataSource):
    """CDC Influenza Surveillance data source."""

    name = "flu_surveillance"
    description = "CDC Influenza Surveillance (ILI data)"

    REGION_NAME_MAP = {
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

    def extract(self) -> list[dict[str, Any]]:
        """Extract influenza surveillance data from CDC API."""
        self.logger.info("Starting data extraction from CDC Delphi Epidata API")

        api_config = self.config.get('api', {})
        url = api_config.get('base_url')
        timeout = api_config.get('timeout', 30)
        regions = self.config.get('regions', list(self.REGION_NAME_MAP.keys()))

        # Calculate epiweek range
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

            result = self.http_client.get(url, params=params, timeout=timeout)

            if result.get('result') == 1 and 'epidata' in result:
                all_data.extend(result['epidata'])

        self.logger.info(f"Extracted {len(all_data)} records from CDC API")
        return all_data

    def transform(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform raw CDC data into normalized format."""
        self.logger.info(f"Transforming {len(raw_data)} records")

        transformed = []

        for record in raw_data:
            try:
                epiweek = record.get('epiweek')
                if not epiweek:
                    continue

                region_code = record.get('region', 'nat')

                transformed.append({
                    'week_ending': epiweek_to_date(epiweek),
                    'season': get_season_from_epiweek(epiweek),
                    'region': self.REGION_NAME_MAP.get(region_code, region_code),
                    'percent_positive': float(record.get('ili', 0) or 0),
                    'total_specimens': int(record.get('num_patients', 0) or 0),
                    'timestamp': datetime.utcnow()
                })

            except (ValueError, KeyError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e}")

        return transformed

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate data quality."""
        self.logger.info(f"Validating {len(data)} records")

        validation_config = self.config.get('validation', {})
        max_error_rate = validation_config.get('max_error_rate', 0.5)
        percent_range = validation_config.get('percent_positive_range', [0, 100])
        min_specimens = validation_config.get('total_specimens_min', 0)

        valid_records = []
        errors = []

        for i, record in enumerate(data):
            record_errors = []

            if record.get('week_ending') is None:
                record_errors.append(f"Record {i}: Missing week_ending")

            if not record.get('season'):
                record_errors.append(f"Record {i}: Missing season")

            if not record.get('region'):
                record_errors.append(f"Record {i}: Missing region")

            pct = record.get('percent_positive', -1)
            if pct < percent_range[0] or pct > percent_range[1]:
                record_errors.append(f"Record {i}: Invalid percent_positive ({pct})")

            specimens = record.get('total_specimens', -1)
            if specimens < min_specimens:
                record_errors.append(f"Record {i}: Invalid total_specimens ({specimens})")

            if record_errors:
                errors.extend(record_errors)
            else:
                valid_records.append(record)

        if errors:
            self.logger.warning(f"Found {len(errors)} validation issues")
            for error in errors[:10]:
                self.logger.warning(error)

        error_rate = len(errors) / len(data) if data else 0
        if error_rate > max_error_rate:
            raise ValueError(f"Data quality check failed: {error_rate*100:.1f}% error rate")

        return valid_records

    def load(self, data: list[dict[str, Any]]) -> dict[str, int]:
        """Load validated data into database."""
        self.logger.info(f"Loading {len(data)} records to database")

        inserted = 0
        updated = 0

        with self.db_factory.get_session() as session:
            for record in data:
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
                    updated += 1
                else:
                    session.add(CDCFluData(**record))
                    inserted += 1

        return {'inserted': inserted, 'updated': updated, 'total': len(data)}
