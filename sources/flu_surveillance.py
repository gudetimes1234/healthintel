"""Flu surveillance data source - CDC Delphi Epidata API."""

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


# Map API region codes to standardized geo values
REGION_TO_GEO = {
    'nat': ('nation', 'us'),
    'hhs1': ('hhs_region', 'hhs1'),
    'hhs2': ('hhs_region', 'hhs2'),
    'hhs3': ('hhs_region', 'hhs3'),
    'hhs4': ('hhs_region', 'hhs4'),
    'hhs5': ('hhs_region', 'hhs5'),
    'hhs6': ('hhs_region', 'hhs6'),
    'hhs7': ('hhs_region', 'hhs7'),
    'hhs8': ('hhs_region', 'hhs8'),
    'hhs9': ('hhs_region', 'hhs9'),
    'hhs10': ('hhs_region', 'hhs10'),
}


@register
class FluSurveillanceSource(BaseDataSource):
    """CDC Influenza Surveillance data source."""

    name = "flu_surveillance"
    description = "CDC Influenza Surveillance (ILI data)"

    def extract(self) -> list[dict[str, Any]]:
        """Extract influenza surveillance data from CDC API."""
        self.logger.info("Starting data extraction from CDC Delphi Epidata API")

        api_config = self.config.get('api', {})
        url = api_config.get('base_url')
        timeout = api_config.get('timeout', 30)
        regions = self.config.get('regions', list(REGION_TO_GEO.keys()))

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
        """Transform raw CDC data into unified observation format."""
        self.logger.info(f"Transforming {len(raw_data)} records")

        transformed = []

        for record in raw_data:
            try:
                epiweek = record.get('epiweek')
                if not epiweek:
                    continue

                region_code = record.get('region', 'nat')
                geo_type, geo_value = REGION_TO_GEO.get(region_code, ('unknown', region_code))

                # Create observation for ILI percentage
                ili_value = float(record.get('ili', 0) or 0)
                transformed.append({
                    'date': epiweek_to_date(epiweek),
                    'geo_type': geo_type,
                    'geo_value': geo_value,
                    'source': 'fluview',
                    'signal': 'ili_pct',
                    'value': ili_value,
                    'sample_size': int(record.get('num_patients', 0) or 0),
                })

                # Create observation for total specimens
                specimens = int(record.get('num_patients', 0) or 0)
                transformed.append({
                    'date': epiweek_to_date(epiweek),
                    'geo_type': geo_type,
                    'geo_value': geo_value,
                    'source': 'fluview',
                    'signal': 'total_specimens',
                    'value': float(specimens),
                })

            except (ValueError, KeyError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e}")

        return transformed

    def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Validate data quality."""
        self.logger.info(f"Validating {len(data)} records")

        validation_config = self.config.get('validation', {})
        max_error_rate = validation_config.get('max_error_rate', 0.5)

        valid_records = []
        errors = []

        for i, record in enumerate(data):
            record_errors = []

            if record.get('date') is None:
                record_errors.append(f"Record {i}: Missing date")

            if not record.get('geo_type'):
                record_errors.append(f"Record {i}: Missing geo_type")

            if not record.get('signal'):
                record_errors.append(f"Record {i}: Missing signal")

            # Signal-specific validation
            if record.get('signal') == 'ili_pct':
                value = record.get('value', -1)
                if value < 0 or value > 100:
                    record_errors.append(f"Record {i}: Invalid ili_pct ({value})")

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
                    existing.sample_size = record.get('sample_size')
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
                        sample_size=record.get('sample_size'),
                    )
                    session.add(obs)
                    inserted += 1

        return {'inserted': inserted, 'updated': updated, 'total': len(data)}
