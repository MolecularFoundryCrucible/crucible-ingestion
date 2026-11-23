import logging
from datetime import datetime
from pathlib import Path

import tifffile

from .crucible_ingestor import CrucibleDatasetIngestor
from .image_ingestor import TifIngestor
from ..utils import sanitize_metadata

logger = logging.getLogger(__name__)

FEI_DATE_FORMATS = ['%m/%d/%Y', '%d.%m.%Y', '%Y-%m-%d']
FEI_TIME_FORMATS = ['%I:%M:%S %p', '%H:%M:%S']
PA_TO_TORR = 0.00750062


def read_fei_metadata(path):
    # a sniffing check: any failure here must not abort the registry scan
    try:
        with tifffile.TiffFile(path) as tif:
            return tif.fei_metadata
    except Exception as err:
        logger.info(f'no FEI metadata readable from {path}: {err}')
        return None


def vacuum_level(mode):
    mode = str(mode).lower()
    if any(s in mode for s in ('esem', 'wet', 'environ')):
        return 'ESEM'
    if 'low' in mode:
        return 'Low'
    return 'High'


def acquisition_time(fei_md):
    user = fei_md.get('User', {})
    date, time = str(user.get('Date', '')).strip(), str(user.get('Time', '')).strip()
    if not date or not time:
        return None
    for date_fmt in FEI_DATE_FORMATS:
        for time_fmt in FEI_TIME_FORMATS:
            try:
                return datetime.strptime(f'{date} {time}', f'{date_fmt} {time_fmt}')
            except ValueError:
                continue
    logger.warning(f'unrecognized FEI acquisition date/time: {date!r} {time!r}')
    return None


def pa_to_torr(value):
    try:
        return float(value) * PA_TO_TORR
    except (TypeError, ValueError):
        logger.warning(f'non-numeric FEI chamber pressure: {value!r}')
        return None


def first_present(fei_md, *paths):
    for section, key in paths:
        value = (fei_md.get(section) or {}).get(key)
        if value is not None and str(value).strip() != '':
            return value
    return None


class FeiSemTifIngestor(TifIngestor):

    def is_file_supported(self):
        if not TifIngestor.is_file_supported(self):
            return False
        self.fei_metadata = read_fei_metadata(self.file_to_upload)
        return bool(self.fei_metadata)

    def parse_measurement(self):
        self.measurement = 'Scanning electron microscope'

    def parse_data_type(self):
        self.data_type = 'SEM Images'

    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        fei_md = sanitize_metadata(self.fei_metadata)
        vacuum_mode = first_present(fei_md, ('Vacuum', 'UserMode'))
        pressure_pa = first_present(fei_md, ('Vacuum', 'ChPressure'))
        summary = {
            'vacuum_level': vacuum_level(vacuum_mode) if vacuum_mode is not None else None,
            'spot_size': first_present(fei_md, ('Beam', 'Spot'), ('EBeam', 'Spot')),
            'high_voltage_V': first_present(fei_md, ('Beam', 'HV'), ('EBeam', 'HV')),
            'emission_current_A': first_present(fei_md, ('EBeam', 'EmissionCurrent')),
            'chamber_pressure_Torr': pa_to_torr(pressure_pa) if pressure_pa is not None else None,
        }
        # the B30 SEM form stores these as strings
        self.scientific_metadata.update({k: str(v) for k, v in summary.items() if v is not None})
        self.scientific_metadata['tiff_metadata'] = {Path(self.match_name).name: fei_md}

    def parse_file_timestamp(self):
        acquired = acquisition_time(self.fei_metadata)
        if acquired is None:
            return CrucibleDatasetIngestor.parse_file_timestamp(self)
        self.timestamp = acquired.isoformat()
