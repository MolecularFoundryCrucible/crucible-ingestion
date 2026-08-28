import logging
import re
import statistics
from datetime import datetime as dt
from io import BytesIO
from pathlib import Path

from PIL import Image
import matplotlib.pyplot as plt

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

ENCODING = 'latin-1'

TIME_COLUMN = 'time/s'
CURRENT_COLUMN = 'I/mA'
POTENTIAL_COLUMN = 'Ewe/V'

TIMESTAMP_FORMATS = ('%m/%d/%Y %H:%M:%S.%f', '%m/%d/%Y %H:%M:%S')


def _blank_to_none(value):
    return value if value else None


def _leading_float(value):
    """Pull the number off a value written with its unit, e.g. '4.000 cm²'."""
    if not value:
        return None
    match = re.match(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', value.strip())
    return float(match.group()) if match else None


def _to_isoformat(value):
    for fmt in TIMESTAMP_FORMATS:
        try:
            return dt.strptime(value.strip(), fmt).isoformat()
        except (ValueError, AttributeError):
            continue
    return None


class BiologicMptIngestor(CrucibleDatasetIngestor):
    """Bio-Logic EC-Lab ASCII export.

    The header is three different shapes at once: 'key : value' lines, a
    fixed-width technique settings table, and bare statement lines. 
    """

    def is_file_supported(self):
        if not self.file_to_upload.lower().endswith('.mpt'):
            return False
        try:
            with open(self.file_to_upload, encoding=ENCODING) as f:
                first_line = f.readline().strip()
        except OSError:
            return False
        
        return first_line == 'EC-Lab ASCII FILE'

    def _read_file(self):
        with open(self.file_to_upload, encoding=ENCODING) as f:
            lines = f.read().splitlines()

        match = re.search(r'(\d+)', lines[1])
        if not match:
            raise ValueError(f'{self.file_to_upload} has no header line count on line 2')
        header_length = int(match.group(1))
        return lines[:header_length - 1], lines[header_length - 1], lines[header_length:]

    def _parse_header(self, header_lines):
        fields, settings, notes = {}, {}, []
        for line in header_lines[2:]:
            stripped = line.strip()
            if not stripped:
                continue
            # ' : ' separates a key from its value. A bare ':' does not, because
            # settings rows carry colons inside the key, as in 'ti (h:m:s)'.
            if ' : ' in stripped or stripped.endswith(' :'):
                key, _, value = stripped.partition(':')
                fields[key.strip()] = _blank_to_none(value.strip())
                continue
            parts = [part.strip() for part in re.split(r'\s{2,}', stripped) if part.strip()]
            if len(parts) == 1:
                notes.append(stripped)
            elif len(parts) == 2:
                settings[parts[0]] = parts[1]
            else:
                settings[parts[0]] = parts[1:]
        return fields, settings, notes

    def _read_trace(self, column_line, data_lines):
        """Read the measured columns. Used for point counts and the thumbnail
        only; the values themselves are not written to scientific_metadata."""
        columns = [name.strip() for name in column_line.split('\t') if name.strip()]
        wanted = {TIME_COLUMN, CURRENT_COLUMN, POTENTIAL_COLUMN}
        trace = {name: [] for name in columns if name in wanted}
        for line in data_lines:
            if not line.strip():
                continue
            values = line.split('\t')
            for name, value in zip(columns, values):
                if name in trace:
                    try:
                        trace[name].append(float(value))
                    except ValueError:
                        trace[name].append(None)
        return columns, trace

    def get_scientific_metadata(self):
        header_lines, column_line, data_lines = self._read_file()
        fields, settings, notes = self._parse_header(header_lines)
        columns, trace = self._read_trace(column_line, data_lines)

        times = [t for t in trace.get(TIME_COLUMN, []) if t is not None]
        intervals = [b - a for a, b in zip(times, times[1:])]

        self.scientific_metadata = {
            'technique': next((line.strip() for line in header_lines[2:] if line.strip()), None),
            'device': fields.get('Device'),
            'channel': fields.get('Run on channel'),
            'software': next((n for n in notes if n.startswith('EC-Lab for windows')), None),
            'acquisition_started_on': _to_isoformat(fields.get('Acquisition started on')),
            'technique_started_on': _to_isoformat(fields.get('Technique started on')),
            'acquisition_started_on_raw': fields.get('Acquisition started on'),
            'saved_file': fields.get('File'),
            'saved_directory': fields.get('Directory'),
            'host': fields.get('Host'),

            'potential_control': fields.get('Potential control'),
            'electrode_connection': fields.get('Electrode connection'),
            'electrode_material': fields.get('Electrode material'),
            'electrolyte': fields.get('Electrolyte'),
            'initial_state': fields.get('Initial state'),
            'comments': fields.get('Comments'),
            'electrode_surface_area_cm2': _leading_float(fields.get('Electrode surface area')),
            'ewe_control_range': fields.get('Ewe ctrl range'),
            'technique_settings': settings,
            'point_count': len(times),
            'duration_s': times[-1] if times else None,
            'sampling_interval_s': statistics.median(intervals) if intervals else None,

            # Present in the ISAAC CO2RR record but absent from this file. Ewe is
            # recorded against an unnamed reference, so converting it to V vs RHE
            # needs all three of the electrode fields below.
            'reference_electrode': None,
            'reference_electrode_potential_V_vs_SHE': None,
            'electrolyte_pH': None,
            'ir_compensation_ohm': None,
            'cell_temperature_C': None,
        }

    def parse_measurement(self):
        self.measurement = self.scientific_metadata.get('technique')

    def parse_data_type(self):
        self.data_type = 'Electrochemistry Time Series'

    def parse_dataset_name(self):
        if self.dataset_name:
            return
        self.dataset_name = f'{Path(self.file_to_upload).stem}'

    def parse_file_timestamp(self):
        if self.timestamp:
            return
        started = self.scientific_metadata.get('acquisition_started_on')
        if started:
            self.timestamp = started
        else:
            super().parse_file_timestamp()

    def get_thumbnails(self):
        self.thumbnails = []
        try:
            _, column_line, data_lines = self._read_file()
            _, trace = self._read_trace(column_line, data_lines)
            times = trace.get(TIME_COLUMN)
            currents = trace.get(CURRENT_COLUMN)
            potentials = trace.get(POTENTIAL_COLUMN)
            if not times or not currents:
                return

            fig, ax = plt.subplots()
            ax.plot(times, currents, color='tab:blue')
            ax.set_xlabel('Time (s)')
            ax.set_ylabel('I (mA)', color='tab:blue')
            if potentials:
                twin = ax.twinx()
                twin.plot(times, potentials, color='tab:red')
                twin.set_ylabel('Ewe (V)', color='tab:red')

            buf = BytesIO()
            fig.savefig(buf, format='png', dpi=300, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), 'Current and potential vs time', size = (500,500))
        except Exception as err:
            logger.error(f'Failed to generate EC thumbnail: {err}')
