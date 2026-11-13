import logging
import math
import struct
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

# Horiba LabSpec 6 .ngs is undocumented; this layout was reverse-engineered from single
# LabRAM spectra. Every step is checked so an unfamiliar variant (maps, other versions)
# fails to parse and is left unclaimed instead of being ingested wrong.
MAGIC = b'NGSNextGen'
PARAM_DATE_FORMAT = '%m/%d/%y %H:%M:%S'


class NgsFormatError(ValueError):
    pass


class Reader:
    def __init__(self, data, pos=0):
        self.data, self.pos = data, pos

    def take(self, fmt):
        size = struct.calcsize(fmt)
        if self.pos + size > len(self.data):
            raise NgsFormatError(f'unexpected end of file at byte {self.pos}')
        values = struct.unpack_from(fmt, self.data, self.pos)
        self.pos += size
        return values if len(values) > 1 else values[0]

    def pstr(self):
        n = self.take('<B')
        if self.pos + n > len(self.data):
            raise NgsFormatError(f'string runs past end of file at byte {self.pos}')
        s = self.data[self.pos:self.pos + n].decode('latin-1')
        self.pos += n
        return s

    def pstrs(self, count):
        return [self.pstr() for _ in range(count)]

    def expect(self, value):
        found = self.pstr()
        if found != value:
            raise NgsFormatError(f'expected {value!r}, found {found!r} at byte {self.pos}')

    def skip_to_count(self, count, limit=16):
        # tables pad a variable number of bytes before each column's uint16 value count
        for _ in range(limit):
            if struct.unpack_from('<H', self.data, self.pos)[0] == count:
                self.pos += 2
                return
            self.pos += 1
        raise NgsFormatError(f'no value count {count} near byte {self.pos}')


def read_table(data, name):
    marker = b'\x05Table' + bytes([len(name)]) + name.encode()
    start = data.find(marker)
    if start < 0:
        return None
    r = Reader(data, start + len(marker))
    r.pstr()
    r.take('<I')
    rows = r.pstrs(r.take('<H'))
    columns = r.pstrs(r.take('<H'))
    if not columns:
        raise NgsFormatError(f'{name} table has no columns')
    # the first column has a value for every row; later columns (units) omit trailing blanks
    r.skip_to_count(len(rows))
    table = {columns[0]: r.pstrs(len(rows))}
    for column in columns[1:]:
        count = r.take('<H')
        if count > len(rows):
            raise NgsFormatError(f'{name} column {column!r} has more values than rows')
        table[column] = r.pstrs(count) + [''] * (len(rows) - count)
    return rows, table


def read_history(data):
    start = data.find(b'\x05Table\x04Hist')
    if start < 0:
        return []
    label = data.find(b'\tOperation', start)
    if label < 0:
        raise NgsFormatError('history table without an Operation column')
    # the operation names follow a uint16 count, which follows that many uint32 epochs
    for p in range(label + 10, min(label + 80, len(data) - 2)):
        count = struct.unpack_from('<H', data, p)[0]
        if not 0 < count < 100 or p - 4 * count < label + 10:
            continue
        stamps = struct.unpack_from(f'<{count}I', data, p - 4 * count)
        if not all(1e9 < s < 2.2e9 for s in stamps):
            continue
        r = Reader(data, p + 2)
        operations = r.pstrs(count)
        if all(op.isprintable() and op for op in operations):
            return [{'time': datetime.fromtimestamp(s, timezone.utc).isoformat().replace('+00:00', 'Z'),
                     'operation': op} for s, op in zip(stamps, operations)]
    raise NgsFormatError('could not locate history entries')


def parse_ngs(data):
    if not data.startswith(MAGIC):
        raise NgsFormatError('missing NGSNextGen header')
    r = Reader(data, len(MAGIC) + 8)
    r.expect('DataMatrix')
    r.expect('Spectrum')
    title = r.pstr()

    _block, _zero, _eight, marker, n, dims, n_again, _pad = r.take('<IIIiIHIH')
    if marker != -1 or dims != 1 or n != n_again or n < 2:
        raise NgsFormatError(f'not a single spectrum (dims={dims}, n={n}/{n_again})')
    if r.pos + 4 * n > len(data):
        raise NgsFormatError(f'file ends inside the {n}-point spectrum')
    intensities = np.frombuffer(data, '<f4', n, r.pos).astype(float)
    r.pos += 4 * n

    if r.take('<H') != 2:
        raise NgsFormatError('unexpected axis label count')
    x_label, y_label = r.pstrs(2)
    if r.take('<H') != 2:
        raise NgsFormatError('unexpected axis unit count')
    x_unit, y_unit = r.pstrs(2)
    r.pos += 16
    x_start, x_end = r.take('<ff')
    if not (math.isfinite(x_start) and math.isfinite(x_end) and x_start != x_end):
        raise NgsFormatError(f'implausible axis range {x_start}..{x_end}')

    param = read_table(data, 'Param')
    acq = read_table(data, 'Acq')
    if param is None or acq is None:
        raise NgsFormatError('missing Param or Acq table')
    param_rows, param_cols = param
    acq_rows, acq_cols = acq
    if 'Value' not in param_cols or 'Value' not in acq_cols:
        raise NgsFormatError('table without a Value column')

    return {
        'title': title,
        'x_label': x_label, 'y_label': y_label,
        'x_unit': x_unit, 'y_unit': y_unit,
        'x_start': float(x_start), 'x_end': float(x_end),
        'intensities': intensities,
        'info': dict(zip(param_rows, param_cols['Value'])),
        'acquisition': {row: {'value': value, 'unit': unit or None}
                        for row, value, unit in zip(acq_rows, acq_cols['Value'],
                                                    acq_cols.get('Unit', [''] * len(acq_rows)))},
        'history': read_history(data),
    }


def as_number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class HoribaNgsIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        if not self.match_name.lower().endswith('.ngs'):
            return False
        with open(self.file_to_upload, 'rb') as f:
            data = f.read()
        try:
            self.ngs = parse_ngs(data)
        except (NgsFormatError, struct.error, UnicodeDecodeError) as err:
            logger.info(f'{self.match_name} is not a readable LabSpec spectrum: {err}')
            return False
        return True

    def parse_measurement(self):
        self.measurement = 'Raman spectroscopy'

    def parse_data_type(self):
        self.data_type = 'Raman Spectrum'

    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        ngs = self.ngs
        acq = {name: field['value'] for name, field in ngs['acquisition'].items()}
        summary = {
            'laser_wavelength_nm': as_number(acq.get('Laser')),
            'exposure_time_s': as_number(acq.get('Exposition')),
            'accumulation': acq.get('Accumulation') or None,
            'grating': acq.get('Grating') or None,
            'objective': acq.get('Objective') or None,
        }
        if ngs['x_unit'] == '1/cm':
            summary['raman_shift_range_cm-1'] = [ngs['x_start'], ngs['x_end']]
        self.scientific_metadata.update({k: v for k, v in summary.items() if v is not None})
        # keyed by file so several spectra can share one dataset, like tiff_metadata for SEM
        self.scientific_metadata['raman_spectra'] = {Path(self.match_name).name: {
            'spectrum': {
                'title': ngs['title'],
                'n_points': len(ngs['intensities']),
                'x_unit': ngs['x_unit'],
                'y_unit': ngs['y_unit'],
                'x_start': ngs['x_start'],
                'x_end': ngs['x_end'],
            },
            'acquisition': ngs['acquisition'],
            'labspec_info': ngs['info'],
            'history': ngs['history'],
        }}

    def parse_file_timestamp(self):
        date = self.ngs['info'].get('Date', '').strip()
        try:
            self.timestamp = datetime.strptime(date, PARAM_DATE_FORMAT).isoformat()
        except ValueError:
            logger.warning(f'unrecognized LabSpec date {date!r}')
            return CrucibleDatasetIngestor.parse_file_timestamp(self)

    def get_thumbnails(self):
        self.thumbnails = []
        ngs = self.ngs
        try:
            y = ngs['intensities']
            x = np.linspace(ngs['x_start'], ngs['x_end'], len(y))
            fig, ax = plt.subplots()
            ax.plot(x, y, color='tab:blue', linewidth=0.8)
            ax.set_xlabel('Raman shift (cm$^{-1}$)' if ngs['x_unit'] == '1/cm' else ngs['x_unit'])
            ax.set_ylabel(ngs['y_unit'] or 'Intensity')
            ax.set_title(ngs['title'])
            buf = BytesIO()
            fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            self.add_thumbnail(Image.open(buf), ngs['title'], size=(500, 500))
        except Exception as err:
            logger.error(f'Failed to generate Raman thumbnail: {err}')
