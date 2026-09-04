import logging
import re
import statistics
from collections import defaultdict
from datetime import datetime as dt
from io import BytesIO
from pathlib import Path

from PIL import Image
import matplotlib.pyplot as plt

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)


class GCLogIngestor(CrucibleDatasetIngestor):
    """Tab-delimited gas chromatography results log, one line per injection.

    The log is append-only across runs, so a single file can span days and more
    than one experiment. Which compounds appear varies line to line, because the
    peak table only lists components the software matched in that injection.
    """

    def is_file_supported(self):
        if not self.file_to_upload.lower().endswith('.log'):
            return False
        try:
            with open(self.file_to_upload, encoding='utf-8', errors='replace') as f:
                lines = [line for line in f.read().splitlines() if line.strip()]
        except OSError:
            return False

        # GC files should have min X cols; tab separated
        return len(lines[0].split('\t')) > 3


    def _read_rows(self):
        """Return the injections in the log, skipping lines that do not fit."""
        with open(self.file_to_upload, encoding='utf-8', errors='replace') as f:
            lines = [line for line in f.read().splitlines() if line.strip()]

        rows = [row for row in (self._parse_row(line) for line in lines) if row]
        skipped = len(lines) - len(rows)
        if skipped and rows:
            logger.warning(f'{self.file_to_upload}: skipped {skipped} unparseable '
                           f'lines of {len(lines)}')
        return rows

    def _parse_row(self, line):
        fields = line.split('\t')
        run_name_pattern = re.compile(r'([A-Za-z]+)(\d+)\.CHR$', re.IGNORECASE)

        # check - run_name always first column?
        detector_and_number = run_name_pattern.search(fields[0].strip()) 

        if not detector_and_number:
            return None
        
        detector = detector_and_number.group(1).upper()

        # check - timestamp always col 2-3?
        try:
            timestamp = dt.strptime(f'{fields[1].strip()} {fields[2].strip()}',
                                    '%m/%d/%Y %H:%M:%S')
        except ValueError:
            return None

        return {'detector': detector, 'timestamp': timestamp}


    def get_scientific_metadata(self):
        # One block per detector: the FID and TCD logs of a run are pushed to the
        # same dataset, and unprefixed keys would overwrite each other.
        by_detector = defaultdict(list)
        for row in self._read_rows():
            by_detector[row['detector']].append(row['timestamp'])

        self.scientific_metadata = {'technique': 'gas chromatography'}
        for detector, timestamps in sorted(by_detector.items()):
            self.scientific_metadata[detector] = {
                'log_file': Path(self.file_to_upload).name,
                'injection_count': len(timestamps),
                'first_injection_timestamp': timestamps[0].isoformat(),
                'last_injection_timestamp': timestamps[-1].isoformat(),

                # Present in the ISAAC CO2RR record but absent from this file.
                'injection_volume_uL': None,
                'carrier_gas': None,
                'product_gas_flow_rate_sccm': None,
            }

    def _detectors(self):
        return [key for key in self.scientific_metadata if key != 'technique']

    def parse_measurement(self):
        self.measurement = 'Gas Chromatography'

    def parse_data_type(self):
        self.data_type = 'GC Peak Table'

    def parse_dataset_name(self):
        if self.dataset_name:
            return
        self.dataset_name = f'{Path(self.file_to_upload).stem}'

    def parse_file_timestamp(self):
        if self.timestamp:
            return
        self.timestamp = min(self.scientific_metadata[detector]['first_injection_timestamp']
                             for detector in self._detectors())

    def parse_keywords(self):
        self.keywords += self._detectors()
        super().parse_keywords()

    # def get_thumbnails(self):
    #     self.thumbnails = []
    #     try:
    #         rows = self._read_rows()
    #         start = rows[0]['timestamp']
    #         elapsed_h = [(row['timestamp'] - start).total_seconds() / 3600 for row in rows]

    #         fig, ax = plt.subplots()
    #         for compound in self.scientific_metadata['compounds']:
    #             areas = [next((p['area'] for p in row['peaks'] if p['compound'] == compound), None)
    #                      for row in rows]
    #             ax.plot(elapsed_h, areas, marker='.', linestyle='-', label=compound)
    #         ax.set_xlabel('Time since first injection (h)')
    #         ax.set_ylabel('Peak area')
    #         ax.legend(fontsize='small')

    #         buf = BytesIO()
    #         fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    #         plt.close(fig)
    #         buf.seek(0)
    #         detector = self.scientific_metadata.get('detector') or 'GC'
    #         self.add_thumbnail(Image.open(buf), f'{detector} peak area vs time')
    #     except Exception as err:
    #         logger.error(f'Failed to generate GC thumbnail: {err}')
