import logging
from io import BytesIO
from pathlib import Path

from PIL import Image
import matplotlib.pyplot as plt

from .crucible_ingestor import CrucibleDatasetIngestor
from ..client import get_client
logger = logging.getLogger(__name__)


class InorganicXRDIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        if not self.file_to_upload.endswith('.txt'):
            return False
        try:
            with open(self.file_to_upload, 'r') as f:
                lines = f.readlines()
            return len(lines) >= 2 and 'Intensity, cps' in lines[1]
        except Exception:
            return False

    def get_scientific_metadata(self):
        """
        Detect mode via create-time fields in Crucible scimd (both written by the
        uploader before create_dataset() returns, so timing-safe):
          'position'    → child dataset; extract that column
          'upload_mode' == 'parent' → parent dataset; skip data extraction
          neither       → standalone single-sample dataset; extract column 0
        """
        self.scientific_metadata = {}
        self._xrd_sample_idx = 0
        self._xrd_is_parent = False

        ds = get_client().datasets.get(self.unique_id, include_metadata=True)
        if ds:
            raw_scimd = ds.get('scientific_metadata', {})
            if isinstance(raw_scimd, dict) and 'scientific_metadata' in raw_scimd:
                actual_scimd = raw_scimd['scientific_metadata']
            else:
                actual_scimd = raw_scimd or {}
            position = actual_scimd.get('position')
            upload_mode = actual_scimd.get('upload_mode')
        else:
            position = None
            upload_mode = None

        if position:
            self._xrd_sample_idx = int(position[1:]) - 1
        elif upload_mode == 'parent':
            self._xrd_is_parent = True
            return

        angles, intensities = self._read_column(self._xrd_sample_idx)
        self.scientific_metadata = {
            'angle_deg': angles,
            'intensity_cps': intensities,
        }

    def _read_column(self, sample_idx):
        """
        Parse the XRD txt file and return (angles, intensities) for the
        given 0-based sample index. Each sample occupies a pair of columns:
        column 0 carries the shared angle axis; intensity for sample i is at
        column 2*i+1.
        """
        with open(self.file_to_upload, 'r') as f:
            lines = f.readlines()

        data_rows = [line.strip().split('\t') for line in lines[2:] if line.strip()]
        if not data_rows:
            return [], []

        intensity_col = 2 * sample_idx + 1
        angles, intensities = [], []
        for row in data_rows:
            if len(row) > intensity_col:
                try:
                    angles.append(float(row[0]))
                    intensities.append(float(row[intensity_col]))
                except ValueError:
                    pass

        return angles, intensities

    def parse_instrument(self):
        self.instrument_name = 'Inorganic XRD'
        super().parse_instrument()

    def parse_measurement(self):
        self.measurement = 'XRD'

    def parse_data_type(self):
        self.data_type = 'XRD Pattern'

    def parse_dataset_name(self):
        if self.dataset_name:
            return
        self.dataset_name = f'XRD — {Path(self.file_to_upload).stem}'

    def parse_samples(self):
        pass

    def parse_children(self):
        pass

    def get_thumbnails(self):
        self.thumbnails = []
        if self._xrd_is_parent:
            return
        try:
            angles, intensities = self._read_column(self._xrd_sample_idx)
            if not angles:
                return
            fig, ax = plt.subplots()
            ax.plot(angles, intensities)
            ax.set_xlabel("2θ (°)")
            ax.set_ylabel("Intensity (cps)")
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=150)
            plt.close(fig)
            buf.seek(0)
            label = f"XRD Pattern (S{self._xrd_sample_idx + 1:02d})"
            self.add_thumbnail(Image.open(buf), label)
        except Exception as err:
            logger.error(f"Failed to generate XRD thumbnail: {err}")
