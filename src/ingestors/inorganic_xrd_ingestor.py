import logging
from io import BytesIO
from pathlib import Path

from PIL import Image
import matplotlib.pyplot as plt

from .crucible_ingestor import CrucibleDatasetIngestor, client

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
        Detect child vs standalone via 'position' in Crucible scimd.

        'position' is written at dataset create time (before ingestion runs) so
        it's timing-safe. Hierarchy checks (list_children, include_links) are NOT
        used: parent-child dataset links are created by the uploader *after*
        create_dataset() returns, meaning they don't exist yet when this ingestor
        runs. To distinguish a parent dataset from a standalone, the uploader should
        write a create-time marker (e.g. upload_mode='parent') to the dataset's
        scimd — not yet implemented on the uploader side.
        """
        self.scientific_metadata = {}
        self._xrd_sample_idx = 0

        ds = client.datasets.get(self.unique_id, include_metadata=True)
        if ds:
            raw_scimd = ds.get('scientific_metadata', {})
            if isinstance(raw_scimd, dict) and 'scientific_metadata' in raw_scimd:
                actual_scimd = raw_scimd['scientific_metadata']
            else:
                actual_scimd = raw_scimd or {}
            position = actual_scimd.get('position')
        else:
            position = None

        if position:
            self._xrd_sample_idx = int(position[1:]) - 1

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
            plt.clf()
            buf.seek(0)
            label = f"XRD Pattern (S{self._xrd_sample_idx + 1:02d})"
            self.add_thumbnail(Image.open(buf), label)
        except Exception as err:
            logger.error(f"Failed to generate XRD thumbnail: {err}")
