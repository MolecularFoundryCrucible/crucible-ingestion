import os
import logging
from io import BytesIO
from pathlib import Path

import numpy as np
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
        self.scientific_metadata = {}
        self._xrd_mode = 'standalone'
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
            self._xrd_mode = 'child'
            self._xrd_sample_idx = int(position[1:]) - 1
        elif self._is_xrd_parent():
            self._xrd_mode = 'parent'

        if self._xrd_mode in ('child', 'standalone'):
            angles, intensities = self._read_column(self._xrd_sample_idx)
            self.scientific_metadata = {
                'angle_deg': angles,
                'intensity_cps': intensities,
            }

    def _is_xrd_parent(self):
        """
        True if this dataset is a from-holders parent: it has child datasets in
        Crucible AND those children's samples are child samples of this dataset's
        samples (dataset hierarchy agrees with sample hierarchy).
        """
        try:
            children = client.datasets.list_children(parent_dataset_id=self.unique_id)
            if not children:
                return False

            my_links = client.datasets.get(self.unique_id, include_links=True)
            my_sample_ids = set()
            if my_links and 'links' in my_links:
                my_sample_ids = {lnk.get('unique_id') or lnk.get('id')
                                 for lnk in my_links['links']
                                 if lnk.get('resource_type') == 'sample'}
            my_sample_ids.discard(None)

            if not my_sample_ids:
                return False

            for child_ds in children:
                child_dsid = child_ds['unique_id']
                child_links = client.datasets.get(child_dsid, include_links=True)
                if not child_links or 'links' not in child_links:
                    continue
                child_sample_ids = {lnk.get('unique_id') or lnk.get('id')
                                    for lnk in child_links['links']
                                    if lnk.get('resource_type') == 'sample'}
                child_sample_ids.discard(None)

                for child_sid in child_sample_ids:
                    child_sample_parents = client.samples.list_parents(child_sid) or []
                    ancestor_ids = {p['unique_id'] for p in child_sample_parents}
                    if ancestor_ids & my_sample_ids:
                        return True
        except Exception as err:
            logger.warning(f"_is_xrd_parent check failed: {err}")

        return False

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
        if self._xrd_mode == 'parent':
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
            plt.clf()
            buf.seek(0)
            label = f"XRD Pattern (S{self._xrd_sample_idx + 1:02d})"
            self.add_thumbnail(Image.open(buf), label)
        except Exception as err:
            logger.error(f"Failed to generate XRD thumbnail: {err}")
