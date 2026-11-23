import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO

import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
# not public API, but the only parser for the TRTSpectrum element that .spx files and
# .rtx projects share; rsciio's public reader handles .spx alone
from rsciio.bruker._api import EDXSpectrum

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

SPECTRUM_PATHS = {
    'TRTSpectrum': "./ClassInstance[@Type='TRTSpectrum']",
    'TRTProject': "./ClassInstance/ChildClassInstances/ClassInstance[@Type='TRTSpectrum']",
}
MAX_THUMBNAILS = 12


def ms_to_s(value):
    return value / 1000 if isinstance(value, (int, float)) else None


def unique_name(name, taken):
    candidate, n = name, 2
    while candidate in taken:
        candidate, n = f'{name} ({n})', n + 1
    return candidate


def read_spectra(path):
    """(name, element, EDXSpectrum) for every spectrum rsciio can parse, in file order."""
    root = ET.parse(path).getroot()
    spectra = []
    for i, node in enumerate(root.findall(SPECTRUM_PATHS.get(root.tag, './nonexistent'))):
        name = node.get('Name') or f'Spectrum {i + 1}'
        try:
            spectra.append((name, node, EDXSpectrum(node)))
        except Exception as err:
            logger.warning(f'skipping unparseable Bruker spectrum {name!r}: {err}')
    return spectra


def spectrum_metadata(node, spectrum):
    hardware = spectrum.hardware_metadata
    elements = node.find("./ClassInstance[@Type='TRTPSEElementList']")
    return {
        'generation_type': node.findtext('GenerationType'),
        'primary_energy_keV': spectrum.hv,
        'acquisition_date': getattr(spectrum, 'date', None),
        'acquisition_time': getattr(spectrum, 'time', None),
        'live_time_s': ms_to_s(hardware.get('LifeTime')),
        'real_time_s': ms_to_s(hardware.get('RealTime')),
        'detector': spectrum.detector_type,
        'elevation_angle_deg': spectrum.elev_angle,
        'azimuth_angle_deg': spectrum.esma_metadata.get('AzimutAngle'),
        'n_channels': len(spectrum.data),
        'energy_offset_keV': spectrum.offset,
        'energy_scale_keV': spectrum.scale,
        'total_counts': int(spectrum.data.sum()),
        'elements': [] if elements is None else
                    [e.get('Name') for e in elements.iter('ClassInstance')
                     if e.get('Type') == 'TRTPSEElement'],
    }


def acquisition_time(spectrum):
    date, time = getattr(spectrum, 'date', None), getattr(spectrum, 'time', None)
    if not date or not time:
        return None
    try:
        return datetime.fromisoformat(f'{date}T{time}')
    except ValueError:
        return None


class BrukerEdxIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        if self.match_name.lower().rsplit('.', 1)[-1] not in ('spx', 'rtx'):
            return False
        try:
            self.spectra = read_spectra(self.file_to_upload)
        except ET.ParseError as err:
            logger.info(f'{self.match_name} is not Bruker XML: {err}')
            return False
        return len(self.spectra) > 0

    def parse_measurement(self):
        self.measurement = 'Energy-dispersive X-ray spectroscopy'

    def parse_data_type(self):
        self.data_type = 'EDX Spectrum'

    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        per_spectrum = {}
        for name, node, spectrum in self.spectra:
            per_spectrum[unique_name(name, per_spectrum)] = spectrum_metadata(node, spectrum)
        self.scientific_metadata['edx_spectra'] = per_spectrum

        energy = next((s.hv for _, _, s in self.spectra if s.hv is not None), None)
        if energy is not None:
            self.scientific_metadata['primary_energy_keV'] = str(float(energy))

    def parse_file_timestamp(self):
        times = [t for t in (acquisition_time(s) for _, _, s in self.spectra) if t]
        if not times:
            return CrucibleDatasetIngestor.parse_file_timestamp(self)
        self.timestamp = min(times).isoformat()

    def get_thumbnails(self):
        self.thumbnails = []
        for name, _, spectrum in self.spectra[:MAX_THUMBNAILS]:
            try:
                energy = spectrum.offset + spectrum.scale * np.arange(len(spectrum.data))
                fig, ax = plt.subplots()
                ax.plot(energy, spectrum.data, color='tab:blue', linewidth=0.8)
                if spectrum.hv:
                    ax.set_xlim(0, spectrum.hv)
                ax.set_xlabel('Energy (keV)')
                ax.set_ylabel('Counts')
                ax.set_title(name)
                buf = BytesIO()
                fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                buf.seek(0)
                self.add_thumbnail(Image.open(buf), name, size=(500, 500))
            except Exception as err:
                logger.error(f'Failed to generate EDX thumbnail for {name!r}: {err}')
