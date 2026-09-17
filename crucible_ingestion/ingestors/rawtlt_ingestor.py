import logging
from pathlib import Path
from typing import ClassVar

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class RawtltIngestor(CrucibleDatasetIngestor):
    """Ingest tomography tilt angles stored one per line in a RAWTLT file."""

    supported_filetypes: ClassVar[list[str]] = [".rawtlt"]

    def is_file_supported(self) -> bool:
        """Return whether the input has a supported file extension."""
        file_to_upload = getattr(self, "file_to_upload")
        return Path(file_to_upload).suffix.lower() in self.supported_filetypes

    def get_scientific_metadata(self):
        """Extract tomography tilt angles, ignoring blank lines."""
        super().get_scientific_metadata()
        tilts = []
        file_to_upload = getattr(self, "file_to_upload")
        with Path(file_to_upload).open(encoding="utf-8") as rawtlt_file:
            for line_number, line in enumerate(rawtlt_file, start=1):
                value = line.strip()
                if not value:
                    continue
                try:
                    tilts.append(float(value))
                except ValueError as error:
                    raise ValueError(
                        f"Invalid tilt angle on line {line_number}: {value!r}"
                    ) from error

        self.scientific_metadata["tilt angles"] = tilts

    def get_dataset_metadata(self):
        '''
        Set the structured metadata according to Crucible's schema.
        Suggested ones are: dataset_name, instrument_name, measurement,
        session_name, timestamp, data_format
        '''
        super().get_dataset_metadata()
        self.dataset_name = Path(getattr(self, "file_to_upload")).name
