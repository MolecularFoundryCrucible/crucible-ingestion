from rsciio import digitalsurf
from pathlib import Path
from typing import ClassVar

import logging

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class SurIngestor(CrucibleDatasetIngestor):
    '''Skeleton code for .sur file ingestor'''

    supported_filetypes: ClassVar[list[str]] = ['.sur']
    
    def is_file_supported(self):
        """Check if the file to upload is a supported sur file based on its extension and content."""
        file_extension = Path(self.file_to_upload).suffix.lower()
        return file_extension in self.supported_filetypes

    def get_scientific_metadata(self):
        sur = digitalsurf.file_reader(self.file_to_upload, lazy=False)
        logger.info("# datasets: "+len(sur))
        self.scientific_metadata.update(sur[0].get("original_metadata"))
        logger.info(f'Got metadata from SUR: {self.scientific_metadata=}')
    
    def get_dataset_metadata(self):
        # Use parent class method to set data_format and size
        CrucibleDatasetIngestor.get_dataset_metadata(self)