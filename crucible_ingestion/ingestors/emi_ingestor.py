from datetime import datetime as dt
from pathlib import Path
from typing import ClassVar

import ncempy.io as nio

from .crucible_ingestor import CrucibleDatasetIngestor


class EmiIngestor(CrucibleDatasetIngestor):
    """Ingest metadata from EMI files."""
    
    supported_filetypes: ClassVar[list[str]] = [".emi"]
    
    def is_file_supported(self):
        return Path(self.file_to_upload).suffix.lower() in self.supported_filetypes


    def get_scientific_metadata(self):
        """Extract scientific metadata from the EMI file using ncempy."""
        super().get_scientific_metadata()
        emi_md = nio.ser.read_emi(self.file_to_upload)
        self.scientific_metadata.update(emi_md)

    
    def get_dataset_metadata(self):
        """Set EMI-specific structured metadata."""
        acquired_date = self.scientific_metadata.get('AcquireDate')
        if acquired_date:
            tia_date_format = "%a %b %d %H:%M:%S %Y"
            self.timestamp = dt.strptime(acquired_date, tia_date_format).isoformat()

        super().get_dataset_metadata()

    def get_thumbnails(self):
        """EMI files contain metadata; image data is stored in the SER file."""
        return





    

    
    
    





