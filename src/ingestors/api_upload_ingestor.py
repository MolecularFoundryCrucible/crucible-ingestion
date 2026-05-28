
import os
import logging
from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ApiUploadIngestor(CrucibleDatasetIngestor):
        
    def is_file_supported(self):
        return True

    def get_dataset_metadata(self):
        logger.info(f'{self.unique_id} getting dataset metadata..')
        self.size = os.path.getsize(self.file_to_upload)
        logger.info(f'{self.unique_id} size = {self.size}')

            
