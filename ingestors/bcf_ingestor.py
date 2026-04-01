import os
from PIL import Image
import numpy as np
import logging
import hyperspy.api as hs
import matplotlib.pyplot as plt
from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class BcfIngestor(CrucibleDatasetIngestor):
    '''
    subclass for bcf files
    '''
    supported_filetypes = ['bcf']
    

    def is_file_supported(self):
        return np.any([self.file_to_upload.endswith(ftype) for ftype in self.supported_filetypes])

        
    def get_dataset_metadata(self):
        '''
        previously parsed data that should be passed through API now is: 
        - session name
        - tags
        - instrument name
        '''
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.dataset_name = os.path.basename(self.file_to_upload)
        self.keywords += [self.session_name] 


    def get_thumbnails(self):
        out_image_file_name = f"./generated_files/{os.path.basename(self.file_to_upload)}.png"
        # Load the BCF file
        data = hs.load(self.file_to_upload)
        
        # Access the data
        data[0].plot(cmap = "turbo")
        plt.savefig(out_image_file_name)
        
        try:
            single_image = Image.open(out_image_file_name)
            self.add_thumbnail(single_image, "EDS Thumbnail", size = (200,200))
        except:
            logger.warning("failed to extract thumbnail")
