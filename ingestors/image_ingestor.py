import os
import json

import logging
import numpy as np
from PIL import Image
from PIL.TiffTags import TAGS

from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ImageIngestor(CrucibleDatasetIngestor):
        
    def is_file_supported(self):
        supported_image_formats = ['png', 'jpeg', 'jpg']
        logger.info(f"Supported image formats: {supported_image_formats}")
        res= np.any([self.file_to_upload.lower().endswith(imformat) for imformat in supported_image_formats])
        logger.info(f"File {self.file_to_upload} is supported: {res}")
        return res


    def get_thumbnails(self):
        single_image = Image.open(self.file_to_upload)
        self.add_thumbnail(single_image, os.path.basename(self.file_to_upload))

        
class TifIngestor(ImageIngestor):

    def is_file_supported(self):
        supported_image_formats = ['tif', 'tiff']
        return np.any([self.file_to_upload.lower().endswith(imformat) for imformat in supported_image_formats])

    
    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        with Image.open(self.file_to_upload) as im:
            raw_md = im.tag_v2
            for tag, value in raw_md.items():
                tag_name = TAGS.get(tag, tag)
                self.scientific_metadata[tag_name] = value
