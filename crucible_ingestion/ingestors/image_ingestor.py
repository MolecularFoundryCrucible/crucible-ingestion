import os
import json

import logging
import numpy as np
from PIL import Image
from PIL.TiffTags import TAGS

from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ImageIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        supported_image_formats = ['png', 'jpeg', 'jpg']
        logger.info(f"Supported image formats: {supported_image_formats}")
        res= np.any([self.file_to_upload.lower().endswith(imformat) for imformat in supported_image_formats])
        logger.info(f"File {self.file_to_upload} is supported: {res}")
        return res


    def parse_measurement(self):
        self.measurement = "Image"


    def get_thumbnails(self):
        single_image = Image.open(self.file_to_upload)
        self.add_thumbnail(single_image, os.path.basename(self.file_to_upload))


class TifIngestor(ImageIngestor):

    def is_file_supported(self):
        supported_image_formats = ['tif', 'tiff']
        return np.any([self.file_to_upload.lower().endswith(imformat) for imformat in supported_image_formats])


    def parse_measurement(self):
        self.measurement = "TIFF Image"


    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        skip_tags = ['StripOffsets', 'StripByteCounts', '34682']
        with Image.open(self.file_to_upload) as im:
            raw_md = im.tag_v2
            for tag, value in raw_md.items():
                tag_name = TAGS.get(tag, str(tag))
                if tag_name in skip_tags:
                    print(tag_name)
                    continue
                
                self.scientific_metadata[tag_name] = value
