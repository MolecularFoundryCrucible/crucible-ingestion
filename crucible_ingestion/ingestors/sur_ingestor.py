from rsciio.digitalsurf import file_reader
from pathlib import Path
from typing import ClassVar
import re

import numpy as np
import matplotlib.pyplot as plt
import io
from PIL import Image

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
        sur = file_reader(self.file_to_upload, lazy=False)

        logger.info("# datasets: "+ str(len(sur)))

        sci_met = { "Dataset " + str(i) : sur[i].get("original_metadata") for i in range(len(sur)) }

        self.recursive_dict_iteration(sci_met, self.clean_unicode)

        self.scientific_metadata.update(sci_met)

        logger.info(f'Got metadata from SUR: {self.scientific_metadata=}')
    
    def get_dataset_metadata(self):
        # Use parent class method to set data_format and size
        CrucibleDatasetIngestor.get_dataset_metadata(self)

    def generate_thumbnail(self, target_size=(200, 200), dpi=100):
        fig_size = (target_size[0] / dpi, target_size[1] / dpi)

        sur = file_reader(self.file_to_upload, lazy=False)

        logger.info("# datasets: "+ str(len(sur)))

        files = []

        for i in range(len(sur)):
            logger.info("Dataset " + str(i))
            logger.info("# Axes: " + str(len(sur[i]["axes"])))

            if (len(sur[i]["axes"]) == 3):
                im = np.sum(np.array(sur[i]["data"]), axis=2)
            elif (len(sur[i]["axes"]) == 2):
                im = np.array(sur[i]["data"])
            else:
                raise ValueError("Data array has unsupported number of dimensions for thumbnail generation.")

            fg, ax = plt.subplots(1, 1, figsize=fig_size, dpi=dpi)
            ax.imshow(im, cmap='gray')
            ax.axis('off')
            buf = io.BytesIO()
            fg.savefig(buf, bbox_inches='tight', pad_inches=0.05, dpi=dpi)
            im = Image.open(buf)
            plt.close(fg)
            files.append(im)
        return files

    def get_thumbnails(self):
        try:
            thumbnails = self.generate_thumbnail()
            if (len(thumbnails) == 1):
                self.add_thumbnail(thumbnails[0], "SUR_Thumbnail")
            elif (len(thumbnails) > 1):
                for i in range(len(thumbnails)):
                    self.add_thumbnail(thumbnails[i], "SUR_Thumbnail_" + str(i))
        except Exception as e:
            logger.info(f"failed to extract thumbnail: {e}")

    @staticmethod
    def clean_unicode(seq):
        if isinstance(seq, str):
            return re.sub(r'\\u[0-9a-fA-F]{4}', '', seq)
        elif isinstance(seq, bytes):
            return re.sub(rb'[\x00-\x1f\x7f]', b'', seq)
        else:
            raise TypeError("Sequence must be type 'str' or 'bytes'") 

    # # # For if the metadata contains any alphanumeric Unicode sequences
    # def decode_16_bit_alphanumeric(match):
    #     raw_char = match[0]
    #     decoded_char = raw_char.encode('utf-8').decode('unicode_escape')
    #     return decoded_char if decoded_char.isalnum() else ""

    @staticmethod
    def recursive_dict_iteration(dctry, callback):
        for key, val in dctry.items():
            if isinstance(val, dict):
                SurIngestor.recursive_dict_iteration(dctry[key], callback)
            elif isinstance(val, str) or isinstance(val, bytes):
                dctry[key] = callback(val)