import os
import re
import io

from pathlib import Path
from PIL import Image
import numpy as np
import ncempy.io as nio
import matplotlib.pyplot as plt
import logging

from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class BerkeleyEmdIngestor(CrucibleDatasetIngestor):
    '''subclass for ingesting Berkeley EMD files'''

    def is_file_supported(self):
        if not self.file_to_upload.endswith('.emd'):
            return False
        
        with nio.emd.fileEMD(self.file_to_upload, readonly=True) as emd1:
            if len(emd1.list_emds) > 0:
                return True
            else:
                return False

    
    def get_scientific_metadata(self):
        with nio.emd.fileEMD(self.file_to_upload, readonly=True) as emd1:
            self.scientific_metadata = emd1.getMetadata(0)
        logger.info(f'Got metadata from Berkeley EMD: {self.scientific_metadata=}')
         
    
        # emd_handle = nio.emd.fileEMD(self.file_to_upload, readonly=True)
        # for device_index, device_name in enumerate(_dset_names(emd_handle)):
        #     logger.info(f'{device_index=}, {device_name=}')
        #     frame_stream_name = f'primary_{device_name}'
        #     stream_metadata = _metadata_from_dset(self.file_to_upload, dset_num=device_index)
        #     self.scientific_metadata[frame_stream_name] = stream_metadata
        # print(f'{self.scientific_metadata=}')
        # return


    def get_dataset_metadata(self):
         # Use parent class method to set data_format, size, and source_folder
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.dataset_name = Path(self.file_to_upload).stem # file name without extension
        # TODO: parse this
        self.measurement = ''

    def generate_thumbnail(self):
        """Generate a thumbnail from an EMD image as a PNG.

        Returns
        -------
        : PIL.Image
            Thumbnail image as a PIL Image object.
        """
        target_size = (200, 200) # pixels
        dpi = 100
        fig_size = (target_size[0] / dpi, target_size[1] / dpi) # inches
       
        try:            
            with nio.emd.fileEMD(self.file_to_upload, readonly=True) as emd1:
                image_array, _ = emd1.get_emdgroup(0)
            fg, ax = plt.subplots(1, 1, figsize=fig_size, dpi=dpi)
            ax.imshow(image_array, cmap = 'gray')
            ax.axis('off')

            # Convert to PIL Image and store in self.thumbnails
            buf = io.BytesIO()
            fg.savefig(buf, bbox_inches='tight', pad_inches=0.05, dpi=100)
            im = Image.open(buf)
            return im
        except Exception as e:
            print(f"Failed to generate thumbnail: {e}")

    def get_thumbnails(self):
        try:
            thumbnail = self.generate_thumbnail()
            if thumbnail:
                self.add_thumbnail(thumbnail, "EMD_Thumbnail")
        except Exception as e:
            print(f"Failed to extract thumbnail: {e}")