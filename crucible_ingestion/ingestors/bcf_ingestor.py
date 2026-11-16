from io import BytesIO
from typing import ClassVar
from PIL import Image
import numpy as np
import logging
import hyperspy.api as hs
import matplotlib.pyplot as plt
from .crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class BcfIngestor(CrucibleDatasetIngestor):
    '''
    subclass for bcf files
    '''
    supported_filetypes: ClassVar[list[str]] = ['bcf']

    def is_file_supported(self):
        return np.any([self.match_name.endswith(ftype) for ftype in self.supported_filetypes])


    def get_thumbnails(self):
        data = hs.load(self.file_to_upload)
        data[0].plot(cmap = "turbo")

        buffer = BytesIO()
        plt.savefig(buffer, format="png")
        plt.close("all")
        buffer.seek(0)

        try:
            single_image = Image.open(buffer)
            self.add_thumbnail(single_image, "EDS Thumbnail", size = (200,200))
        except Exception:
            logger.warning("failed to extract thumbnail")
