from .crucible_ingestor import CrucibleDatasetIngestor

import logging

from rsciio.digitalsurf import file_reader
from pathlib import Path
from typing import ClassVar

import re

import numpy as np
import matplotlib.pyplot as plt
import io
from PIL import Image

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class SurIngestor(CrucibleDatasetIngestor):
    '''Full ingestor for reading .sur files from SPECTRE and other cathodoluminesence-capable microscopes'''

    dataset: list = []
    general_info: dict = {}
    instrument_info: dict = {}
    supported_filetypes: ClassVar[list[str]] = ['.sur']

    def is_file_supported(self):
        """Check if the file to upload is a supported sur file based on its extension and content."""
        file_extension = Path(self.file_to_upload).suffix.lower()
        return file_extension in self.supported_filetypes

    def setup_data(self):
        """Verify dataset length, logging if there are irregularities, and prepare dataset for ingestion"""
        try:
            dataset = file_reader(self.file_to_upload, lazy=False)
        except Exception as e:
            raise ValueError(f"Failed to parse file {e}")

        logger.info("# datasets: "+ str(len(dataset)))
        logger.info("NOTE: This number should be 1.")

        if len(dataset) < 1:
            raise ValueError("No dataset found.")
        if len(dataset) > 1:
            logger.info(
                "This ingestor is built for single-dataset files, but there are " + str(len(dataset)) + " datasets found in the file.\n" +
                "This ingestor will parse only the first dataset found. Please contact Edan Wong (ehwong@lbl.gov) with any questions."
            )

        self.dataset = dataset[0]

        super().setup_data()

    def get_scientific_metadata(self):
        """Extract scientific metadata values from .sur file via a recursive dictionary parse"""

        dataset = self.dataset

        scientific_metadata = dataset.get("original_metadata")

        self.recursive_dict_parse(scientific_metadata, self.clean_unicode)

        self.scientific_metadata.update(scientific_metadata)

        try:
            self.instrument_info = scientific_metadata["Object_0_Channel_0"]["Parsed"]["MICROSCOPE"]
            self.general_info = scientific_metadata["Object_0_Channel_0"]["Parsed"]["GENERAL"]
        except KeyError as e:
            raise KeyError("Scientific metadata not found or in unsupported format.")

        logger.info(f'Successfully parsed scientific metadata from .sur file.')

    # def get_dataset_metadata(self):
    #   super().get_dataset_metadata()

    def parse_measurement(self):
        """Build measurement name from metadata fields"""
        illumination = self.instrument_info.get("operating_mode", "")
        projection = self.instrument_info.get("projection_mode", "")
        acquisition = self.general_info.get("acquisition_type", "")

        if projection != "":
            projection = projection[0] + projection[1:].lower()

        if acquisition == "HAADF" or acquisition == "ROI":
            detector = "Non-Pixelated"
        elif acquisition == "HYP" or acquisition == "CL":
            detector = "Cathodoluminescence"
        else:
            detector = ""

        measurements = [illumination, projection, detector]
        self.measurement = " ".join(filter(None, measurements))

    def get_thumbnails(self):
        """Add the provided thumbnail to the dataset's thumbnails list"""
        try:
            thumbnail = self.generate_thumbnail()
            if thumbnail is not None:
                self.add_thumbnail(thumbnail, "SUR_Thumbnail")
        except Exception as e:
            logger.error(f"failed to extract thumbnail: {e}")

    def generate_thumbnail(self, target_size=(200, 200), dpi=100):
        """Generate the thumbnail from the dataset's provided data via Python Imaging Library (PIL) and pyplot"""
        fig_size = (target_size[0] / dpi, target_size[1] / dpi)

        im = self.handle_thumbnail_dimensions()

        if im is not None:
            fg, ax = plt.subplots(1, 1, figsize=fig_size, dpi=dpi)
            ax.imshow(im, cmap='gray')
            ax.axis('off')
            buf = io.BytesIO()
            fg.savefig(buf, bbox_inches='tight', pad_inches=0.05, dpi=dpi)
            im = Image.open(buf)
            plt.close(fg)
        return im
    
    def handle_thumbnail_dimensions(self):
        """Handle thumbnail generation based on data axis dimensionality"""
        dataset = self.dataset
        acquisition = self.general_info.get("acquisition_type", "")

        if (acquisition == "HYP"):
            assert len(dataset["axes"]) == 3, "Thumbnail must be a 3-dimensional image"
            im = np.sum(np.array(dataset["data"]), axis=2) # Sum data across third dimension to generate 2D image
        elif (acquisition == "HAADF" or acquisition == "ROI"):
            assert len(dataset["axes"]) == 2, "Thumbnail must be a 2-dimensional image"
            im = np.array(dataset["data"])
        else:
            logger.info("Image has unsupported number of dimensions for thumbnail generation. No thumbnail generated.")
            im = None
        
        return im

    @staticmethod
    def clean_unicode(seq):
        """Helper method for removing unicode escape sequences from a string or bytes file"""
        if isinstance(seq, str):
            return re.sub(r'\\u[0-9a-fA-F]{4}', '', seq)
        elif isinstance(seq, bytes):
            return re.sub(rb'[\x00-\x1f\x7f]', b'', seq)
        else:
            raise TypeError("Sequence must be type 'str' or 'bytes'") 

    # # # For if the metadata contains any alphanumeric Unicode sequences
    # @staticmethod
    # def decode_16_bit_alphanumeric(match):
    #     raw_char = match[0]
    #     decoded_char = raw_char.encode('utf-8').decode('unicode_escape')
    #     return decoded_char if decoded_char.isalnum() else ""

    @staticmethod
    def recursive_dict_parse(dctry, callback):
        """Helper method for parsing a dictionary recursively while mutating the string via a callback function"""
        """In this ingestor, the callback function is just the clean_unicode() function from above"""
        for key, val in dctry.items():
            if isinstance(val, dict):
                SurIngestor.recursive_dict_parse(val, callback)
            elif isinstance(val, str) or isinstance(val, bytes):
                dctry[key] = callback(val)