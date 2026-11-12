from .crucible_ingestor import CrucibleDatasetIngestor

from pathlib import Path
from typing import ClassVar

import logging

import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class AttolightCLIngestor(CrucibleDatasetIngestor):
    '''Ingestor for parsing a full fileset from a SPECTRE cathodoluminescence run'''
    '''Ingests .xml and .txt file contents, triggers separate ingestions for individual .sur files'''

    supported_filetypes: ClassVar[list[str]] = ['.xml']
    child_sur: list[str] = []
    child_txt: str = ""
    acquisition_type: str = ""

    def is_file_supported(self):
        """Check if the file to upload is a supported xml file based on its extension and content."""
        file_extension = Path(self.file_to_upload).suffix.lower()
        return file_extension in self.supported_filetypes

    def setup_data(self):
        try:
            tree = ET.parse(self.file_to_upload)
            root = tree.getroot()

            self.acquisition_type = root.find('AcquisitionType').text

            files = root.find('Files')
        except Exception as e:
            logger.error(f"Failed to parse file: {e}")

        for child in files:
            if child.text.split('.')[1] == 'txt':
                self.child_txt = child.text
            elif child.text.split('.')[1] == 'sur':
                self.child_sur.append(child.text)
            elif child.text != Path(self.file_to_upload).name: 
                # Does not trigger on current file but does on all other non-.txt, non-.sur files
                logger.info(f"Unexpected File encountered: {child.text}. Skipping...")

        logger.info(f"Sur Files: {self.child_sur}")
        super().setup_data()

    def get_scientific_metadata(self):
        # This function natively ingests a .txt file included in the ingestion folder
        if self.child_txt == "":
            raise Exception("missing .txt file")
        txt_file = Path(self.file_to_upload).with_name(self.child_txt)

        metadata = {}
        curr_category = "Uncategorized"
        metadata[curr_category] = {}
        with open(txt_file, 'r', encoding='cp1252') as file:
            for line in file:
                if line[0] == '_':
                    curr_category = line[1:].strip()
                    metadata[curr_category] = {}
                elif ":" in line:
                    key, value = line.split(":", 1)
                    metadata[curr_category][key.strip()] = value.strip()
                else:
                    logger.info(f"Alternate Line Found: {line}. Skipping...")
        if not metadata["Uncategorized"]:
            metadata.pop("Uncategorized", None)
        self.scientific_metadata.update(metadata)

        logger.info(f'Successfully parsed scientific metadata from .txt file.')


    # def get_dataset_metadata(self):
    #   super().get_dataset_metadata()

    def parse_measurement(self):
        acquisition = self.acquisition_type
        logger.info(f"Acquisiton: {acquisition}")

        if acquisition == "HAADF" or acquisition == "ROI":
            self.measurement = "Non-Pixelated"
        elif acquisition == "HYP" or acquisition == "CL":
            self.measurement = "Cathodoluminescence"
        else:
            self.measurement = "Unknown"

    def parse_children(self):
        # This function triggers ingestions on all children .sur files included in the ingestion folder 
        # and associates them with a new dataset linked to the currently generating dataset
        pass