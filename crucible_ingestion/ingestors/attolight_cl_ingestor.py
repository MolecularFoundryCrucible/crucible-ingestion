from .crucible_ingestor import CrucibleDatasetIngestor
from crucible import CrucibleClient
from crucible.models import Dataset

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
    measurement_info: dict = {}

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
            raise Exception(f"Failed to parse file: {e}")

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
                    key, value = [text.strip() for text in line.split(":", 1)]
                    metadata[curr_category][key] = value

                    if key == 'operating_mode' or key == 'projection_mode' or key == 'acquisition_type':
                        self.measurement_info[key] = value
                else:
                    logger.info(f"Alternate Line Found: {line}. Skipping...")
        if not metadata["Uncategorized"]:
            metadata.pop("Uncategorized", None)
        self.scientific_metadata.update(metadata)

        logger.info(f'Successfully parsed scientific metadata from .txt file.')


    # def get_dataset_metadata(self):
    #   super().get_dataset_metadata()

    def parse_measurement(self):
        illumination = self.measurement_info.get("operating_mode", "")
        projection = self.measurement_info.get("projection_mode", "")
        acquisition = self.measurement_info.get("acquisition_type", "")

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

    def parse_children(self):
        # This function triggers ingestions on all children .sur files included in the ingestion folder 
        # and associates them with a new dataset linked to the currently generating dataset

        from ..data_ingestion import data_ingestion # Fixes circular import errors

        client = CrucibleClient()

        i = 1 # Track unknown acquisition types, if necessary
        for file in self.child_sur:
            child_ds = client.datasets.create(
                dataset=Dataset(
                    project_id=self.project_id,
                    instrument_id=self.instrument_id,
                    session_name=self.session_name,
                    # timestamp=self.timestamp, # Uncomment for child datasets to share parent timestamps
                ),
            )

            child_dsid = child_ds["dataset_mfid"]

            # ONLY KEEP WHILE sur_ingestor.py IS ONLY AVAILABLE LOCALLY
            # ELSE SWITCH TO THE FILES/INGESTOR STYLE
            data_ingestion(
                dataset_to_process=str(Path(self.file_to_upload).with_name(file)), 
                dsid=child_dsid, 
                ingestion_class="SurIngestor", 
                include_file=True
            )

            child_metadata = client.datasets.get_scientific_metadata(child_dsid)['scientific_metadata']

            try:
                acquisition = child_metadata["Object_0_Channel_0"]["Parsed"]["GENERAL"].get(
                    "acquisition_type", "Unknown"
                )
            except Exception as e:
                logger.error(f"Error accessing child metadata: {e}")
                acquisition = "Unknown"

            if acquisition == "Unknown":
                acquisition += str(i)
                i += 1

            client.datasets.update(
                child_dsid,
                dataset_name=f"{self.dataset_name}_{acquisition}"
            )

            client.datasets.link_parent_child(
                parent_dataset_id=self.unique_id,
                child_dataset_id=child_ds["dataset_mfid"],
            )
        
