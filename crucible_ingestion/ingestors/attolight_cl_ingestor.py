from .crucible_ingestor import CrucibleDatasetIngestor
from crucible.models import Dataset
from mfid import mfid

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

        if Path(self.file_to_upload).suffix.lower() not in self.supported_filetypes:
            return False

        try:
            with open(self.file_to_upload, "rb") as file:
                _, root = next(ET.iterparse(file, events=("start",)))
                return root.tag == "AttolightWorkspace"
        except (ET.ParseError, StopIteration):
            return False

    def setup_data(self):
        """Read the .xml file to find all files associated with this dataset"""
        try:
            tree = ET.parse(self.file_to_upload)
            root = tree.getroot()

            self.acquisition_type = root.find('AcquisitionType').text

            files = root.find('Files')
        except Exception as e:
            raise ValueError(f"Failed to parse file: {e}")

        for child in files:
            suffix = Path(child.text).suffix.lower()
            if suffix == '.txt':
                self.child_txt = child.text
            elif suffix == '.sur':
                self.child_sur.append(child.text)
            elif child.text != Path(self.file_to_upload).name: 
                # Does not trigger on current file but does on all other non-.txt, non-.sur files
                logger.info(f"Unexpected file encountered: {child.text}. Skipping...")
        logger.info(f"Sur Files: {self.child_sur}")

        super().setup_data()

    def get_scientific_metadata(self):
        """Natively ingest the .txt file included in the same upload as the .xml to use as parent dataset metadata"""
        if self.child_txt == "":
            raise ValueError("missing .txt file")
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
        """Build measurement name from metadata fields"""
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
        """Trigger ingestions on all children .sur files included in the ingestion folder and associates them with a new dataset linked to the currently generating dataset"""

        from ..data_ingestion import parse # Fixes circular import errors

        for file in self.child_sur:
            child = Dataset(
                unique_id=mfid()[0], # Manually generate mfid for pre-server parsing behavior
                project_id=self.project_id,
                instrument_id=self.instrument_id,
                session_name=self.session_name,
                # timestamp=self.timestamp, # Uncomment for child datasets to share parent timestamps
            )

            # ONLY KEEP WHILE sur_ingestor.py IS ONLY AVAILABLE LOCALLY
            # ELSE SWITCH TO THE FILES/INGESTOR STYLE
            child_packet = parse(
                dataset_to_process=str(Path(self.file_to_upload).with_name(file)), 
                dsid=child.unique_id, 
                ingestion_class="SurIngestor",
            )

            if child_packet == None:
                raise ValueError(f"File {file} not supported by current ingestors")

            child_metadata = child_packet.scientific_metadata

            child_packet.dataset_fields.update({
                "dataset_name": file,
                "owner_orcid": self.owner_orcid,
                "project_id": child.project_id,
                "instrument_id": child.instrument_id,
                "session_name": child.session_name,
            })

            self.children.append({
                "dataset": child_packet.dataset_fields,
                "scientific_metadata": child_packet.scientific_metadata,
                "parent_id": self.unique_id,
                "sample_links": [],
                "thumbnails": child_packet.thumbnails,
            })

    def get_thumbnails(self):
        """Select the hyperspectral thumbnail, or the first thumbnail if unavailable, from within the child datasets previously ingested"""

        for child in self.children:
            try:
                acquisition = child["scientific_metadata"]["Object_0_Channel_0"]["Parsed"]["GENERAL"].get(
                    "acquisition_type", "Unknown"
                )
            except Exception as e:
                logger.error(f"Error accessing child metadata: {e}")
                acquisition = "Unknown"

            thumbnails = child.get("thumbnails", [])

            if acquisition == "HYP" and thumbnails:
                self.thumbnails.append(thumbnails[0])
                logger.info("Hyperspectral thumbnail found. Selecting hyperspectral thumbnail as parent dataset thumbnail...")
                return
        
        if self.children:
            thumbnails = self.children[0].get("thumbnails", [])
            if thumbnails:
                self.thumbnails.append(thumbnails[0])
                logger.info("Hyperspectral thumbnail not found, but other thumbnails exist. Selecting first available thumbnail as parent dataset thumbnail...")
                return
        
        logger.error("Suitable thumbnail not found. Continuing without thumbnails...")
