import os
from typing import ClassVar
import requests
from datetime import datetime
from PIL import Image
import numpy as np
import xmltodict
from aicspylibczi import CziFile
import matplotlib.pyplot as plt
import logging

from utils import get_secret
from constants import crucible_api_url
from google_calendar import find_calendar_event, parse_calendar_event_for_ownership
from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def search_and_replace(searchkey, d):
    if isinstance(d, dict):
        for key in d:
            if key == searchkey:
                d[key] = None
            else:
                search_and_replace(searchkey, d[key])
    elif isinstance(d, list):
        for item in d:
            search_and_replace(searchkey, item)
    else:
        pass

class CziIngestor(CrucibleDatasetIngestor):
    
    supported_filetypes: ClassVar[list[str]] = ['czi']
    
    def is_file_supported(self):
        return np.any([self.file_to_upload.endswith(ftype) for ftype in self.supported_filetypes])

   
    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        czi = CziFile(self.file_to_upload)
        metadata = xmltodict.parse(czi.reader.read_meta())['ImageDocument']['Metadata']
        search_and_replace("HotPixelSettings", metadata)
        self.scientific_metadata.update(metadata)

                
    def get_dataset_metadata(self):
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        try:
            self.timestamp = datetime.strptime(self.scientific_metadata['Information']['Document']['CreationDate'], "%Y-%m-%dT%H:%M:%S").isoformat()
        except Exception:
            pass

        self.dataset_name = os.path.basename(self.file_to_upload)
        try:
            self.session_name = self.scientific_metadata['Information']['Image']['Session']['@SessionName']
            self.keywords += [self.session_name]
        except Exception:
            pass

        try:
            ac_settings = self.scientific_metadata['Experiment']['ExperimentBlocks']['AcquisitionBlock']
            tracksetup = ac_settings['MultiTrackSetup']['TrackSetup']

            detector = f"detector:{tracksetup['Detectors']['Detector']['DetectorIdentifier']}"
            detector_mode = f"detector_mode:{tracksetup['Detectors']['Detector']['DetectorMode']}"
            device_mode = f"device_mode:{tracksetup['DeviceMode']}"
            laser = f"laser:{ac_settings['Lasers']['Laser']['LaserName']}"
            objective = f"objective_model:{ac_settings['AcquisitionModeSetup']['Objective']}"
            ex_wl = f"excitation_wavelength:{tracksetup['Attenuators']['Attenuator']['Wavelength']}"

            self.keywords += [detector, detector_mode, device_mode, laser, objective, ex_wl]

        except Exception:
            pass

    def parse_orcid(self):
        calendars = {
                        "Zeiss Elyra 7": "c_8n0083mccvlarkchrr2dm4lfb4@group.calendar.google.com",
                        "Zeiss LSM710 Confocal Microscope": "lbl.gov_58klse6vdp80p7k00trm5fr9cg@group.calendar.google.com"
                    }
        
        if self.owner_orcid:
            return
        
        if self.instrument_name in calendars.keys():
            cal_id = calendars[self.instrument_name]
            cal_event = find_calendar_event(self.timestamp, cal_id, service_account_file = f"{os.getenv('HOME')}/.config/mf-crucible-9009d3780383.json")
        else:
            cal_event = None
        if cal_event:
            self.email, self.project_id = parse_calendar_event_for_ownership(cal_event)

            apikey = get_secret("ADMIN_APIKEY", "crucible_admin_apikey/versions/4")
            by_email = requests.get(f"{crucible_api_url}/users?email={self.email}", headers = {"Authorization":f"Bearer {apikey}"}).json()
            by_lbl_email = requests.get(f"{crucible_api_url}/users?lbl_email={self.email}", headers = {"Authorization":f"Bearer {apikey}"}).json()
            user_info =  by_email + by_lbl_email 
            self.owner_orcid = user_info[-1]['orcid']
        
        else:
            return


    def parse_project_id(self):
                
        calendars = {
                        "Zeiss Elyra 7": "c_8n0083mccvlarkchrr2dm4lfb4@group.calendar.google.com",
                        "Zeiss LSM710 Confocal Microscope": "lbl.gov_58klse6vdp80p7k00trm5fr9cg@group.calendar.google.com"
                    }
        
        found_calendar = self.instrument_name in calendars.keys()
        
        if not self.project_id and found_calendar:
            cal_id = calendars[self.instrument_name]
            cal_event = find_calendar_event(self.timestamp, cal_id, service_account_file = f"{os.getenv('HOME')}/.config/mf-crucible-9009d3780383.json")
            if cal_event:
                self.email, self.project_id = parse_calendar_event_for_ownership(cal_event)
        
        if not self.project_id:
            return
        
        if "Internal Research" in self.project_id and self.email is not None:
            self.project_id = f"MFUSER_{self.email.split('@')[0]}"


    def get_thumbnails(self):
        if not os.path.exists("./generated_files"):
            os.makedirs("./generated_files")
        out_image_file_name = f"./generated_files/{os.path.basename(self.file_to_upload)}.png"
        czi = CziFile(self.file_to_upload)
        logger.info(f"{czi.get_dims_shape()=}")
        full_img, shp = czi.read_image(S = 0, Z=0)
        if len(shp) == 8:
            img_slice = full_img[0, 0, 0, 0, 0, 0, :, :]
        elif len(shp) == 7:
            img_slice = full_img[0, 0, 0, 0, 0, :, :]
        elif len(shp) == 6:
            img_slice = full_img[0, 0, 0, 0, 0, :, :]
        else:
            logger.warning(f"Unexpected shape: {shp=}")
            
        # generate caption
        capadds = [f"{d[0]}=0" for d in shp if d[0] not in ['X', 'Y']]
        caption = f"CZI image ({', '.join(capadds)})"
    
        try:
            plt.figure(figsize=(10, 10))
            plt.imshow(img_slice)
            plt.axis('off')
            plt.savefig(out_image_file_name)
            single_image = Image.open(out_image_file_name)
            self.add_thumbnail(single_image, caption)
        except Exception:
            logger.warning("failed to extract thumbnail")

