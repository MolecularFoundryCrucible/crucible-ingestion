import os
from typing import ClassVar
from datetime import datetime
from PIL import Image
import numpy as np
import xmltodict
from aicspylibczi import CziFile
import matplotlib.pyplot as plt
import logging

from .crucible_ingestor import CrucibleDatasetIngestor

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

    def parse_measurement(self):
        self.measurement = "Confocal Image"


    def parse_file_timestamp(self):
        try:
            self.timestamp = datetime.strptime(self.scientific_metadata['Information']['Document']['CreationDate'], "%Y-%m-%dT%H:%M:%S").isoformat()
        except Exception as e:
            logger.error(f'failed to parse CZI timestamp with error {e}')
                

    def parse_keywords(self):
        try:
            self.session_name = self.scientific_metadata['Information']['Image']['Session']['@SessionName']
            self.keywords += [self.session_name]
        except Exception as e:
            logger.error(f'failed to parse CZI session name with error {e}')

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

        except Exception as e:
            logger.error(f'failed to parse usage keywords with error {e}')
    
        other_kw_fields = [self.instrument_name, self.measurement]
        set_kw_fields = [k for k in other_kw_fields if k is not None]
        self.keywords += set_kw_fields
        return
    
    


    def get_thumbnails(self):
        tmp_dir = './tmp_files'
        os.makedirs(tmp_dir, exist_ok = True)
        out_image_file_name = f"{tmp_dir}/{os.path.basename(self.file_to_upload)}.png"

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

