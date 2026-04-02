import os
from typing import ClassVar
import numpy as np
import requests
import logging
import matplotlib.pyplot as plt
from PIL import Image
import igor2 as igor


from utils import get_secret
from constants import crucible_api_url
from google_calendar import (find_calendar_event,
                             parse_calendar_event_for_ownership)

from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

def decode_recurse(x):
    if isinstance(x, dict):
        for k in list(x.keys()):
            x[k] = decode_recurse(x[k])
    elif isinstance(x, bytes):
        try:
            x = x.decode('latin-1').replace('\u0000', '')
        except Exception:
            x = x
    elif isinstance(x, list):
        x = [decode_recurse(i) for i in x]
    elif isinstance(x, np.ndarray):
        x = np.array([decode_recurse(i) for i in x])
    else:
        x = x     
    return(x)



class AFMIngestor(CrucibleDatasetIngestor):
    supported_filetypes: ClassVar[list[str]] = ['ibw']

    def is_file_supported(self):
        return np.any([self.file_to_upload.endswith(ftype) for ftype in self.supported_filetypes])


    def get_scientific_metadata(self):
        im = igor.binarywave.load(self.file_to_upload)
        im = decode_recurse(im)
        
        newnote = {}
        for y in [x.split(":") for x in im['wave']['note'].split("\r")]:
            if len(y) == 2:
                newnote[y[0].strip()] = y[1].strip()
            elif len(y) > 2:
                newnote[y[0].strip()] = ":".join(y[1:]).strip()
            elif y[0] != "":
                newnote[y[0].strip()] = None
            else:
                continue
        
        for x in ['SaveImage', 'SaveForce',  'LastSaveImage', 'LastSaveForce']:
            newnote[x] = newnote[x].replace(":", "/")
        im['wave']['note'] = newnote

        self.scientific_metadata = im['wave']
        self.scientific_metadata['version'] = im['version']


    def get_kw_from_dataset_name(self):
        name_components = self.dataset_name.split("-")[:-1]
        kw = [x for x in name_components if x != ""]
        return kw
    

    def get_dataset_metadata(self):
        # instrument_name
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.data_format = self.file_to_upload.split('.')[-1]
        self.dataset_name = self.file_to_upload.split("Asylum Research Data")[-1].strip("/").replace("/", "-")
        
        fpath = os.path.dirname(self.file_to_upload)
        
        self.session_name = fpath.split("/")[-1]
        self.keywords += [self.instrument_name, self.session_name]

        kw_from_filename = self.get_kw_from_dataset_name()
        self.keywords += kw_from_filename
        self.keywords = list(set(self.keywords))
    
    
    def parse_orcid(self):

        if self.owner_orcid:
            return
        
        cal_id = 'c_550eaa9a91952a820fb6d76a3306f5583abcffc7cf42e72573fd2a0cae1b1c8f@group.calendar.google.com'
        cal_event = find_calendar_event(self.timestamp, cal_id, service_account_file = f"{os.getenv('HOME')}/.config/mf-crucible-9009d3780383.json")
        
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
                
        cal_id = 'c_550eaa9a91952a820fb6d76a3306f5583abcffc7cf42e72573fd2a0cae1b1c8f@group.calendar.google.com'
        
        if not self.project_id:
            cal_event = find_calendar_event(self.timestamp, cal_id, service_account_file = f"{os.getenv('HOME')}/.config/mf-crucible-9009d3780383.json")
            if cal_event:
                self.email, self.project_id = parse_calendar_event_for_ownership(cal_event)
        
        if not self.project_id:
            return
        
        if "Internal Research" in self.project_id and self.email is not None:
            self.project_id = f"MFUSER_{self.email.split('@')[0]}"

    
    def make_retrace_plot(self, array, pname):
        spec_map_filename = f"./generated_files/{os.path.basename(self.file_to_upload)}_{pname}.png"
     
        plt.imshow(array, cmap='Greys')  # You can choose any colormap you like
        plt.title(pname)
        plt.axis('off')  # Turn off axis labels and ticks
    
        plt.savefig(spec_map_filename, dpi = 1000 )
        return(Image.open(spec_map_filename))

    
    def get_thumbnails(self):
        im = igor.binarywave.load(self.file_to_upload)
        
        w = np.array(im['wave']['wData'])
        labels = [x.decode('latin-1') for x in im['wave']['labels'][2] if x.decode('latin-1') != ""]
        if len(w.shape) == 2:
            traceim = self.make_retrace_plot(w, "AFM Image")
            self.add_thumbnail(traceim, "AFM Image")
        elif len(w.shape) == 3:
            traceim = self.make_retrace_plot(w[:,:,1], labels[1])
            self.add_thumbnail(traceim, labels[1])
        else:
            logger.error(f"Failed to add thumbnail - wData had dim: {w.shape}")
            return(w.shape)
        




    

























