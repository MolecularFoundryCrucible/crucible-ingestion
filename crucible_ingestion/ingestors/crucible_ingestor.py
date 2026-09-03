import os
import shutil
from PIL import Image
from datetime import datetime as dt
from mfid import mfid
import logging
import json
from pathlib import Path
from crucible.models import Dataset

from ..utils import (build_b64_thumbnail,
                     EnhancedJSONEncoder,
                     deep_merge_skip_empty)

from ..client import get_client

logger = logging.getLogger(__name__)

TMP_DIR = '/tmp/crucible_tmp_files'


def cleanup_tmp_files():
    shutil.rmtree(TMP_DIR, ignore_errors=True)


class CrucibleDatasetIngestor(Dataset):
    ingestion_githash: str = os.environ.get("GITHASH")
    scientific_metadata: dict = {} 
    keywords: list = []
    acl: list = []
    associated_files: dict = {} 
    thumbnails: list = []
    samples: list = []
    children: list = []
    
    @property
    def ingestion_class(self):
        return type(self).__name__

    def is_file_supported(self):
        return True


    def setup_data(self):
        self.get_scientific_metadata()
        logger.info("getting scientific metadata complete")
        self.get_dataset_metadata()
        logger.info("getting dataset metadata complete")
        self.get_acl_information()
        logger.info("getting acl information complete")
        self.parse_batch()
        logger.info("parsing batch complete")
        self.parse_samples()
        logger.info("parsing samples complete")
        self.parse_children()
        logger.info("parsing children complete")
        # self.get_data_files()
        # logger.info("getting data files complete")
        self.get_thumbnails()
        logger.info("getting thumbnails complete")


    def cleanup(self):
        cleanup_tmp_files()


    def get_scientific_metadata(self):
        """
        Base function that gets called
        during setup_data() - should
        populate the metadata_dictionary
        of the object.
        """
        self.scientific_metadata = {}
    
    
    def parse_dataset_name(self):
        if self.dataset_name:
            return
        else:
            self.dataset_name = Path(self.file_to_upload).name
            logger.info(f"{self.dataset_name=}")
            return
    

    def parse_file_timestamp(self):
        if self.timestamp:
            return
        else:
            file_ctime = os.path.getctime(self.file_to_upload)
            self.timestamp = dt.fromtimestamp(file_ctime).isoformat()
            logger.info(f"{self.timestamp=}")
            return


    def parse_instrument(self):
        if self.instrument_name:
            self.acl.append(self.instrument_name)


    def parse_measurement(self):
        """Subclasses override to set self.measurement."""
        pass


    def parse_data_type(self):
        """Default: mirror measurement. Subclasses may override."""
        if self.measurement:
            self.data_type = self.measurement


    def parse_keywords(self):
        kw_fields = [self.instrument_name, self.measurement, self.data_type, self.session_name]
        set_kw_fields = [k for k in kw_fields if k is not None]
        self.keywords += set_kw_fields
        self.keywords = list(set(self.keywords))
        logger.info(f"{self.keywords=}")
        return


    def get_dataset_metadata(self):
        """
        Base function that gets called
        during setup_data()
        should update unique_id, timestamp,
        size, dataset_name, data_format
        """
        if self.unique_id is None:
            self.unique_id = mfid()[0]
        
        if self.size is None:
            self.size = 0

        self.size += os.path.getsize(self.file_to_upload)
        if not self.data_format:
            self.data_format = self.file_to_upload.split('.')[-1]

        self.parse_dataset_name()
        self.parse_file_timestamp()
        self.parse_instrument()
        self.parse_measurement()
        self.parse_data_type()
        self.parse_keywords()
        return


    def parse_batch(self):
        pass


    def parse_samples(self):
        pass
        
    def parse_children(self):
        pass
    
    def parse_orcid(self):
        if self.owner_orcid:
            return


    def parse_project_id(self):
        if self.project_id:
            return


    def get_acl_information(self):
        # OWNER
        self.parse_orcid()
        logger.info("parse orcid complete")
        if self.owner_orcid:
            self.acl.append(self.owner_orcid)

        
        # PROJECT
        self.parse_project_id()
        logger.info("parse project_id complete")
        if self.project_id:
            project = get_client().projects.get(self.project_id)
            if not project:
                raise ValueError(f"Project with ID '{self.project_id}' does not exist in the database.")
            else:
                self.project_id = project['project_id']
                self.acl.append(self.project_id)
                logger.info(f"Project info appended: {self.project_id}")


    def get_thumbnails(self):
        """
        Base function that gets called
        during setup_data() - should call
        self.add_thumbnail().
        """
        self.thumbnails = []
        return "get_thumbnails completed"


    def add_thumbnail(self, image: Image.Image, caption: str, size=(200,200)):
        tn = build_b64_thumbnail(image, size)
        self.thumbnails += [{"thumbnail": tn,
                             'caption':caption}]


    def to_ig_from_sql(self, dataset_obj, sql_import_attr):

        for attr in sql_import_attr:
            if attr in dataset_obj.keys():
                if dataset_obj[attr] is None:
                    continue

                if dataset_obj[attr] == "":
                    continue

                if dataset_obj[attr] == 'unknown':
                    continue
                    
                if attr == "scientific_metadata":
                    if 'scientific_metadata' in dataset_obj[attr]:
                        existing_metadata = dataset_obj[attr]['scientific_metadata']
                    else:
                        # should be an empty dictionary; but just in case
                        existing_metadata = dataset_obj[attr]
                    deep_merge_skip_empty(self.scientific_metadata, existing_metadata)
                    continue
                    
                logger.info(f"setting {attr} to {dataset_obj[attr]} as set in sql")
                setattr(self, attr, dataset_obj[attr]) 
                
            else:
               continue


    def to_json_from_ig(self, jsonfile, sql_export_attr, allow_missing=False):
        export_metadata = {}
        for attr in sql_export_attr:
            if allow_missing and (not hasattr(self,attr)):
                logger.warning(f"# ============================================== {attr} is missing!!")
                continue    

            export_metadata[attr] = getattr(self, attr)
            
        export_metadata['thumbnails'] = [{"thumbnail": tn['thumbnail'], "caption": tn['caption']} for tn in self.thumbnails]

        with open(jsonfile, "w") as f:
            json.dump(export_metadata, f, cls = EnhancedJSONEncoder, indent = 4)





