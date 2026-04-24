import os
from PIL import Image
from datetime import datetime as dt
from mfid import mfid
import logging
import json
from joblib import Parallel, delayed

from crucible import CrucibleClient
from crucible.utils.io import checkhash
from crucible.models import Dataset

from utils import (get_secret,
                   run_rclone_command,
                   build_b64_thumbnail,
                   EnhancedJSONEncoder,
                   reduce_filename_and_copy)

from constants import (INSTRUMENT_DRIVES,
                       sql_export_attr,
                       crucible_api_url)


logger = logging.getLogger(__name__)

# Crucible Client
apikey = get_secret("ADMIN_APIKEY", "crucible_admin_apikey/versions/4")
client = CrucibleClient(api_url=crucible_api_url, api_key=apikey)

class CrucibleDatasetIngestor(Dataset):
    githash: str = os.environ.get("GITHASH") 
    scientific_metadata: dict = {} 
    keywords: list = []
    acl: list = []
    associated_files: dict = {} 
    thumbnails: list = []
    samples: list = []

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
        self.get_data_files()
        logger.info("getting data files complete")
        self.get_thumbnails()
        logger.info("getting thumbnails complete")


    def get_scientific_metadata(self):
        """
        Base function that gets called
        during setup_data() - should
        populate the metadata_dictionary
        of the object.
        """
        self.scientific_metadata.update({
            'git_repo': 'https://github.com/MolecularFoundryCrucible/crucible-ingestion',
            'githash': self.githash,
            'ingestion_class': self.ingestion_class,
        })
    
    
    def parse_dataset_name(self):
        if self.dataset_name:
            return
        else:
            no_ext_fname = os.path.splitext(self.file_to_upload)[0]
            self.dataset_name = os.path.basename(no_ext_fname)
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


    def parse_source_folder(self):
        if self.source_folder:
            return
        else:
            file_dir = os.path.dirname(self.file_to_upload)
            drive_name = None

            if file_dir.startswith('/mnt/gcs'):
                crucible_upload_subdir = file_dir.split('/')[3]
                drive_name = INSTRUMENT_DRIVES.get(crucible_upload_subdir)
            
            if drive_name:
                self.source_folder = file_dir.replace(f'/mnt/gcs/{crucible_upload_subdir}', drive_name)
            else:
                self.source_folder = file_dir
            logger.info(f"{self.source_folder=}")
            return
        

    def parse_instrument(self):
        if self.instrument_id and self.instrument_name:
            self.acl.append(self.instrument_name)

        elif self.instrument_name:
            instrument = client.instruments.get(instrument_name=self.instrument_name)
            if instrument:
                self.instrument_id = instrument['id']
                self.acl.append(self.instrument_name)
            else:
                raise ValueError(f'Provided instrument does not exist: {self.instrument_name}')
    
        else:
            pass
    

    def parse_keywords(self):
        kw_fields = [self.instrument_name, self.measurement, self.session_name]
        set_kw_fields = [k for k in kw_fields if k is not None]
        self.keywords += set_kw_fields
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
        
        if not self.sha256_hash_file_to_upload:
            self.sha256_hash_file_to_upload = checkhash(self.file_to_upload)

        self.size = os.path.getsize(self.file_to_upload)
        self.data_format = self.file_to_upload.split('.')[-1]

        self.parse_dataset_name()
        self.parse_file_timestamp()
        self.parse_source_folder()
        self.parse_instrument()
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
        if self.owner_orcid and not self.owner_user_id:
            owner = client.users.get(self.owner_orcid)
            try:
                self.owner_user_id = owner['id']
                self.acl.append(self.owner_orcid)
                logger.info(f"Owner info appended: {self.owner_user_id}, {self.owner_orcid}")

            except Exception as err:
                logger.warning(f"Failed to append owner info due to error {err}. "
                               f"parsed_owner_orcid: {self.owner_orcid} "
                               f"owner_query_results: {owner}")
        
        # PROJECT
        self.parse_project_id()
        logger.info("parse project_id complete")
        if self.project_id:
            project = client.projects.get(self.project_id)
            if not project:
                raise ValueError(f"Project with ID '{self.project_id}' does not exist in the database.")
            else:
                self.project_id = project['project_id']
                self.acl.append(self.project_id)
                logger.info(f"Project info appended: {self.project_id}")


    def get_data_files(self):
        """
        Base function that gets called
        during setup_data()- should call
        self.add_file(). 

        Default adds only self.file_to_upload.  
        """
        self.add_file(self.file_to_upload)
        return "get_data_files completed"


    def get_thumbnails(self):
        """
        Base function that gets called
        during setup_data() - should call
        self.add_thumbnail().
        """
        self.thumbnails = []
        return "get_thumbnails completed"
        
        
    def add_file(self, file):
        fsize = os.path.getsize(file)
        if file == self.file_to_upload and self.sha256_hash_file_to_upload:
            fhash = self.sha256_hash_file_to_upload
        else:
            fhash = checkhash(file)
        for f,fattr in self.associated_files.items():
            if fattr['sha256_hash'] == fhash:
                logger.info(f'{file} already in associated files')
                return
            
        logger.info(f'adding {file}')
        self.associated_files[file] = {'size': fsize,
                                       'sha256_hash': fhash}


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
                  #  logger.info(f'{existing_metadata=}')
                    self.scientific_metadata.update(existing_metadata)
                    continue
                    
                logger.info(f"setting {attr} to {dataset_obj[attr]} as set in sql")
                setattr(self, attr, dataset_obj[attr]) 
                
            else:
               continue
            
        if self.project_id:
            self.project_id = self.project_id.split(" ")[0]


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


    def to_google_cloud_storage(self, storage_bucket, jsonfile, copy_assoc_files = False, common_file_paths = [], num_cores = os.cpu_count()):  
        file_to_upload_path = os.path.dirname(self.file_to_upload)
        logger.info(f'{file_to_upload_path=}')
        common_file_paths += ["./generated_files", file_to_upload_path, f'/mnt/gcs/{file_to_upload_path}', 
                              f'/mnt/gcs/api-uploads/{file_to_upload_path}', f'/mnt/gcs/manual-uploads/{file_to_upload_path}']
        
        dsid = self.unique_id
        destination = f"{storage_bucket}/{dsid}"
        
        # copy data
        if copy_assoc_files is True:
            logger.info(f'Copying associated files to {destination}')
            Parallel(n_jobs=num_cores)(delayed(reduce_filename_and_copy)\
                                      (f, common_file_paths, destination) \
                                       for f in self.associated_files.keys())

        self.to_json_from_ig(jsonfile, sql_export_attr)
        logger.info(f'Final destination: {destination}')
        out = run_rclone_command(source_path= jsonfile, 
                                 destination_path= f"{destination}/",
                                 cmd="copy",
                                 checkflag = False)
        logger.info(out.stdout)
        if out.stderr is not None:
            logger.error(out.stderr)
        return 





