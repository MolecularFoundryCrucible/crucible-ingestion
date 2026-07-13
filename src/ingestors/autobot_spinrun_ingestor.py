import os
import yaml
import logging

from .crucible_ingestor import CrucibleDatasetIngestor
from crucible import CrucibleClient
from crucible.models import Dataset
from ..utils import get_secret

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Crucible Client
crucible_api_url = os.environ.get('CRUCIBLE_API_URL')
apikey = get_secret("ADMIN_APIKEY", "crucible_admin_apikey/versions/4")
client = CrucibleClient(api_url=crucible_api_url, api_key=apikey)


class SpinRunIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        # currently expects yaml file
        if not self.file_to_upload.endswith('yaml'):
            return False
        
        # open the file
        with open(self.file_to_upload, 'r') as f:
            file_content = yaml.safe_load(f)
        
        # check that measurement = spin_run
        if file_content.get('measurement') != 'spin_run':
            return False
        
        # second check for instrument
        if file_content.get('instrument_name') != 'SpinBot':
            return False
        
        run_id = file_content.get('run_id')
        if run_id != self.unique_id:
            raise Exception(f'Run ID in file does not match Crucible Dataset MFID. {run_id=} and {self.unique_id=}')
        
        self.file_contents = file_content
        return True


    def get_scientific_metadata(self):
        self.scientific_metadata = self.file_contents


    def parse_sample_metadata(self, sample_mfid):
        logger.info(sample_mfid)
        samples = self.file_contents.get('samples')
        logger.info([s['sample_id'] for s in samples])
        additional_info = {k:v for k,v in self.file_contents.items() if k != 'samples'}
        sample_info = [s for s in samples if s['sample_id'] == sample_mfid][0]
        sample_info.update(additional_info)
        return sample_info

    def parse_measurement(self):
        self.measurement = self.file_contents.get('measurement')
        logger.info(f"{self.measurement=}")
    

    def parse_dataset_name(self):
        run_id = self.file_contents.get('run_id', None)
        self.dataset_name = f'Spin Run - {run_id[0:13]}'

    
    def parse_file_timestamp(self):
        self.file_timestamp = self.file_contents.get('creation_time', None)


    def parse_instrument(self):
        self.instrument_name = self.file_contents.get('instrument_name')
        logger.info(f"{self.instrument_name=}")
    

    def parse_data_type(self):
        self.data_type = 'Thin Film Deposition Run'
    

    def parse_samples(self):
        parent_list = []
        sample_list = self.file_contents.get('samples', [])
        for s in sample_list: 
            # collect the basic sample info
            sample = {
                'unique_id': s.get('sample_id'),
                'sample_name': s.get('sample_name'),
                'owner_orcid': self.owner_orcid,
                'project_id': self.project_id,
                'parent_ids': []
            }

            # samples have 'batch' (tray) parents
            batch_id = s.get('batch_id')
            if not batch_id in parent_list:
                parent_list.append(batch_id)
                batch_sample = {
                    'unique_id': s.get('batch_id'),
                    'sample_name': s.get('batch_name'),
                    'owner_orcid': self.owner_orcid,
                    'project_id': self.project_id,
                    'parent_ids': []
                }

                # add batch to sample list to make sure it gets created
                self.samples.append(batch_sample)

            sample['parent_ids'].append(batch_id)

            # samples have precursor parents
            precursor_name = s.get('precursor_solution_name')
            found_ps = client.samples.list(sample_name = precursor_name,
                                           project_id = self.project_id)
            
            # if the PS exists; add to sample parents
            if len(found_ps) > 0:
                precursor_id = found_ps[-1]['unique_id']
                sample['parent_ids'].append(precursor_id)
        
        # add sample to list to be created
        self.samples.append(sample)
        return


    def parse_children(self):
        for sample in self.samples:
            sample_name = sample['sample_name']
            child_ds_name = f'Spin Run for {sample_name} - {self.unique_id[0:13]}'
            child_ds = Dataset(
                        measurement = self.measurement,
                        project_id = self.project_id,
                        owner_orcid = self.owner_orcid,
                        dataset_name = child_ds_name,
                        data_format = 'yaml' )
            
            md = self.parse_sample_metadata(sample['unique_id'])
            resp = client.datasets.create(child_ds, md)
            child_dsid = resp['dsid']

            # link to run dataset
            client.datasets.link(parent_id = self.unique_id, child_id = child_dsid)

            # link to thin film
            client.datasets.add_sample(dataset_id = child_dsid, sample_id = sample['unique_id'])


    def parse_orcid(self):
        self.owner_orcid = self.file_contents.get('user_orcid', None)


    def parse_project_id(self):
        self.project_id = self.file_contents.get('project_id', None)



    