import os
import h5py
import numpy as np
import logging

from crucible.utils.io import checkhash
from ingestors.h5_ingestor import H5Ingestor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class PtychographyH5Ingestor(H5Ingestor):

    def is_file_supported(self):
        """Check if the file is a Dectris NXmx master file for ptychography."""
        if not self.file_to_upload.endswith('_master.h5'):
            return False
        try:
            with h5py.File(self.file_to_upload, 'r') as f:
                if 'entry/definition' not in f:
                    return False
                definition = f['entry/definition'][()]
                if isinstance(definition, bytes):
                    definition = definition.decode('utf-8')
                return definition == 'NXmx'
        except Exception:
            return False

     
    @staticmethod
    def _convert_h5_value(val):
        """Convert HDF5/numpy values to JSON-serializable Python types."""
        if isinstance(val, (bytes, np.bytes_)):
            return val.decode('utf-8', errors='replace')
        if isinstance(val, np.ndarray):
            if val.size > 10:
                return None
            return [PtychographyH5Ingestor._convert_h5_value(v) for v in val.flat]
        if isinstance(val, np.integer):
            return int(val)
        if isinstance(val, np.floating):
            return float(val)
        if isinstance(val, np.bool_):
            return bool(val)
        return val


    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        d = dict()

        def nest_json(k, v, d=d):
            keys = k.split("/")
            current = d
            for key in keys:
                if key in current:
                    current = current[key]
                else:
                    current[key] = {}
                    current = current[key]

            # Extract attributes
            for attr_key in v.attrs:
                converted = self._convert_h5_value(v.attrs[attr_key])
                if converted is not None:
                    current[attr_key] = converted

            # Extract dataset values
            if isinstance(v, h5py.Dataset):
                converted = self._convert_h5_value(v[()])
                if converted is not None:
                    current['value'] = converted

        self.h5file = h5py.File(self.file_to_upload, 'r')
        self.h5file.visititems(nest_json)
        self.scientific_metadata.update(d)
        

    def get_dataset_metadata(self):
        """
        Base function that gets called
        during setup_data()
        should update unique_id, timestamp,
        size, dataset_name, data_format
        """
        self.measurement = '4D-STEM'
        self.data_format = self.file_to_upload.split('.')[-1]
        self.sha256_hash_file_to_upload = checkhash(self.file_to_upload)
        
        super().parse_source_folder()
        super().parse_keywords()
        return "get_dataset_metadata completed"
    

    def get_data_files(self):
        """
        Base function that gets called
        during setup_data()- should call
        self.add_file(). 

        Default adds only self.file_to_upload.  
        """
        base_path = os.path.dirname(self.file_to_upload)
        dsname = os.path.basename(self.file_to_upload).split('_master')[0]
        logger.info(f'{base_path=}, {dsname=}')

        associated_files = [os.path.join(base_path, f) for f in os.listdir(base_path) if dsname in f]
        logger.info(f'{associated_files=}')

        self.add_file(self.file_to_upload)
        [self.add_file(f) for f in associated_files]

        return "get_data_files completed"



