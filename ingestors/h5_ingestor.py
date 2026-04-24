import h5py
from ingestors.crucible_ingestor import CrucibleDatasetIngestor


class H5Ingestor(CrucibleDatasetIngestor):
    
    def is_file_supported(self):
        return self.file_to_upload.endswith('h5')
           
 
    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        d = dict()
        def nest_json(k,v, d=d):
            keys=k.split("/")
            for key in keys:
                if key in d.keys():
                    d = d[key]
                else:
                    d[key] = {}
            for eachkey in v.attrs.keys():
                d[key][eachkey] = v.attrs[eachkey]

        self.h5file = h5py.File(self.file_to_upload, 'r')
        self.h5file.visititems(nest_json)
        self.scientific_metadata.update(d)

    
    def get_dataset_metadata(self):
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.data_format = "H5"

    

