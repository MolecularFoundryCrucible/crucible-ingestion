#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 15:42:49 2025

@author: roncofaber but also mkwall
"""

import os

from ingestors.crucible_ingestor import CrucibleDatasetIngestor

#%%

def store_variable(varname, varvalue, vardict):
    vardict[varname] = varvalue
    return

class LmpIngestor(CrucibleDatasetIngestor):
    
    def is_file_supported(self):
        #TODO make resilient to different conventions
        return self.file_to_upload.endswith('lmp')
    
    def get_scientific_metadata(self):
        """
        Main driver, reads input file and find other relevant files, then
        reads each one to extract metadata.
        """
        
        # read input file
        self.scientific_metadata = self.read_lmp_input_file()

        # read data file
        data_file_metadata = self.read_data_file()
        self.scientific_metadata.update(data_file_metadata)
        
        # read LOG file
        log_file_metadata = self.read_log_file()
        self.scientific_metadata.update(log_file_metadata)
        
        return
    
    
    # main driver: reads input file and find relevant associated files
    def read_lmp_input_file(self):
        
        data = dict()
        vardict = dict()
        
        # store path
        data["data_path"] = os.path.dirname(self.file_to_upload)
        
        # initialize empty arrays
        data["dump_files"] = []
        data["log_files"]  = []
        
        with open(self.file_to_upload, "r") as fin:
            
            for line in fin:
                
                if line.startswith("read_data"): # see dump section
                    data_file = line.split()[1]
                    data["data_file"] = data_file
                
                if line.startswith("variable"):
                    varname  = line.split()[1]
                    varvalue = line.split()[3]
                    store_variable(varname, varvalue, vardict)
                    
                if line.startswith("dump "): #those should end up into self.associated_files
                    dumpname = line.split()[5]
                    dumpname = dumpname.replace("$", "")
                    data["dump_files"].append(dumpname.format(**vardict))
                    
                if line.startswith("log "):
                    logname = line.split()[1]
                    logname = logname.replace("$", "")
                    data["log_files"].append(logname.format(**vardict))
                    
        # if no log specified use the standard one
        if not data["log_files"]:
            data["log_files"]  = ["log.lammps"]

        return data
    
    def read_data_file(self):
        
        try:
            import ase.io.lammpsdata
        except ImportError:
            raise ImportError("ASE needs to be installed for LMP ingestor to work!")
            
        data = {}

        data_file = self.scientific_metadata['data_path'] + "/" + self.scientific_metadata['data_file']
        
        ase_atoms = ase.io.lammpsdata.read_lammps_data(data_file)
        
        #TODO this should not stay like that --> should be a json
        # data["atoms"] = ase_atoms.todict()
        
        # store some info about the system to metadata
        data['elements'] = list(set(ase_atoms.get_chemical_symbols()))
        data['natoms']   = len(ase_atoms.get_chemical_symbols())
        data["volume"]   = ase_atoms.get_volume()

        # what else do we want from the data_file

        return data    


    def read_log_file(self):

        data = dict()

        log_file = self.scientific_metadata['data_path'] + "/" + self.scientific_metadata['log_files'][0]
        
        # just read the first
        with open(log_file) as f:
            first_line = f.readline()
            
        data["lammps_version"] = first_line.strip()

        return data

    def get_dataset_metadata(self):
        
        # sets dataset_name, timestamp, size, data_format, sha256, source_folder, instrument_name, keywords

        # dataset_name set to the name of file_to_upload without the extension
        # timestamp set using file metadata ctime
        # data_format default to file extension
        # source_folder will be the path in the crucible-uploads gcs bucket
        # tries to set instrument_name based on path in crucible-uploads; defaults to None
        # default keywords are instrument_name, measurement, and session_name if those attributes exist 
        CrucibleDatasetIngestor.get_dataset_metadata(self) 
        
        # if you want to change any of those you can do it here 
        self.data_format = "LAMMPS"

        # add keywords to tag the dataset with (elements used?)
        self.keywords += self.scientific_metadata['data_file_metadata']['elements']


    def get_ownership_metadata(self):
        '''
        Parse information to associate the data with a User ORCID and Project. 
        Might be easiest to send owner_orcid and project_id in the API call,
        then we don't have to define this method here. 
        '''
        return
    

    def get_data_files(self):
        '''
        Adds file info (path, size, hash) to the associated files attribute
        '''
        data_file = self.scientific_metadata['data_file']
        dump_files = self.scientific_metadata['dump_files']
        log_files = self.scientific_metadata['log_files']

        self.add_file(self.file_to_upload)
        self.add_file(data_file)
        for each_file in dump_files + log_files:
            self.add_file(each_file)

        return


    def get_thumbnails(self):
        '''
        Optional if you want to add a thumbnail of the dataset to the database
        '''
        return
    
    

