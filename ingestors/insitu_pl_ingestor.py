import os
import shutil
import re
import logging
from crucible.utils.io import run_shell
from ingestors.crucible_ingestor import CrucibleDatasetIngestor

logger = logging.getLogger(__name__)

def get_insitu_spec_headers(fpath):
        with open(fpath) as mdf:
            md = [x.replace("\n", "").strip() for x in mdf.readlines()]
        n = 1
        d = {}
        while md[n] !=  '>>>>>Begin Spectral Data<<<<<' and n <= len(md):
            if md[n] != "":
                md_fields = md[n].split(":")
                d[md_fields[0].strip()] = md_fields[1].strip()
            n+=1
        return(d)


class InSituPlIngestor(CrucibleDatasetIngestor):

    def is_file_supported(self):
        '''
        checks that all of the following conditions are true: 
        - data came from the insitu pl computer
        - data is zipped
        - at least one file in the zip folder follows the expected naming convention for insitu pl data
        '''
        
        if all([self.file_to_upload.endswith('zip')]):
            zipcontents = [x.split(" ")[-1] for x in run_shell(f"unzip -l '{self.file_to_upload}'").stdout.split("\n") if re.match(".*/.*[.]txt", x)]
            return len(zipcontents) > 0
        else:
            return False

    def get_scientific_metadata(self):
        CrucibleDatasetIngestor.get_scientific_metadata(self)
        logger.info("running get scientific metadata")

        self.tmp_folder = os.path.join("./generated_files/", os.path.basename(self.file_to_upload.replace(".zip", "")))
        logger.info(f"{self.tmp_folder=}")

        # extract the files
        if os.path.exists(self.tmp_folder):
            shutil.rmtree(self.tmp_folder)
        xx = run_shell(f"unzip -qq '{self.file_to_upload}' -d ./generated_files/")
        logger.info(xx.stderr)

        # sample parsing
        self.instrument_name = ""
        for (root,dirs,files) in os.walk(self.tmp_folder, topdown=True): 
            files = [f for f in files if not f[0] == '.']
            dirs[:] = [d for d in dirs if not d[0] == '.']

            
            if root == self.tmp_folder and len(dirs) == 0:
                logger.info("no subdirectories found")
                sample_name = root.split("/")[-1]
                self.scientific_metadata[sample_name] = {'sample_folder': root, "settings":{}}
            elif root == self.tmp_folder and len(dirs) > 0:
                for d in dirs:
                    sample_name = d
                    self.scientific_metadata[sample_name] = {'sample_folder': os.path.join(root,d), "settings":{}}
            else:
                pass
            
            for f in files:               
                sample_name = root.split("/")[-1]
                if sample_name in self.scientific_metadata.keys():
                    if self.scientific_metadata[sample_name]['settings'] == {}:
                        fpath = os.path.join(root, f)
                        self.scientific_metadata[sample_name]['settings'] = get_insitu_spec_headers(fpath)
                        self.header_file = f
                        self.header_sample = sample_name
        
    def get_dataset_metadata(self):

        self.instrument_name = self.scientific_metadata[self.header_sample]['settings']['Spectrometer']

        # if "QEP" in self.header_file or "FLM" in self.header_file:
        #     self.measurement = "In Situ PL"
        if "transmission" in self.header_file.lower():
            self.measurement = "In Situ UV-Vis"
        else:
            self.measurement = "In Situ PL"
        CrucibleDatasetIngestor.get_dataset_metadata(self)
        self.keywords += ["In Situ Spectroscopy"]
        self.keywords += list(self.scientific_metadata.keys())

    # TODO - pass orcid and project_id in API call



