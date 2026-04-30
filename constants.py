# EVENTUALLY ALL THESE CAN GET HANDLED THROUGH CONFIG MAP 
crucible_api_url = "https://crucible.lbl.gov/api/v1"
secret_store = "projects/776258882599/secrets"
gcs_client_id = "776258882599-v17f82atu67g16na3oiq6ga3bnudoqrh.apps.googleusercontent.com"
rmq_host =  '10.128.0.31'
rmq_port = 5672

sql_import_attr = [ 'dataset_name', # default is not none
                    'unique_id', 
                    'timestamp', 
                    'size',
                    'source_folder',
                    'owner_user_id',
                    'owner_orcid',
                    'project_id',
                    'scientific_metadata',
                    'public',
                    'file_to_upload',
                   # 'instrument_id',
                    'instrument_name',
                    'measurement',
                    'data_format',
                    'session_name',
                    'sha256_hash_file_to_upload',
                    'githash']

sql_export_attr = sql_import_attr + ['thumbnails', 'associated_files', 'keywords', 'acl', 'githash']

INSTRUMENT_DRIVES = {
          "aldbot":"CRUCIBLE - MF Inorganic ALDbot",
          "hip_microscope": 'CRUCIBLE - MF Imaging HipMicroscope',
          "insitu_pl": 'CRUCIBLE - MF Inorganic InSitu',
          'jupiterafm': 'CRUCIBLE - MF Imaging JupiterAFM',
          'minion':'CRUCIBLE - MF Bio Minion',
          'qspleem':'CRUCIBLE - MF Imaging QSpleem', 
          'spinbot':'CRUCIBLE - MF Inorganic Spinbot',
          'supracl_microscope':'CRUCIBLE - MF Imaging SupraCLMicroscope',
          'team05': 'CRUCIBLE - MF NCEM TEAM05',
          'team01': 'CRUCIBLE - MF NCEM TEAM01',
          'themisx': 'CRUCIBLE - MF NCEM ThemisX',
          'titanx':'CRUCIBLE - MF NCEM TitanX',
          'zeiss_elyra':'CRUCIBLE - MF Bio Elyra7',
          'zeiss_lsm710_confocal':'CRUCIBLE - MF Bio LSM710'
}