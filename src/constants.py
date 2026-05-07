sql_import_attr = [ 'dataset_name', 
                    'unique_id', 
                    'timestamp', 
                    'size',
                    'source_folder',
                    'owner_orcid',
                    'project_id',
                    'scientific_metadata',
                    'public',
                    'instrument_name',
                    'measurement',
                    'data_format',
                    'session_name',
                    'ingestion_class',
                    'ingestion_githash']

sql_export_attr = sql_import_attr + ['thumbnails',
                                     'keywords', 
                                     'acl']

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