sql_import_attr = [ 'dataset_name',
                    'unique_id',
                    'owner_orcid',
                    'project_id',
                    'scientific_metadata',
                    'public',
                    'instrument_name',
                    'measurement',
                    'data_type',
                    'data_format',
                    'session_name',
                    'ingestion_class',
                    'ingestion_githash',
                    'timestamp']

# size stays export-only: it is measured from the file, so a stored value should never
# win over what the parse finds.
sql_export_attr = sql_import_attr + ['size',
                                     'thumbnails',
                                     'keywords',
                                     'acl']