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
                    'ingestion_githash']

# Note: if a user manually edited timestamp in the DB and then re-ingests with
# one of those subclasses, the file-parsed value will win over their edit.
sql_export_attr = sql_import_attr + ['size',
                                     'timestamp',
                                     'thumbnails',
                                     'keywords',
                                     'acl']