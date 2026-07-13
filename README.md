# crucible-ingestion
Python classes and framework for server-side metadata parsing from data files into the Crucible Data Platform.

To test a new ingestor locally you can run: 
```
uv sync
cd src
uv run python -m dry_run_data_ingestion.py {local_file_path} {dsid} {ingestion-class}
```

Local testing will pull information from the SQL database if a dataset record with the provided dsid exists, otherwise it will start from scratch. 
No data will be uploaded or created in the Crucible platform during a dry run. 

