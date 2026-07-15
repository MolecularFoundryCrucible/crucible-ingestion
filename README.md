# crucible-ingestion
Python classes and framework for server-side metadata parsing from data files into the Crucible Data Platform.

To test a new ingestor locally you can run: 
```
# set your CRUCIBLE_APIKEY as an env var
export CRUCIBLE_APIKEY = 'your-api-key'

# create a virtual env
uv sync

# run a dry run of the ingestion process
# only --file is required; --dsid and --ingestor are optional
cd src
uv run python -m dry_run_data_ingestion.py --file {local_file_path} [--dsid {dsid}] [--ingestor {ingestion-class}] [--output-dir {output_dir}]
```

`--dsid` defaults to `xxx` if not provided. If `--ingestor` is not provided, the dry run
will auto-detect a supported ingestor via `find_supported_ingestor`.

Local testing will pull information from the SQL database if a dataset record with the provided dsid exists, otherwise it will start from scratch. 
No data will be uploaded or created in the Crucible platform during a dry run. 

