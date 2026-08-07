# crucible-ingestion
Python classes and framework for server-side metadata parsing from data files into the Crucible Data Platform.

To test a new ingestor locally you can run: 
```
# set your CRUCIBLE_APIKEY as an env var
export CRUCIBLE_APIKEY = 'your-api-key'

# create a virtual env
uv sync

# parse a file locally and write out the packet that would be sent
# only --file is required
uv run crucible-ingest --file {local_file_path} [--dsid {dsid}] [--ingestor {ingestion-class}] [--output-dir {output_dir}]
```

`--dsid` defaults to `xxx` if not provided. If `--ingestor` is not provided, a supported
ingestor is auto-detected via `find_supported_ingestor`.

Parsing pulls information from the SQL database if a dataset record with the provided dsid
exists, otherwise it starts from scratch. Nothing is written to the Crucible platform
unless you pass `--push`, which sends the parsed packet for real.

The parsed packet is written to `{output_dir}/packet.json`, and any thumbnails are decoded
to `{output_dir}/thumbnails/` so you can look at them.

