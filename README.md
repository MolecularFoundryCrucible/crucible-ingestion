# crucible-ingestion

Python classes and framework for parsing metadata out of scientific data files and into
the Crucible Data Platform.

Parsing and sending are separate. `parse()` reads a file and returns an
`IngestionPacket` describing everything that would be sent; `push_packet()` takes that
packet and writes it to Crucible. Nothing is uploaded during parsing, and the file bytes
themselves are never sent — only the parsed metadata.

## Install

```
uv sync
```

## Authentication

Run `crucible config init` once to store your API url and key. Everything in this package
builds its client through `get_client()`, which picks up that config.

## Parse a file locally

```
uv run crucible-ingest --file {local_file_path} [--dsid {dsid}] [--ingestor {ingestion-class}] [--output-dir {output_dir}] [--push] [--test]
```

Only `--file` is required.

- `--dsid` is the dataset the parsed information belongs to. If a dataset mfid is not provided, an mfid will be generated locally. If you are running --push and are not in test mode (--test) you should first create a dataset in Crucible using `crucible dataset create` or 
```python 
client.datasets.create(Dataset())
``` 
and provide the dataset ID for the dataset. 

- `--ingestor` names an ingestion class explicitly. Without it, a supported ingestor is
  auto-detected by `find_supported_ingestor`.
- `--output-dir` defaults to `./dry_run_output/{dsid}`.
- `--push` sends the parsed packet to Crucible. Without it nothing is written.
- `--test` runs`--push` in test mode: If a dataset ID is not provided, one will be generated and a temporary dataset will be created for you in the `crucible-test` project.

The parsed packet is written to `{output_dir}/packet.json` and any thumbnails are decoded
to `{output_dir}/thumbnails/` so they can be inspected.

## Push to Crucible

`--push` needs a dataset to push to, so pair it with a `--dsid`:

```
crucible dataset create
uv run crucible-ingest --file {local_file_path} --dsid {new_dsid} --push
```

To test the push() function, use `--test` and the command will create a temporary dataset for
you in the `crucible-test` project, logging the id it picked:

```
uv run crucible-ingest --file {local_file_path} --push --test
```

Running `--push` with neither a `--dsid` nor `--test` will result in an error.

The parse() function reads from Crucible if a dataset record with the given dsid already exists, so
that values a user set at dataset creation are not overwritten by parsed ones. If no such
record exists, parsing starts from scratch.

## Using it from Python

```python
from crucible_ingestion import parse, push_packet

packet = parse(path_to_file, dsid)
if packet is not None:
    # inspect packet.dataset_fields, packet.scientific_metadata,
    # packet.keywords, packet.samples, packet.children, packet.thumbnails
    push_packet(packet)
```

`parse` returns `None` when no ingestor supports the file.

## Adding a new ingestor

Ingestor classes live in `crucible_ingestion/ingestors/` and subclass
`CrucibleDatasetIngestor` (`ingestors/crucible_ingestor.py`), which defines the parsing
hooks `setup_data()` calls: `get_scientific_metadata`, `get_dataset_metadata`,
`get_acl_information`, `parse_batch`, `parse_samples`, `parse_children`, and
`get_thumbnails`.

A new class needs `is_file_supported()` and must be added to `ingestor_list` in
`crucible_ingestion/ingestors/registry.py`. Order matters — `find_supported_ingestor`
returns the first class in the list that claims the file, so specific ingestors belong
above general ones.

Parsing must not post to Crucible. All writes to Crucible should be part of the `push_packet` function. 
Anything that needs to be created or linked can be added to the packet during parsing, for example: 
`self.samples`, `self.children`, `self.keywords`, or `self.thumbnails`.

## Test data

The files the tests parse live in GCS, not in this repo. Pull them before running the
tests:

```
gcloud storage rsync -r gs://crucible-ingestion-test-data tests/data
```

Push new or updated files back up:

```
gcloud storage rsync -r tests/data gs://crucible-ingestion-test-data
```

Both directions only transfer what is missing or changed, and neither deletes anything at
the destination. Removing a file from the bucket has to be done explicitly.

## Running in the cloud

The RabbitMQ consumer that runs this package in GCP lives in a separate repo,
`ingestion-cloud-consumer`.
