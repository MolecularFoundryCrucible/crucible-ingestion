# crucible-ingestion

Python classes and framework for parsing metadata out of scientific data files and into
the Crucible Data Platform.

Parsing and sending are separate. `build_packet()` reads a file and returns an
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

Services that already hold their own credentials can inject a client instead:

```python
from crucible import CrucibleClient
from crucible_ingestion import set_client

set_client(CrucibleClient(api_url=..., api_key=...))
```

## Parse a file locally

```
uv run crucible-ingest --file {local_file_path} [--dsid {dsid}] [--ingestor {ingestion-class}] [--output-dir {output_dir}] [--push]
```

Only `--file` is required.

- `--dsid` defaults to `xxx`.
- `--ingestor` names an ingestor class explicitly. Without it, a supported ingestor is
  auto-detected by `find_supported_ingestor`.
- `--output-dir` defaults to `./dry_run_output/{dsid}`.
- `--push` sends the parsed packet to Crucible. Without it nothing is written.

The parsed packet is written to `{output_dir}/packet.json` and any thumbnails are decoded
to `{output_dir}/thumbnails/` so they can be inspected.

Parsing reads from Crucible if a dataset record with the given dsid already exists, so
that values a user set at dataset creation are not overwritten by parsed ones. If no such
record exists, parsing starts from scratch.

## Using it from Python

```python
from crucible_ingestion import build_packet, push_packet

packet = build_packet(path_to_file, dsid)
if packet is not None:
    # inspect packet.dataset_fields, packet.scientific_metadata,
    # packet.keywords, packet.samples, packet.children, packet.thumbnails
    push_packet(packet)
```

`build_packet` returns `None` when no ingestor supports the file.

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

Parsing must not call Crucible. Anything that needs to be created or linked belongs on
`self.samples`, `self.children`, `self.keywords`, or `self.thumbnails`, and is written by
`push_packet`.

## Running in the cloud

The RabbitMQ consumer that runs this package in GCP lives in a separate repo,
`crucible-consumers`.
