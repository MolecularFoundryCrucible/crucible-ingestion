# Crucible Ingestion agent guide

## Scope

`crucible-ingestion` is the Python package and CLI (`crucible-ingest`) for extracting metadata, thumbnails, samples, and child datasets from scientific files into an `IngestionPacket` and applying that packet to the Crucible Data Platform via `nano-crucible`.

It does not own HTTP transport or API resource schemas (owned by `nano-crucible` and `crucible-api`) and does not own RabbitMQ queue consumption or cloud worker deployment (owned by `ingestion-cloud-consumer`).

## Production deploy hazard

Pushing to `main` on GitHub triggers `.github/workflows/notify-consumer.yml`, which sends a `repository_dispatch` (`crucible-ingestion-updated`) to `ingestion-cloud-consumer`. That workflow automatically commits a `uv.lock` bump to `ingestion-cloud-consumer` `main` and triggers an immediate production Cloud Build and GKE rollout.

Never push directly to `main` without explicit user authorization and coordination with `ingestion-cloud-consumer`.

## Repository map

- `crucible_ingestion/data_ingestion.py`: Orchestrates ingestor detection (`find_supported_ingestor`), packet construction (`parse`), existing-record reconciliation, and packet application (`push_packet`).
- `crucible_ingestion/packet.py`: Defines `IngestionPacket`, `SamplePacket`, and `ChildDatasetPacket` plus JSON serialization (`to_dict`, `from_dict`, `save`, `load`).
- `crucible_ingestion/client.py`: Provides the injectable `nano-crucible` client boundary (`get_client`, `set_client`, `reset_client`).
- `crucible_ingestion/cli.py`: Exposes the `crucible-ingest` CLI (`--file`, `--dsid`, `--ingestor`, `--output-dir`, `--push`, `--test`).
- `crucible_ingestion/ingestors/crucible_ingestor.py`: Defines `CrucibleDatasetIngestor` and the `setup_data()` lifecycle hooks (`get_scientific_metadata`, `get_dataset_metadata`, `get_acl_information`, `parse_batch`, `parse_samples`, `parse_children`, `get_thumbnails`).
- `crucible_ingestion/ingestors/registry.py`: Orders `ingestor_list` and implements `find_supported_ingestor` and `get_ingestor_by_name`.
- `tests/make_schemas.py`, `tests/schema_registry.json`, and `tests/schemas/`: Record and validate expected `IngestionPacket` JSON Schemas against scientific test files by SHA-256 hash.

## Architecture invariants

- **Separation of `parse()` and `push_packet()`**: Ingestors and `parse()` must never write to Crucible. Anything to be created or linked (`samples`, `children`, `keywords`, `thumbnails`, `scientific_metadata`) is attached to the returned `IngestionPacket` and applied only inside `push_packet()`.
- **Parsing is not purely offline**: When a dataset record for `dsid` already exists in Crucible, `parse()` reads the existing dataset, files, project users, and sessions via `nano-crucible` so user-supplied fields at dataset creation take precedence over parsed defaults.
- **Logical filename vs local path (`match_name`)**: Callers such as `ingestion-cloud-consumer` download staged files to a temporary local path (`/tmp/...`) while passing the original logical filename via `match_name` to `find_supported_ingestor(file_path, match_name=...)` and `parse(file_path, dsid=..., match_name=...)`. `is_file_supported()` matches against `match_name` (when provided) while reading bytes from `file_path`, and `packet.dataset_fields["file_to_upload"]` preserves the logical `match_name` rather than the temporary disk path.
- **Registry ordering**: `find_supported_ingestor()` returns the first class in `ingestor_list` whose `is_file_supported()` returns `True`. Specific file-format ingestors must precede general fallback ingestors.

## Validation tiers and safety boundaries

1. **Offline static and unit checks (safe by default)**:
   - `uv run pytest tests/test_push_inputs.py` and `uv run pytest tests/test_match_name.py` validate Nano model/method signatures and `match_name` handling using mocks or temp files without network or GCS access.
2. **Schema-baseline tests (`tests/test_parsed_schema.py`)**:
   - Require external scientific fixture files in `tests/data/` (`gcloud storage rsync -r gs://crucible-ingestion-test-data tests/data`) and may perform authenticated Crucible reads unless the Nano client is mocked via `set_client()`.
3. **Shared GCS test storage (`gs://crucible-ingestion-test-data`)**:
   - Pulling (`gcloud storage rsync -r gs://crucible-ingestion-test-data tests/data`) is an external read. Pushing (`gcloud storage rsync -r tests/data gs://crucible-ingestion-test-data`) or deleting objects mutates shared organization storage and requires explicit user authorization.
4. **Live packet push and `--test` mode (persistent live mutations)**:
   - `push_packet()` and `crucible-ingest --push` mutate live Crucible records across multiple API calls and can partially succeed.
   - `crucible-ingest --push --test` creates a persistent dataset record in the shared `crucible-test` project with no automatic cleanup. Treat `--test` with `--push` as a live service mutation requiring explicit user authorization.

## Adding or modifying an ingestor

1. Subclass `CrucibleDatasetIngestor` in `crucible_ingestion/ingestors/` and implement `is_file_supported(cls, file_to_upload, **kwargs)` plus the relevant `setup_data()` hooks without performing any Crucible writes.
2. Register the class in `crucible_ingestion/ingestors/registry.py` at the appropriate precedence position in `ingestor_list`.
3. When authorized to use fixture data, place a representative file in `tests/data/`, generate its baseline with `uv run python tests/make_schemas.py --file tests/data/<file>`, and inspect the resulting schema in `tests/schemas/` and entry in `tests/schema_registry.json` before committing.

## Writing and hygiene

- Never commit credentials, API keys, signed URLs, or unreviewed sensitive scientific metadata.
- Do not use em dashes in documentation, comments, or commit messages; use `-` or `--`.
