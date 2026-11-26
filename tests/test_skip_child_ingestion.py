"""Tests for skip_child_ingestion support in push_packet.

Validates that when skip_child_ingestion=True:
- Child datasets are created without the files parameter
- Child files are uploaded individually via push_file, recording provenance
- Deduplication and missing ingestion_class are handled cleanly
- Existing-child skipping and top-level include_file behavior are preserved
"""

import importlib
import pytest
from crucible.models import Dataset

from crucible_ingestion.client import reset_client, set_client
from crucible_ingestion.data_ingestion import push_packet
from crucible_ingestion.packet import IngestionPacket

data_ingestion = importlib.import_module("crucible_ingestion.data_ingestion")


class _Recorder:
    def __init__(self, children_by_parent=None, child_mfid_map=None, deduped_paths=None):
        self.calls = []
        self.children_by_parent = dict(children_by_parent or {})
        self.child_mfid_map = dict(child_mfid_map or {})
        self.deduped_paths = set(deduped_paths or ())
        self._child_counter = 10
        self._request_counter = 100

    def __getattr__(self, name):
        return _Method(self, name)


class _Method:
    def __init__(self, recorder, name):
        self.recorder, self.name = recorder, name

    def __getattr__(self, name):
        return _Call(self.recorder, f"{self.name}.{name}")


class _Call:
    def __init__(self, recorder, name):
        self.recorder, self.name = recorder, name

    def __call__(self, *args, **kwargs):
        self.recorder.calls.append((self.name, args, kwargs))

        if self.name == "datasets.update":
            dsid = args[0] if args else "0t000000000000000000000001"
            return {"unique_id": dsid}

        if self.name == "datasets.list_children":
            parent_id = args[0] if args else ""
            return self.recorder.children_by_parent.get(parent_id, [])

        if self.name == "datasets.create":
            dataset_arg = args[0] if args else None
            ds_name = getattr(dataset_arg, "dataset_name", None) if dataset_arg else None
            if ds_name and ds_name in self.recorder.child_mfid_map:
                child_id = self.recorder.child_mfid_map[ds_name]
            else:
                child_id = f"0t0000000000000000000000{self.recorder._child_counter:02d}"
                self.recorder._child_counter += 1

            return {
                "created_record": {"unique_id": child_id},
                "dataset_mfid": child_id,
                "scientific_metadata_record": None,
                "files": kwargs.get("files", []),
            }

        if self.name == "datasets.add_file":
            path = kwargs.get("file_path") or (args[1] if len(args) > 1 else None)
            if path in self.recorder.deduped_paths:
                return {"associated_file": {}, "ingestion_request": None}
            req_id = self.recorder._request_counter
            self.recorder._request_counter += 1
            return {"associated_file": {}, "ingestion_request": {"id": req_id}}

        if self.name == "ingestions.update":
            req_id = args[0] if args else 1
            return {"id": req_id}

        if self.name == "samples.list":
            return []

        return {"unique_id": "rec1"}


@pytest.fixture(autouse=True)
def clean_client():
    reset_client()
    yield
    reset_client()


def test_default_push_packet_passes_child_files_to_datasets_create():
    """Default skip_child_ingestion=False passes child files directly to datasets.create.

    Ensures backward compatibility: files are passed to datasets.create and neither
    datasets.add_file nor ingestions.update is called for child files.
    """
    client = _Recorder()
    set_client(client)
    packet = IngestionPacket(
        unique_id="0t000000000000000000000001",
        ingestion_class="ParentIngestor",
        children=[
            {
                "parent_id": "0t000000000000000000000001",
                "files_to_upload": ["/tmp/one.sur"],
                "sample_links": [],
                "scientific_metadata": {"key": "val"},
                "dataset": {
                    "dataset_name": "one.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                    "project_id": "MFP00001",
                },
            }
        ],
    )

    try:
        push_packet(packet)
    finally:
        reset_client()

    create_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.create"]
    assert len(create_calls) == 1
    _, create_kwargs = create_calls[0]
    assert create_kwargs.get("files") == ["/tmp/one.sur"]

    add_file_calls = [c for c in client.calls if c[0] == "datasets.add_file"]
    update_calls = [c for c in client.calls if c[0] == "ingestions.update"]
    assert len(add_file_calls) == 0
    assert len(update_calls) == 0


def test_skip_child_ingestion_creates_empty_child_and_pushes_files_with_provenance(monkeypatch):
    """When skip_child_ingestion=True, creates child without files and pushes via push_file."""
    client = _Recorder()
    set_client(client)
    monkeypatch.setattr(data_ingestion, "get_ingestion_githash", lambda: "deadbeef1234")

    parent_id = "0t000000000000000000000001"
    child_declared_id = "declared_c1"
    child_sample_id = "0t000000000000000000000099"
    packet = IngestionPacket(
        unique_id=parent_id,
        ingestion_class="ParentIngestor",
        children=[
            {
                "parent_id": parent_id,
                "files_to_upload": ["/tmp/one.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": [child_sample_id],
                "thumbnails": [{"thumbnail": "dGVzdA==", "caption": "CL map"}],
                "scientific_metadata": {"scan": "area"},
                "dataset": {
                    "unique_id": child_declared_id,
                    "dataset_name": "one.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                    "project_id": "MFP00001",
                },
            }
        ],
    )

    try:
        push_packet(packet, skip_child_ingestion=True)
    finally:
        reset_client()

    create_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.create"]
    assert len(create_calls) == 1
    (child_ds_arg, child_md_arg), create_kwargs = create_calls[0]
    assert isinstance(child_ds_arg, Dataset)
    assert child_md_arg == {"scan": "area"}
    assert "files" not in create_kwargs
    body = child_ds_arg.model_dump(exclude_none=True)
    assert "ingestion_class" not in body

    created_child_mfid = "0t000000000000000000000010"
    assert ("datasets.link", (parent_id, created_child_mfid), {}) in client.calls
    assert ("datasets.link_sample", (created_child_mfid, child_sample_id), {}) in client.calls
    assert ("datasets.add_thumbnail", (created_child_mfid, "dGVzdA==", "CL map"), {}) in client.calls

    add_file_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.add_file"]
    assert len(add_file_calls) == 1
    _, add_kwargs = add_file_calls[0]
    assert add_kwargs["dataset_mfid"] == created_child_mfid
    assert add_kwargs["file_path"] == "/tmp/one.sur"
    assert add_kwargs["ingestion_class"] == "SurIngestor"
    assert add_kwargs["skip_ingestion"] is True

    update_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "ingestions.update"]
    assert len(update_calls) == 1
    update_args, update_kwargs = update_calls[0]
    assert update_args == (100,)
    assert update_kwargs["status"] == "not_requested"
    assert update_kwargs["ingestion_githash"] == "deadbeef1234"


def test_skip_child_ingestion_omits_class_when_not_provided_and_handles_deduped_file():
    """Child without ingestion_class passes None, and deduped files skip ingestions.update."""
    client = _Recorder(deduped_paths={"/tmp/deduped.sur"})
    set_client(client)

    packet = IngestionPacket(
        unique_id="0t000000000000000000000001",
        ingestion_class="ParentIngestor",
        children=[
            {
                "parent_id": "0t000000000000000000000001",
                "files_to_upload": ["/tmp/deduped.sur"],
                "sample_links": [],
                "thumbnails": [],
                "scientific_metadata": {},
                "dataset": {
                    "dataset_name": "deduped.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            }
        ],
    )

    try:
        push_packet(packet, skip_child_ingestion=True)
    finally:
        reset_client()

    add_file_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.add_file"]
    assert len(add_file_calls) == 1
    _, add_kwargs = add_file_calls[0]
    assert add_kwargs["ingestion_class"] is None
    assert add_kwargs["skip_ingestion"] is True

    update_calls = [c for c in client.calls if c[0] == "ingestions.update"]
    assert len(update_calls) == 0


def test_skip_child_ingestion_multiple_files_and_resolved_ids_chain():
    """Multiple files per child are pushed, and resolved_ids chains parents correctly."""
    child1_mfid = "0t000000000000000000000011"
    child2_mfid = "0t000000000000000000000012"
    client = _Recorder(child_mfid_map={"child_a": child1_mfid, "child_b": child2_mfid})
    set_client(client)

    packet = IngestionPacket(
        unique_id="0t000000000000000000000001",
        ingestion_class="ParentIngestor",
        children=[
            {
                "parent_id": "0t000000000000000000000001",
                "files_to_upload": ["/tmp/a.sur", "/tmp/b.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": [],
                "scientific_metadata": {},
                "dataset": {
                    "unique_id": "temp_c1",
                    "dataset_name": "child_a",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            },
            {
                "parent_id": "temp_c1",
                "files_to_upload": ["/tmp/c.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": [],
                "scientific_metadata": {},
                "dataset": {
                    "dataset_name": "child_b",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            },
        ],
    )

    try:
        push_packet(packet, skip_child_ingestion=True)
    finally:
        reset_client()

    assert ("datasets.link", (child1_mfid, child2_mfid), {}) in client.calls

    add_file_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.add_file"]
    assert len(add_file_calls) == 3

    child1_pushes = [kw["file_path"] for _, kw in add_file_calls if kw["dataset_mfid"] == child1_mfid]
    child2_pushes = [kw["file_path"] for _, kw in add_file_calls if kw["dataset_mfid"] == child2_mfid]
    assert child1_pushes == ["/tmp/a.sur", "/tmp/b.sur"]
    assert child2_pushes == ["/tmp/c.sur"]


def test_skip_child_ingestion_preserves_existing_child_skip_behavior():
    """Existing children are skipped without file uploads, while resolved_ids is preserved."""
    parent_id = "0t000000000000000000000001"
    existing_child_mfid = "0t000000000000000000000021"
    downstream_child_mfid = "0t000000000000000000000022"

    client = _Recorder(
        children_by_parent={parent_id: [{"dataset_name": "child_exists.sur", "unique_id": existing_child_mfid}]},
        child_mfid_map={"child_downstream.sur": downstream_child_mfid},
    )
    set_client(client)

    packet = IngestionPacket(
        unique_id=parent_id,
        ingestion_class="ParentIngestor",
        children=[
            {
                "parent_id": parent_id,
                "files_to_upload": ["/tmp/existing.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": ["0t000000000000000000000099"],
                "thumbnails": [{"thumbnail": "dGVzdA==", "caption": "cap"}],
                "scientific_metadata": {"key": "val"},
                "dataset": {
                    "unique_id": "temp_existing",
                    "dataset_name": "child_exists.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            },
            {
                "parent_id": "temp_existing",
                "files_to_upload": ["/tmp/downstream.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": [],
                "thumbnails": [],
                "scientific_metadata": {},
                "dataset": {
                    "dataset_name": "child_downstream.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            },
        ],
    )

    try:
        push_packet(packet, skip_child_ingestion=True)
    finally:
        reset_client()

    create_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.create"]
    assert len(create_calls) == 1
    (created_ds, _), _ = create_calls[0]
    assert created_ds.dataset_name == "child_downstream.sur"

    assert ("datasets.link", (existing_child_mfid, downstream_child_mfid), {}) in client.calls

    add_file_calls = [(args, kwargs) for name, args, kwargs in client.calls if name == "datasets.add_file"]
    assert len(add_file_calls) == 1
    _, add_kw = add_file_calls[0]
    assert add_kw["dataset_mfid"] == downstream_child_mfid
    assert add_kw["file_path"] == "/tmp/downstream.sur"


def test_skip_child_ingestion_preserves_top_level_include_file_behavior():
    """Verify include_file=False vs True when skip_child_ingestion=True."""
    parent_id = "0t000000000000000000000001"
    child_mfid = "0t000000000000000000000002"

    packet1 = IngestionPacket(
        unique_id=parent_id,
        ingestion_class="ParentIngestor",
        file_to_upload="/tmp/top.sur",
        children=[
            {
                "parent_id": parent_id,
                "files_to_upload": ["/tmp/child.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": [],
                "scientific_metadata": {},
                "dataset": {
                    "dataset_name": "child.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            }
        ],
    )

    client1 = _Recorder(child_mfid_map={"child.sur": child_mfid})
    set_client(client1)
    try:
        push_packet(packet1, include_file=False, skip_child_ingestion=True)
    finally:
        reset_client()

    add_file_calls1 = [(args, kwargs) for name, args, kwargs in client1.calls if name == "datasets.add_file"]
    assert len(add_file_calls1) == 1
    assert add_file_calls1[0][1]["dataset_mfid"] == child_mfid
    assert add_file_calls1[0][1]["file_path"] == "/tmp/child.sur"

    packet2 = IngestionPacket(
        unique_id=parent_id,
        ingestion_class="ParentIngestor",
        file_to_upload="/tmp/top.sur",
        children=[
            {
                "parent_id": parent_id,
                "files_to_upload": ["/tmp/child.sur"],
                "ingestion_class": "SurIngestor",
                "sample_links": [],
                "scientific_metadata": {},
                "dataset": {
                    "dataset_name": "child.sur",
                    "owner_orcid": "0000-0001-2345-6789",
                },
            }
        ],
    )

    client2 = _Recorder(child_mfid_map={"child.sur": child_mfid})
    set_client(client2)
    try:
        push_packet(packet2, include_file=True, skip_child_ingestion=True)
    finally:
        reset_client()

    add_file_calls2 = [(args, kwargs) for name, args, kwargs in client2.calls if name == "datasets.add_file"]
    assert len(add_file_calls2) == 2
    child_upload = [kw for _, kw in add_file_calls2 if kw["dataset_mfid"] == child_mfid][0]
    parent_upload = [kw for _, kw in add_file_calls2 if kw["dataset_mfid"] == parent_id][0]
    assert child_upload["file_path"] == "/tmp/child.sur"
    assert child_upload["ingestion_class"] == "SurIngestor"
    assert parent_upload["file_path"] == "/tmp/top.sur"
    assert parent_upload["ingestion_class"] == "ParentIngestor"
