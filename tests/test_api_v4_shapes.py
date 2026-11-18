import importlib

from crucible.models import Dataset, Sample

from crucible_ingestion.client import set_client
from crucible_ingestion.data_ingestion import flatten_dataset_record, push_file, push_packet
from crucible_ingestion.packet import IngestionPacket

data_ingestion = importlib.import_module("crucible_ingestion.data_ingestion")


def test_nested_owner_project_instrument_fill_the_flat_fields():
    record = {
        "unique_id": "ds1",
        "owner": {"unique_id": "u1", "orcid": "0000-0001-2345-6789"},
        "project": {"project_id": "MFP00001"},
        "instrument": {"instrument_id": "spinbot", "instrument_name": "SpinBot"},
    }
    flat = flatten_dataset_record(record)
    assert flat["owner_orcid"] == "0000-0001-2345-6789"
    assert flat["project_id"] == "MFP00001"
    assert flat["instrument_id"] == "spinbot"
    assert flat["instrument_name"] == "SpinBot"
    assert "owner_orcid" not in record


def test_owner_without_orcid_falls_back_to_unique_id():
    flat = flatten_dataset_record({"owner": {"unique_id": "svc1", "orcid": None}})
    assert flat["owner_orcid"] == "svc1"


def test_flat_values_from_the_server_are_kept():
    flat = flatten_dataset_record({"project_id": "MFP00002", "project": {"project_id": "MFP00003"}})
    assert flat["project_id"] == "MFP00002"


def test_missing_nested_objects_leave_fields_unset():
    assert flatten_dataset_record({"owner": None}).get("owner_orcid") is None
    assert flatten_dataset_record(None) is None


class _Recorder:
    def __init__(self):
        self.calls = []

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
        if self.name == "datasets.add_file":
            return {"associated_file": {}, "ingestion_request": {"id": 7}}
        if self.name in ("samples.list", "datasets.list_children"):
            return []
        if self.name == "datasets.create":
            return {"created_record": {"unique_id": "child1"}, "dataset_mfid": "child1",
                    "scientific_metadata_record": None, "files": []}
        return {"unique_id": "rec1"}


def test_push_file_records_the_class_at_creation_not_by_patch(monkeypatch):
    client = _Recorder()
    set_client(client)
    monkeypatch.setattr(data_ingestion, "get_ingestion_githash", lambda: "abc123")
    try:
        push_file("ds1", "/tmp/f.h5", "SomeIngestor")
    finally:
        set_client(None)

    (add_name, _, add_kwargs), (update_name, update_args, update_kwargs) = client.calls
    assert add_name == "datasets.add_file"
    assert add_kwargs["ingestion_class"] == "SomeIngestor"
    assert add_kwargs["skip_ingestion"] is True
    assert update_name == "ingestions.update"
    assert update_args == (7,)
    assert "ingestion_class" not in update_kwargs
    assert update_kwargs["ingestion_githash"] == "abc123"


def test_push_packet_creates_samples_and_children_with_owner_ref():
    packet = IngestionPacket(
        unique_id="ds1", ingestion_class="SomeIngestor",
        samples=[{"sample_name": "film", "owner_orcid": "0000-0001-2345-6789"}],
        children=[{"parent_id": "ds1", "sample_links": [], "scientific_metadata": {},
                   "dataset": {"dataset_name": "child", "owner_orcid": "0000-0001-2345-6789"}}],
    )
    client = _Recorder()
    set_client(client)
    try:
        push_packet(packet)
    finally:
        set_client(None)

    calls = {name: (args, kwargs) for name, args, kwargs in client.calls}
    (sample,), sample_kwargs = calls["samples.create"]
    assert isinstance(sample, Sample) and not sample_kwargs
    (child, _), _ = calls["datasets.create"]
    assert isinstance(child, Dataset)
    for body in (sample.model_dump(exclude_none=True), child.model_dump(exclude_none=True)):
        assert body["owner_ref"] == "0000-0001-2345-6789"
        assert "owner" not in body and "owner_orcid" not in body
    assert packet.samples[0]["owner_orcid"] == "0000-0001-2345-6789"
    assert ("datasets.link", ("ds1", "child1"), {}) in client.calls
