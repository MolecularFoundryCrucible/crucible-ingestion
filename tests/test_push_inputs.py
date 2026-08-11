"""Check that a parsed packet is shaped the way the crucible client expects.

push_packet hands the packet straight to the client: dataset_fields become the body of
datasets.update, each sample becomes samples.create(**fields), each child becomes
Dataset(**child_ds). Each test below takes one of those hand-offs and validates the
packet against what the client accepts -- the pydantic model where there is one, the
method signature where there is not.

Nothing is written. Building a packet does still read from Crucible, because parse()
looks the dataset up before parsing.

    uv run pytest tests/test_push_inputs.py
    uv run pytest tests/test_push_inputs.py -k berkeley      # one file

Both models set extra='allow', so an undeclared key is not rejected by the client -- it
is carried into the request body and refused by the API. Key names are therefore checked
separately from value types.
"""

import inspect
import json

import pytest
from crucible.models import Dataset, Sample
from crucible.resources.datasets import DatasetOperations
from crucible.resources.samples import SampleOperations
from crucible.utils import is_base64

from packets import for_each_file, packet_for

# push_packet reads these two off a sample to decide what to link, and does not forward
# them to samples.create.
PUSH_ONLY_SAMPLE_KEYS = {"parent_ids", "link_to_dataset"}

# Every client method push_packet calls, with how many arguments it passes positionally
# and the keyword names it passes.
CALL_SITES = [
    (DatasetOperations, "update", 1, ()),
    (DatasetOperations, "create", 2, ()),
    (DatasetOperations, "list_children", 1, ()),
    (DatasetOperations, "add_sample", 0, ("dataset_id", "sample_id")),
    (DatasetOperations, "link_parent_child", 0, ("parent_dataset_id", "child_dataset_id")),
    (DatasetOperations, "add_thumbnail", 3, ()),
    (DatasetOperations, "add_keyword", 2, ()),
    (DatasetOperations, "update_scientific_metadata", 2, ("overwrite",)),
    (SampleOperations, "get", 1, ()),
    (SampleOperations, "list", 0, ("sample_name", "project_id")),
    (SampleOperations, "create", 0, ()),
    (SampleOperations, "link", 0, ("parent_id", "child_id")),
]


@pytest.mark.parametrize("owner, method, positional, keywords", CALL_SITES,
                         ids=[f"{o.__name__}.{m}" for o, m, _, _ in CALL_SITES])
def test_call_sites_match_client_signatures(owner, method, positional, keywords):
    """The method still exists on the client and still accepts the arguments push_packet
    binds to it."""
    call = getattr(owner, method, None)
    assert call is not None, f"push_packet calls {owner.__name__}.{method}, which no longer exists"

    # None stands in for self, since the attribute is read off the class.
    inspect.signature(call).bind(None, *range(positional),
                                 **{name: None for name in keywords})


@for_each_file
def test_dataset_fields_fit_the_dataset_model(entry):
    fields = packet_for(entry).dataset_fields
    assert not undeclared(Dataset, fields), \
        f"datasets.update would send {undeclared(Dataset, fields)}, which Dataset does not declare"
    Dataset.model_validate(fields)


@for_each_file
def test_sample_fields_fit_the_sample_model(entry):
    for i, sample in enumerate(packet_for(entry).samples):
        fields = {k: v for k, v in sample.items() if k not in PUSH_ONLY_SAMPLE_KEYS}
        assert not undeclared(Sample, fields), \
            f"samples[{i}] carries {undeclared(Sample, fields)}, which Sample does not declare"
        Sample(**fields)


@for_each_file
def test_children_build_a_dataset(entry):
    for i, child in enumerate(packet_for(entry).children):
        fields = child["dataset"]
        assert not undeclared(Dataset, fields), \
            f"children[{i}]['dataset'] carries {undeclared(Dataset, fields)}, " \
            f"which Dataset does not declare"
        Dataset(**fields)


@for_each_file
def test_ids_are_strings(entry):
    """Every id push_packet interpolates into a request path is a non-empty string."""
    packet = packet_for(entry)
    assert_id(packet.unique_id, "unique_id")

    for i, sample in enumerate(packet.samples):
        if sample.get("unique_id") is not None:
            assert_id(sample["unique_id"], f"samples[{i}]['unique_id']")
        for j, parent in enumerate(sample.get("parent_ids", [])):
            assert_id(parent, f"samples[{i}]['parent_ids'][{j}]")

    for i, child in enumerate(packet.children):
        assert_id(child["parent_id"], f"children[{i}]['parent_id']")
        for j, sample_id in enumerate(child["sample_links"]):
            assert_id(sample_id, f"children[{i}]['sample_links'][{j}]")


@for_each_file
def test_request_bodies_are_json_serializable(entry):
    """Every dict that becomes a request body survives json.dumps, as requests calls it."""
    packet = packet_for(entry)
    bodies = {"dataset_fields": packet.dataset_fields,
              "scientific_metadata": packet.scientific_metadata}
    for i, sample in enumerate(packet.samples):
        bodies[f"samples[{i}]"] = {k: v for k, v in sample.items()
                                   if k not in PUSH_ONLY_SAMPLE_KEYS}
    for i, child in enumerate(packet.children):
        bodies[f"children[{i}]['dataset']"] = child["dataset"]
        bodies[f"children[{i}]['scientific_metadata']"] = child["scientific_metadata"]

    for label, body in bodies.items():
        try:
            json.dumps(body)
        except TypeError as err:
            pytest.fail(f"{label} cannot be sent as JSON: {err}")


@for_each_file
def test_thumbnails_take_the_base64_branch(entry):
    for i, thumbnail in enumerate(packet_for(entry).thumbnails):
        assert is_base64(thumbnail["thumbnail"]), \
            f"thumbnails[{i}] is not base64, so add_thumbnail would hand it to " \
            f"data2thumbnail as an image object or a file path"
        assert isinstance(thumbnail["caption"], str), \
            f"thumbnails[{i}]['caption'] becomes the thumbnail_name"


def undeclared(model, fields):
    """The keys the model does not declare."""
    return sorted(set(fields) - set(model.model_fields))


def assert_id(value, label):
    assert isinstance(value, str) and value.strip(), \
        f"{label} is {value!r}; it goes into a request path"
