"""Parse each registered test data file and check the packet still matches its baseline.

This is the refactor guard. It checks that the resulting data packet from parse()
contains the expected keys and value types. For a stricter test you can run with the
flag --check-values to ensure that the values themselves are the same as before.

    uv run pytest tests/test_parsed_schema.py                  # key paths and types
    uv run pytest tests/test_parsed_schema.py --check-values   # values as well
    uv run pytest tests/test_parsed_schema.py -k berkeley      # one file

Every test runs once per entry in tests/schema_registry.json; the entries and the parsed
packets come from packets.py.
"""

import base64
import json
from io import BytesIO

import jsonschema
import pytest
from PIL import Image

from make_schemas import SCHEMA_DIR, VALUE_KEYWORDS, plain_document
from packets import for_each_file, packet_for

# The four keys push_packet indexes on every child.
CHILD_PUSH_FIELDS = {"dataset", "scientific_metadata", "parent_id", "sample_links"}


@for_each_file
def test_expected_ingestor_claims_the_file(entry):
    """The class that parsed the file is the one the baseline was built with."""
    assert packet_for(entry).ingestion_class == entry["ingestor"]


@for_each_file
def test_packet_matches_schema(entry, request):
    packet = packet_for(entry)
    schema = schema_for(entry, request.config.getoption("--check-values"))

    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(plain_document(packet.to_dict())),
                    key=lambda err: list(err.absolute_path))
    if not errors:
        return

    report = "\n".join(
        f"  packet{''.join(f'[{step!r}]' for step in err.absolute_path)}: {err.message}"
        for err in errors)
    pytest.fail(f"{len(errors)} difference(s) from the baseline:\n{report}")


@for_each_file
def test_packet_json_can_be_written(entry, tmp_path):
    """to_json writes the packet to disk; a value its encoder cannot serialize raises."""
    packet_for(entry).to_json(tmp_path / "packet.json")


@for_each_file
def test_thumbnails_decode(entry):
    for i, thumbnail in enumerate(packet_for(entry).thumbnails):
        assert "caption" in thumbnail, f"thumbnails[{i}] has no caption; push_packet reads it"
        image = Image.open(BytesIO(base64.b64decode(thumbnail["thumbnail"])))
        image.verify()


@for_each_file
def test_samples_can_be_looked_up(entry):
    """Every sample carries a unique_id or a sample_name for find_existing_sample."""
    for i, sample in enumerate(packet_for(entry).samples):
        assert sample.get("unique_id") or sample.get("sample_name"), \
            f"samples[{i}] has neither unique_id nor sample_name"


@for_each_file
def test_children_carry_what_push_needs(entry):
    for i, child in enumerate(packet_for(entry).children):
        missing = CHILD_PUSH_FIELDS - set(child)
        assert not missing, f"children[{i}] is missing {sorted(missing)}"


def schema_for(entry, check_values):
    path = SCHEMA_DIR / entry["schema"]
    if not path.is_file():
        pytest.fail(f"the registry lists {entry['schema']} but it is not under {SCHEMA_DIR}")

    baseline = json.loads(path.read_text())
    return baseline if check_values else relax(baseline)


def relax(node):
    """Strip the keywords that pin values, leaving key paths and types behind."""
    if isinstance(node, dict):
        return {k: relax(v) for k, v in node.items() if k not in VALUE_KEYWORDS}
    if isinstance(node, list):
        return [relax(v) for v in node]
    return node
