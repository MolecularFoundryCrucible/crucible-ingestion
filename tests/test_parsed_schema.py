"""Parse each registered test data file and check the packet still matches its baseline.

This is the refactor guard. It checks that the resulting data packet from parse()
contains the expected keys and value types. For a stricter test you can run with the
flag --check-values to ensure that the values themselves are the same as before.

    uv run pytest tests/test_dry_run_parse.py                  # key paths and types
    uv run pytest tests/test_dry_run_parse.py --check-values   # values as well
    uv run pytest tests/test_dry_run_parse.py -k berkeley      # one file

Every test runs once per entry in tests/schema_registry.json. Each entry pairs a data
file with the schema make_schemas.py generated from it. Adding coverage for an ingestor
means adding a data file and generating its schema, not editing this file.
"""

import base64
import json
from io import BytesIO

import jsonschema
import pytest
from PIL import Image

from crucible_ingestion.data_ingestion import parse
from make_schemas import (DATA_DIR, SCHEMA_DIR, VALUE_KEYWORDS, load_registry,
                          plain_document)

# push_packet indexes all four of these directly, so a child missing any of them is a
# KeyError at push time rather than a validation failure.
CHILD_PUSH_FIELDS = {"dataset", "scientific_metadata", "parent_id", "sample_links"}

ENTRIES = sorted(load_registry().values(), key=lambda e: e["file"])

if not ENTRIES:
    pytest.skip("no baselines yet; run 'uv run python tests/make_schemas.py'",
                allow_module_level=True)

# Applied to every test below. The test id is the data file, so `-k berkeley` selects by
# file and a failure names the file it came from.
for_each_file = pytest.mark.parametrize("entry", ENTRIES,
                                        ids=[e["file"] for e in ENTRIES])


@for_each_file
def test_expected_ingestor_claims_the_file(entry):
    """find_supported_ingestor returns the first class in registry.py that claims a file,
    so inserting a broader ingestor above a narrower one silently reroutes parsing."""
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
    """cli.py writes this before anything is pushed, so a packet that won't serialize
    cannot be inspected in a dry run at all."""
    packet_for(entry).to_json(tmp_path / "packet.json")


@for_each_file
def test_thumbnails_decode(entry):
    for i, thumbnail in enumerate(packet_for(entry).thumbnails):
        assert "caption" in thumbnail, f"thumbnails[{i}] has no caption; push_packet reads it"
        image = Image.open(BytesIO(base64.b64decode(thumbnail["thumbnail"])))
        image.verify()


@for_each_file
def test_samples_can_be_looked_up(entry):
    """find_existing_sample needs one or the other to decide create-or-reuse."""
    for i, sample in enumerate(packet_for(entry).samples):
        assert sample.get("unique_id") or sample.get("sample_name"), \
            f"samples[{i}] has neither unique_id nor sample_name"


@for_each_file
def test_children_carry_what_push_needs(entry):
    for i, child in enumerate(packet_for(entry).children):
        missing = CHILD_PUSH_FIELDS - set(child)
        assert not missing, f"children[{i}] is missing {sorted(missing)}"


_packets = {}


def packet_for(entry):
    """The packet parse produces for this entry, parsed once and reused.

    Parsing an .emd or .h5 is slow enough that rebuilding it for each test would dominate
    the run, so packets are cached for the session -- failures included, to keep an
    unparseable file to one attempt rather than one per test.
    """
    if entry["file"] not in _packets:
        _packets[entry["file"]] = build(entry)

    built = _packets[entry["file"]]
    if isinstance(built, BaseException):
        pytest.fail(f"parse raised {type(built).__name__}: {built}")
    if built is None:
        pytest.fail(f"no ingestor claims {entry['file']}, "
                    f"but the baseline was built with {entry['ingestor']}")
    return built


def build(entry):
    """Parse one file, returning the exception rather than raising so it can be cached."""
    path = DATA_DIR / entry["file"]
    if not path.is_file():
        return FileNotFoundError(
            f"the registry lists {entry['file']} but it is not under {DATA_DIR}")
    try:
        # No ingestor is forced: which class claims the file is part of what these tests
        # pin, since find_supported_ingestor returns the first match and the order in
        # registry.py is hand-maintained.
        return parse(str(path), entry["dsid"])
    except Exception as err:
        return err


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
