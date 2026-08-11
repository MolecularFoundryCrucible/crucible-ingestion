"""The parsed packet for each registered test data file.

Every entry in tests/schema_registry.json pairs a data file with the schema
make_schemas.py generated from it. The test modules run once per entry, so this module
holds the entry list, the parametrize mark that applies it, and the parsed packets
themselves. Adding coverage for an ingestor means adding a data file and generating its
schema, not editing a test.
"""

import pytest

from crucible_ingestion.data_ingestion import parse
from make_schemas import DATA_DIR, load_registry

ENTRIES = sorted(load_registry().values(), key=lambda e: e["file"])

if not ENTRIES:
    pytest.skip("no baselines yet; run 'uv run python tests/make_schemas.py'",
                allow_module_level=True)

# The test id is the data file, so `-k berkeley` selects by file and a failure names the
# file it came from.
for_each_file = pytest.mark.parametrize("entry", ENTRIES,
                                        ids=[e["file"] for e in ENTRIES])

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
        # No ingestor is forced, so which class claims the file is part of what the
        # tests check.
        return parse(str(path), entry["dsid"])
    except Exception as err:
        return err
