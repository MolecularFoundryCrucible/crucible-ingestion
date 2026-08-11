#!/usr/bin/env python
"""Generate a JSON Schema of the key paths and types in each test file's ingestion packet.

These schemas are the regression check for the test suite. They record every key path a
packet carries, the type at each one, and the value it held when the schema was made.

The test module reads one schema two ways:

  loose (default)      strip the value keywords and check key paths and types only
  strict --check-values  validate as written, so a changed value fails too

Strictness is therefore chosen at test time, not here. There is one generation path so
the two modes can never disagree about what the baseline was.

Two things are never pinned, because a failure there would mean a dependency moved rather
than the code regressing: strings longer than MAX_PINNED_STRING, which is what excludes
the base64 thumbnails, and anything that was None at generation time.

Lists of plain values are pinned without regard to order. parse_keywords builds its list
from a set (crucible_ingestor.py), so element order varies between runs and pinning
positions would fail at random. Lists of objects are pinned positionally, since their
order comes from iterating the file and is stable.

Schemas are named {IngestionClass}_{first 6 of the data file's sha256}.json, and
tests/schema_registry.json records the pairing. A data file whose hash is already in the
registry is left alone, so after the initial setup the only reason to run this is to add
a schema for a newly added ingestor.

To replace an existing schema, delete its registry entry and its schema file by hand and
re-run. There is no override flag, because adopting whatever a refactor just produced as
the new expectation should take a deliberate edit.

Review the output before committing. This records what the code does today, bugs included.

    uv run python tests/make_schemas.py                         # every file in tests/data/
    uv run python tests/make_schemas.py --data-dir tests/data/team05
    uv run python tests/make_schemas.py --file tests/data/scan.emd
    uv run python tests/make_schemas.py --file scan.emd --ingestor BerkeleyEmdIngestor
"""

import argparse
import hashlib
import json
import logging
import traceback
from pathlib import Path

from crucible_ingestion.data_ingestion import parse

TESTS_DIR = Path(__file__).resolve().parent
DATA_DIR = TESTS_DIR / "data"
SCHEMA_DIR = TESTS_DIR / "schemas"
REGISTRY = TESTS_DIR / "schema_registry.json"

# Strings longer than this keep their type but not their value. Base64 thumbnails are the
# reason: their bytes shift with the PIL and matplotlib versions, so pinning them would
# fail on a dependency bump, and they would bury the schema diff under kilobytes of noise.
MAX_PINNED_STRING = 256

# Keywords the test module drops to relax a schema from strict to loose. Everything here
# is emitted only to pin values; the type and key-path structure lives elsewhere, so
# removing these leaves a working keys-and-types schema behind.
VALUE_KEYWORDS = ("const", "allOf", "minItems", "maxItems")

logger = logging.getLogger(__name__)


def _to_plain(value):
    """Unwrap the non-JSON types a packet can still hold at this point.

    children[i]['dataset'] is a pydantic Dataset rather than a dict, so packets with
    children have to be unwrapped here to be walkable at all.
    """
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (set, tuple)):
        return list(value)
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "item"):
        return value.item()
    return value


def plain_document(value):
    """Convert a packet into the plain JSON structure schema_for described.

    The test module validates through this so it normalises identically to the generator.
    That does mean a regression _to_plain papers over -- a str becoming bytes, say -- is
    invisible to both, which is the price of being able to walk a packet holding numpy
    scalars and pydantic models at all.
    """
    value = _to_plain(value)
    if isinstance(value, dict):
        return {k: plain_document(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain_document(v) for v in value]
    return value


def merge(schemas):
    """Combine schemas so the result accepts every input. Returns {} when unconstrained."""
    schemas = [s for s in schemas if s]
    if not schemas:
        return {}

    types = set()
    for schema in schemas:
        declared = schema["type"]
        types.update(declared if isinstance(declared, list) else [declared])

    merged = {"type": types.pop() if len(types) == 1 else sorted(types)}

    if merged["type"] == "object":
        properties = {}
        for schema in schemas:
            for key, sub in schema["properties"].items():
                properties[key] = merge([properties[key], sub]) if key in properties else sub
        merged["properties"] = properties
        # Required only where every element carried the key, so a heterogeneous list
        # doesn't make one element's optional field mandatory for all of them.
        merged["required"] = sorted(set.intersection(*(set(s["required"]) for s in schemas)))
        merged["additionalProperties"] = True

    elif merged["type"] == "array":
        items = merge([s["items"] for s in schemas if "items" in s])
        if items:
            merged["items"] = items

    return merged


def schema_for(value):
    value = _to_plain(value)

    if value is None:
        # Unconstrained. The key still has to be present, but a field that is None today
        # and populated tomorrow is not the kind of loss these schemas exist to catch.
        return {}

    if isinstance(value, dict):
        return {
            "type": "object",
            "properties": {k: schema_for(v) for k, v in value.items()},
            "required": sorted(value),
            "additionalProperties": True,
        }

    if isinstance(value, list):
        return schema_for_list([_to_plain(v) for v in value])

    if isinstance(value, bool):
        return {"type": "boolean", "const": value}

    if isinstance(value, (int, float)):
        # int and float are deliberately not distinguished: a count that becomes a float
        # is not a regression worth failing a build over.
        return {"type": "number", "const": value}

    if isinstance(value, str):
        if len(value) > MAX_PINNED_STRING:
            return {"type": "string"}
        return {"type": "string", "const": value}

    raise TypeError(f"no schema rule for {type(value).__name__}")


def schema_for_list(values):
    """Pin a list's length, types, and contents.

    Every element schema carries the type structure that survives into loose mode, so the
    length and value keywords layered on top can be dropped without losing the shape.
    """
    schema = {"type": "array", "minItems": len(values), "maxItems": len(values)}
    if not values:
        return schema

    if all(isinstance(v, (str, int, float, bool)) for v in values):
        # Order-insensitive: `contains` asserts the value is somewhere in the list. The
        # merged `items` alongside it is what carries the type into loose mode.
        schema["items"] = merge([schema_for(v) for v in values])
        pinned = [v for v in values
                  if not (isinstance(v, str) and len(v) > MAX_PINNED_STRING)]
        if pinned:
            schema["allOf"] = [{"contains": {"const": v}}
                               for v in sorted(set(pinned), key=repr)]
    else:
        # Positional, so a failure names the index that moved rather than reporting one
        # opaque mismatch across the whole list.
        schema["prefixItems"] = [schema_for(v) for v in values]

    return schema


def count_keys(schema):
    """Number of key paths the schema pins, for the summary line."""
    if schema.get("type") == "object":
        return sum(1 + count_keys(s) for s in schema["properties"].values())
    if schema.get("type") == "array":
        if "items" in schema:
            return count_keys(schema["items"])
        return sum(count_keys(s) for s in schema.get("prefixItems", []))
    return 0


def sha256_of(path):
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_registry():
    if not REGISTRY.exists():
        return {}
    return json.loads(REGISTRY.read_text())


def save_registry(registry):
    REGISTRY.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n")


def select_files(explicit, data_dir):
    if explicit:
        return [Path(f) for f in explicit]

    root = DATA_DIR
    if data_dir:
        root = Path(data_dir).resolve()
        if not root.is_relative_to(DATA_DIR.resolve()):
            raise SystemExit(f"--data-dir must be a subfolder of {DATA_DIR}")

    if not root.is_dir():
        raise SystemExit(f"no data directory at {root}")

    return sorted(p for p in root.iterdir()
                  if p.is_file() and not p.name.startswith("."))


def registry_name(path):
    """Path to record for a data file, relative to tests/data where possible.

    Bare filenames collide once suite subfolders hold same-named files, and the test
    runner needs a path it can resolve back to the file. A --file outside tests/data
    has no such path, so it keeps its name.
    """
    resolved, base = path.resolve(), DATA_DIR.resolve()
    if resolved.is_relative_to(base):
        return str(resolved.relative_to(base))
    return path.name


def main():
    parser = argparse.ArgumentParser(
        prog="make_schemas",
        description="Generate packet key/type schemas from the test data files.")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--file", action="append", default=[],
                        help="path to a data file; repeatable (default: everything in the data dir)")
    source.add_argument("--data-dir", dest="data_dir", default=None,
                        help="subfolder of tests/data to run over instead of tests/data itself")
    parser.add_argument("--dsid", default="xxx", help="dataset unique id to parse against")
    parser.add_argument("--ingestor", default=None,
                        help="ingestor class name (default: auto-detect)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show the ingestion logs and tracebacks")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)

    files = select_files(args.file, args.data_dir)
    if not files:
        raise SystemExit(f"no data files found in {args.data_dir or DATA_DIR}")

    SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    registry = load_registry()

    generated, unchanged, unsupported, failed = [], [], [], []
    for path in files:
        try:
            name = registry_name(path)
            file_hash = sha256_of(path)
            if file_hash in registry:
                unchanged.append(f"{name} -> {registry[file_hash]['schema']}")
                continue

            # run the crucible-ingestion parse function that would be uploaded to Crucible
            packet = parse(str(path), args.dsid, args.ingestor)
            if packet is None:
                unsupported.append(name)
                continue

            # create a schema for the output
            schema = schema_for(packet.to_dict())
            schema_name = f"{packet.ingestion_class}_{file_hash[:6]}.json"
            (SCHEMA_DIR / schema_name).write_text(
                json.dumps(schema, indent=2, sort_keys=True) + "\n")

            registry[file_hash] = {"file": name,
                                   "ingestor": packet.ingestion_class,
                                   "schema": schema_name,
                                   "dsid": args.dsid}
            generated.append(f"{name} -> {schema_name} ({count_keys(schema)} keys)")

        except Exception as err:
            # One file that won't parse must not cost every file after it its schema.
            # SpinRunIngestor, for one, raises outright when the yaml's run_id doesn't
            # match the dsid it was given.
            failed.append(f"{path.name}  {type(err).__name__}: {err}")
            if args.verbose:
                traceback.print_exc()

    save_registry(registry)

    for label, entries in (("generated", generated),
                           ("unchanged", unchanged),
                           ("not supported", unsupported),
                           ("failed", failed)):
        if entries:
            print(f"\n{label} ({len(entries)})")
            for entry in entries:
                print(f"  {entry}")

    if failed and not args.verbose:
        print("\nre-run with -v for tracebacks")

    return 1 if (failed or unsupported) else 0


if __name__ == "__main__":
    raise SystemExit(main())
