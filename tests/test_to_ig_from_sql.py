"""to_ig_from_sql lets an existing SQL dataset record overwrite freshly-parsed fields,
to preserve edits made after the first ingest (e.g. a user correcting instrument_name
in the UI). timestamp is an exception: the API defaults it to "now" the moment a
dataset row is created, before the file is ever parsed, so an existing SQL value is
not evidence of a real prior edit worth protecting -- unlike every other field here.

    uv run pytest tests/test_to_ig_from_sql.py
"""

from crucible_ingestion.ingestors.crucible_ingestor import CrucibleDatasetIngestor


def make_ingestor(**overrides):
    fields = dict(file_to_upload="dummy.h5", unique_id="test-unique-id")
    fields.update(overrides)
    return CrucibleDatasetIngestor(**fields)


def test_parsed_timestamp_is_not_overwritten_by_sql_placeholder():
    ig = make_ingestor(timestamp="2026-01-01T00:00:00")
    ig.to_ig_from_sql({"timestamp": "2026-09-29T12:00:00"}, ["timestamp"])
    assert ig.timestamp == "2026-01-01T00:00:00"


def test_sql_timestamp_is_used_when_ingestor_parsed_none():
    ig = make_ingestor(timestamp=None)
    ig.to_ig_from_sql({"timestamp": "2026-09-29T12:00:00"}, ["timestamp"])
    assert ig.timestamp == "2026-09-29T12:00:00"


def test_other_fields_are_still_overwritten_by_sql():
    ig = make_ingestor(instrument_name="parsed-instrument")
    ig.to_ig_from_sql({"instrument_name": "sql-instrument"}, ["instrument_name"])
    assert ig.instrument_name == "sql-instrument"
