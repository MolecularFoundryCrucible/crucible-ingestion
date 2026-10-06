"""is_file_supported() runs during auto-detection against every candidate file,
including unrelated binary formats (e.g. a Nirvana .h5). It must reject those cleanly
rather than raising, or it takes down the whole auto-detect scan for every file type.

    uv run pytest tests/test_mrc_txt_is_file_supported.py
"""

import os

from crucible_ingestion.ingestors.mrc_txt_ingestor import MrcTxtIngestor


def make_ingestor(path):
    return MrcTxtIngestor(file_to_upload=path, unique_id="test-unique-id")


def test_rejects_non_txt_file_without_reading_it(tmp_path):
    binary_path = tmp_path / "example.h5"
    binary_path.write_bytes(bytes([0x89, 0x48, 0x44, 0x46, 0x90, 0x0D, 0x0A, 0x1A]))
    assert make_ingestor(str(binary_path)).is_file_supported() is False


def test_rejects_txt_file_that_is_not_valid_text(tmp_path):
    # byte 0x90 is undefined in both utf-8 and cp1252 -- the fallback decode also fails.
    bad_path = tmp_path / "not_fei.txt"
    bad_path.write_bytes(bytes([0x90, 0x0D, 0x0A]))
    assert make_ingestor(str(bad_path)).is_file_supported() is False


def test_rejects_txt_file_without_fei_header(tmp_path):
    plain_path = tmp_path / "plain.txt"
    plain_path.write_text("just some text\nnothing special\n")
    assert make_ingestor(str(plain_path)).is_file_supported() is False


def test_accepts_well_formed_fei_parameter_file(tmp_path):
    fei_path = tmp_path / "params.txt"
    fei_path.write_text("Date/Time: 01/01/26 00:00:00\n---------------\n")
    assert make_ingestor(str(fei_path)).is_file_supported() is True
