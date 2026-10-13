"""file_name is optional everywhere; when absent, match_name must fall back to
file_to_upload so every legacy (no file_name) call path is unaffected.
"""
from pathlib import Path

from crucible_ingestion.ingestors.crucible_ingestor import CrucibleDatasetIngestor


def test_match_name_falls_back_to_file_to_upload():
    ig = CrucibleDatasetIngestor(file_to_upload="/mnt/gcs/foo/bar.h5", unique_id="x")
    assert ig.match_name == "/mnt/gcs/foo/bar.h5"


def test_match_name_prefers_file_name_when_given():
    ig = CrucibleDatasetIngestor(
        file_to_upload="/mnt/gcs-staging/MF123/_files/abcd1234",
        file_name="scan_001.h5",
        unique_id="x",
    )
    assert ig.match_name == "scan_001.h5"


def test_dataset_name_derived_from_match_name_not_opaque_path(tmp_path):
    opaque = tmp_path / "abcd1234"
    opaque.write_bytes(b"data")
    ig = CrucibleDatasetIngestor(
        file_to_upload=str(opaque), file_name="scan_001.h5", unique_id="x"
    )
    ig.parse_dataset_name()
    assert ig.dataset_name == "scan_001.h5"


def test_data_format_derived_from_match_name_not_opaque_path(tmp_path):
    opaque = tmp_path / "abcd1234"
    opaque.write_bytes(b"data")
    ig = CrucibleDatasetIngestor(
        file_to_upload=str(opaque), file_name="scan_001.h5", unique_id="x"
    )
    ig.get_dataset_metadata()
    assert ig.data_format == "h5"


def test_data_format_still_derived_from_file_to_upload_when_no_file_name(tmp_path):
    legacy = tmp_path / "scan_001.h5"
    legacy.write_bytes(b"data")
    ig = CrucibleDatasetIngestor(file_to_upload=str(legacy), unique_id="x")
    ig.get_dataset_metadata()
    assert ig.data_format == "h5"


def test_find_supported_ingestor_threads_file_name_to_constructed_instance(tmp_path):
    """find_supported_ingestor must pass file_name through to every candidate it
    constructs -- proven here against the base class directly (is_file_supported()
    always True), since no registered ingestor yet reads match_name (that lands
    per-ingestor in later tasks; see test_file_name_matching_simple.py for the
    end-to-end case once mrc_txt_ingestor.py is updated).
    """
    from crucible_ingestion.ingestors.registry import find_supported_ingestor
    from crucible_ingestion.ingestors.crucible_ingestor import CrucibleDatasetIngestor as Base

    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("irrelevant")
    ig, cls_name = find_supported_ingestor(
        str(opaque), "dsid", ingestor_list=[Base], file_name="run.txt"
    )
    assert cls_name == "CrucibleDatasetIngestor"
    assert ig.match_name == "run.txt"


def test_registry_auto_detects_opaque_path_via_file_name_with_a_real_ingestor(tmp_path):
    """Restores the plan's original Task 1 Step 9 test, now that Task 2 has
    switched MrcTxtIngestor.is_file_supported() to match_name."""
    from crucible_ingestion.ingestors.registry import find_supported_ingestor
    from crucible_ingestion.ingestors.mrc_txt_ingestor import MrcTxtIngestor

    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("Date/Time: 01/01/26 00:00:00\n---------------\n")
    ig, cls_name = find_supported_ingestor(str(opaque), "dsid", file_name="run.txt")
    assert cls_name == "MrcTxtIngestor"
    assert isinstance(ig, MrcTxtIngestor)
