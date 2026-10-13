"""Each of these ingestors claims files by extension/suffix. With the new
signed-upload flow the real path is an opaque key and the extension only
exists in file_name — these tests pin that file_name-based classification
works, and that the legacy (file_name absent) path still works too.
"""
from crucible_ingestion.ingestors.bcf_ingestor import BcfIngestor
from crucible_ingestion.ingestors.h5_ingestor import H5Ingestor
from crucible_ingestion.ingestors.emi_ingestor import EmiIngestor
from crucible_ingestion.ingestors.mrc_ingestor import MrcIngestor
from crucible_ingestion.ingestors.autobot_spinrun_yaml_ingestor import SpinRunIngestor_10kLegacy
from crucible_ingestion.ingestors.rga_tey_batch_ingestor import RgaTeyBatchIngestor


def test_h5_ingestor_claims_opaque_path_via_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    ig = H5Ingestor(file_to_upload=str(opaque), file_name="scan.h5", unique_id="x")
    assert ig.is_file_supported() is True


def test_h5_ingestor_rejects_opaque_path_with_mismatched_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    ig = H5Ingestor(file_to_upload=str(opaque), file_name="scan.mpt", unique_id="x")
    assert ig.is_file_supported() is False


def test_h5_ingestor_still_claims_legacy_path_without_file_name(tmp_path):
    legacy = tmp_path / "scan.h5"
    legacy.touch()
    assert H5Ingestor(file_to_upload=str(legacy), unique_id="x").is_file_supported() is True


def test_emi_ingestor_uses_file_name_suffix(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    assert EmiIngestor(file_to_upload=str(opaque), file_name="run.emi", unique_id="x").is_file_supported() is True


def test_rga_tey_batch_checks_both_extension_and_substring_on_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.touch()
    ig = RgaTeyBatchIngestor(file_to_upload=str(opaque), file_name="10k_RGA_batch.zip", unique_id="x")
    assert ig.is_file_supported() is True
    ig_wrong_name = RgaTeyBatchIngestor(file_to_upload=str(opaque), file_name="other.zip", unique_id="x")
    assert ig_wrong_name.is_file_supported() is False
