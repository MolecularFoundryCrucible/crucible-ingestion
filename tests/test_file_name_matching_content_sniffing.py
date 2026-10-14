from crucible_ingestion.ingestors.biologic_mpt_ingestor import BiologicMptIngestor
from crucible_ingestion.ingestors.gc_log_ingestor import GCLogIngestor
from crucible_ingestion.ingestors.inorganic_xrd_ingestor import InorganicXRDIngestor


def test_biologic_mpt_rejects_opaque_path_with_wrong_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("whatever")
    ig = BiologicMptIngestor(file_to_upload=str(opaque), file_name="run.h5", unique_id="x")
    assert ig.is_file_supported() is False


def test_gc_log_rejects_opaque_path_without_matching_file_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("a\tb\tc\td\n")
    ig = GCLogIngestor(file_to_upload=str(opaque), file_name="run.txt", unique_id="x")
    assert ig.is_file_supported() is False


def test_inorganic_xrd_accepts_opaque_path_via_file_name_and_still_sniffs_content(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("header\nIntensity, cps\n")
    ig = InorganicXRDIngestor(file_to_upload=str(opaque), file_name="scan.txt", unique_id="x")
    assert ig.is_file_supported() is True
    bad_content = tmp_path / "f3a9c1e1"
    bad_content.write_text("header\nnot the right second line\n")
    ig_bad = InorganicXRDIngestor(file_to_upload=str(bad_content), file_name="scan.txt", unique_id="x")
    assert ig_bad.is_file_supported() is False
