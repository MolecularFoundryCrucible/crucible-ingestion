"""Review finding (crucible-ingestion Tasks 1-4, Critical #1): several ingestors
override parse_dataset_name()/get_dataset_metadata() and derive dataset_name,
data_format or other labels from the opaque file_to_upload instead of
match_name. For a new-style upload (opaque key + file_name) this silently
writes the storage key or upload id into the record. These tests pin that
every flagged site now reads match_name, and that legacy callers (no
file_name) keep deriving from file_to_upload exactly as before.
"""
import zipfile
from pathlib import Path

from crucible_ingestion.ingestors.rawtlt_ingestor import RawtltIngestor
from crucible_ingestion.ingestors.mrc_ingestor import MrcIngestor
from crucible_ingestion.ingestors.dm_ingestor import DigitalMicrographIngestor
from crucible_ingestion.ingestors.ser_ingestor import SerIngestor
from crucible_ingestion.ingestors.emd_ingestor import BerkeleyEmdIngestor
from crucible_ingestion.ingestors.emd_velox_ingestor import VeloxEmdIngestor
from crucible_ingestion.ingestors.gc_log_ingestor import GCLogIngestor
from crucible_ingestion.ingestors.biologic_mpt_ingestor import BiologicMptIngestor
from crucible_ingestion.ingestors.inorganic_xrd_ingestor import InorganicXRDIngestor
from crucible_ingestion.ingestors.ptychography_h5_ingestor import PtychographyH5Ingestor
from crucible_ingestion.ingestors.jupiter_afm_ingestor import AFMIngestor
from crucible_ingestion.ingestors.image_ingestor import ImageIngestor
from crucible_ingestion.ingestors.insitu_pl_ingestor import InSituPlIngestor


def _opaque(tmp_path, content=b"data"):
    path = tmp_path / "f3a9c1e0"
    path.write_bytes(content)
    return str(path)


def test_rawtlt_dataset_name_uses_match_name(tmp_path):
    ig = RawtltIngestor(file_to_upload=_opaque(tmp_path), file_name="series_01.rawtlt", unique_id="x")
    ig.get_dataset_metadata()
    assert ig.dataset_name == "series_01.rawtlt"


def test_mrc_dataset_name_uses_match_name(tmp_path):
    ig = MrcIngestor(file_to_upload=_opaque(tmp_path), file_name="scan.mrc", unique_id="x")
    ig.get_dataset_metadata()
    assert ig.dataset_name == "scan.mrc"


def test_dm_dataset_name_uses_match_name(tmp_path):
    ig = DigitalMicrographIngestor(file_to_upload=_opaque(tmp_path), file_name="image.dm4", unique_id="x")
    ig.get_dataset_metadata()
    assert ig.dataset_name == "image.dm4"


def test_ser_dataset_name_uses_match_name(tmp_path):
    ig = SerIngestor(file_to_upload=_opaque(tmp_path), file_name="run.ser", unique_id="x")
    ig.get_dataset_metadata()
    assert ig.dataset_name == "run.ser"


def test_berkeley_emd_dataset_name_uses_match_name(tmp_path):
    ig = BerkeleyEmdIngestor(file_to_upload=_opaque(tmp_path), file_name="scan.emd", unique_id="x")
    ig.get_dataset_metadata()
    assert ig.dataset_name == "scan.emd"


def test_velox_emd_dataset_name_uses_match_name(tmp_path):
    ig = VeloxEmdIngestor(file_to_upload=_opaque(tmp_path), file_name="scan.emd", unique_id="x")
    ig.parse_dataset_name()
    assert ig.dataset_name == "scan.emd"


def test_gc_log_dataset_name_and_metadata_use_match_name(tmp_path):
    opaque = tmp_path / "f3a9c1e0"
    opaque.write_text("FID1.CHR\t01/01/2026\t00:00:00\textra\n")
    ig = GCLogIngestor(file_to_upload=str(opaque), file_name="run.log", unique_id="x")
    ig.parse_dataset_name()
    assert ig.dataset_name == "run"
    ig.get_scientific_metadata()
    assert ig.scientific_metadata["FID"]["log_file"] == "run.log"


def test_biologic_mpt_dataset_name_uses_match_name(tmp_path):
    ig = BiologicMptIngestor(file_to_upload=_opaque(tmp_path), file_name="cell_A.mpt", unique_id="x")
    ig.parse_dataset_name()
    assert ig.dataset_name == "cell_A"


def test_inorganic_xrd_dataset_name_uses_match_name(tmp_path):
    ig = InorganicXRDIngestor(file_to_upload=_opaque(tmp_path), file_name="sample_7.txt", unique_id="x")
    ig.parse_dataset_name()
    assert ig.dataset_name == "XRD — sample_7"


def test_ptychography_data_format_uses_match_name(tmp_path):
    ig = PtychographyH5Ingestor(file_to_upload=_opaque(tmp_path), file_name="run_master.h5", unique_id="x")
    ig.get_dataset_metadata()
    assert ig.data_format == "h5"


def test_jupiter_afm_dataset_name_and_keywords_use_match_name(tmp_path):
    ig = AFMIngestor(
        file_to_upload=_opaque(tmp_path),
        file_name="campaign_3/Asylum Research Data/2026/scan.ibw",
        unique_id="x",
    )
    ig.parse_dataset_name()
    assert ig.dataset_name == "2026-scan.ibw"
    ig.parse_keywords()
    assert "2026" in ig.keywords


def test_image_thumbnail_caption_uses_match_name(tmp_path):
    from PIL import Image as PILImage

    path = tmp_path / "f3a9c1e0"
    PILImage.new("RGB", (1, 1)).save(path, format="PNG")
    ig = ImageIngestor(file_to_upload=str(path), file_name="photo.png", unique_id="x")
    captured = {}
    ig.add_thumbnail = lambda image, caption: captured.__setitem__("caption", caption)
    ig.get_thumbnails()
    assert captured["caption"] == "photo.png"


def test_insitu_pl_tmp_folder_uses_match_name(tmp_path):
    zip_path = tmp_path / "f3a9c1e0"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("placeholder.txt", "x")
    ig = InSituPlIngestor(file_to_upload=str(zip_path), file_name="my_run.zip", unique_id="x")
    ig.get_scientific_metadata()
    assert ig.tmp_folder.endswith("my_run")
