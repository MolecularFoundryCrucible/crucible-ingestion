"""FeiSemTifIngestor and BrukerEdxIngestor, fully offline.

Synthetic FEI TIFFs always run; the real-instrument tests skip unless the fixtures from
gs://crucible-ingestion-test-data are synced into tests/data/.

    uv run pytest tests/test_fei_sem_and_bruker_edx_ingestors.py
"""
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import tifffile

from crucible_ingestion.client import set_client
from crucible_ingestion.data_ingestion import parse
from crucible_ingestion.ingestors.bruker_edx_ingestor import BrukerEdxIngestor
from crucible_ingestion.ingestors.fei_sem_ingestor import FeiSemTifIngestor, vacuum_level
from crucible_ingestion.ingestors.image_ingestor import TifIngestor
from crucible_ingestion.ingestors.registry import find_supported_ingestor

DATA = Path(__file__).parent / "data"
REAL_FEI_TIF = DATA / "tmpewufz11q.tif"
REAL_SPX = DATA / "EDX_SiN-TiA-CFO_window3.spx"
REAL_RTX_SPECTRA = DATA / "EDX_spectra_data_evaluation.rtx"
REAL_RTX_MAPS = DATA / "EDX_data_with_maps.rtx"


def requires(path):
    return pytest.mark.skipif(not path.exists(), reason=f"fixture {path.name} not synced")


def opaque_copy(src, tmp_path):
    dst = tmp_path / "f3a9c1e0"
    shutil.copy(src, dst)
    return str(dst)

FEI_TEXT = ("[User]\r\nDate=05/23/2023\r\nTime=02:15:31 PM\r\n"
            "[Beam]\r\nHV=5000\r\nSpot=3.0\r\n"
            "[EBeam]\r\nHV=5000\r\nEmissionCurrent=1.5e-10\r\nSpot=4\r\n"
            "[Vacuum]\r\nChPressure=0.000123\r\nUserMode=Low vacuum\r\n"
            "[Stage]\r\nTilt=nan\r\n")


def write_tif(path, fei_text=FEI_TEXT):
    extratags = [(34682, 's', 0, fei_text, True)] if fei_text else []
    tifffile.imwrite(path, np.zeros((32, 32), np.uint8), extratags=extratags)
    return str(path)


class FakeDatasets:
    def __init__(self, existing):
        self.existing = existing

    def get(self, dsid, include_metadata=True):
        return self.existing

    def list_files(self, dsid):
        return []


@pytest.fixture
def fake_client():
    def install(existing=None):
        set_client(SimpleNamespace(datasets=FakeDatasets(existing)))
    yield install
    set_client(None)


def existing_dataset(**overrides):
    record = {k: None for k in ('dataset_name', 'public', 'owner_orcid', 'project_id',
                                'measurement', 'session_name', 'instrument_name',
                                'data_type', 'timestamp', 'data_format', 'size',
                                'unique_id', 'ingestion_class', 'ingestion_githash')}
    record['scientific_metadata'] = {}
    record.update(overrides)
    return record


# ---------- FEI SEM ----------

def test_fei_tif_claimed_via_file_name_on_opaque_key(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0")
    assert FeiSemTifIngestor(file_to_upload=path, file_name="img.tif", unique_id="x").is_file_supported()


def test_plain_tif_is_left_to_generic_tif_ingestor(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0", fei_text=None)
    assert not FeiSemTifIngestor(file_to_upload=path, file_name="img.tif", unique_id="x").is_file_supported()
    _, name = find_supported_ingestor(path, "x", file_name="img.tif")
    assert name == "TifIngestor"


def test_auto_detect_keeps_generic_tif_ingestor_for_fei_tiffs(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0")
    _, name = find_supported_ingestor(path, "x", file_name="img.tif")
    assert name == "TifIngestor"


def test_explicit_ingestion_class_wins_despite_later_registry_position(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0")
    ig, name = find_supported_ingestor(path, "x", "FeiSemTifIngestor", file_name="img.tif")
    assert name == "FeiSemTifIngestor" and isinstance(ig, FeiSemTifIngestor)


def test_non_tif_file_name_is_rejected_without_reading(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0")
    assert not FeiSemTifIngestor(file_to_upload=path, file_name="img.png", unique_id="x").is_file_supported()


@pytest.mark.parametrize("mode,expected", [("High vacuum", "High"), ("Low vacuum", "Low"),
                                           ("ESEM", "ESEM"), ("Wet mode", "ESEM"),
                                           ("Environmental", "ESEM")])
def test_vacuum_level_mapping(mode, expected):
    assert vacuum_level(mode) == expected


def test_fei_metadata_layout_and_summary_keys(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0")
    ig = FeiSemTifIngestor(file_to_upload=path, file_name="area1_001.tif", unique_id="x")
    assert ig.is_file_supported()
    ig.get_scientific_metadata()
    md = ig.scientific_metadata

    assert md["vacuum_level"] == "Low"
    assert md["spot_size"] == "3.0"
    assert md["high_voltage_V"] == "5000"
    assert md["emission_current_A"] == "1.5e-10"
    assert float(md["chamber_pressure_Torr"]) == pytest.approx(0.000123 * 0.00750062)
    assert md["tiff_metadata"]["area1_001.tif"]["Vacuum"]["ChPressure"] == 0.000123
    fei = md["tiff_metadata"]["area1_001.tif"]
    assert fei["EBeam"]["Spot"] == 4
    assert fei["Stage"]["Tilt"] is None
    assert not any(k.isdigit() or k in ("ImageWidth", "ImageLength") for k in md)
    json.dumps(md, allow_nan=False)


def test_fei_summary_key_omitted_when_source_missing(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0", fei_text="[User]\r\nDate=05/23/2023\r\n[EBeam]\r\nHV=2000\r\n")
    ig = FeiSemTifIngestor(file_to_upload=path, file_name="a.tif", unique_id="x")
    assert ig.is_file_supported()
    ig.get_scientific_metadata()
    assert ig.scientific_metadata["high_voltage_V"] == "2000"
    assert "vacuum_level" not in ig.scientific_metadata
    assert "spot_size" not in ig.scientific_metadata


def test_new_dataset_gets_acquisition_timestamp_and_thumbnail(tmp_path, fake_client):
    fake_client(existing=None)
    path = write_tif(tmp_path / "f3a9c1e0")
    packet = parse(path, "dsid1", "FeiSemTifIngestor", file_name="a.tif")
    assert packet.ingestion_class == "FeiSemTifIngestor"
    assert packet.dataset_fields["timestamp"] == "2023-05-23T14:15:31"
    assert packet.dataset_fields["measurement"] == "Scanning electron microscope"
    assert packet.dataset_fields["data_type"] == "SEM Images"
    assert len(packet.thumbnails) == 1


@pytest.mark.parametrize("existing", ["2026-11-11T13:05:00.123456-08:00",
                                      "2020-01-01T00:00:00",
                                      "not a date"])
def test_acquisition_time_overwrites_existing_timestamp(tmp_path, fake_client, existing):
    fake_client(existing_dataset(timestamp=existing))
    path = write_tif(tmp_path / "f3a9c1e0")
    packet = parse(path, "dsid1", "FeiSemTifIngestor", file_name="a.tif")
    assert packet.dataset_fields["timestamp"] == "2023-05-23T14:15:31"


def test_24_hour_fei_time_is_parsed(tmp_path, fake_client):
    fake_client(existing=None)
    path = write_tif(tmp_path / "f3a9c1e0", fei_text="[User]\r\nDate=01/15/2024\r\nTime=19:00:00\r\n")
    packet = parse(path, "dsid1", "FeiSemTifIngestor", file_name="a.tif")
    assert packet.dataset_fields["timestamp"] == "2024-01-15T19:00:00"


def test_unparseable_acquisition_keeps_existing_timestamp(tmp_path, fake_client):
    fake_client(existing_dataset(timestamp="2026-11-11T13:05:00-08:00"))
    path = write_tif(tmp_path / "f3a9c1e0", fei_text="[User]\r\nDate=garbage\r\nTime=10:00:00\r\n")
    packet = parse(path, "dsid1", "FeiSemTifIngestor", file_name="a.tif")
    assert packet.dataset_fields["timestamp"] == "2026-11-11T13:05:00-08:00"


def test_user_values_and_other_files_survive_merge(tmp_path, fake_client):
    fake_client(existing_dataset(
        measurement="Scanning electron microscope",
        instrument_name="b30 - FEI SEM",
        scientific_metadata={"vacuum_level": "ESEM",
                             "tiff_metadata": {"earlier.tif": {"User": {"Date": "x"}}}}))
    path = write_tif(tmp_path / "f3a9c1e0")
    packet = parse(path, "dsid1", "FeiSemTifIngestor", file_name="a.tif")
    md = packet.scientific_metadata
    assert md["vacuum_level"] == "ESEM"
    assert md["spot_size"] == "3.0"
    assert set(md["tiff_metadata"]) == {"earlier.tif", "a.tif"}
    assert packet.dataset_fields["instrument_name"] == "b30 - FEI SEM"


def test_generic_tif_ingestor_unchanged(tmp_path):
    path = write_tif(tmp_path / "f3a9c1e0", fei_text=None)
    ig = TifIngestor(file_to_upload=path, file_name="a.tif", unique_id="x")
    ig.get_scientific_metadata()
    assert "ImageWidth" in ig.scientific_metadata


# ---------- real FEI TIFF ----------

@requires(REAL_FEI_TIF)
def test_real_fei_tif(tmp_path, fake_client):
    fake_client(existing=None)
    path = opaque_copy(REAL_FEI_TIF, tmp_path)
    assert find_supported_ingestor(path, "x", file_name="sem.tif")[1] == "TifIngestor"
    packet = parse(path, "dsid1", "FeiSemTifIngestor", file_name="sem.tif")
    md = packet.scientific_metadata
    assert {k: md[k] for k in ("vacuum_level", "spot_size", "high_voltage_V")} == \
        {"vacuum_level": "High", "spot_size": "3", "high_voltage_V": "10000"}
    assert float(md["chamber_pressure_Torr"]) == pytest.approx(0.000587242 * 0.00750062)
    assert "PrivateFei" in md["tiff_metadata"]["sem.tif"]
    assert packet.dataset_fields["timestamp"] == "2026-06-18T11:02:18"
    assert len(packet.thumbnails) == 1
    json.dumps(md, allow_nan=False)


# ---------- Bruker EDX ----------

def test_bruker_rejects_other_extensions(tmp_path):
    path = tmp_path / "f3a9c1e0"
    path.write_text("<TRTSpectrum/>")
    assert not BrukerEdxIngestor(file_to_upload=str(path), file_name="s.xml", unique_id="x").is_file_supported()


def test_bruker_rejects_non_xml_bytes(tmp_path):
    path = tmp_path / "f3a9c1e0"
    path.write_bytes(b"\x00\x01 not xml")
    assert not BrukerEdxIngestor(file_to_upload=str(path), file_name="s.spx", unique_id="x").is_file_supported()


def test_bruker_rejects_xml_without_spectra(tmp_path):
    path = tmp_path / "f3a9c1e0"
    path.write_text("<TRTProject><ClassInstance Type='TRTProject'/></TRTProject>")
    assert not BrukerEdxIngestor(file_to_upload=str(path), file_name="p.rtx", unique_id="x").is_file_supported()


@requires(REAL_SPX)
def test_real_spx(tmp_path, fake_client):
    fake_client(existing=None)
    path = opaque_copy(REAL_SPX, tmp_path)
    assert find_supported_ingestor(path, "x", file_name="window3.spx")[1] == "BrukerEdxIngestor"
    packet = parse(path, "dsid1", "BrukerEdxIngestor", file_name="window3.spx")
    md = packet.scientific_metadata
    assert md["primary_energy_keV"] == "10.0"
    spectrum = md["edx_spectra"]["SiN-TiA-CFO_window3.txt"]
    assert spectrum["elements"] == ["Cu", "Fe", "O", "N"]
    assert spectrum["live_time_s"] == pytest.approx(122.817)
    assert spectrum["n_channels"] == 4096
    assert packet.dataset_fields["timestamp"] == "2022-03-23T12:40:48"
    assert packet.dataset_fields["measurement"] == "Energy-dispersive X-ray spectroscopy"
    assert len(packet.thumbnails) == 1
    json.dumps(md, allow_nan=False)


@requires(REAL_RTX_SPECTRA)
def test_real_rtx_project_with_several_spectra(tmp_path, fake_client):
    fake_client(existing_dataset(timestamp="2026-11-12T09:00:00-08:00"))
    path = opaque_copy(REAL_RTX_SPECTRA, tmp_path)
    packet = parse(path, "dsid1", "BrukerEdxIngestor", file_name="evaluation.rtx")
    spectra = packet.scientific_metadata["edx_spectra"]
    assert list(spectra) == ["Center_Spectrum 1", "Center_Spectrum 2", "Grey_Spectrum 1",
                             "Grey_Spectrum 2", "ring_Spectrum 1", "ring_Spectrum 1 (2)",
                             "reference_spectrum"]
    # the earliest spectrum in the project overwrites the upload time
    assert packet.dataset_fields["timestamp"] == "2026-07-30T16:39:10"
    assert len(packet.thumbnails) == 7


@requires(REAL_RTX_MAPS)
def test_real_rtx_project_with_maps(tmp_path, fake_client):
    fake_client(existing=None)
    path = opaque_copy(REAL_RTX_MAPS, tmp_path)
    packet = parse(path, "dsid1", "BrukerEdxIngestor", file_name="maps.rtx")
    spectra = packet.scientific_metadata["edx_spectra"]
    assert list(spectra) == ["Map"]
    assert spectra["Map"]["elements"] == ["Au", "Cu", "Ti", "Si", "C"]
    assert packet.scientific_metadata["primary_energy_keV"] == "10.0"
