"""KratosXpsIngestor (Kratos Vision .kal XPS spectra), fully offline.

The real-instrument tests skip unless gs://crucible-ingestion-test-data/B30_XPS is
synced into tests/data/B30_XPS.

    uv run pytest tests/test_kratos_xps_ingestor.py
"""
from __future__ import annotations

import base64
import json
import shutil
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from crucible.models import Dataset
from PIL import Image

from crucible_ingestion.client import set_client
from crucible_ingestion.data_ingestion import parse, push_packet
from crucible_ingestion.ingestors.kratos_xps_ingestor import (
    B30_XPS_INSTRUMENT_ID,
    B30_XPS_INSTRUMENT_MFID,
    B30_XPS_INSTRUMENT_NAME,
    KalFormatError,
    KratosXpsIngestor,
    XPSSpectrum,
    load_kal_file,
    parse_kal,
)
from crucible_ingestion.ingestors.registry import find_supported_ingestor
from crucible_ingestion.packet import IngestionPacket

XPS_DATA = Path(__file__).parent / "data" / "B30_XPS"
REAL_S37 = XPS_DATA / "s37.kal"
requires_xps = pytest.mark.skipif(not REAL_S37.exists(), reason="B30_XPS fixtures not synced")


class FakeDatasets:
    def __init__(self, existing=None):
        self.existing = existing

    def get(self, dsid, include_metadata=True):
        return self.existing

    def list_files(self, dsid):
        return []


@pytest.fixture
def offline_client():
    set_client(SimpleNamespace(datasets=FakeDatasets()))
    yield
    set_client(None)


def opaque_copy(src, tmp_path):
    dst = tmp_path / "f3a9c1e0"
    shutil.copy(src, dst)
    return str(dst)


def opaque_text(tmp_path, text):
    path = tmp_path / "f3a9c1e0"
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_rejects_non_kal_extension(tmp_path):
    valid_text = (
        "Dataset filename = test.dset\n"
        "Object name = Cu 2p/1\n"
        "  1 Technique = F_XPS\n"
        "  12 Ordinate values = {100, 200}\n"
    )
    path = opaque_text(tmp_path, valid_text)
    assert not KratosXpsIngestor(file_to_upload=path, file_name="s.txt", unique_id="x").is_file_supported()


def test_rejects_binary_nul_bytes_with_kal_extension(tmp_path):
    path = tmp_path / "f3a9c1e0"
    path.write_bytes(b"\x00" * 100)
    assert not KratosXpsIngestor(file_to_upload=str(path), file_name="s.kal", unique_id="x").is_file_supported()


def test_rejects_malformed_kal_text(tmp_path):
    # Field before Object name
    bad_field = opaque_text(
        tmp_path,
        "Technique = XPS\nObject name = Cu 2p/1\n  12 Ordinate values = {1, 2}\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_field, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="field 'Technique' outside object"):
        load_kal_file(bad_field)

    # Stray { before Object name
    bad_open = opaque_text(
        tmp_path,
        "{\nObject name = Cu 2p/1\n  12 Ordinate values = {1, 2}\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_open, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="unexpected opening brace before object"):
        load_kal_file(bad_open)

    # Extra }
    bad_close = opaque_text(
        tmp_path,
        "Object name = Cu 2p/1\n  12 Ordinate values = {1, 2}\n}\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_close, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="unmatched closing brace"):
        load_kal_file(bad_close)

    # Unclosed { at EOF
    bad_unclosed = opaque_text(
        tmp_path,
        "Object name = Cu 2p/1\n  12 Ordinate values = {1, 2}\n  Scan Sweep OBJECT = {\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_unclosed, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="unclosed brace block"):
        load_kal_file(bad_unclosed)

    # Only non-spectrum events (Counter/1) and zero spectra
    bad_no_spectra = opaque_text(
        tmp_path,
        "Object name = Counter/1\n  37 Acquisition name = Counter\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_no_spectra, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="no XPS spectra found"):
        load_kal_file(bad_no_spectra)

    # Spectrum with < 2 ordinate values
    bad_one_point = opaque_text(
        tmp_path,
        "Object name = Cu 2p/1\n  12 Ordinate values = {100}\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_one_point, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="ordinate values must have length >= 2"):
        load_kal_file(bad_one_point)

    # Spectrum with step_size = 0
    bad_zero_step = opaque_text(
        tmp_path,
        "Object name = Cu 2p/1\n  4 Spectrum scan step size = 0 eV\n  12 Ordinate values = {100, 200}\n",
    )
    assert not KratosXpsIngestor(file_to_upload=bad_zero_step, file_name="bad.kal", unique_id="x").is_file_supported()
    with pytest.raises(KalFormatError, match="step .* must be finite with step != 0"):
        load_kal_file(bad_zero_step)


@requires_xps
def test_real_spectrum(tmp_path, offline_client):
    path = opaque_copy(REAL_S37, tmp_path)
    ig, ingestor_name = find_supported_ingestor(path, "dsid1", file_name="s37.kal")
    assert ingestor_name == "KratosXpsIngestor"

    packet = parse(path, "dsid1", "KratosXpsIngestor", file_name="s37.kal")
    df = packet.dataset_fields
    assert df["measurement"] == "XPS"
    assert df["data_type"] == "XPS Spectrum"
    assert df["instrument_name"] == B30_XPS_INSTRUMENT_NAME
    assert df["instrument_id"] == B30_XPS_INSTRUMENT_ID
    assert df["instrument_mfid"] == B30_XPS_INSTRUMENT_MFID
    assert df["dataset_name"] == "s37.kal"
    assert df["data_format"] == "kal"
    assert df["timestamp"] == "2026-07-22T23:38:57"
    assert set(df) <= set(Dataset.model_fields)
    Dataset.model_validate(df)

    md = packet.scientific_metadata
    assert md["source_file"] == "s37.kal"
    assert md["dataset_filename"] == "s37.dset"
    assert md["stage_position_name"] == "S37"
    assert md["technique"] == "XPS"
    assert md["anode_material"] == "Al Mono"
    assert md["anode_voltage_V"] == 15000
    assert md["emission_current_A"] == 0.005
    assert md["photon_energy_eV"] == 1486.69
    assert md["total_spectra"] == 5
    assert len(md["spectra_summary"]) == 5

    spectra = md["spectra"]
    assert [s["region"] for s in spectra] == ["Cu 2p", "Cu_auger", "C 1s", "O 1s", "survey"]
    for s in spectra:
        assert "x" not in s
        assert "y" not in s

    cu2p = spectra[0]
    survey = spectra[4]
    assert cu2p["binding_energy_range_eV"] == [975.0, 923.0]
    assert survey["binding_energy_range_eV"] == [1215.0, -15.0]

    assert "events" not in md
    # Validates serialization with no NaN / infinity
    json.dumps(md, allow_nan=False)

    thumbnails = packet.thumbnails
    assert [tn["caption"] for tn in thumbnails] == ["Cu 2p", "Cu_auger", "C 1s", "O 1s", "survey"]
    for tn in thumbnails:
        raw_png = base64.b64decode(tn["thumbnail"])
        img = Image.open(BytesIO(raw_png))
        assert img.format == "PNG"
        assert max(img.size) == 500 and min(img.size) > 0


def test_synthetic_depth_profile(tmp_path, offline_client):
    text = (
        "Dataset filename = depth.dset\n"
        "Dataset Comment = Depth profile test\n"
        "Object name = Sample Position/1\n"
        "  3080 Xray Reference Energy = F_REFER_TO_XRAY_MONO_AL\n"
        "  3191 NICPU X-ray Gun Active Anode Material = F_NICPU_XRAY_ANODE_STD_MONO\n"
        "  3192 NICPU X-ray Gun Emission Current = 0.005 A\n"
        "  3193 NICPU X-ray Gun Anode HT Voltage = 15000 V\n"
        "Object name = Ion Gun Etch/2\n"
        "  37 Acquisition name = Ion Gun Etch\n"
        "  Etch time = 30 seconds\n"
        "  Pre-etch delay = 5 seconds\n"
        "  Post etch delay = 2 seconds\n"
        "  NICPU Ion Gun PSU beam_ht = 2000 V\n"
        "  NICPU Ion Gun PSU Emission Current = 0.01 A\n"
        "  NICPU Ion Gun PSU Extractor Voltage = 500 V\n"
        "  NICPU Ion Gun PSU Raster Size = 2 mm\n"
        "  NICPU Ion Gun Standby Delay Time = 10 seconds\n"
        "Object name = C 1s/3\n"
        "  1 Technique = F_XPS\n"
        "  2 Scan type = F_SPECTRUM\n"
        "  3 Spectrum scan start = 1188.69 eV\n"
        "  4 Spectrum scan step size = 0.1 eV\n"
        "  5 Abscissa label = Kinetic Energy\n"
        "  6 Abscissa units = eV\n"
        "  7 Dwell time = 0.1 seconds\n"
        "  8 Ordinate label = Intensity\n"
        "  9 Ordinate units = counts\n"
        "  12 Ordinate values = {100, 200, 300, 250, 150}\n"
        "  42 Pass energy = 20 eV\n"
        "  99 # Sweeps completed = 5\n"
        "  151 Date Acquired = 26/07/23 01:00:00\n"
        "  3080 Xray Reference Energy = F_REFER_TO_XRAY_MONO_AL\n"
        "  3282 Descriptor for aperture size used in acquisition = Slot\n"
        "Object name = C 1s/4\n"
        "  1 Technique = F_XPS\n"
        "  2 Scan type = F_SPECTRUM\n"
        "  3 Spectrum scan start = 1188.69 eV\n"
        "  4 Spectrum scan step size = 0.1 eV\n"
        "  5 Abscissa label = Kinetic Energy\n"
        "  6 Abscissa units = eV\n"
        "  7 Dwell time = 0.1 seconds\n"
        "  8 Ordinate label = Intensity\n"
        "  9 Ordinate units = counts\n"
        "  12 Ordinate values = {120, 220, 320, 270, 170}\n"
        "  42 Pass energy = 20 eV\n"
        "  99 # Sweeps completed = 5\n"
        "  151 Date Acquired = 26/07/23 01:10:00\n"
        "  3080 Xray Reference Energy = F_REFER_TO_XRAY_MONO_AL\n"
        "  3282 Descriptor for aperture size used in acquisition = Slot\n"
    )
    path = opaque_text(tmp_path, text)
    packet = parse(path, "dsid_depth", file_name="depth.kal")

    events = packet.scientific_metadata.get("events")
    assert isinstance(events, list) and len(events) == 1
    ev = events[0]
    assert ev["event_type"] == "Ion Gun Etch"
    assert ev["cycle"] == 1
    assert ev["etch_time_s"] == 30
    assert ev["pre_etch_delay_s"] == 5
    assert ev["post_etch_delay_s"] == 2
    assert ev["beam_voltage_V"] == 2000
    assert ev["emission_current_A"] == 0.01
    assert ev["extractor_voltage_V"] == 500
    assert ev["raster_size_mm"] == 2
    assert ev["standby_delay_s"] == 10

    summary = packet.scientific_metadata["spectra_summary"]
    assert any(s.startswith("C 1s (2x,") for s in summary)
    assert [tn["caption"] for tn in packet.thumbnails] == ["C 1s", "C 1s (2)"]


def test_unknown_xray_source_synthetic(tmp_path, offline_client):
    text = (
        "Dataset filename = unk.dset\n"
        "Object name = Cu 2p/1\n"
        "  1 Technique = F_XPS\n"
        "  2 Scan type = F_SPECTRUM\n"
        "  3 Spectrum scan start = 500 eV\n"
        "  4 Spectrum scan step size = 0.5 eV\n"
        "  5 Abscissa label = Kinetic Energy\n"
        "  6 Abscissa units = eV\n"
        "  7 Dwell time = 0.1 seconds\n"
        "  8 Ordinate label = Intensity\n"
        "  9 Ordinate units = counts\n"
        "  12 Ordinate values = {100, 200, 300, 250, 150}\n"
        "  3080 Xray Reference Energy = F_UNKNOWN_SOURCE\n"
    )
    path = opaque_text(tmp_path, text)
    xps_file = load_kal_file(path)
    spec = xps_file.spectra[0]
    assert spec.binding_energy() is None
    spec_dict = spec.to_dict()
    assert "binding_energy_range_eV" not in spec_dict

    packet = parse(path, "dsid_unk", file_name="unk.kal")
    assert len(packet.thumbnails) == 1
    img = Image.open(BytesIO(base64.b64decode(packet.thumbnails[0]["thumbnail"])))
    assert max(img.size) == 500 and min(img.size) > 0


def test_xps_spectrum_to_dict_omits_arrays():
    spec = XPSSpectrum(
        region="C 1s",
        index=1,
        x=np.array([1000.0, 1001.0, 1002.0]),
        y=np.array([50.0, 60.0, 70.0]),
        start_energy=1000.0,
        step_size=1.0,
        abscissa_label="Kinetic Energy",
        abscissa_unit="eV",
        photon_energy_eV=1486.69,
    )
    d = spec.to_dict()
    assert "x" not in d
    assert "y" not in d
    assert "binding_energy_eV" not in d
    expected_be = [round(1486.69 - val, 6) for val in [1000.0, 1001.0, 1002.0]]
    assert d["binding_energy_range_eV"] == [expected_be[0], expected_be[-1]]
    assert d["num_points"] == 3


def test_existing_sql_dataset_reconciliation(tmp_path):
    existing_record = {
        "dataset_name": None,
        "public": None,
        "owner_orcid": None,
        "project_id": None,
        "measurement": "User Custom XPS",
        "session_name": None,
        "instrument_name": None,
        "instrument_id": None,
        "instrument_mfid": None,
        "data_type": None,
        "timestamp": "2020-01-01T00:00:00",
        "data_format": None,
        "size": None,
        "unique_id": "dsid_recon",
        "ingestion_class": None,
        "ingestion_githash": None,
        "scientific_metadata": {"custom_user_key": "user_value"},
    }
    set_client(SimpleNamespace(datasets=FakeDatasets(existing_record)))
    try:
        text = (
            "Dataset filename = recon.dset\n"
            "Object name = C 1s/1\n"
            "  1 Technique = F_XPS\n"
            "  2 Scan type = F_SPECTRUM\n"
            "  3 Spectrum scan start = 1188.69 eV\n"
            "  4 Spectrum scan step size = 0.1 eV\n"
            "  5 Abscissa label = Kinetic Energy\n"
            "  6 Abscissa units = eV\n"
            "  12 Ordinate values = {100, 200}\n"
            "  151 Date Acquired = 26/07/22 23:38:57\n"
            "  3080 Xray Reference Energy = F_REFER_TO_XRAY_MONO_AL\n"
        )
        path = opaque_text(tmp_path, text)
        packet = parse(path, "dsid_recon", file_name="recon.kal")
        assert packet.dataset_fields["measurement"] == "User Custom XPS"
        assert packet.scientific_metadata["custom_user_key"] == "user_value"
        assert packet.dataset_fields["timestamp"] == "2026-07-22T23:38:57"
    finally:
        set_client(None)


def test_push_packet_instrument_mfid_assignment():
    class FakeDatasetsClient:
        def __init__(self, updated_record):
            self.updated_record = updated_record
            self.assigned = []

        def update(self, unique_id, **kwargs):
            return dict(self.updated_record)

        def assign_instrument(self, unique_id, instrument_mfid=None):
            self.assigned.append((unique_id, instrument_mfid))
            return SimpleNamespace(
                instrument=SimpleNamespace(
                    unique_id=instrument_mfid,
                    instrument_id="b30_xps",
                    instrument_name="B30 XPS",
                )
            )

        def update_scientific_metadata(self, *args, **kwargs):
            return {}

    # 1. Dataset without instrument_mfid assigned -> calls assign_instrument
    client1 = SimpleNamespace(datasets=FakeDatasetsClient({"unique_id": "dsid1", "instrument_mfid": None}))
    set_client(client1)
    try:
        packet1 = IngestionPacket(
            unique_id="dsid1",
            file_to_upload="/tmp/test.kal",
            ingestion_class="KratosXpsIngestor",
            dataset_fields={"instrument_mfid": "0tqx1eq8b5t8h000y3zm23mwmw"},
            scientific_metadata={},
            keywords=[],
            samples=[],
            children=[],
            thumbnails=[],
        )
        push_packet(packet1)
        assert client1.datasets.assigned == [("dsid1", "0tqx1eq8b5t8h000y3zm23mwmw")]
    finally:
        set_client(None)

    # 2. Dataset with instrument_mfid already assigned -> does NOT call assign_instrument
    client2 = SimpleNamespace(datasets=FakeDatasetsClient({"unique_id": "dsid1", "instrument_mfid": "existing_mfid"}))
    set_client(client2)
    try:
        packet2 = IngestionPacket(
            unique_id="dsid1",
            file_to_upload="/tmp/test.kal",
            ingestion_class="KratosXpsIngestor",
            dataset_fields={"instrument_mfid": "0tqx1eq8b5t8h000y3zm23mwmw"},
            scientific_metadata={},
            keywords=[],
            samples=[],
            children=[],
            thumbnails=[],
        )
        push_packet(packet2)
        assert client2.datasets.assigned == []
    finally:
        set_client(None)


def test_reconciliation_existing_registered_instrument(tmp_path):
    record = {
        "unique_id": "dsid_recon_registered",
        "instrument_mfid": "0tqx_other",
        "instrument_name": "Other XPS",
        "instrument_id": None,
    }
    set_client(SimpleNamespace(datasets=FakeDatasets(record)))
    try:
        text = (
            "Dataset filename = recon.dset\n"
            "Object name = C 1s/1\n"
            "  1 Technique = F_XPS\n"
            "  12 Ordinate values = {100, 200}\n"
        )
        path = opaque_text(tmp_path, text)
        packet = parse(path, "dsid_recon_registered", file_name="recon.kal")
        df = packet.dataset_fields
        assert df["instrument_mfid"] == "0tqx_other"
        assert df["instrument_name"] == "Other XPS"
        assert df.get("instrument_id") is None
    finally:
        set_client(None)


def test_reconciliation_legacy_freetext_instrument(tmp_path):
    record = {
        "unique_id": "dsid_recon_legacy",
        "instrument_name": "Custom XPS",
        "instrument_mfid": None,
        "instrument_id": None,
    }
    set_client(SimpleNamespace(datasets=FakeDatasets(record)))
    try:
        text = (
            "Dataset filename = recon.dset\n"
            "Object name = C 1s/1\n"
            "  1 Technique = F_XPS\n"
            "  12 Ordinate values = {100, 200}\n"
        )
        path = opaque_text(tmp_path, text)
        packet = parse(path, "dsid_recon_legacy", file_name="recon.kal")
        df = packet.dataset_fields
        assert df["instrument_name"] == "Custom XPS"
        assert df.get("instrument_mfid") is None
        assert df.get("instrument_id") is None
    finally:
        set_client(None)


def test_reconciliation_unknown_instrument_placeholder(tmp_path):
    record = {
        "unique_id": "dsid_recon_unknown",
        "instrument_name": "unknown",
        "instrument_mfid": None,
        "instrument_id": None,
    }
    set_client(SimpleNamespace(datasets=FakeDatasets(record)))
    try:
        text = (
            "Dataset filename = recon.dset\n"
            "Object name = C 1s/1\n"
            "  1 Technique = F_XPS\n"
            "  12 Ordinate values = {100, 200}\n"
        )
        path = opaque_text(tmp_path, text)
        packet = parse(path, "dsid_recon_unknown", file_name="recon.kal")
        df = packet.dataset_fields
        assert df["instrument_name"] == B30_XPS_INSTRUMENT_NAME
        assert df["instrument_id"] == B30_XPS_INSTRUMENT_ID
        assert df["instrument_mfid"] == B30_XPS_INSTRUMENT_MFID
    finally:
        set_client(None)


def test_object_name_parsing_edge_cases(tmp_path):
    text = (
        "Dataset filename = objnames.dset\n"
        "Object name = Survey\n"
        "  1 Technique = F_XPS\n"
        "  12 Ordinate values = {100, 200}\n"
        "Object name = V2O5/SiO2/2\n"
        "  1 Technique = F_XPS\n"
        "  12 Ordinate values = {300, 400}\n"
        "Object name = V2O5/SiO2\n"
        "  1 Technique = F_XPS\n"
        "  12 Ordinate values = {500, 600}\n"
    )
    path = opaque_text(tmp_path, text)
    xps_file = load_kal_file(path)
    assert len(xps_file.spectra) == 3

    s1, s2, s3 = xps_file.spectra
    assert s1.region == "Survey"
    assert s1.index is None

    assert s2.region == "V2O5/SiO2"
    assert s2.index == 2

    assert s3.region == "V2O5/SiO2"
    assert s3.index is None


def test_field_extraction_edge_cases(tmp_path, offline_client):
    text = (
        "Dataset filename = fields.dset\n"
        "Object name = Position/1\n"
        "  Stage Position Name = 1 A\n"
        "Object name = C 1s/2\n"
        "  1 Technique = F_XPS\n"
        "  2 Scan type = F_SPECTRUM\n"
        "  12 Ordinate values = {100, 200}\n"
        "  Stage X Position = {1, 2}\n"
        "  Stage Y Position = 0.05 mm\n"
        "  Transition or charge state = 0\n"
        "  Descriptor for aperture size used in acquisition = 110 um\n"
    )
    path = opaque_text(tmp_path, text)
    packet = parse(path, "dsid_fields", file_name="fields.kal")
    md = packet.scientific_metadata
    assert md["stage_position_name"] == "1 A"

    spec = md["spectra"][0]
    assert spec["transition"] == "0"
    assert spec["aperture_size"] == "110 um"
    assert "x" not in spec.get("stage_position_m", {})
    assert spec["stage_position_m"]["y"] == 0.05


def test_rejects_standalone_brace_inside_object_dict(tmp_path):
    text = (
        "Object name = C 1s/1\n"
        "  1 Technique = F_XPS\n"
        "  {\n"
        "  12 Ordinate values = {100, 200}\n"
    )
    path = opaque_text(tmp_path, text)
    with pytest.raises(KalFormatError, match="unexpected standalone opening brace outside list"):
        load_kal_file(path)


def test_spectrum_recorded_in_binding_energy(tmp_path, offline_client):
    text = (
        "Dataset filename = be.dset\n"
        "Object name = C 1s/1\n"
        "  1 Technique = F_XPS\n"
        "  2 Scan type = F_SPECTRUM\n"
        "  3 Spectrum scan start = 280 eV\n"
        "  4 Spectrum scan step size = 0.1 eV\n"
        "  5 Abscissa label = Binding Energy\n"
        "  6 Abscissa units = eV\n"
        "  12 Ordinate values = {100, 200, 300}\n"
    )
    path = opaque_text(tmp_path, text)
    packet = parse(path, "dsid_be", file_name="be.kal")
    spec = packet.scientific_metadata["spectra"][0]
    assert spec["abscissa_label"] == "Binding Energy"
    assert spec["abscissa_unit"] == "eV"
    assert spec["binding_energy_range_eV"] == [280.0, 280.2]
    assert len(packet.thumbnails) == 1


def test_max_thumbnails_cap(tmp_path, offline_client):
    lines = ["Dataset filename = max_tn.dset"]
    for i in range(1, 28):
        lines.extend([
            f"Object name = Region {i}/{i}",
            "  1 Technique = F_XPS",
            "  2 Scan type = F_SPECTRUM",
            "  3 Spectrum scan start = 280 eV",
            "  4 Spectrum scan step size = 0.5 eV",
            "  5 Abscissa label = Binding Energy",
            "  6 Abscissa units = eV",
            "  12 Ordinate values = {10, 20, 30}",
        ])
    text = "\n".join(lines) + "\n"
    path = opaque_text(tmp_path, text)
    packet = parse(path, "dsid_max_tn", file_name="max_tn.kal")
    assert len(packet.scientific_metadata["spectra"]) == 27
    assert len(packet.thumbnails) == 25
