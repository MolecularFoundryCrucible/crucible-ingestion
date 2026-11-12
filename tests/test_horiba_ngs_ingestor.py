"""HoribaNgsIngestor (LabSpec 6 .ngs Raman spectra), fully offline.

The real-instrument tests skip unless gs://crucible-ingestion-test-data/B30_Raman is
synced into tests/data/B30_Raman.

    uv run pytest tests/test_horiba_ngs_ingestor.py
"""
import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from crucible_ingestion.client import set_client
from crucible_ingestion.data_ingestion import parse
from crucible_ingestion.ingestors.horiba_ngs_ingestor import HoribaNgsIngestor, NgsFormatError, parse_ngs
from crucible_ingestion.ingestors.registry import find_supported_ingestor

RAMAN = Path(__file__).parent / "data" / "B30_Raman"
SPECTRUM = RAMAN / "CFO158_6h600C_43.ngs"
UNCORRECTED = RAMAN / "CFO160_6h600C_2_49.ngs"
requires_raman = pytest.mark.skipif(not SPECTRUM.exists(), reason="B30_Raman fixtures not synced")


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


def existing_dataset(scientific_metadata):
    record = {k: None for k in ('dataset_name', 'public', 'owner_orcid', 'project_id',
                                'measurement', 'session_name', 'instrument_name',
                                'data_type', 'timestamp', 'data_format', 'size',
                                'unique_id', 'ingestion_class', 'ingestion_githash')}
    record['scientific_metadata'] = scientific_metadata
    return record


def opaque(tmp_path, data):
    path = tmp_path / "f3a9c1e0"
    path.write_bytes(data)
    return str(path)


def test_rejects_other_extensions(tmp_path):
    path = opaque(tmp_path, b"NGSNextGen")
    assert not HoribaNgsIngestor(file_to_upload=path, file_name="s.txt", unique_id="x").is_file_supported()


def test_rejects_files_without_ngs_header(tmp_path):
    path = opaque(tmp_path, b"\x00" * 200)
    assert not HoribaNgsIngestor(file_to_upload=path, file_name="s.ngs", unique_id="x").is_file_supported()


@requires_raman
def test_rejects_truncated_file(tmp_path):
    path = opaque(tmp_path, SPECTRUM.read_bytes()[:5000])
    assert not HoribaNgsIngestor(file_to_upload=path, file_name="s.ngs", unique_id="x").is_file_supported()


@requires_raman
def test_rejects_multidimensional_data():
    data = bytearray(SPECTRUM.read_bytes())
    dims_at = data.index(b"CFO158_6h600C_43") + len("CFO158_6h600C_43") + 20
    assert data[dims_at] == 1
    data[dims_at] = 2
    with pytest.raises(NgsFormatError, match="not a single spectrum"):
        parse_ngs(bytes(data))


@requires_raman
def test_real_spectrum(tmp_path, offline_client):
    path = opaque(tmp_path, SPECTRUM.read_bytes())
    assert find_supported_ingestor(path, "x", file_name="spec.ngs")[1] == "HoribaNgsIngestor"
    packet = parse(path, "dsid1", "HoribaNgsIngestor", file_name="spec.ngs")
    md = packet.scientific_metadata

    assert md["laser_wavelength_nm"] == 532.06
    assert md["exposure_time_s"] == 60.0
    assert md["accumulation"] == "1 x 9"
    assert md["grating"] == "1800"
    assert md["objective"] == "x50"
    assert md["raman_shift_range_cm-1"] == [150.0, 1500.0]
    assert list(md["raman_spectra"]) == ["spec.ngs"]
    entry = md["raman_spectra"]["spec.ngs"]
    assert entry["spectrum"] == {"title": "CFO158_6h600C_43", "n_points": 8586, "x_unit": "1/cm",
                                 "y_unit": "Cnt", "x_start": 150.0, "x_end": 1500.0}
    acq = entry["acquisition"]
    assert len(acq) == 20
    assert acq["Exposition"] == {"value": "60", "unit": "sec"}
    assert acq["Laser"] == {"value": "532.06", "unit": "nm"}
    assert acq["X"] == {"value": "4483.3", "unit": "µm"}
    assert acq["Spec. width"] == {"value": "3.84291", "unit": "1/cm"}
    assert acq["Detector"] == {"value": "Synapse CCD", "unit": None}
    assert entry["labspec_info"]["Date"] == "11/03/21 15:01:25"
    assert entry["history"] == [{"time": "2021-11-03T22:01:25Z", "operation": "Acquired"},
                             {"time": "2021-11-03T22:46:35Z", "operation": "Manual:Correct"}]

    assert packet.dataset_fields["timestamp"] == "2021-11-03T15:01:25"
    assert packet.dataset_fields["measurement"] == "Raman spectroscopy"
    assert packet.dataset_fields["data_type"] == "Raman Spectrum"
    assert len(packet.thumbnails) == 1
    json.dumps(md, allow_nan=False)


@requires_raman
def test_spectrum_with_single_history_entry(tmp_path, offline_client):
    path = opaque(tmp_path, UNCORRECTED.read_bytes())
    packet = parse(path, "dsid1", "HoribaNgsIngestor", file_name="spec.ngs")
    assert packet.scientific_metadata["raman_spectra"]["spec.ngs"]["history"] == \
        [{"time": "2021-11-03T23:12:19Z", "operation": "Acquired"}]
    assert packet.dataset_fields["timestamp"] == "2021-11-03T16:12:19"


@requires_raman
@pytest.mark.parametrize("path", sorted(RAMAN.glob("*.ngs")), ids=lambda p: p.name)
def test_every_fixture_parses(path):
    ngs = parse_ngs(path.read_bytes())
    assert ngs["title"] == path.stem
    assert len(ngs["intensities"]) == 8586
    assert ngs["acquisition"]["Instrument"]["value"] == "LabRam"


@requires_raman
def test_second_spectrum_joins_existing_dataset(tmp_path):
    set_client(SimpleNamespace(datasets=FakeDatasets(existing_dataset({
        "exposure_time_s": 30.0,
        "raman_spectra": {"first.ngs": {"spectrum": {"title": "first"}}},
    }))))
    try:
        path = opaque(tmp_path, SPECTRUM.read_bytes())
        packet = parse(path, "dsid1", "HoribaNgsIngestor", file_name="second.ngs")
    finally:
        set_client(None)
    md = packet.scientific_metadata
    assert set(md["raman_spectra"]) == {"first.ngs", "second.ngs"}
    # values already on the dataset (user-entered or from an earlier file) win
    assert md["exposure_time_s"] == 30.0
    assert md["laser_wavelength_nm"] == 532.06
