"""Comprehensive offline tests for Nirvana holder splitting, exclusion handling,
child metadata, thumbnails, and split-file re-ingestion safety.
"""

import os
from unittest.mock import MagicMock
import h5py
import numpy as np
import pytest
from PIL import Image

from crucible.utils import is_base64
from crucible_ingestion.client import set_client, reset_client
from crucible_ingestion.data_ingestion import parse
from crucible_ingestion.ingestors.scope_foundry_ingestors import (
    NirvanaMultiPosLineScanIngestor,
    NirvanaMultiPosLineScanIngestor_10kLegacy,
    NirvanaMultiPosSpecRunIngestor,
)


@pytest.fixture(autouse=True)
def clean_client_boundary():
    reset_client()
    yield
    reset_client()


def create_synthetic_linescan_h5(path, positions, sample_attrs=None):
    """Create a synthetic HDF5 file for NirvanaMultiPosLineScanIngestor."""
    sample_attrs = sample_attrs or {}
    with h5py.File(path, "w") as f:
        f.attrs["unique_id"] = "0t000000000000000000000000"
        f.attrs["time_id"] = 1700000000

        # ScopeFoundry settings
        app_grp = f.create_group("app")
        hw_grp = f.create_group("hardware/mf_crucible_nirvana/settings")
        hw_grp.attrs["orcid"] = "0000-0000-0000-0001"
        hw_grp.attrs["project"] = "MFP12345 (Project Name)"
        hw_grp.attrs["tags"] = "tag1, tag2"
        hw_grp.attrs["session_name"] = "session_test"

        meas_grp = f.create_group("measurement/pollux_oospec_multipos_line_scan")
        meas_grp.create_dataset("wavelengths", data=np.linspace(400, 800, 40))
        meas_grp.create_group("settings")

        pos_root = meas_grp.create_group("positions")
        for pos in positions:
            p_grp = pos_root.create_group(pos)
            s_attr = sample_attrs.get(pos, {})
            p_grp.attrs["integration_time"] = 1.2
            p_grp.attrs["x_center"] = 5.0
            p_grp.attrs["y_center"] = 10.0
            p_grp.attrs["x_positions"] = np.array([5.0, 5.5])
            p_grp.attrs["y_positions"] = np.array([10.0, 10.5])
            p_grp.attrs["sample_uuid"] = s_attr.get("sample_uuid", "")
            p_grp.attrs["sample_name"] = s_attr.get("sample_name", "")
            p_grp.attrs["tray_uuid"] = s_attr.get("tray_uuid", "")
            p_grp.attrs["tray_name"] = s_attr.get("tray_name", "")

            # Intensity datasets for thumbnail generation
            p_grp.create_dataset("raw_intensities", data=np.ones((2, 40)) * 500.0)
            p_grp.create_dataset("dark_intensities", data=np.ones(40) * 10.0)
            p_grp.create_dataset("blank_intensities", data=np.ones(40) * 1000.0)


def create_synthetic_specrun_h5(
    path, positions, sample_attrs=None, measure_uvvis=True, measure_pl=True
):
    """Create a synthetic HDF5 file for NirvanaMultiPosSpecRunIngestor."""
    sample_attrs = sample_attrs or {}
    with h5py.File(path, "w") as f:
        f.attrs["unique_id"] = "0t000000000000000000000000"
        f.attrs["time_id"] = 1700000000

        hw_grp = f.create_group("hardware/mf_crucible_nirvana/settings")
        hw_grp.attrs["orcid"] = "0000-0000-0000-0001"
        hw_grp.attrs["project"] = "MFP12345"
        hw_grp.attrs["tags"] = "tag1"
        hw_grp.attrs["session_name"] = "session_test"

        meas_grp = f.create_group("measurement/pollux_multipos_spec_run")
        settings_grp = meas_grp.create_group("settings")
        settings_grp.attrs["measure_uvvis"] = measure_uvvis
        settings_grp.attrs["measure_pl"] = measure_pl

        if measure_uvvis:
            uv_grp = meas_grp.create_group("uvvis")
            uv_grp.create_dataset("wavelengths", data=np.linspace(400, 800, 40))
            uv_pos_root = uv_grp.create_group("positions")
            for pos in positions:
                p_grp = uv_pos_root.create_group(pos)
                s_attr = sample_attrs.get(pos, {})
                p_grp.attrs["integration_time"] = 1.0
                p_grp.attrs["x_center"] = 5.0
                p_grp.attrs["y_center"] = 10.0
                p_grp.attrs["x_positions"] = np.array([5.0, 5.5])
                p_grp.attrs["y_positions"] = np.array([10.0, 10.5])
                p_grp.attrs["sample_uuid"] = s_attr.get("sample_uuid", "")
                p_grp.attrs["sample_name"] = s_attr.get("sample_name", "")
                p_grp.attrs["tray_uuid"] = s_attr.get("tray_uuid", "")
                p_grp.attrs["tray_name"] = s_attr.get("tray_name", "")

                p_grp.create_dataset("raw_intensities", data=np.ones((2, 40)) * 500.0)
                p_grp.create_dataset("dark_intensities", data=np.ones(40) * 10.0)
                p_grp.create_dataset("blank_intensities", data=np.ones(40) * 1000.0)

        if measure_pl:
            pl_grp = meas_grp.create_group("pl")
            pl_grp.create_dataset("wavelengths", data=np.linspace(400, 800, 40))
            pl_grp.create_dataset("dark_intensities", data=np.ones(40) * 10.0)
            pl_grp.create_dataset("reference_intensities", data=np.ones(40) * 50.0)
            pl_pos_root = pl_grp.create_group("positions")
            for pos in positions:
                p_grp = pl_pos_root.create_group(pos)
                s_attr = sample_attrs.get(pos, {})
                p_grp.attrs["integration_time"] = 2.0
                p_grp.attrs["x_center"] = 6.0
                p_grp.attrs["y_center"] = 12.0
                p_grp.attrs["pl_x_center"] = 6.0
                p_grp.attrs["pl_y_center"] = 12.0
                p_grp.attrs["sample_uuid"] = s_attr.get("sample_uuid", "")
                p_grp.attrs["sample_name"] = s_attr.get("sample_name", "")
                p_grp.attrs["tray_uuid"] = s_attr.get("tray_uuid", "")
                p_grp.attrs["tray_name"] = s_attr.get("tray_name", "")

                p_grp.create_dataset("raw_intensities", data=np.ones((2, 40)) * 300.0)


def build_mock_client(get_return=None):
    mock = MagicMock()
    mock.datasets.get.return_value = get_return or {"unique_id": "0t000000000000000000000000"}
    mock.datasets.list_files.return_value = []
    mock.projects.get.return_value = {"project_id": "MFP12345"}
    mock.samples.get.return_value = None
    mock.samples.list.return_value = []
    return mock


def test_linescan_from_file_mode(tmp_path):
    h5_file = tmp_path / "test_pollux_oospec_multipos_line_scan.h5"
    tray_mfid = "0t00000000000000000000000t"
    tf1_mfid = "0t000000000000000000000001"
    tf2_mfid = "0t000000000000000000000002"

    sample_attrs = {
        "000": {
            "sample_uuid": tf1_mfid,
            "sample_name": "TF_1",
            "tray_uuid": tray_mfid,
            "tray_name": "Tray_1",
        },
        "001": {
            "sample_uuid": tf2_mfid,
            "sample_name": "TF_2",
            "tray_uuid": tray_mfid,
            "tray_name": "Tray_1",
        },
        "002": {
            "sample_uuid": "not_an_mfid",
            "sample_name": "Unassigned",
            "tray_uuid": tray_mfid,
            "tray_name": "Tray_1",
        },
    }
    create_synthetic_linescan_h5(h5_file, ["000", "001", "002"], sample_attrs)

    mock_client = build_mock_client({"unique_id": "0t000000000000000000000000"})
    set_client(mock_client)

    packet = parse(str(h5_file), "0t000000000000000000000000", cleanup=False)
    assert packet is not None

    # Check tray link to parent
    trays = [s for s in packet.samples if s["unique_id"] == tray_mfid]
    assert len(trays) == 1
    assert trays[0]["link_to_dataset"] is True

    # Check thin films
    tf_samples = [s for s in packet.samples if s["unique_id"] in (tf1_mfid, tf2_mfid)]
    assert len(tf_samples) == 2
    for tf in tf_samples:
        assert tf["link_to_dataset"] is False
        assert tf["parent_ids"] == [tray_mfid]

    # Check invalid sample was skipped
    assert not any(s["unique_id"] == "not_an_mfid" for s in packet.samples)

    # Check children: exactly 2 children (000 and 001)
    assert len(packet.children) == 2
    for child in packet.children:
        assert child["scientific_metadata"]["position"] in ("000", "001")
        assert len(child["files_to_upload"]) == 1
        split_path = child["files_to_upload"][0]
        assert os.path.exists(split_path)
        with h5py.File(split_path, "r") as sf:
            pos_keys = list(sf["measurement/pollux_oospec_multipos_line_scan/positions"].keys())
            assert len(pos_keys) == 1

        # Check thumbnails
        thumbs = child.get("thumbnails", [])
        assert len(thumbs) == 1
        assert is_base64(thumbs[0]["thumbnail"])
        assert "Absorbance Spectrum" in thumbs[0]["caption"]


def test_linescan_from_holders_mode(tmp_path):
    h5_file = tmp_path / "test_pollux_oospec_multipos_line_scan.h5"
    tray_mfid = "0t00000000000000000000000t"
    tf1_mfid = "0t000000000000000000000001"
    tf3_mfid = "0t000000000000000000000003"

    # HDF5 file has placeholder / empty sample attrs
    sample_attrs = {
        "000": {"sample_uuid": "", "sample_name": ""},
        "001": {"sample_uuid": "", "sample_name": ""},
        "002": {"sample_uuid": "", "sample_name": ""},
    }
    create_synthetic_linescan_h5(h5_file, ["000", "001", "002"], sample_attrs)

    parent_record = {
        "unique_id": "0t000000000000000000000000",
        "scientific_metadata": {
            "sample_positions": {
                "S01": {
                    "sample_uuid": tf1_mfid,
                    "sample_name": "TF_A1",
                    "tray_uuid": tray_mfid,
                },
                "S03": {
                    "sample_uuid": tf3_mfid,
                    "sample_name": "TF_A3",
                    "tray_uuid": tray_mfid,
                },
            },
            "tray_uuids": [tray_mfid],
            "skipped_positions": ["S03"],
        },
    }
    mock_client = build_mock_client(parent_record)
    set_client(mock_client)

    packet = parse(str(h5_file), "0t000000000000000000000000", cleanup=False)
    assert packet is not None

    # Verify tray is linked to parent
    trays = [s for s in packet.samples if s["unique_id"] == tray_mfid]
    assert len(trays) == 1
    assert trays[0]["link_to_dataset"] is True

    # S01 should produce child dataset for TF_A1
    # S02 was unassigned, S03 was skipped
    assert len(packet.children) == 1
    child = packet.children[0]
    assert child["sample_links"] == [tf1_mfid]
    assert child["scientific_metadata"]["position"] == "000"
    assert "TF_A1" in child["dataset"]["dataset_name"]


def test_linescan_split_reingestion_safety(tmp_path):
    h5_file = tmp_path / "test_pollux_oospec_multipos_line_scan.h5"
    tf2_mfid = "0t000000000000000000000002"
    sample_attrs = {
        "000": {"sample_uuid": "0t000000000000000000000001", "sample_name": "TF1"},
        "001": {"sample_uuid": tf2_mfid, "sample_name": "TF2"},
    }
    create_synthetic_linescan_h5(h5_file, ["000", "001"], sample_attrs)

    mock_client = build_mock_client({"unique_id": "0t000000000000000000000000"})
    set_client(mock_client)

    # First parse parent to generate split files
    packet = parse(str(h5_file), "0t000000000000000000000000", cleanup=False)
    assert len(packet.children) == 2

    child2 = [c for c in packet.children if c["sample_links"] == [tf2_mfid]][0]
    split_h5 = child2["files_to_upload"][0]

    # Re-ingest the split child file as child dataset S02
    child_mfid = "0t0000000000000000000000c2"
    child_record = {
        "unique_id": child_mfid,
        "scientific_metadata": {
            "position": "S02",
        },
    }
    mock_child_client = build_mock_client(child_record)
    set_client(mock_child_client)

    child_packet = parse(split_h5, child_mfid, cleanup=False)
    assert child_packet is not None

    # No grandchild recursion!
    assert child_packet.children == []
    assert child_packet.scientific_metadata["position"] == "S02"
    assert "integration_time" in child_packet.scientific_metadata
    assert len(child_packet.thumbnails) == 1
    assert "Absorbance Spectrum (S02)" in child_packet.thumbnails[0]["caption"]


def test_specrun_from_file_and_holders_modes(tmp_path):
    h5_file = tmp_path / "test_pollux_multipos_spec_run.h5"
    tray_mfid = "0t00000000000000000000000t"
    tf1_mfid = "0t000000000000000000000001"
    tf2_mfid = "0t000000000000000000000002"

    sample_attrs = {
        "000": {
            "sample_uuid": tf1_mfid,
            "sample_name": "TF_1",
            "tray_uuid": tray_mfid,
            "tray_name": "Tray_1",
        },
        "001": {
            "sample_uuid": tf2_mfid,
            "sample_name": "TF_2",
            "tray_uuid": tray_mfid,
            "tray_name": "Tray_1",
        },
    }
    create_synthetic_specrun_h5(
        h5_file, ["000", "001"], sample_attrs, measure_uvvis=True, measure_pl=True
    )

    mock_client = build_mock_client({"unique_id": "0t000000000000000000000000"})
    set_client(mock_client)

    packet = parse(str(h5_file), "0t000000000000000000000000", cleanup=False)
    assert packet is not None
    assert len(packet.children) == 2

    for child in packet.children:
        assert "uvvis_integration_time" in child["scientific_metadata"]
        assert "pl_integration_time" in child["scientific_metadata"]
        assert child["scientific_metadata"]["position"] in ("000", "001")
        # UVVis and PL thumbnails both present
        assert len(child["thumbnails"]) == 2
        captions = [t["caption"] for t in child["thumbnails"]]
        assert any("UVVis Absorbance" in c for c in captions)
        assert any("PL Intensity" in c for c in captions)

    # Test "From holders" mode with SpecRun
    holders_record = {
        "unique_id": "0t000000000000000000000000",
        "scientific_metadata": {
            "sample_positions": {
                "S01": {"sample_uuid": tf1_mfid, "sample_name": "TF_A1", "tray_uuid": tray_mfid},
            },
            "trays": [{"unique_id": tray_mfid, "sample_name": "Holder Tray"}],
            "skipped_positions": ["001"],
        },
    }
    mock_holders_client = build_mock_client(holders_record)
    set_client(mock_holders_client)

    holders_packet = parse(str(h5_file), "0t000000000000000000000000", cleanup=False)
    assert len(holders_packet.children) == 1
    assert holders_packet.children[0]["sample_links"] == [tf1_mfid]


def test_specrun_split_reingestion_safety(tmp_path):
    h5_file = tmp_path / "test_pollux_multipos_spec_run.h5"
    tf2_mfid = "0t000000000000000000000002"
    sample_attrs = {
        "000": {"sample_uuid": "0t000000000000000000000001", "sample_name": "TF1"},
        "001": {"sample_uuid": tf2_mfid, "sample_name": "TF2"},
    }
    create_synthetic_specrun_h5(
        h5_file, ["000", "001"], sample_attrs, measure_uvvis=True, measure_pl=True
    )

    mock_client = build_mock_client({"unique_id": "0t000000000000000000000000"})
    set_client(mock_client)

    packet = parse(str(h5_file), "0t000000000000000000000000", cleanup=False)
    child2 = [c for c in packet.children if c["sample_links"] == [tf2_mfid]][0]
    split_h5 = child2["files_to_upload"][0]

    child_mfid = "0t0000000000000000000000c2"
    child_record = {
        "unique_id": child_mfid,
        "scientific_metadata": {
            "position": "S02",
        },
    }
    mock_child_client = build_mock_client(child_record)
    set_client(mock_child_client)

    child_packet = parse(split_h5, child_mfid, cleanup=False)
    assert child_packet is not None

    # No grandchild recursion!
    assert child_packet.children == []
    assert child_packet.scientific_metadata["position"] == "S02"
    assert "uvvis_integration_time" in child_packet.scientific_metadata
    assert "pl_integration_time" in child_packet.scientific_metadata
    assert len(child_packet.thumbnails) == 2


def test_linescan_legacy_10k_behavior(tmp_path):
    h5_file = tmp_path / "test_pollux_oospec_multipos_line_scan.h5"
    sample_attrs = {
        "000": {"sample_uuid": "0t000000000000000000000001", "sample_name": "TF1"},
        "001": {"sample_uuid": "0t000000000000000000000002", "sample_name": "TF2"},
    }
    create_synthetic_linescan_h5(h5_file, ["000", "001"], sample_attrs)

    mock_client = build_mock_client({"unique_id": "0t000000000000000000000000"})
    set_client(mock_client)

    packet = parse(
        str(h5_file),
        "0t000000000000000000000000",
        ingestion_class="NirvanaMultiPosLineScanIngestor_10kLegacy",
        cleanup=False,
    )
    assert packet is not None
    assert packet.children == []
    assert len(packet.samples) == 2
