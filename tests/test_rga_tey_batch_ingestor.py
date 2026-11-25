import unittest.mock
import zipfile
import pytest
import requests

from crucible_ingestion.client import reset_client, set_client
from crucible_ingestion.data_ingestion import parse
from crucible_ingestion.ingestors.registry import find_supported_ingestor
from crucible_ingestion.ingestors.rga_tey_batch_ingestor import RgaTeyBatchIngestor


def _create_zip(zip_path, file_contents_map):
    with zipfile.ZipFile(zip_path, "w") as zf:
        for archive_name, content in file_contents_map.items():
            zf.writestr(archive_name, content)


def _make_mock_client(samples=None):
    fake_client = unittest.mock.MagicMock()
    mock_resp = requests.Response()
    mock_resp.status_code = 404
    fake_client.datasets.get.side_effect = requests.exceptions.HTTPError(response=mock_resp)
    if samples is None:
        samples = [
            {"sample_name": "TF003580", "unique_id": "0t000000000000000000003580"},
            {"sample_name": "TF003581", "unique_id": "0t000000000000000000003581"},
        ]
    fake_client.samples.list.return_value = samples
    return fake_client


def test_opaque_file_to_upload_with_nested_subfolder(tmp_path):
    opaque_path = tmp_path / "8b8df98bd75c482fab4f5243a1591ca7"
    folder = "2026-07-01_10k_RGA000059"
    readout_1 = (
        "sample spot\tsample_name\tx\ty\n"
        "A1\tTF003580\t10.0\t20.0\n"
        "A2\tTF003581\t15.0\t25.0\n"
        "A3\tTF999999\t20.0\t30.0\n"
    )
    readout_2 = (
        "sample spot\tsample_name\tx\ty\n"
        "A1\tTF003580\t10.0\t20.0\n"
    )
    _create_zip(
        opaque_path,
        {
            f"{folder}/sample_holder_position_readout_RGA000059.txt": readout_1,
            f"{folder}/sample_holder_position_readout_RGA000059_2.txt": readout_2,
        },
    )

    fake_client = _make_mock_client()
    set_client(fake_client)
    try:
        packet = parse(
            str(opaque_path),
            dsid="0t000000000000000000000001",
            ingestion_class="RgaTeyBatchIngestor",
            file_name="2026-07-01_10k_RGA000059.zip",
        )
        assert packet is not None
        assert packet.ingestion_class == "RgaTeyBatchIngestor"
        assert packet.dataset_fields["dataset_name"] == "2026-07-01_10k_RGA000059.zip"
        assert packet.dataset_fields["data_format"] == "zip"

        samples = packet.scientific_metadata["samples"]
        assert "A1" in samples
        assert "A2" in samples
        assert "A3" in samples
        assert samples["A1"]["sample_id"] == "0t000000000000000000003580"
        assert samples["A2"]["sample_id"] == "0t000000000000000000003581"
        assert samples["A3"]["sample_id"] is None
    finally:
        reset_client()


def test_auto_detection_with_opaque_path(tmp_path):
    opaque_path = tmp_path / "8b8df98bd75c482fab4f5243a1591ca7"
    opaque_path.touch()

    ig, cls_name = find_supported_ingestor(
        str(opaque_path),
        "0t000000000000000000000001",
        file_name="2026-07-01_10k_RGA000059.zip",
    )
    assert cls_name == "RgaTeyBatchIngestor"
    assert isinstance(ig, RgaTeyBatchIngestor)


def test_single_subdirectory_fallback_when_zip_stem_differs(tmp_path):
    opaque_path = tmp_path / "9c9ea09ce86d593fbc5f6354b2602db8"
    readout = (
        "sample spot\tsample_name\tx\ty\n"
        "A1\tTF003580\t10.0\t20.0\n"
    )
    _create_zip(
        opaque_path,
        {
            "original_folder_name/sample_holder_position_readout_RGA000060.txt": readout,
            "__MACOSX/._foo": "apple_double_content",
        },
    )

    fake_client = _make_mock_client()
    set_client(fake_client)
    try:
        packet = parse(
            str(opaque_path),
            dsid="0t000000000000000000000001",
            ingestion_class="RgaTeyBatchIngestor",
            file_name="renamed_10k_RGA000060.zip",
        )
        assert packet is not None
        assert packet.ingestion_class == "RgaTeyBatchIngestor"
        assert packet.dataset_fields["dataset_name"] == "renamed_10k_RGA000060.zip"
        assert packet.dataset_fields["data_format"] == "zip"

        samples = packet.scientific_metadata["samples"]
        assert "A1" in samples
        assert samples["A1"]["sample_id"] == "0t000000000000000000003580"
    finally:
        reset_client()


def test_missing_readout_raises_wildcard_error_message(tmp_path):
    opaque_path = tmp_path / "11111111111111111111111111111111"
    _create_zip(
        opaque_path,
        {
            "2026-07-01_10k_RGA000061/unrelated_file.txt": "not a readout\n",
        },
    )

    fake_client = _make_mock_client()
    set_client(fake_client)
    try:
        with pytest.raises(FileNotFoundError) as exc_info:
            parse(
                str(opaque_path),
                dsid="0t000000000000000000000001",
                ingestion_class="RgaTeyBatchIngestor",
                file_name="2026-07-01_10k_RGA000061.zip",
            )
        assert "sample_holder_position_readout_*.txt" in str(exc_info.value)
    finally:
        reset_client()


def test_conflicting_spot_readouts_raise_value_error(tmp_path):
    opaque_path = tmp_path / "22222222222222222222222222222222"
    folder = "2026-07-01_10k_RGA000062"
    readout_1 = (
        "sample spot\tsample_name\tx\ty\n"
        "A1\tTF003580\t10.0\t20.0\n"
    )
    readout_2 = (
        "sample spot\tsample_name\tx\ty\n"
        "A1\tTF003581\t10.0\t20.0\n"
    )
    _create_zip(
        opaque_path,
        {
            f"{folder}/sample_holder_position_readout_RGA000062_part1.txt": readout_1,
            f"{folder}/sample_holder_position_readout_RGA000062_part2.txt": readout_2,
        },
    )

    fake_client = _make_mock_client()
    set_client(fake_client)
    try:
        with pytest.raises(ValueError) as exc_info:
            parse(
                str(opaque_path),
                dsid="0t000000000000000000000001",
                ingestion_class="RgaTeyBatchIngestor",
                file_name="2026-07-01_10k_RGA000062.zip",
            )
        assert "Conflicting readouts" in str(exc_info.value)
    finally:
        reset_client()


def test_dotted_zip_stem_matches_nested_subfolder_with_sibling_dir(tmp_path):
    opaque_path = tmp_path / "33333333333333333333333333333333"
    folder = "BL12.0.1.2_10k_RGA000063"
    readout = (
        "sample spot\tsample_name\tx\ty\n"
        "A1\tTF003580\t10.0\t20.0\n"
    )
    _create_zip(
        opaque_path,
        {
            f"{folder}/sample_holder_position_readout_RGA000063.txt": readout,
            "extra_sibling_dir/notes.log": "extra\n",
        },
    )

    fake_client = _make_mock_client()
    set_client(fake_client)
    try:
        packet = parse(
            str(opaque_path),
            dsid="0t000000000000000000000001",
            ingestion_class="RgaTeyBatchIngestor",
            file_name="BL12.0.1.2_10k_RGA000063.zip",
        )
        assert packet is not None
        assert packet.scientific_metadata["samples"]["A1"]["sample_id"] == "0t000000000000000000003580"
    finally:
        reset_client()
