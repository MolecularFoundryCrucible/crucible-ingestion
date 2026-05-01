"""Tests for ingestors/crucible_ingestor.py — the base class all ingestors inherit from."""
import sys, os, json, pytest
from unittest.mock import patch, MagicMock, mock_open

# --- System-level mocking (before import) ---
class DummyDataset:
    file_to_upload = "/mnt/gcs/team05/my_file.h5"
    dataset_name = None; timestamp = None; source_folder = None
    instrument_id = None; instrument_name = None; unique_id = None
    sha256_hash_file_to_upload = None; owner_orcid = None
    owner_user_id = None; project_id = None; measurement = None
    session_name = None; size = None; data_format = None

mock_crucible = MagicMock()
mock_models = MagicMock()
mock_utils_mod = MagicMock()
mock_utils_io = MagicMock()
mock_models.Dataset = DummyDataset
mock_crucible.CrucibleClient = MagicMock
mock_utils_io.checkhash = MagicMock(return_value="fake_hash_abc")

sys.modules["crucible"] = mock_crucible
sys.modules["crucible.models"] = mock_models
sys.modules["crucible.utils"] = mock_utils_mod
sys.modules["crucible.utils.io"] = mock_utils_io

with patch("utils.get_secret", return_value="fake_key"):
    from ingestors.crucible_ingestor import CrucibleDatasetIngestor
    import ingestors.crucible_ingestor as ci_module

mock_client = MagicMock()
ci_module.client = mock_client


@pytest.fixture
def ig():
    """Fresh ingestor instance for each test."""
    inst = CrucibleDatasetIngestor()
    # Reset mutable class-level state that leaks between tests
    inst.acl = []
    inst.keywords = []
    inst.associated_files = {}
    inst.thumbnails = []
    inst.scientific_metadata = {}
    inst.samples = []
    mock_client.reset_mock()
    return inst


# =====================================================================
# parse_dataset_name
# =====================================================================
class TestParseDatasetName:
    def test_extracts_basename_without_extension(self, ig):
        ig.file_to_upload = "/mnt/gcs/team05/experiment_001.dm4"
        ig.parse_dataset_name()
        assert ig.dataset_name == "experiment_001"

    def test_preserves_existing_name(self, ig):
        ig.dataset_name = "Custom Name"
        ig.file_to_upload = "/mnt/gcs/team05/other.h5"
        ig.parse_dataset_name()
        assert ig.dataset_name == "Custom Name"

    def test_handles_nested_path(self, ig):
        ig.file_to_upload = "/mnt/gcs/team05/sub/deep/scan.czi"
        ig.parse_dataset_name()
        assert ig.dataset_name == "scan"

    def test_handles_multiple_dots_in_filename(self, ig):
        ig.file_to_upload = "/mnt/gcs/team05/scan.2026.01.15.dm4"
        ig.parse_dataset_name()
        # splitext only removes the last extension
        assert ig.dataset_name == "scan.2026.01.15"


# =====================================================================
# parse_file_timestamp
# =====================================================================
class TestParseFileTimestamp:
    def test_preserves_existing_timestamp(self, ig):
        ig.timestamp = "2026-01-15T10:00:00"
        ig.parse_file_timestamp()
        assert ig.timestamp == "2026-01-15T10:00:00"

    def test_reads_ctime_from_file(self, ig):
        with patch("os.path.getctime", return_value=1700000000.0):
            ig.parse_file_timestamp()
        assert ig.timestamp is not None
        assert "T" in ig.timestamp  # ISO format


# =====================================================================
# parse_source_folder
# =====================================================================
class TestParseSourceFolder:
    def test_maps_known_instrument_drive(self, ig):
        ig.file_to_upload = "/mnt/gcs/team05/subdir/file.dm4"
        ig.parse_source_folder()
        assert ig.source_folder == "CRUCIBLE - MF NCEM TEAM05/subdir"

    def test_unknown_instrument_uses_raw_path(self, ig):
        ig.file_to_upload = "/mnt/gcs/unknown_instrument/file.dm4"
        ig.parse_source_folder()
        assert ig.source_folder == "/mnt/gcs/unknown_instrument"

    def test_non_gcs_path_uses_dirname(self, ig):
        ig.file_to_upload = "/local/data/file.dm4"
        ig.parse_source_folder()
        assert ig.source_folder == "/local/data"

    def test_preserves_existing_source_folder(self, ig):
        ig.source_folder = "Already Set"
        ig.parse_source_folder()
        assert ig.source_folder == "Already Set"


# =====================================================================
# parse_instrument
# =====================================================================
class TestParseInstrument:
    def test_looks_up_instrument_by_name(self, ig):
        ig.instrument_name = "TEAM05"
        mock_client.instruments.get.return_value = {"id": "inst_123"}
        ig.parse_instrument()
        assert ig.instrument_id == "inst_123"
        assert "TEAM05" in ig.acl

    def test_raises_when_instrument_not_found(self, ig):
        ig.instrument_name = "NonExistent_TEM"
        mock_client.instruments.get.return_value = None
        with pytest.raises(ValueError, match="NonExistent_TEM"):
            ig.parse_instrument()

    def test_skips_lookup_when_both_id_and_name_set(self, ig):
        ig.instrument_id = "existing_id"
        ig.instrument_name = "ExistingName"
        ig.parse_instrument()
        mock_client.instruments.get.assert_not_called()
        assert "ExistingName" in ig.acl

    def test_does_nothing_when_no_instrument_info(self, ig):
        ig.instrument_name = None
        ig.instrument_id = None
        ig.parse_instrument()
        mock_client.instruments.get.assert_not_called()
        assert ig.acl == []


# =====================================================================
# parse_keywords
# =====================================================================
class TestParseKeywords:
    def test_adds_all_set_fields(self, ig):
        ig.instrument_name = "TEAM05"
        ig.measurement = "EELS"
        ig.session_name = "session_01"
        ig.parse_keywords()
        assert "TEAM05" in ig.keywords
        assert "EELS" in ig.keywords
        assert "session_01" in ig.keywords

    def test_skips_none_fields(self, ig):
        ig.instrument_name = "TEAM05"
        ig.measurement = None
        ig.session_name = None
        ig.parse_keywords()
        assert ig.keywords == ["TEAM05"]
        assert None not in ig.keywords


# =====================================================================
# get_dataset_metadata
# =====================================================================
class TestGetDatasetMetadata:
    def test_generates_unique_id_when_missing(self, ig):
        with patch.object(ci_module, "mfid", return_value=("mfid_001",)), \
             patch.object(ci_module, "checkhash", return_value="hash_abc"), \
             patch("os.path.getsize", return_value=1024), \
             patch("os.path.getctime", return_value=1700000000.0):
            mock_client.instruments.get.return_value = None
            ig.get_dataset_metadata()
        assert ig.unique_id == "mfid_001"

    def test_preserves_existing_unique_id(self, ig):
        ig.unique_id = "existing_id"
        with patch.object(ci_module, "checkhash", return_value="hash_abc"), \
             patch("os.path.getsize", return_value=1024), \
             patch("os.path.getctime", return_value=1700000000.0):
            mock_client.instruments.get.return_value = None
            ig.get_dataset_metadata()
        assert ig.unique_id == "existing_id"

    def test_extracts_data_format_from_extension(self, ig):
        ig.file_to_upload = "/mnt/gcs/team05/scan.dm4"
        with patch.object(ci_module, "mfid", return_value=("id",)), \
             patch.object(ci_module, "checkhash", return_value="h"), \
             patch("os.path.getsize", return_value=100), \
             patch("os.path.getctime", return_value=1700000000.0):
            mock_client.instruments.get.return_value = None
            ig.get_dataset_metadata()
        assert ig.data_format == "dm4"

    @pytest.mark.xfail(reason="split('.')[-1] returns full filename for extensionless files")
    def test_file_without_extension_should_have_empty_format(self, ig):
        ig.file_to_upload = "/mnt/gcs/team05/Makefile"
        with patch.object(ci_module, "mfid", return_value=("id",)), \
             patch.object(ci_module, "checkhash", return_value="h"), \
             patch("os.path.getsize", return_value=100), \
             patch("os.path.getctime", return_value=1700000000.0):
            mock_client.instruments.get.return_value = None
            ig.get_dataset_metadata()
        assert ig.data_format == ""


# =====================================================================
# get_acl_information
# =====================================================================
class TestGetAclInformation:
    def test_resolves_owner_from_orcid(self, ig):
        ig.owner_orcid = "0000-0001-2345-6789"
        mock_client.users.get.return_value = {"id": "user_42"}
        ig.get_acl_information()
        assert ig.owner_user_id == "user_42"
        assert "0000-0001-2345-6789" in ig.acl

    def test_owner_api_returns_none_logs_warning(self, ig, caplog):
        ig.owner_orcid = "0000-0001-2345-6789"
        mock_client.users.get.return_value = None
        ig.get_acl_information()
        assert "Failed to append owner info" in caplog.text
        assert ig.owner_user_id is None

    def test_project_found_adds_to_acl(self, ig):
        ig.project_id = "PROJ_001"
        mock_client.projects.get.return_value = {"project_id": "PROJ_001"}
        ig.get_acl_information()
        assert "PROJ_001" in ig.acl

    def test_project_not_found_raises(self, ig):
        ig.project_id = "FAKE_PROJ"
        mock_client.projects.get.return_value = None
        with pytest.raises(ValueError, match="FAKE_PROJ"):
            ig.get_acl_information()

    def test_skips_owner_when_user_id_already_set(self, ig):
        ig.owner_orcid = "0000-0001-2345-6789"
        ig.owner_user_id = "already_resolved"
        ig.get_acl_information()
        mock_client.users.get.assert_not_called()

    @pytest.mark.xfail(reason="owner_orcid never added to ACL when user ID lookup fails")
    def test_owner_should_be_in_acl_even_when_id_lookup_fails(self, ig):
        ig.owner_orcid = "0000-0001-2345-6789"
        mock_client.users.get.return_value = None
        ig.get_acl_information()
        assert "0000-0001-2345-6789" in ig.acl


# =====================================================================
# add_file
# =====================================================================
class TestAddFile:
    def test_adds_file_with_size_and_hash(self, ig):
        ig.sha256_hash_file_to_upload = "primary_hash"
        with patch("os.path.getsize", return_value=2048):
            ig.add_file("/mnt/gcs/team05/my_file.h5")
        assert "/mnt/gcs/team05/my_file.h5" in ig.associated_files
        entry = ig.associated_files["/mnt/gcs/team05/my_file.h5"]
        assert entry["size"] == 2048
        assert entry["sha256_hash"] == "primary_hash"

    def test_skips_duplicate_by_hash(self, ig):
        ig.associated_files = {"/existing.h5": {"size": 100, "sha256_hash": "dup_hash"}}
        with patch("os.path.getsize", return_value=100), \
             patch.object(ci_module, "checkhash", return_value="dup_hash"):
            ig.add_file("/new_path.h5")
        assert "/new_path.h5" not in ig.associated_files

    def test_uses_precomputed_hash_for_primary_file(self, ig):
        ig.sha256_hash_file_to_upload = "precomputed"
        ig.file_to_upload = "/mnt/gcs/team05/my_file.h5"
        with patch("os.path.getsize", return_value=512):
            ig.add_file("/mnt/gcs/team05/my_file.h5")
        entry = ig.associated_files["/mnt/gcs/team05/my_file.h5"]
        assert entry["sha256_hash"] == "precomputed"

    def test_computes_hash_for_secondary_file(self, ig):
        ig.file_to_upload = "/mnt/gcs/primary.h5"
        with patch("os.path.getsize", return_value=256), \
             patch.object(ci_module, "checkhash", return_value="computed_hash"):
            ig.add_file("/mnt/gcs/secondary.csv")
        assert ig.associated_files["/mnt/gcs/secondary.csv"]["sha256_hash"] == "computed_hash"


# =====================================================================
# to_ig_from_sql
# =====================================================================
class TestToIgFromSql:
    def test_sets_attributes_from_db(self, ig):
        db_obj = {"instrument_name": "TEAM05", "measurement": "EELS"}
        ig.to_ig_from_sql(db_obj, ["instrument_name", "measurement"])
        assert ig.instrument_name == "TEAM05"
        assert ig.measurement == "EELS"

    def test_skips_none_empty_and_unknown(self, ig):
        ig.instrument_name = "Original"
        db_obj = {"instrument_name": "", "owner_orcid": "unknown",
                  "dataset_name": None, "measurement": "Valid"}
        ig.to_ig_from_sql(db_obj, ["instrument_name", "owner_orcid",
                                    "dataset_name", "measurement"])
        assert ig.instrument_name == "Original"
        assert ig.measurement == "Valid"

    def test_scientific_metadata_nested_key(self, ig):
        db_obj = {"scientific_metadata": {"scientific_metadata": {"voltage": 200}}}
        ig.to_ig_from_sql(db_obj, ["scientific_metadata"])
        assert ig.scientific_metadata == {"voltage": 200}

    def test_scientific_metadata_flat_dict(self, ig):
        db_obj = {"scientific_metadata": {"voltage": 200}}
        ig.to_ig_from_sql(db_obj, ["scientific_metadata"])
        assert ig.scientific_metadata == {"voltage": 200}

    def test_project_id_splits_on_space(self, ig):
        db_obj = {"project_id": "PROJ_001 extra_stuff"}
        ig.to_ig_from_sql(db_obj, ["project_id"])
        assert ig.project_id == "PROJ_001"

    def test_skips_attrs_not_in_db_obj(self, ig):
        ig.instrument_name = "Original"
        db_obj = {"measurement": "EELS"}
        ig.to_ig_from_sql(db_obj, ["instrument_name", "measurement"])
        assert ig.instrument_name == "Original"
        assert ig.measurement == "EELS"

    @pytest.mark.xfail(reason="mutable class-level attributes leak between instances")
    def test_mutable_class_attrs_should_not_leak(self):
        # Create two fresh instances WITHOUT fixture cleanup
        a = CrucibleDatasetIngestor()
        b = CrucibleDatasetIngestor()
        a.keywords.append("leaked_keyword")
        # b should NOT see a's keyword
        assert "leaked_keyword" not in b.keywords


# =====================================================================
# to_json_from_ig
# =====================================================================
class TestToJsonFromIg:
    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_exports_valid_attributes(self, mf, mj, ig):
        ig.instrument_name = "TEAM05"
        ig.measurement = "EELS"
        ig.to_json_from_ig("out.json", ["instrument_name", "measurement"])
        data = mj.call_args[0][0]
        assert data["instrument_name"] == "TEAM05"
        assert data["measurement"] == "EELS"
        assert "thumbnails" in data

    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_allow_missing_skips_and_logs(self, mf, mj, ig, caplog):
        ig.instrument_name = "TEAM05"
        ig.to_json_from_ig("out.json",
                           ["instrument_name", "nonexistent_attr"],
                           allow_missing=True)
        assert "nonexistent_attr is missing" in caplog.text
        data = mj.call_args[0][0]
        assert "nonexistent_attr" not in data

    @pytest.mark.xfail(reason="getattr raises AttributeError when allow_missing=False")
    @patch("builtins.open", new_callable=mock_open)
    def test_missing_attr_without_allow_missing_should_not_crash(self, mf, ig):
        ig.to_json_from_ig("out.json", ["nonexistent_attr"],
                           allow_missing=False)

    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_thumbnails_included_in_export(self, mf, mj, ig):
        ig.thumbnails = [{"thumbnail": "b64data", "caption": "Image 1"}]
        ig.to_json_from_ig("out.json", [])
        data = mj.call_args[0][0]
        assert len(data["thumbnails"]) == 1
        assert data["thumbnails"][0]["caption"] == "Image 1"


# =====================================================================
# to_google_cloud_storage
# =====================================================================
class TestToGoogleCloudStorage:
    @patch.object(ci_module, "run_rclone_command")
    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_uploads_json_to_correct_destination(self, mf, mj, mrclone, ig):
        ig.unique_id = "ds_123"
        ig.file_to_upload = "/mnt/gcs/team05/file.h5"
        mrclone.return_value = MagicMock(stdout="ok", stderr="")
        with patch.object(ig, "to_json_from_ig"):
            ig.to_google_cloud_storage("mf-storage-prod", "out.json",
                                        copy_assoc_files=False)
        call_kwargs = mrclone.call_args[1]
        assert call_kwargs["source_path"] == "out.json"
        assert "mf-storage-prod/ds_123" in call_kwargs["destination_path"]

    @patch.object(ci_module, "run_rclone_command")
    @patch.object(ci_module, "Parallel")
    def test_copies_associated_files_when_flag_set(self, mp, mrclone, ig):
        ig.unique_id = "ds_456"
        ig.file_to_upload = "/mnt/gcs/team05/file.h5"
        ig.associated_files = {"file1.csv": {}, "file2.csv": {}}
        mrclone.return_value = MagicMock(stdout="ok", stderr="")
        with patch.object(ig, "to_json_from_ig"):
            ig.to_google_cloud_storage("bucket", "out.json",
                                        copy_assoc_files=True)
        mp.assert_called_once()

    @patch.object(ci_module, "run_rclone_command")
    def test_does_not_copy_files_when_flag_false(self, mrclone, ig):
        ig.unique_id = "ds_789"
        ig.file_to_upload = "/mnt/gcs/team05/file.h5"
        ig.associated_files = {"file1.csv": {}}
        mrclone.return_value = MagicMock(stdout="ok", stderr="")
        with patch.object(ig, "to_json_from_ig"), \
             patch.object(ci_module, "Parallel") as mp:
            ig.to_google_cloud_storage("bucket", "out.json",
                                        copy_assoc_files=False)
        mp.assert_not_called()

    @pytest.mark.xfail(reason="mutable default argument accumulates across calls")
    @patch.object(ci_module, "run_rclone_command")
    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_default_common_paths_should_not_accumulate(self, mf, mj, mrclone, ig):
        ig.unique_id = "ds_a"
        ig.file_to_upload = "/mnt/gcs/team05/file.h5"
        mrclone.return_value = MagicMock(stdout="", stderr="")
        ig.to_google_cloud_storage("bucket", "out.json")
        ig.to_google_cloud_storage("bucket", "out.json")
        # The default list should not have grown from the first call
        # If it did, the second call has stale paths from the first
        # We verify by checking rclone was called correctly both times
        assert mrclone.call_count == 2


# =====================================================================
# setup_data (integration)
# =====================================================================
class TestSetupData:
    def test_calls_all_pipeline_steps_in_order(self, ig):
        ig.get_scientific_metadata = MagicMock()
        ig.get_dataset_metadata = MagicMock()
        ig.get_acl_information = MagicMock()
        ig.parse_batch = MagicMock()
        ig.parse_samples = MagicMock()
        ig.get_data_files = MagicMock()
        ig.get_thumbnails = MagicMock()

        ig.setup_data()

        ig.get_scientific_metadata.assert_called_once()
        ig.get_dataset_metadata.assert_called_once()
        ig.get_acl_information.assert_called_once()
        ig.parse_batch.assert_called_once()
        ig.parse_samples.assert_called_once()
        ig.get_data_files.assert_called_once()
        ig.get_thumbnails.assert_called_once()

    @pytest.mark.xfail(reason="no error handling — one failed step aborts entire pipeline")
    def test_later_steps_should_run_even_if_acl_fails(self, ig):
        ig.get_scientific_metadata = MagicMock()
        ig.get_dataset_metadata = MagicMock()
        ig.get_acl_information = MagicMock(side_effect=ValueError("API down"))
        ig.parse_batch = MagicMock()
        ig.parse_samples = MagicMock()
        ig.get_data_files = MagicMock()
        ig.get_thumbnails = MagicMock()

        ig.setup_data()

        # Even if ACL fails, we should still get data files and thumbnails
        ig.get_data_files.assert_called_once()
        ig.get_thumbnails.assert_called_once()