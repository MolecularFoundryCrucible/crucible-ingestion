import sys
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open, call

# --- 1. SYSTEM-LEVEL MOCKING ---
# Must mock the 'crucible' namespace before importing data_ingestion,
# since it transitively imports from crucible via all ingestor classes.

class DummyDataset:
    """Minimal stand-in for crucible.models.Dataset so all ingestors can inherit from it."""
    file_to_upload = None
    dataset_name = None
    timestamp = None
    source_folder = None
    instrument_id = None
    instrument_name = None
    unique_id = None
    sha256_hash_file_to_upload = None
    owner_orcid = None
    owner_user_id = None
    project_id = None
    measurement = None
    session_name = None
    size = None
    data_format = None

mock_crucible = MagicMock()
mock_models = MagicMock()
mock_utils = MagicMock()
mock_utils_io = MagicMock()

mock_models.Dataset = DummyDataset
mock_crucible.CrucibleClient = MagicMock
mock_utils_io.run_shell = MagicMock()
mock_utils_io.checkhash = MagicMock(return_value="fake_hash_abc123")

sys.modules["crucible"] = mock_crucible
sys.modules["crucible.models"] = mock_models
sys.modules["crucible.utils"] = mock_utils
sys.modules["crucible.utils.io"] = mock_utils_io

# --- 2. PATCH SECRETS DURING IMPORT ---
patcher_secret = patch("utils.get_secret", return_value="fake_api_key")
patcher_secret.start()

# --- 3. SAFE IMPORTS ---
import data_ingestion
from data_ingestion import find_supported_ingestor, populate_existing_ds_info, data_ingestion as data_ingestion_fn

patcher_secret.stop()


# ============================================================================
# HELPERS: Lightweight mock ingestor classes for testing find_supported_ingestor
# without coupling to real ingestor implementations.
# ============================================================================

class SupportedIngestor:
    """An ingestor that always claims it supports the file."""
    def __init__(self, file_to_upload=None, unique_id=None):
        self.file_to_upload = file_to_upload
        self.unique_id = unique_id

    def is_file_supported(self):
        return True


class UnsupportedIngestor:
    """An ingestor that never supports any file."""
    def __init__(self, file_to_upload=None, unique_id=None):
        self.file_to_upload = file_to_upload
        self.unique_id = unique_id

    def is_file_supported(self):
        return False


class PngOnlyIngestor:
    """An ingestor that only supports .png files."""
    def __init__(self, file_to_upload=None, unique_id=None):
        self.file_to_upload = file_to_upload
        self.unique_id = unique_id

    def is_file_supported(self):
        return self.file_to_upload is not None and self.file_to_upload.endswith(".png")


# ============================================================================
# TESTS: find_supported_ingestor
# ============================================================================

class TestFindSupportedIngestor:

    def test_returns_first_matching_ingestor_from_list(self):
        """When iterating through the ingestor list, the first class that
        reports support should be returned — not one further down the list."""
        result = find_supported_ingestor(
            "/data/sample.png", "ds001",
            specified_ingestor=None,
            ingestor_list=[UnsupportedIngestor, PngOnlyIngestor, SupportedIngestor]
        )
        # PngOnlyIngestor should match first for a .png file, not the universal SupportedIngestor
        assert isinstance(result, PngOnlyIngestor)

    def test_returns_none_when_no_ingestor_supports_file(self):
        """If no ingestor in the list supports the file, the function must return None
        so the caller can route the dataset to the 'not-supported' queue."""
        result = find_supported_ingestor(
            "/data/file.xyz", "ds002",
            specified_ingestor=None,
            ingestor_list=[UnsupportedIngestor, PngOnlyIngestor]
        )
        assert result is None

    def test_returns_none_for_empty_ingestor_list(self):
        """Edge case: an empty ingestor list should gracefully return None."""
        result = find_supported_ingestor(
            "/data/file.h5", "ds003",
            specified_ingestor=None,
            ingestor_list=[]
        )
        assert result is None

    def test_specified_ingestor_used_when_supported(self):
        """When a specific ingestor class is named AND it supports the file,
        it should be returned directly without iterating the list."""
        # Inject our test class into the module's global namespace so globals() lookup works
        data_ingestion.SupportedIngestor = SupportedIngestor
        try:
            result = find_supported_ingestor(
                "/data/file.h5", "ds004",
                specified_ingestor="SupportedIngestor",
                ingestor_list=[UnsupportedIngestor]
            )
            assert isinstance(result, SupportedIngestor)
            assert result.file_to_upload == "/data/file.h5"
            assert result.unique_id == "ds004"
        finally:
            del data_ingestion.SupportedIngestor

    def test_specified_ingestor_not_supported_falls_through_to_list(self):
        """When a specific ingestor is named but does NOT support the file,
        the function should fall through and iterate through the ingestor_list."""
        data_ingestion.UnsupportedIngestor = UnsupportedIngestor
        try:
            result = find_supported_ingestor(
                "/data/file.png", "ds005",
                specified_ingestor="UnsupportedIngestor",
                ingestor_list=[PngOnlyIngestor]
            )
            # Should have fallen through to PngOnlyIngestor
            assert isinstance(result, PngOnlyIngestor)
        finally:
            del data_ingestion.UnsupportedIngestor

    def test_specified_ingestor_not_in_globals_raises_keyerror(self):
        """FAILURE POINT: If a specified_ingestor class name doesn't exist in the module's
        globals(), the globals()[specified_ingestor] lookup should raise a KeyError.
        This is a realistic failure scenario — e.g. a typo in the ingestion_class field."""
        with pytest.raises(KeyError):
            find_supported_ingestor(
                "/data/file.h5", "ds006",
                specified_ingestor="NonExistentIngestorClassName",
                ingestor_list=[]
            )

    def test_ingestor_receives_correct_file_and_dsid(self):
        """The ingestor instance returned should have the correct file_to_upload
        and unique_id attributes set from the function arguments."""
        result = find_supported_ingestor(
            "/mnt/gcs/team05/experiment_001.dm4", "ds_unique_007",
            specified_ingestor=None,
            ingestor_list=[SupportedIngestor]
        )
        assert result.file_to_upload == "/mnt/gcs/team05/experiment_001.dm4"
        assert result.unique_id == "ds_unique_007"

    def test_specified_ingestor_not_supported_and_list_also_fails(self):
        """When the specified ingestor doesn't support the file AND no ingestor
        in the list supports it either, the function should return None."""
        data_ingestion.UnsupportedIngestor = UnsupportedIngestor
        try:
            result = find_supported_ingestor(
                "/data/file.xyz", "ds008",
                specified_ingestor="UnsupportedIngestor",
                ingestor_list=[UnsupportedIngestor]
            )
            assert result is None
        finally:
            del data_ingestion.UnsupportedIngestor


# ============================================================================
# TESTS: populate_existing_ds_info
# ============================================================================

class TestPopulateExistingDsInfo:

    def _make_mock_ingestor(self, unique_id="ds_abc"):
        """Helper to create a simple ingestor-like object with real attributes
        that populate_existing_ds_info can setattr on."""
        class SimpleIngestor:
            pass
        ig = SimpleIngestor()
        ig.unique_id = unique_id
        ig.sha256_hash_file_to_upload = None
        ig.associated_files = {}
        return ig

    def test_populates_fields_from_dataset_found_by_unique_id(self):
        """When the dataset is found by unique_id on the first lookup,
        the ingestor's attributes should be populated from the returned data."""
        ig = self._make_mock_ingestor()
        mock_client = MagicMock()

        found_ds_data = {
            "owner_orcid": "0000-0001-2345-6789",
            "project_id": "MFP00123",
            "measurement": "TEM_imaging",
            "session_name": "session_42",
            "instrument_name": "TEAM05",
        }
        mock_client.datasets.get.return_value = found_ds_data
        mock_client.get_associated_files.return_value = []

        ig_out, found_ds = populate_existing_ds_info(
            ig, "/mnt/gcs/test.dm4", mock_client,
            populate_fields=["owner_orcid", "project_id", "measurement", "session_name", "instrument_name"]
        )

        # Verify each field was populated correctly on the ingestor
        assert ig_out.owner_orcid == "0000-0001-2345-6789"
        assert ig_out.project_id == "MFP00123"
        assert ig_out.measurement == "TEM_imaging"
        assert ig_out.session_name == "session_42"
        assert ig_out.instrument_name == "TEAM05"
        assert found_ds == found_ds_data

    def test_falls_back_to_hash_lookup_when_id_not_found(self):
        """When the dataset is NOT found by unique_id, the function should
        compute the file hash and try again with that hash."""
        ig = self._make_mock_ingestor()
        mock_client = MagicMock()

        # First call (by unique_id) returns None, second (by hash) returns data
        found_ds_data = {"owner_orcid": "0000-9999-8888-7777", "project_id": None}
        mock_client.datasets.get.side_effect = [None, found_ds_data]
        mock_client.get_associated_files.return_value = []

        ig_out, found_ds = populate_existing_ds_info(
            ig, "/mnt/gcs/test.dm4", mock_client,
            populate_fields=["owner_orcid", "project_id"]
        )

        # checkhash should have been called to compute the hash
        assert ig_out.sha256_hash_file_to_upload == "fake_hash_abc123"
        # Should have made two get calls
        assert mock_client.datasets.get.call_count == 2
        assert found_ds == found_ds_data

    def test_skips_none_and_empty_fields(self):
        """Fields with None or empty string values in the found dataset
        should NOT be set on the ingestor — they should be skipped."""
        ig = self._make_mock_ingestor()
        # Set initial values so we can verify they weren't overwritten
        ig.owner_orcid = "original_orcid"
        ig.project_id = "original_project"
        ig.measurement = "original_measurement"
        mock_client = MagicMock()

        found_ds_data = {
            "owner_orcid": None,         # Should skip
            "project_id": "",             # Should skip
            "measurement": "STEM",        # Should set
        }
        mock_client.datasets.get.return_value = found_ds_data
        mock_client.get_associated_files.return_value = []

        ig_out, found_ds = populate_existing_ds_info(
            ig, "/mnt/gcs/test.dm4", mock_client,
            populate_fields=["owner_orcid", "project_id", "measurement"]
        )

        # owner_orcid (None) and project_id ("") should have been skipped
        assert ig_out.owner_orcid == "original_orcid"
        assert ig_out.project_id == "original_project"
        # measurement should have been updated
        assert ig_out.measurement == "STEM"

    def test_returns_none_for_found_ds_when_neither_lookup_finds_anything(self):
        """When neither the unique_id nor the hash lookup finds a dataset,
        found_ds should be None and no attributes should be populated."""
        ig = self._make_mock_ingestor()
        mock_client = MagicMock()
        mock_client.datasets.get.return_value = None
        mock_client.get_associated_files.return_value = []

        ig_out, found_ds = populate_existing_ds_info(
            ig, "/mnt/gcs/test.dm4", mock_client,
            populate_fields=["owner_orcid"]
        )

        assert found_ds is None

    def test_associated_files_are_added_to_ingestor(self):
        """Associated files returned by the API should be added into the
        ingestor's associated_files dictionary with the correct structure."""
        ig = self._make_mock_ingestor()
        mock_client = MagicMock()
        mock_client.datasets.get.return_value = None
        mock_client.get_associated_files.return_value = [
            {"filename": "aux_data.csv", "size": 1024, "sha256_hash": "hash_aaa"},
            {"filename": "log.txt", "size": 256, "sha256_hash": "hash_bbb"},
        ]

        ig_out, _ = populate_existing_ds_info(
            ig, "/mnt/gcs/test.dm4", mock_client,
            populate_fields=[]
        )

        assert "aux_data.csv" in ig_out.associated_files
        assert ig_out.associated_files["aux_data.csv"] == {"size": 1024, "sha256_hash": "hash_aaa"}
        assert "log.txt" in ig_out.associated_files
        assert ig_out.associated_files["log.txt"] == {"size": 256, "sha256_hash": "hash_bbb"}

    def test_field_not_in_found_ds_raises_keyerror(self):
        """FAILURE POINT: If populate_fields contains a key that is NOT present
        in the found dataset dictionary, the found_ds[k] access should raise
        a KeyError. This tests that the code does not silently skip missing keys."""
        ig = self._make_mock_ingestor()
        mock_client = MagicMock()

        # found_ds is missing the "session_name" key entirely
        found_ds_data = {"owner_orcid": "0000-1111-2222-3333"}
        mock_client.datasets.get.return_value = found_ds_data
        mock_client.get_associated_files.return_value = []

        with pytest.raises(KeyError):
            populate_existing_ds_info(
                ig, "/mnt/gcs/test.dm4", mock_client,
                populate_fields=["owner_orcid", "session_name"]
            )


# ============================================================================
# TESTS: data_ingestion (the main orchestration function)
# ============================================================================

class TestDataIngestion:

    def test_returns_none_tuple_when_no_ingestor_found(self):
        """When no ingestor supports the given file, data_ingestion should
        return (None, None) so the caller can route to the 'not-supported' queue."""
        with patch.object(data_ingestion, "find_supported_ingestor", return_value=None):
            result = data_ingestion_fn(
                dataset_to_process="/data/unsupported.xyz",
                dsid="ds_unsupported",
                reqid="req_001",
                timestamp="20260101T000000",
                client=MagicMock()
            )
        assert result == (None, None)

    def test_setup_data_is_called_on_ingestor(self):
        """After finding a supported ingestor and populating existing info,
        setup_data() must be called on the ingestor to parse the file."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_test"
        mock_ig.associated_files = {"file.h5": {"size": 100, "sha256_hash": "abc"}}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_client._request.return_value = MagicMock()

        json_data = {
            "keywords": ["kw1"],
            "acl": [],
            "associated_files": {"file.h5": {"size": 100, "sha256_hash": "abc"}},
            "thumbnails": [],
            "scientific_metadata": {},
            "dataset_name": "test",
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_test",
                reqid="req_002",
                timestamp="20260101T000000",
                client=mock_client
            )

        mock_ig.setup_data.assert_called_once()

    def test_to_ig_from_sql_called_when_existing_dataset_found(self):
        """When populate_existing_ds_info finds an existing dataset (found_ds is truthy),
        to_ig_from_sql must be called to overlay existing SQL data onto the ingestor."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_existing"
        mock_ig.associated_files = {}

        existing_ds = {"dataset_name": "already_in_db", "owner_orcid": "0000-1111-2222-3333"}
        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, existing_ds)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_existing",
                reqid="req_003",
                timestamp="20260101T000000",
                client=mock_client
            )

        # to_ig_from_sql should be called with the found dataset and sql_import_attr
        mock_ig.to_ig_from_sql.assert_called_once()
        call_args = mock_ig.to_ig_from_sql.call_args
        assert call_args[0][0] == existing_ds  # first positional arg is the found dataset

    def test_to_ig_from_sql_not_called_when_no_existing_dataset(self):
        """When populate_existing_ds_info does NOT find an existing dataset,
        to_ig_from_sql should NOT be called — parsed data should stand as-is."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_new"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_new",
                reqid="req_004",
                timestamp="20260101T000000",
                client=mock_client
            )

        mock_ig.to_ig_from_sql.assert_not_called()

    def test_num_cores_capped_at_32(self):
        """The number of parallel cores for GCS upload should be capped at 32,
        even when there are many associated files."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_many_files"
        # Simulate 200 associated files — int(200/4)+1 = 51, should be capped to 32
        mock_ig.associated_files = {f"file_{i}.dat": {} for i in range(200)}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_client._request.return_value = MagicMock()

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {f"file_{i}.dat": {"size": 100, "sha256_hash": f"h{i}"} for i in range(200)},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_many_files",
                reqid="req_005",
                timestamp="20260101T000000",
                client=mock_client
            )

        # Verify to_google_cloud_storage was called with num_cores = 32 (the cap)
        gcs_call = mock_ig.to_google_cloud_storage.call_args
        assert gcs_call[1]["num_cores"] == 32

    def test_num_cores_scales_with_file_count(self):
        """When there are few files, num_cores should be int(num_files/4)+1."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_few"
        # 8 files → int(8/4)+1 = 3
        mock_ig.associated_files = {f"file_{i}.dat": {} for i in range(8)}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_few",
                reqid="req_006",
                timestamp="20260101T000000",
                client=mock_client
            )

        gcs_call = mock_ig.to_google_cloud_storage.call_args
        assert gcs_call[1]["num_cores"] == 3

    def test_json_filename_constructed_correctly(self):
        """The JSON output filename should incorporate dsid, timestamp, and reqid
        to ensure uniqueness per ingestion request."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_json_test"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))) as mocked_open:
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="DS123",
                reqid="REQ456",
                timestamp="20260422T120000",
                client=mock_client
            )

        expected_fname = "DS123_ingest_20260422T120000_REQ456.json"
        # to_google_cloud_storage should receive this filename
        gcs_call = mock_ig.to_google_cloud_storage.call_args
        assert gcs_call[1]["jsonfile"] == expected_fname

    def test_keywords_filters_out_empty_strings_and_non_strings(self):
        """The keyword filtering should remove empty strings and non-string values
        before sending them to the API."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_kw"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()

        json_data = {
            "keywords": ["valid_kw", "", "another_kw", 123, None, "last_kw"],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_kw",
                reqid="req_kw",
                timestamp="20260101T000000",
                client=mock_client
            )

        # Only valid string keywords should be added
        kw_calls = mock_client.add_dataset_keyword.call_args_list
        added_keywords = [c[0][1] for c in kw_calls]
        assert "valid_kw" in added_keywords
        assert "another_kw" in added_keywords
        assert "last_kw" in added_keywords
        assert "" not in added_keywords
        assert len(added_keywords) == 3

    def test_thumbnail_addition_failure_does_not_crash(self):
        """If adding a thumbnail raises an exception, the ingestion process
        should log the error but continue processing the remaining work."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_tn_fail"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_client.datasets.add_thumbnail.side_effect = Exception("Thumbnail upload failed")

        json_data = {
            "keywords": ["test"],
            "acl": [],
            "associated_files": {},
            "thumbnails": [{"thumbnail": "base64data", "caption": "Test Image"}],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            # Should NOT raise — the exception should be caught internally
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_tn_fail",
                reqid="req_tn",
                timestamp="20260101T000000",
                client=mock_client
            )

        # Thumbnail addition was attempted
        mock_client.datasets.add_thumbnail.assert_called_once()
        # Keywords should still have been processed despite the thumbnail failure
        mock_client.add_dataset_keyword.assert_called()

    def test_associated_files_early_return_bug(self):
        """BUG DETECTION: The associated files loop contains a 'return' statement
        inside the for-loop body (line 204). This causes the function to return
        after processing only the FIRST associated file, skipping all remaining
        associated files, keywords, and scientific metadata updates.

        This test verifies the current (buggy) behavior: with multiple associated
        files, only the first one is processed and the function returns early,
        preventing keyword addition and metadata updates from executing."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_assoc_bug"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_client._request.return_value = "posted_result"

        json_data = {
            "keywords": ["should_be_added"],
            "acl": [],
            "associated_files": {
                "file_1.csv": {"size": 100, "sha256_hash": "hash1"},
                "file_2.csv": {"size": 200, "sha256_hash": "hash2"},
                "file_3.csv": {"size": 300, "sha256_hash": "hash3"},
            },
            "thumbnails": [],
            "scientific_metadata": {"key": "value"},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            result = data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_assoc_bug",
                reqid="req_assoc",
                timestamp="20260101T000000",
                client=mock_client
            )

        # Due to the early return, the function returns the _request result
        # instead of completing the full flow
        assert result == "posted_result"

        # Only ONE _request call should have been made (the early return)
        assert mock_client._request.call_count == 1

        # Keywords should NOT have been added because the function returned early
        mock_client.add_dataset_keyword.assert_not_called()

        # Scientific metadata should NOT have been updated
        mock_client.datasets.update_scientific_metadata.assert_not_called()

    def test_no_associated_files_allows_keywords_and_metadata(self):
        """When there are NO associated files, the for-loop body never executes,
        so the function should continue processing keywords and metadata normally."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_no_assoc"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_res = MagicMock()
        mock_res.content = '{"status": "ok"}'
        mock_client.datasets.update_scientific_metadata.return_value = mock_res

        json_data = {
            "keywords": ["kw1", "kw2"],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {"experiment": "test"},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_no_assoc",
                reqid="req_noassoc",
                timestamp="20260101T000000",
                client=mock_client
            )

        # Keywords should be added
        assert mock_client.add_dataset_keyword.call_count == 2

        # Scientific metadata should be updated
        mock_client.datasets.update_scientific_metadata.assert_called_once_with(
            "ds_no_assoc", {"experiment": "test"}, overwrite=False
        )

    def test_client_datasets_update_receives_correct_payload(self):
        """The dataset update call should receive the JSON payload with
        keywords/acl/associated_files/thumbnails/scientific_metadata popped out,
        leaving only the core dataset fields."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_payload"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_res = MagicMock()
        mock_res.content = "{}"
        mock_client.datasets.update_scientific_metadata.return_value = mock_res

        json_data = {
            "dataset_name": "my_dataset",
            "timestamp": "2026-01-01T00:00:00",
            "keywords": ["kw"],
            "acl": ["orcid"],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {"key": "val"},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_payload",
                reqid="req_payload",
                timestamp="20260101T000000",
                client=mock_client
            )

        # The update call should have the popped fields removed
        update_kwargs = mock_client.datasets.update.call_args[1]
        assert "keywords" not in update_kwargs
        assert "acl" not in update_kwargs
        assert "associated_files" not in update_kwargs
        assert "thumbnails" not in update_kwargs
        assert "scientific_metadata" not in update_kwargs
        # But core fields should be present
        assert update_kwargs["dataset_name"] == "my_dataset"
        assert update_kwargs["timestamp"] == "2026-01-01T00:00:00"

    def test_keyword_addition_failure_does_not_crash(self):
        """If adding a keyword to the API raises an exception, the function
        should catch it and continue with the remaining keywords."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_kw_fail"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        # First keyword call fails, second succeeds
        mock_client.add_dataset_keyword.side_effect = [Exception("API Error"), None]
        mock_res = MagicMock()
        mock_res.content = "{}"
        mock_client.datasets.update_scientific_metadata.return_value = mock_res

        json_data = {
            "keywords": ["fail_kw", "success_kw"],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            # Should NOT raise
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_kw_fail",
                reqid="req_kwf",
                timestamp="20260101T000000",
                client=mock_client
            )

        # Both keywords should have been attempted
        assert mock_client.add_dataset_keyword.call_count == 2
        # Scientific metadata should still be updated after keyword failures
        mock_client.datasets.update_scientific_metadata.assert_called_once()

    def test_gcs_upload_uses_correct_storage_bucket(self):
        """The GCS upload should always use the hardcoded 'mf-storage-prod' bucket."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_bucket"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_res = MagicMock()
        mock_res.content = "{}"
        mock_client.datasets.update_scientific_metadata.return_value = mock_res

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {},
            "thumbnails": [],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_bucket",
                reqid="req_bucket",
                timestamp="20260101T000000",
                client=mock_client
            )

        gcs_call = mock_ig.to_google_cloud_storage.call_args
        assert gcs_call[0][0] == "mf-storage-prod"

    def test_associated_file_post_failure_is_caught(self):
        """If the _request call for an associated file raises an exception, it
        should be caught and logged, not propagate up."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_af_fail"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_client._request.side_effect = Exception("Network Error posting associated file")
        mock_res = MagicMock()
        mock_res.content = "{}"
        mock_client.datasets.update_scientific_metadata.return_value = mock_res

        json_data = {
            "keywords": ["kw_after_fail"],
            "acl": [],
            "associated_files": {
                "broken_file.csv": {"size": 999, "sha256_hash": "hash_broken"},
            },
            "thumbnails": [],
            "scientific_metadata": {"data": True},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            # Should NOT raise — the exception should be caught
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_af_fail",
                reqid="req_af_fail",
                timestamp="20260101T000000",
                client=mock_client
            )

        # The _request was attempted
        mock_client._request.assert_called_once()
        # After the caught exception, keywords should still be processed
        mock_client.add_dataset_keyword.assert_called_once_with("ds_af_fail", "kw_after_fail")

    def test_client_none_raises_attribute_error(self):
        """FAILURE POINT: If client is None (the default parameter), calling
        populate_existing_ds_info will attempt client.datasets.get(...) which
        should raise an AttributeError."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_no_client"
        mock_ig.associated_files = {}

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig):
            with pytest.raises(AttributeError):
                data_ingestion_fn(
                    dataset_to_process="/data/file.h5",
                    dsid="ds_no_client",
                    reqid="req_no_client",
                    timestamp="20260101T000000",
                    client=None
                )

    def test_multiple_thumbnails_all_attempted(self):
        """When there are multiple thumbnails, the function should attempt
        to add each one, regardless of whether earlier ones succeed or fail."""
        mock_ig = MagicMock()
        mock_ig.unique_id = "ds_multi_tn"
        mock_ig.associated_files = {}

        mock_client = MagicMock()
        mock_client.datasets.update.return_value = MagicMock()
        mock_client.datasets.add_thumbnail.return_value = "ok"
        mock_res = MagicMock()
        mock_res.content = "{}"
        mock_client.datasets.update_scientific_metadata.return_value = mock_res

        json_data = {
            "keywords": [],
            "acl": [],
            "associated_files": {},
            "thumbnails": [
                {"thumbnail": "b64_img1", "caption": "Image 1"},
                {"thumbnail": "b64_img2", "caption": "Image 2"},
                {"thumbnail": "b64_img3", "caption": "Image 3"},
            ],
            "scientific_metadata": {},
        }

        with patch.object(data_ingestion, "find_supported_ingestor", return_value=mock_ig), \
             patch.object(data_ingestion, "populate_existing_ds_info", return_value=(mock_ig, None)), \
             patch("builtins.open", mock_open(read_data=json.dumps(json_data))):
            data_ingestion_fn(
                dataset_to_process="/data/file.h5",
                dsid="ds_multi_tn",
                reqid="req_multi_tn",
                timestamp="20260101T000000",
                client=mock_client
            )

        assert mock_client.datasets.add_thumbnail.call_count == 3
