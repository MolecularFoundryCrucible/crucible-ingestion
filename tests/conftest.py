import sys
from unittest.mock import MagicMock

# --- Global Test Suite Setup ---
# The crucible-ingestion project heavily imports from the 'crucible' namespace package
# (crucible.models, crucible.utils.io, etc.) which is not available in the local repo.
# We must mock these globally before any application code is imported by any test file,
# otherwise module caching will cause cross-test pollution where one test file gets a
# MagicMock base class and another expects DummyDataset.

class DummyDataset:
    """Minimal stand-in for crucible.models.Dataset so all ingestors can inherit from it."""
    file_to_upload = "/mnt/gcs/team05/my_file.h5"
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
mock_utils_io.get_tz_isoformat = MagicMock(return_value="2026-01-15T10:00:00-08:00")

sys.modules["crucible"] = mock_crucible
sys.modules["crucible.models"] = mock_models
sys.modules["crucible.utils"] = mock_utils
sys.modules["crucible.utils.io"] = mock_utils_io

# Make these available to tests that need to assert on them
import pytest
@pytest.fixture(autouse=True)
def reset_global_mocks():
    """Reset the mock call counts before every test."""
    mock_utils_io.run_shell.reset_mock()
    mock_utils_io.checkhash.reset_mock()
    mock_utils_io.get_tz_isoformat.reset_mock()
