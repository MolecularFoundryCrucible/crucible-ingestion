import sys
import pytest
from unittest.mock import patch, MagicMock, mock_open

# --- 1. SYSTEM-LEVEL MOCKING ---
# We must mock the entire 'crucible' namespace so Python doesn't look in your Anaconda site-packages.

# Create a Dummy Base Class for Dataset to inherit from
class DummyDataset:
    file_to_upload = "/mnt/gcs/test_instrument/my_file.h5"
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

# Create fake module objects
mock_crucible = MagicMock()
mock_models = MagicMock()
mock_utils = MagicMock()
mock_utils_io = MagicMock()

# Populate the fake modules with the specific classes/functions your code imports
mock_models.Dataset = DummyDataset
mock_crucible.CrucibleClient = MagicMock
mock_utils_io.run_shell = MagicMock()
mock_utils_io.checkhash = MagicMock(return_value="fake_hash")

# Inject the fake modules into sys.modules BEFORE importing the ingestor
sys.modules["crucible"] = mock_crucible
sys.modules["crucible.models"] = mock_models
sys.modules["crucible.utils"] = mock_utils
sys.modules["crucible.utils.io"] = mock_utils_io

# --- 2. PATCH LOCAL SECRETS ---
patcher_secret = patch("utils.get_secret", return_value="fake_api_key")
patcher_secret.start()

# --- 3. SAFE IMPORTS ---
# Now it is completely safe to import the module without triggering live API calls or Anaconda errors
from ingestors.crucible_ingestor import CrucibleDatasetIngestor
import ingestors.crucible_ingestor as ci_module

patcher_secret.stop()

# Ensure the module-level client is specifically using our mock instance
mock_client = MagicMock()
ci_module.client = mock_client

# --- SETUP FIXTURE ---
@pytest.fixture
def ingestor():
    """Creates a basic ingestor instance inheriting from our DummyDataset."""
    return CrucibleDatasetIngestor()

# --- parse_instrument failures ---

def test_parse_instrument_does_not_exist(ingestor):
    """FAILURE POINT: Instrument name is provided, but Crucible API returns None."""
    ingestor.instrument_name = "NonExistent_TEM"
    mock_client.instruments.get.return_value = None
    
    with pytest.raises(ValueError, match="Provided instrument does not exist: NonExistent_TEM"):
        ingestor.parse_instrument()

# --- get_acl_information failures ---

def test_get_acl_information_owner_api_exception(ingestor, caplog):
    """EDGE CASE: The API returns bad data causing a parsing error. 
    The ingestor is supposed to catch it, log a warning, and continue without crashing."""
    ingestor.owner_orcid = "0000-0001-2345-6789"
    
    # Return None instead of throwing an exception
    mock_client.users.get.return_value = None 
    
    # It shouldn't raise an error
    ingestor.get_acl_information()
    
    # Assert that the ingestor logged the failure
    assert "Failed to append owner info due to error" in caplog.text
    assert ingestor.owner_user_id is None # State shouldn't be updated

def test_get_acl_information_project_not_found(ingestor):
    """FAILURE POINT: The Project ID is set, but the API returns None (project missing)."""
    ingestor.project_id = "FAKE_PROJ_99"
    mock_client.users.get.return_value = {"id": "123"} # Mock owner to pass
    mock_client.projects.get.return_value = None
    
    with pytest.raises(ValueError, match="Project with ID 'FAKE_PROJ_99' does not exist in the database"):
        ingestor.get_acl_information()

# --- to_ig_from_sql edge cases ---

def test_to_ig_from_sql_skips_empty_and_unknown(ingestor):
    """EDGE CASE: The SQL DB returns data with 'unknown', empty strings, or None. 
    These should be explicitly skipped by the function and not overwrite class attributes."""
    
    ingestor.instrument_name = "Original_Name" # Set a starting value
    
    dataset_obj_from_db = {
        "instrument_name": "", # Should skip
        "owner_orcid": "unknown", # Should skip
        "dataset_name": None, # Should skip
        "measurement": "New_Measurement" # Should apply
    }
    sql_import_attr = ["instrument_name", "owner_orcid", "dataset_name", "measurement"]
    
    ingestor.to_ig_from_sql(dataset_obj_from_db, sql_import_attr)
    
    assert ingestor.instrument_name == "Original_Name" # Was not overwritten
    assert getattr(ingestor, "owner_orcid", None) is None
    assert ingestor.measurement == "New_Measurement"

# --- to_json_from_ig edge cases ---

@patch("json.dump")
@patch("builtins.open", new_callable=mock_open)
def test_to_json_from_ig_allow_missing(mock_file, mock_json_dump, ingestor, caplog):
    """EDGE CASE: Exporting metadata, but the class is missing an attribute listed in sql_export_attr.
    If allow_missing is True, it should log a warning and skip, not throw an AttributeError."""
    
    ingestor.instrument_name = "Valid_Instrument"
    # Note: 'missing_attribute' is NOT an attribute on the ingestor class
    
    ingestor.to_json_from_ig(
        jsonfile="output.json", 
        sql_export_attr=["instrument_name", "missing_attribute"], 
        allow_missing=True
    )
    
    assert "missing_attribute is missing!!" in caplog.text
    
    # Assert json.dump was called with the dictionary ONLY containing valid attributes and thumbnails
    dump_args = mock_json_dump.call_args[0][0]
    assert "instrument_name" in dump_args
    assert "missing_attribute" not in dump_args