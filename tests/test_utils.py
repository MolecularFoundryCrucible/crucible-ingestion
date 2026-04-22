import os
import json
import base64
import pytest
import numpy as np
from datetime import datetime
from unittest.mock import patch, MagicMock, mock_open
from PIL import Image

# Import functions from utils.py
from utils import (
    get_cloud_secret_selfauth,
    get_credentials_from_env,
    get_secret,
    setup_pika_client,
    _get_sa_credentials,
    run_rclone_command,
    build_b64_thumbnail,
    reduce_filename_and_copy,
    EnhancedJSONEncoder
)

# --- get_cloud_secret_selfauth ---

@patch("utils.secretmanager.SecretManagerServiceClient")
def test_get_cloud_secret_selfauth_exception(mock_client):
    """EDGE CASE: SecretManager throws an exception (e.g., network error or permission denied)."""
    mock_instance = mock_client.return_value
    mock_instance.access_secret_version.side_effect = Exception("Permission Denied")
    
    result = get_cloud_secret_selfauth("fake-secret")
    assert result is None

# --- get_credentials_from_env ---

@patch.dict(os.environ, {}, clear=True)
def test_get_credentials_from_env_missing():
    """FAILURE POINT: GCS_SA environment variable is completely missing."""
    assert get_credentials_from_env() is None

@patch.dict(os.environ, {"GCS_SA": "{invalid-json"}, clear=True)
def test_get_credentials_from_env_invalid_json():
    """EDGE CASE: GCS_SA contains malformed JSON."""
    with pytest.raises(json.decoder.JSONDecodeError):
        get_credentials_from_env()

# --- get_secret ---

@patch("utils.load_dotenv")
@patch.dict(os.environ, {}, clear=True)
@patch("utils.get_cloud_secret_selfauth", return_value=None)
@patch("utils.get_credentials_from_env", return_value=None)
def test_get_secret_no_credentials_raises(mock_creds, mock_selfauth, mock_dotenv):
    """FAILURE POINT: Secret isn't in env, self-auth fails, and no credentials are provided."""
    with pytest.raises(Exception, match="No credentials available to access GCS secret"):
        get_secret("MISSING_ENV_VAR", gcs_secret_name="some_secret_name")

@patch("utils.load_dotenv")
@patch.dict(os.environ, {}, clear=True)
def test_get_secret_no_env_no_gcs_name(mock_dotenv):
    """FAILURE POINT: Neither environment variable nor GCS fallback is provided."""
    with pytest.raises(Exception, match="Secret MISSING_ENV_VAR not found"):
        get_secret("MISSING_ENV_VAR")

# --- _get_sa_credentials ---

@patch("glob.glob", return_value=["/home/fake/.config/mf-crucible-1.json"])
@patch("builtins.open", new_callable=mock_open, read_data="")
@patch("utils.get_secret", return_value="fallback_secret")
def test_get_sa_credentials_empty_file(mock_get_secret, mock_file, mock_glob):
    """EDGE CASE: Config file exists but is completely empty. Should fallback to get_secret."""
    result = _get_sa_credentials("/home/fake")
    assert result == "fallback_secret"
    mock_get_secret.assert_called_once()

# --- run_rclone_command ---

@patch("utils.get_secret", return_value="fake_client_secret")
@patch("utils._get_sa_credentials", return_value='{"type": "service_account"}')
@patch("utils.run_shell")
def test_run_rclone_command_exception_retry(mock_run_shell, mock_get_sa, mock_get_secret):
    """FAILURE POINT: The initial rclone command fails. It should catch the exception, 
    rewrite the path with the config name 'mf-cloud-storage', and retry."""
    
    # First call raises an exception, second call succeeds
    mock_run_shell.side_effect = [Exception("Rclone crashed!"), MagicMock(stdout="success", stderr="")]
    
    source = "/mnt/gcs/source:gcs"
    dest = "/mnt/gcs/dest:gcs"
    
    result = run_rclone_command(source_path=source, destination_path=dest)
    
    assert mock_run_shell.call_count == 2
    # Ensure the string replacement logic (replace ':gcs' with config name) was executed on the retry
    second_call_args = mock_run_shell.call_args_list[1][0][0]
    assert "mf-cloud-storage" in second_call_args
    assert result.stdout == "success"

# --- EnhancedJSONEncoder ---

def test_enhanced_json_encoder_unsupported_type():
    """EDGE CASE: Passed a type that Numpy/Datetime JSON encoder does not know how to handle."""
    class CustomObj:
        pass
        
    obj = {"unsupported": CustomObj()}
    
    with pytest.raises(TypeError, match="Object of type CustomObj is not JSON serializable"):
        json.dumps(obj, cls=EnhancedJSONEncoder)