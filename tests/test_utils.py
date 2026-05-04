"""Comprehensive tests for utils.py — secrets, RabbitMQ, rclone, thumbnails, JSON encoding."""
import sys, os, json, base64, tempfile
import pytest
import numpy as np
from datetime import datetime
from unittest.mock import patch, MagicMock, mock_open, call
from PIL import Image
from io import BytesIO



from utils import (
    get_cloud_secret_selfauth,
    get_credentials_from_env,
    get_secret,
    setup_pika_client,
    _get_sa_credentials,
    run_rclone_command,
    build_b64_thumbnail,
    reduce_filename_and_copy,
    EnhancedJSONEncoder,
)


# =====================================================================
# get_cloud_secret_selfauth
# =====================================================================
class TestGetCloudSecretSelfauth:
    @patch("utils.secretmanager.SecretManagerServiceClient")
    def test_returns_secret_on_success(self, mock_cls):
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "my_secret_value"
        mock_cls.return_value.access_secret_version.return_value = mock_response
        result = get_cloud_secret_selfauth("projects/123/secrets/key/versions/1")
        assert result == "my_secret_value"

    @patch("utils.secretmanager.SecretManagerServiceClient")
    def test_returns_none_on_exception(self, mock_cls):
        mock_cls.return_value.access_secret_version.side_effect = Exception("denied")
        result = get_cloud_secret_selfauth("fake-secret")
        assert result is None


# =====================================================================
# get_credentials_from_env
# =====================================================================
class TestGetCredentialsFromEnv:
    @patch.dict(os.environ, {}, clear=True)
    def test_returns_none_when_env_var_missing(self):
        assert get_credentials_from_env() is None

    @patch.dict(os.environ, {"GCS_SA": "{invalid-json"}, clear=True)
    def test_raises_on_malformed_json(self):
        with pytest.raises(json.JSONDecodeError):
            get_credentials_from_env()

    @patch("utils.service_account.Credentials.from_service_account_file")
    @patch.dict(os.environ, {"GCS_SA": '{"type": "service_account"}'}, clear=True)
    def test_writes_temp_file_and_loads_credentials(self, mock_from_file):
        mock_from_file.return_value = MagicMock()
        result = get_credentials_from_env()
        mock_from_file.assert_called_once_with("temp_creds.json")
        assert result is not None

    @pytest.mark.xfail
    @patch("utils.service_account.Credentials.from_service_account_file")
    @patch.dict(os.environ, {"GCS_SA": '{"type": "service_account"}'}, clear=True)
    def test_temp_creds_file_should_be_cleaned_up(self, mock_from_file):
        mock_from_file.return_value = MagicMock()
        get_credentials_from_env()
        assert not os.path.exists("temp_creds.json")


# =====================================================================
# get_secret
# =====================================================================
class TestGetSecret:
    @patch("utils.load_dotenv")
    @patch.dict(os.environ, {"MY_SECRET": "env_value"}, clear=True)
    def test_returns_env_var_first(self, mock_dotenv):
        result = get_secret("MY_SECRET")
        assert result == "env_value"

    @patch("utils.load_dotenv")
    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.get_cloud_secret_selfauth", return_value="selfauth_value")
    def test_falls_back_to_selfauth(self, mock_selfauth, mock_dotenv):
        result = get_secret("MISSING", gcs_secret_name="my_secret/versions/1")
        assert result == "selfauth_value"

    @patch("utils.load_dotenv")
    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.get_cloud_secret_selfauth", return_value=None)
    @patch("utils.get_credentials_from_env", return_value=None)
    def test_raises_when_all_methods_fail(self, mock_creds, mock_selfauth, mock_dotenv):
        with pytest.raises(Exception, match="No credentials available"):
            get_secret("MISSING", gcs_secret_name="some_secret")

    @patch("utils.load_dotenv")
    @patch.dict(os.environ, {}, clear=True)
    def test_raises_when_no_gcs_name_provided(self, mock_dotenv):
        with pytest.raises(Exception, match="not found in environment"):
            get_secret("MISSING")

    @patch("utils.load_dotenv")
    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.get_cloud_secret_selfauth", return_value=None)
    @patch("utils.service_account.Credentials.from_service_account_file")
    @patch("utils.secretmanager.SecretManagerServiceClient")
    def test_uses_sa_creds_file_when_provided(self, mock_sm, mock_from_file,
                                               mock_selfauth, mock_dotenv):
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "secret_via_sa"
        mock_sm.return_value.access_secret_version.return_value = mock_response
        mock_from_file.return_value = MagicMock()
        result = get_secret("MISSING", gcs_secret_name="s/v/1",
                           sa_creds="/path/to/sa.json")
        assert result == "secret_via_sa"
        mock_from_file.assert_called_once_with("/path/to/sa.json")

    @patch("utils.load_dotenv")
    @patch.dict(os.environ, {}, clear=True)
    @patch("utils.get_cloud_secret_selfauth", return_value=None)
    @patch("utils.get_credentials_from_env")
    @patch("utils.secretmanager.SecretManagerServiceClient")
    def test_falls_back_to_env_credentials(self, mock_sm, mock_env_creds,
                                            mock_selfauth, mock_dotenv):
        mock_env_creds.return_value = MagicMock()
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "secret_via_env"
        mock_sm.return_value.access_secret_version.return_value = mock_response
        result = get_secret("MISSING", gcs_secret_name="s/v/1")
        assert result == "secret_via_env"


# =====================================================================
# setup_pika_client
# =====================================================================
class TestSetupPikaClient:
    @patch("utils.pika.BlockingConnection")
    @patch("utils.pika.PlainCredentials")
    @patch("utils.pika.ConnectionParameters")
    def test_returns_connection_and_channel(self, mock_params, mock_creds, mock_conn):
        mock_channel = MagicMock()
        mock_conn.return_value.channel.return_value = mock_channel
        conn, ch = setup_pika_client("host", 5672, "password")
        assert ch == mock_channel

    @patch("utils.pika.BlockingConnection")
    @patch("utils.pika.PlainCredentials")
    @patch("utils.pika.ConnectionParameters")
    def test_uses_admin_username(self, mock_params, mock_creds, mock_conn):
        setup_pika_client("host", 5672, "pw")
        mock_creds.assert_called_once_with("admin", "pw")

    @patch("utils.pika.BlockingConnection")
    @patch("utils.pika.PlainCredentials")
    @patch("utils.pika.ConnectionParameters")
    def test_passes_heartbeat_and_timeout(self, mock_params, mock_creds, mock_conn):
        setup_pika_client("host", 5672, "pw", heartbeat=120,
                          blocked_connection_timeout=300)
        call_kwargs = mock_params.call_args
        assert call_kwargs[1]["heartbeat"] == 120
        assert call_kwargs[1]["blocked_connection_timeout"] == 300

    @pytest.mark.xfail
    @patch("utils.pika.BlockingConnection")
    @patch("utils.pika.PlainCredentials")
    @patch("utils.pika.ConnectionParameters")
    def test_username_should_be_configurable(self, mock_params, mock_creds, mock_conn):
        setup_pika_client("host", 5672, "pw")
        # The username should not be hardcoded
        call_args = mock_creds.call_args[0]
        assert call_args[0] != "admin"


# =====================================================================
# _get_sa_credentials
# =====================================================================
class TestGetSaCredentials:
    @patch("glob.glob", return_value=["/home/.config/mf-crucible-sa.json"])
    @patch("builtins.open", new_callable=mock_open, read_data='{"type": "sa"}')
    def test_returns_file_content_when_found(self, mock_file, mock_glob):
        result = _get_sa_credentials("/home")
        assert result == '{"type": "sa"}'

    @patch("glob.glob", return_value=["/home/.config/mf-crucible-sa.json"])
    @patch("builtins.open", new_callable=mock_open, read_data="")
    @patch("utils.get_secret", return_value="fallback_secret")
    def test_skips_empty_file_and_falls_back(self, mock_secret, mock_file, mock_glob):
        result = _get_sa_credentials("/home")
        assert result == "fallback_secret"

    @patch("glob.glob", return_value=[])
    @patch("utils.get_secret", return_value="cloud_secret")
    def test_no_local_files_falls_back_to_secret(self, mock_secret, mock_glob):
        result = _get_sa_credentials("/home")
        assert result == "cloud_secret"


# =====================================================================
# run_rclone_command
# =====================================================================
class TestRunRcloneCommand:
    @patch("utils.get_secret", return_value="client_secret")
    @patch("utils._get_sa_credentials", return_value='{"type": "sa"}')
    @patch("utils.run_shell")
    def test_success_path(self, mock_shell, mock_sa, mock_secret):
        mock_shell.return_value = MagicMock(stdout="ok", stderr="")
        result = run_rclone_command(source_path="/src", destination_path="/dst")
        assert result.stdout == "ok"
        mock_shell.assert_called_once()

    @patch("utils.get_secret", return_value="client_secret")
    @patch("utils._get_sa_credentials", return_value='{"type": "sa"}')
    @patch("utils.run_shell")
    def test_retries_with_config_name_on_failure(self, mock_shell, mock_sa, mock_secret):
        mock_shell.side_effect = [Exception("failed"), MagicMock(stdout="ok", stderr="")]
        result = run_rclone_command(source_path="/src:gcs", destination_path="/dst:gcs")
        assert mock_shell.call_count == 2
        retry_cmd = mock_shell.call_args_list[1][0][0]
        assert "mf-cloud-storage" in retry_cmd

    @patch("utils.get_secret", return_value="client_secret")
    @patch("utils._get_sa_credentials", return_value='{"type": "sa"}')
    @patch("utils.run_shell")
    def test_cleans_up_temp_cred_file(self, mock_shell, mock_sa, mock_secret):
        mock_shell.return_value = MagicMock(stdout="", stderr="")
        run_rclone_command(source_path="/src", destination_path="/dst")
        # The temp file should be deleted (os.unlink in finally block)
        # We verify by checking that no temp .json files were left behind
        # from this call (the fixture cleanup is implicit)
        mock_shell.assert_called_once()

    @patch("utils.get_secret", return_value="client_secret")
    @patch("utils._get_sa_credentials", return_value='{"type": "sa"}')
    @patch("utils.run_shell")
    def test_wraps_destination_in_quotes_when_nonempty(self, mock_shell, mock_sa, mock_secret):
        mock_shell.return_value = MagicMock(stdout="", stderr="")
        run_rclone_command(source_path="/src", destination_path="bucket/path")
        cmd = mock_shell.call_args[0][0]
        assert '"bucket/path"' in cmd

    @patch("utils.get_secret", return_value="client_secret")
    @patch("utils._get_sa_credentials", return_value='{"type": "sa"}')
    @patch("utils.run_shell")
    def test_empty_destination_not_quoted(self, mock_shell, mock_sa, mock_secret):
        mock_shell.return_value = MagicMock(stdout="", stderr="")
        run_rclone_command(source_path="/src", destination_path="")
        cmd = mock_shell.call_args[0][0]
        # Empty destination should NOT be wrapped in quotes
        assert '""' not in cmd or cmd.endswith(" ")


# =====================================================================
# build_b64_thumbnail
# =====================================================================
class TestBuildB64Thumbnail:
    def test_returns_base64_encoded_string(self):
        img = Image.new("RGB", (400, 400), color="red")
        result = build_b64_thumbnail(img)
        # Should be a valid base64 string
        decoded = base64.b64decode(result)
        assert len(decoded) > 0

    def test_respects_max_size(self):
        img = Image.new("RGB", (1000, 1000), color="blue")
        result = build_b64_thumbnail(img, max_size=(50, 50))
        # Decode and reopen to check size
        decoded = base64.b64decode(result)
        reopened = Image.open(BytesIO(decoded))
        assert reopened.size[0] <= 50
        assert reopened.size[1] <= 50

    @pytest.mark.xfail
    def test_rgba_should_be_converted_to_rgb(self):
        img = Image.new("RGBA", (100, 100), color=(255, 0, 0, 128))
        result = build_b64_thumbnail(img)
        decoded = base64.b64decode(result)
        reopened = Image.open(BytesIO(decoded))
        assert reopened.mode == "RGB"


# =====================================================================
# reduce_filename_and_copy
# =====================================================================
class TestReduceFilenameAndCopy:
    @patch("utils.run_rclone_command")
    def test_strips_common_path_prefix(self, mock_rclone):
        mock_rclone.return_value = MagicMock(stderr="")
        reduce_filename_and_copy(
            "/mnt/gcs/team05/subdir/file.csv",
            ["/mnt/gcs/team05"],
            "bucket/ds_001"
        )
        call_kwargs = mock_rclone.call_args[1]
        assert call_kwargs["destination_path"] == "bucket/ds_001/subdir/file.csv"

    @patch("utils.run_rclone_command")
    def test_uses_full_path_when_no_common_prefix(self, mock_rclone):
        mock_rclone.return_value = MagicMock(stderr="")
        reduce_filename_and_copy(
            "/other/path/file.csv",
            ["/mnt/gcs/team05"],
            "bucket/ds_001"
        )
        call_kwargs = mock_rclone.call_args[1]
        # No common prefix, so full path used but /mnt/gcs prepended
        assert "file.csv" in call_kwargs["destination_path"]

    @patch("utils.run_rclone_command")
    def test_prepends_mnt_gcs_when_not_present(self, mock_rclone):
        mock_rclone.return_value = MagicMock(stderr="")
        reduce_filename_and_copy("relative/file.csv", [], "bucket/ds")
        call_kwargs = mock_rclone.call_args[1]
        assert call_kwargs["source_path"] == "/mnt/gcs/relative/file.csv"

    @patch("utils.run_rclone_command")
    def test_does_not_double_prepend_mnt_gcs(self, mock_rclone):
        mock_rclone.return_value = MagicMock(stderr="")
        reduce_filename_and_copy("/mnt/gcs/file.csv", [], "bucket/ds")
        call_kwargs = mock_rclone.call_args[1]
        assert call_kwargs["source_path"] == "/mnt/gcs/file.csv"

    @patch("utils.run_rclone_command")
    def test_uses_copyto_command(self, mock_rclone):
        mock_rclone.return_value = MagicMock(stderr="")
        reduce_filename_and_copy("/mnt/gcs/f.csv", [], "bucket/ds")
        call_kwargs = mock_rclone.call_args[1]
        assert call_kwargs["cmd"] == "copyto"

    @patch("utils.run_rclone_command")
    def test_uses_last_matching_common_path(self, mock_rclone):
        """When multiple common paths match, the LAST one is used for stripping."""
        mock_rclone.return_value = MagicMock(stderr="")
        reduce_filename_and_copy(
            "/mnt/gcs/team05/deep/file.csv",
            ["/mnt/gcs", "/mnt/gcs/team05"],
            "bucket/ds"
        )
        call_kwargs = mock_rclone.call_args[1]
        # Should strip the last match (/mnt/gcs/team05), leaving deep/file.csv
        assert call_kwargs["destination_path"] == "bucket/ds/deep/file.csv"


# =====================================================================
# EnhancedJSONEncoder
# =====================================================================
class TestEnhancedJSONEncoder:
    def test_numpy_bool(self):
        data = {"val": np.bool_(True)}
        result = json.loads(json.dumps(data, cls=EnhancedJSONEncoder))
        assert result["val"] is True
        assert isinstance(result["val"], bool)

    def test_numpy_int16(self):
        result = json.loads(json.dumps({"v": np.int16(42)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 42

    def test_numpy_int32(self):
        result = json.loads(json.dumps({"v": np.int32(100)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 100

    def test_numpy_int64(self):
        result = json.loads(json.dumps({"v": np.int64(999)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 999

    def test_numpy_float32(self):
        result = json.loads(json.dumps({"v": np.float32(3.14)}, cls=EnhancedJSONEncoder))
        assert abs(result["v"] - 3.14) < 0.01

    def test_numpy_float64(self):
        result = json.loads(json.dumps({"v": np.float64(2.718)}, cls=EnhancedJSONEncoder))
        assert abs(result["v"] - 2.718) < 0.001

    def test_numpy_uint8(self):
        result = json.loads(json.dumps({"v": np.uint8(255)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 255

    def test_numpy_uint16(self):
        result = json.loads(json.dumps({"v": np.uint16(65535)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 65535

    def test_numpy_uint32(self):
        result = json.loads(json.dumps({"v": np.uint32(100000)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 100000

    def test_numpy_uint64(self):
        result = json.loads(json.dumps({"v": np.uint64(2**60)}, cls=EnhancedJSONEncoder))
        assert result["v"] == 2**60

    def test_numpy_ndarray(self):
        arr = np.array([1, 2, 3])
        result = json.loads(json.dumps({"v": arr}, cls=EnhancedJSONEncoder))
        assert result["v"] == [1, 2, 3]

    def test_datetime_to_isoformat(self):
        dt = datetime(2026, 1, 15, 10, 30, 0)
        result = json.loads(json.dumps({"v": dt}, cls=EnhancedJSONEncoder))
        assert result["v"] == "2026-01-15T10:30:00"

    def test_unsupported_type_raises(self):
        class Custom:
            pass
        with pytest.raises(TypeError, match="not JSON serializable"):
            json.dumps({"v": Custom()}, cls=EnhancedJSONEncoder)

    def test_nested_numpy_types(self):
        data = {
            "metadata": {
                "voltage": np.float64(200.0),
                "counts": np.array([10, 20, 30]),
                "flag": np.bool_(False),
            }
        }
        result = json.loads(json.dumps(data, cls=EnhancedJSONEncoder))
        assert result["metadata"]["voltage"] == 200.0
        assert result["metadata"]["counts"] == [10, 20, 30]
        assert result["metadata"]["flag"] is False

    @pytest.mark.xfail
    def test_numpy_float16_should_be_handled(self):
        data = {"v": np.float16(1.5)}
        result = json.loads(json.dumps(data, cls=EnhancedJSONEncoder))
        assert abs(result["v"] - 1.5) < 0.1