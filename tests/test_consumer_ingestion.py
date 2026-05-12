"""
Comprehensive unit tests for consumer-ingestion-process.py

Tests the RabbitMQ consumer worker: message parsing, file validation (lost/too big),
retry logic, ingestion orchestration, status updates, and error handling.

The module executes significant code at import time (secrets, RabbitMQ setup,
CrucibleClient creation), so we mock all of that before importing.
"""

import sys
import json
import pytest
from unittest.mock import patch, MagicMock, call

# ============================================================================
# 1. SYSTEM-LEVEL MOCKING (before import)
# ============================================================================



# Patch module-level calls that execute on import
with patch("utils.get_secret", return_value="fake_secret"), \
     patch("utils.setup_pika_client", return_value=(MagicMock(), MagicMock())):
    # Must use importlib since the filename has a hyphen
    import importlib
    consumer = importlib.import_module("consumer-ingestion-process")

# Grab references to the functions we'll test
is_file_lost = consumer.is_file_lost
is_file_too_big = consumer.is_file_too_big
callback = consumer.callback


# ============================================================================
# HELPERS
# ============================================================================

def make_message(filename="team05/sample.dm4", dsid="ds_001", reqid="req_001",
                 ingestion_class=None):
    """Create a standard RabbitMQ message dict."""
    return {
        "filename": filename,
        "dsid": dsid,
        "reqid": reqid,
        "ingestion_class": ingestion_class,
    }


def make_body(message):
    """Encode a message dict to bytes, as RabbitMQ would deliver it."""
    return json.dumps(message).encode("utf-8")


def make_method():
    """Create a mock RabbitMQ method object with a delivery_tag."""
    m = MagicMock()
    m.delivery_tag = "tag_123"
    return m


# ============================================================================
# TESTS: is_file_lost
# ============================================================================

class TestIsFileLost:

    def test_returns_false_when_file_exists(self):
        """If the file exists on disk, it is not lost."""
        msg = make_message()
        with patch("os.path.exists", return_value=True):
            result = is_file_lost(msg, MagicMock())
        assert result is False

    def test_returns_true_when_file_missing(self):
        """If the file does not exist, it is lost."""
        msg = make_message()
        with patch("os.path.exists", return_value=False):
            result = is_file_lost(msg, MagicMock())
        assert result is True

    def test_updates_status_when_file_missing_and_update_status_true(self):
        """When file is missing and update_status=True, should notify the API."""
        msg = make_message(dsid="ds_lost", reqid="req_lost")
        with patch("os.path.exists", return_value=False):
            with patch.object(consumer, "client") as mock_client:
                is_file_lost(msg, MagicMock(), update_status=True)
                mock_client.update_ingestion_status.assert_called_once_with(
                    "ds_lost", "req_lost", "file not found"
                )

    def test_does_not_update_status_when_update_status_false(self):
        """When update_status=False (retry attempts), should NOT call the API."""
        msg = make_message()
        with patch("os.path.exists", return_value=False):
            with patch.object(consumer, "client") as mock_client:
                is_file_lost(msg, MagicMock(), update_status=False)
                mock_client.update_ingestion_status.assert_not_called()

    def test_normalizes_backslashes_in_filename(self):
        """Windows-style backslashes should be converted to forward slashes."""
        msg = make_message(filename="team05\\subdir\\sample.dm4")
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            is_file_lost(msg, MagicMock())
            mock_exists.assert_called_once_with("/mnt/gcs/team05/subdir/sample.dm4")

    def test_constructs_correct_path(self):
        """The checked path should be /mnt/gcs/ + filename."""
        msg = make_message(filename="jupiterafm/experiment.ibw")
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            is_file_lost(msg, MagicMock())
            mock_exists.assert_called_once_with("/mnt/gcs/jupiterafm/experiment.ibw")


# ============================================================================
# TESTS: is_file_too_big
# ============================================================================

class TestIsFileTooBig:

    def test_returns_false_for_small_file(self):
        """Files under 10GB should not be considered too big."""
        msg = make_message()
        with patch("os.path.getsize", return_value=500_000_000):  # 500MB
            result = is_file_too_big(msg, MagicMock())
        assert result is False

    def test_returns_true_for_file_over_10gb(self):
        """Files over 10GB (1e10 bytes) should be flagged as too big."""
        msg = make_message()
        with patch("os.path.getsize", return_value=int(1.1e10)):
            with patch.object(consumer, "client"):
                result = is_file_too_big(msg, MagicMock())
        assert result is True

    def test_returns_false_for_file_exactly_at_limit(self):
        """A file of exactly 1e10 bytes should NOT be too big (uses > not >=)."""
        msg = make_message()
        with patch("os.path.getsize", return_value=int(1e10)):
            result = is_file_too_big(msg, MagicMock())
        assert result is False

    def test_updates_status_when_too_big(self):
        """Should notify the API that the file is too large."""
        msg = make_message(dsid="ds_big", reqid="req_big")
        with patch("os.path.getsize", return_value=int(2e10)):
            with patch.object(consumer, "client") as mock_client:
                is_file_too_big(msg, MagicMock())
                mock_client.update_ingestion_status.assert_called_once_with(
                    "ds_big", "req_big", "file too large"
                )

    def test_does_not_update_status_when_ok(self):
        """Should NOT call the API if file size is acceptable."""
        msg = make_message()
        with patch("os.path.getsize", return_value=100):
            with patch.object(consumer, "client") as mock_client:
                is_file_too_big(msg, MagicMock())
                mock_client.update_ingestion_status.assert_not_called()

    def test_normalizes_backslashes(self):
        """Filename backslashes should be converted before checking size."""
        msg = make_message(filename="team05\\data\\big.dm4")
        with patch("os.path.getsize") as mock_size:
            mock_size.return_value = 100
            is_file_too_big(msg, MagicMock())
            mock_size.assert_called_once_with("/mnt/gcs/team05/data/big.dm4")




# ============================================================================
# TESTS: callback (the main orchestration function)
# ============================================================================

class TestCallback:

    def _run_callback(self, message, **overrides):
        """Helper to invoke callback with proper mocking.
        Returns (mock_channel, mock_method, mock_client) for assertions."""
        ch = MagicMock()
        method = make_method()
        body = make_body(message)

        defaults = {
            "is_file_lost_return": False,
            "is_file_too_big_return": False,
            "data_ingestion_return": (MagicMock(unique_id="ds_result"), "bucket_name"),
        }
        defaults.update(overrides)

        with patch.object(consumer, "is_file_lost", return_value=defaults["is_file_lost_return"]) as mock_lost, \
             patch.object(consumer, "is_file_too_big", return_value=defaults["is_file_too_big_return"]) as mock_big, \
             patch.object(consumer, "data_ingestion", return_value=defaults["data_ingestion_return"]) as mock_ingest, \
             patch.object(consumer, "client") as mock_client, \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"):
            callback(ch, method, None, body)

        return ch, method, mock_client, mock_ingest

    # --- Happy path ---

    def test_successful_ingestion_marks_complete(self):
        """When ingestion succeeds, status should be updated to 'complete'."""
        msg = make_message()
        ch, method, mock_client, _ = self._run_callback(msg)

        # Should have called update_ingestion_status with "started" then "complete"
        status_calls = mock_client.update_ingestion_status.call_args_list
        statuses = [c[0][2] for c in status_calls]
        assert "started" in statuses
        assert "complete" in statuses

    def test_successful_ingestion_acks_message(self):
        """Successful ingestion must acknowledge the RabbitMQ message."""
        msg = make_message()
        ch, method, _, _ = self._run_callback(msg)
        ch.basic_ack.assert_called_once_with(delivery_tag=method.delivery_tag)

    def test_passes_correct_args_to_data_ingestion(self):
        """The data_ingestion function should receive the correct parameters
        derived from the RabbitMQ message."""
        msg = make_message(filename="team05/exp.dm4", dsid="ds_x", reqid="req_x",
                           ingestion_class="DMIngestor")
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=False), \
             patch.object(consumer, "is_file_too_big", return_value=False), \
             patch.object(consumer, "data_ingestion") as mock_ingest, \
             patch.object(consumer, "client") as mock_client, \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"):
            mock_ingest.return_value = (MagicMock(unique_id="ds_x"), "bucket")
            callback(ch, method, None, body)

        call_kwargs = mock_ingest.call_args[1]
        assert call_kwargs["dataset_to_process"] == "/mnt/gcs/team05/exp.dm4"
        assert call_kwargs["dsid"] == "ds_x"
        assert call_kwargs["reqid"] == "req_x"
        assert call_kwargs["ingestion_class"] == "DMIngestor"

    # --- Not supported ---

    def test_not_supported_publishes_to_queue(self):
        """When data_ingestion returns (None, None), the message should be
        published to the 'not-supported' queue."""
        msg = make_message()
        ch, _, mock_client, _ = self._run_callback(
            msg, data_ingestion_return=(None, None)
        )

        ch.basic_publish.assert_called_once()
        publish_kwargs = ch.basic_publish.call_args[1]
        assert publish_kwargs["routing_key"] == "not-supported"

    def test_not_supported_updates_status(self):
        """Status should be set to 'not supported' when no ingestor matches."""
        msg = make_message()
        _, _, mock_client, _ = self._run_callback(
            msg, data_ingestion_return=(None, None)
        )
        status_calls = mock_client.update_ingestion_status.call_args_list
        statuses = [c[0][2] for c in status_calls]
        assert "not supported" in statuses

    def test_not_supported_still_acks_message(self):
        """Even when not supported, the message must be acknowledged to prevent
        infinite redelivery."""
        msg = make_message()
        ch, method, _, _ = self._run_callback(
            msg, data_ingestion_return=(None, None)
        )
        ch.basic_ack.assert_called_once()

    # --- File too big ---

    def test_file_too_big_acks_and_returns_early(self):
        """When the file is too big, the message should be acked and callback
        should return without attempting ingestion."""
        msg = make_message()
        ch, method, _, mock_ingest = self._run_callback(
            msg, is_file_too_big_return=True
        )
        ch.basic_ack.assert_called_once()
        mock_ingest.assert_not_called()

    # --- File lost (retry exhaustion) ---

    def test_file_lost_after_retries_acks_and_returns(self):
        """When is_file_lost returns True on all attempts, the callback should
        ack the message and return without processing."""
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=True), \
             patch.object(consumer, "data_ingestion") as mock_ingest, \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"), \
             patch("time.sleep"):
            callback(ch, method, None, body)

        ch.basic_ack.assert_called_once()
        mock_ingest.assert_not_called()

    def test_file_found_on_retry_continues_processing(self):
        """If the file is lost on first attempts but found on a retry,
        processing should continue normally."""
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        # Lost twice, then found on third attempt
        with patch.object(consumer, "is_file_lost", side_effect=[True, True, False]), \
             patch.object(consumer, "is_file_too_big", return_value=False), \
             patch.object(consumer, "data_ingestion") as mock_ingest, \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"), \
             patch("time.sleep"):
            mock_ingest.return_value = (MagicMock(unique_id="ds_1"), "bucket")
            callback(ch, method, None, body)

        # Should have proceeded to ingest
        mock_ingest.assert_called_once()

    def test_retry_uses_exponential_backoff(self):
        """Retries should use exponential backoff: sleep(2^attempt)."""
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        # File lost on all 7 attempts
        with patch.object(consumer, "is_file_lost", return_value=True), \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"), \
             patch("time.sleep") as mock_sleep:
            callback(ch, method, None, body)

        # Should sleep for 2^1, 2^2, 2^3, 2^4, 2^5, 2^6 (6 sleeps, not 7)
        sleep_calls = [c[0][0] for c in mock_sleep.call_args_list]
        assert sleep_calls == [2, 4, 8, 16, 32, 64]

    def test_retry_only_updates_status_on_last_attempt(self):
        """is_file_lost should only be called with update_status=True on the
        final attempt (attempt == max_file_retries)."""
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=True) as mock_lost, \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"), \
             patch("time.sleep"):
            callback(ch, method, None, body)

        # Check the update_status argument for each call
        update_flags = [c[1].get("update_status", True) for c in mock_lost.call_args_list]
        # Only the last one should be True
        assert update_flags[-1] is True
        assert all(flag is False for flag in update_flags[:-1])

    # --- Exception handling ---

    def test_ingestion_exception_marks_failed(self):
        """When data_ingestion raises an exception, status should be 'failed'."""
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=False), \
             patch.object(consumer, "is_file_too_big", return_value=False), \
             patch.object(consumer, "data_ingestion", side_effect=Exception("Parse error")), \
             patch.object(consumer, "client") as mock_client, \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"):
            callback(ch, method, None, body)

        status_calls = mock_client.update_ingestion_status.call_args_list
        statuses = [c[0][2] for c in status_calls]
        assert "failed" in statuses



    # --- Message parsing ---

    def test_normalizes_backslashes_in_filename(self):
        """Windows-style backslashes in the filename should be normalized
        to forward slashes before passing to data_ingestion."""
        msg = make_message(filename="team05\\subdir\\file.dm4")
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=False), \
             patch.object(consumer, "is_file_too_big", return_value=False), \
             patch.object(consumer, "data_ingestion") as mock_ingest, \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"):
            mock_ingest.return_value = (MagicMock(unique_id="ds_1"), "bucket")
            callback(ch, method, None, body)

        call_kwargs = mock_ingest.call_args[1]
        assert call_kwargs["dataset_to_process"] == "/mnt/gcs/team05/subdir/file.dm4"

    def test_sets_started_status_at_beginning(self):
        """The very first status update should always be 'started'."""
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=False), \
             patch.object(consumer, "is_file_too_big", return_value=False), \
             patch.object(consumer, "data_ingestion") as mock_ingest, \
             patch.object(consumer, "client") as mock_client, \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"):
            mock_ingest.return_value = (MagicMock(unique_id="ds_1"), "bucket")
            callback(ch, method, None, body)

        first_status_call = mock_client.update_ingestion_status.call_args_list[0]
        assert first_status_call[0][2] == "started"

    def test_malformed_json_body_raises_error(self):
        """FAILURE POINT: If the RabbitMQ message body is not valid JSON,
        json.loads should raise a JSONDecodeError."""
        ch = MagicMock()
        method = make_method()
        body = b"this is not json"

        with pytest.raises(json.JSONDecodeError):
            callback(ch, method, None, body)

    def test_missing_required_fields_raises_key_error(self):
        """FAILURE POINT: If the message is missing required fields (filename,
        dsid, reqid, ingestion_class), the callback should raise KeyError."""
        ch = MagicMock()
        method = make_method()
        body = json.dumps({"filename": "test.dm4"}).encode("utf-8")  # missing dsid, reqid

        with pytest.raises(KeyError):
            callback(ch, method, None, body)


# ============================================================================
# BUG-CATCHING TESTS:
# They are marked xfail because the code does not currently behave correctly.
# When the bugs are fixed, these tests will start passing and the xfail
# marker should be removed.
# ============================================================================

class TestKnownBugs:

    @pytest.mark.xfail
    def test_failed_ingestion_should_ack_message(self):
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=False), \
             patch.object(consumer, "is_file_too_big", return_value=False), \
             patch.object(consumer, "data_ingestion", side_effect=Exception("crash")), \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"):
            callback(ch, method, None, body)

        assert ch.basic_ack.called or ch.basic_nack.called

    @pytest.mark.xfail
    def test_files_over_2gb_should_be_rejected_per_docstring(self):
        msg = make_message()
        with patch("os.path.getsize", return_value=int(5e9)):  # 5GB
            with patch.object(consumer, "client"):
                result = is_file_too_big(msg, MagicMock())
        assert result is True

    @pytest.mark.xfail
    def test_retries_should_match_documented_count(self):
        msg = make_message()
        ch = MagicMock()
        method = make_method()
        body = make_body(msg)

        with patch.object(consumer, "is_file_lost", return_value=True) as mock_lost, \
             patch.object(consumer, "client"), \
             patch.object(consumer, "get_tz_isoformat", return_value="20260115T100000"), \
             patch("time.sleep"):
            callback(ch, method, None, body)

        assert mock_lost.call_count == 5

    @pytest.mark.xfail
    def test_is_file_lost_should_use_channel_parameter(self):
        msg = make_message()
        ch = MagicMock()
        with patch("os.path.exists", return_value=False):
            with patch.object(consumer, "client"):
                is_file_lost(msg, ch, update_status=True)

        ch.basic_publish.assert_called()

    @pytest.mark.xfail
    def test_oversized_file_should_be_published_to_large_file_queue(self):
        msg = make_message()
        ch = MagicMock()
        with patch("os.path.getsize", return_value=int(2e10)):
            with patch.object(consumer, "client"):
                is_file_too_big(msg, ch)

        ch.basic_publish.assert_called()
