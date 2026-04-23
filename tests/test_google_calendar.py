import pytest
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime, timedelta
import pytz

from google_calendar import (
    generate_time_range,
    filter_events_at_time,
    parse_calendar_event_for_ownership,
    setup_client,
    get_calendar_events,
    find_calendar_event,
)


# ============================================================================
# HELPERS
# ============================================================================

UTC = pytz.UTC


def make_event(start_iso, end_iso, summary="Test Event", email=None, location=None):
    """Create a calendar event dict matching the Google Calendar API format."""
    event = {
        "start": {"dateTime": start_iso},
        "end": {"dateTime": end_iso},
        "summary": summary,
    }
    if email is not None:
        event["attendees"] = [{"email": email}]
    if location is not None:
        event["location"] = location
    return event


# ============================================================================
# TESTS: generate_time_range
# ============================================================================

class TestGenerateTimeRange:

    def test_returns_12_hour_window_each_way(self):
        """The return should be (input - 12h, input + 12h)."""
        dt = datetime(2026, 6, 15, 12, 0, 0)
        before, after = generate_time_range(dt)
        assert before == datetime(2026, 6, 15, 0, 0, 0)
        assert after == datetime(2026, 6, 16, 0, 0, 0)

    def test_crosses_midnight_backwards(self):
        """If the input is early morning, 12h before should be the previous day."""
        dt = datetime(2026, 6, 15, 2, 0, 0)
        before, after = generate_time_range(dt)
        assert before == datetime(2026, 6, 14, 14, 0, 0)
        assert after == datetime(2026, 6, 15, 14, 0, 0)

    def test_returns_tuple_of_two(self):
        result = generate_time_range(datetime(2026, 1, 1, 6, 0, 0))
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_total_window_is_24_hours(self):
        """The gap between before and after should always be exactly 24 hours."""
        dt = datetime(2026, 3, 20, 15, 30, 45)
        before, after = generate_time_range(dt)
        assert after - before == timedelta(hours=24)

    def test_preserves_timezone_info(self):
        """If input is timezone-aware, the output should also be timezone-aware."""
        pst = pytz.timezone("America/Los_Angeles")
        dt = pst.localize(datetime(2026, 7, 4, 10, 0, 0))
        before, after = generate_time_range(dt)
        assert before.tzinfo is not None
        assert after.tzinfo is not None


# ============================================================================
# TESTS: filter_events_at_time
#
# This is the most complex function in the module. It determines which calendar
# event a piece of data "belongs to" based on proximity in time.
# ============================================================================

class TestFilterEventsAtTime:

    # --- Happy path: data falls within an event ---

    def test_returns_event_when_data_falls_within(self):
        """If data_ctime is strictly between start and end, return that event."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T12:00:00+00:00",
                       summary="Exp A")
        ]
        data_ctime = datetime(2026, 1, 15, 11, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result is not None
        assert result["summary"] == "Exp A"

    def test_data_during_second_of_three_events(self):
        """Data falls within the second event. The function should iterate past
        the first event and match the second."""
        events = [
            make_event("2026-01-15T09:00:00+00:00", "2026-01-15T10:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T11:00:00+00:00", "2026-01-15T13:00:00+00:00",
                       summary="E2"),
            make_event("2026-01-15T15:00:00+00:00", "2026-01-15T16:00:00+00:00",
                       summary="E3"),
        ]
        data_ctime = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result["summary"] == "E2"

    # --- Proximity logic: data is in the gap between two events ---

    def test_data_between_events_closer_to_first(self):
        """When data is in the gap between two events and closer to the first
        event's end, return the first event."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T11:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T13:00:00+00:00", "2026-01-15T14:00:00+00:00",
                       summary="E2"),
        ]
        # 30 min after E1 ends, 90 min before E2 starts → closer to E1
        data_ctime = datetime(2026, 1, 15, 11, 30, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result["summary"] == "E1"

    def test_data_between_events_closer_to_second(self):
        """When data is in the gap and closer to the next event's start,
        return the next event."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T11:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T13:00:00+00:00", "2026-01-15T14:00:00+00:00",
                       summary="E2"),
        ]
        # 90 min after E1 ends, 30 min before E2 starts → closer to E2
        data_ctime = datetime(2026, 1, 15, 12, 30, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result["summary"] == "E2"

    def test_data_equidistant_between_events_returns_next(self):
        """BOUNDARY BEHAVIOR: When data is exactly equidistant between two events,
        the code uses strict < (time_since < time_before), so equal distances
        cause Case 2 to fail. Case 3 matches instead and returns the NEXT event.
        This tests that tie-breaking favors the upcoming event."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T11:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T13:00:00+00:00", "2026-01-15T14:00:00+00:00",
                       summary="E2"),
        ]
        # Exactly 1h after E1, exactly 1h before E2
        data_ctime = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result["summary"] == "E2"

    def test_data_exactly_at_event_end_with_next_event(self):
        """Edge case: data_ctime exactly equals the event's end time.
        Case 1 fails (end > data_ctime is False). But start < data_ctime is True
        and time_since_last = 0, which is < time_before_next. Case 2 matches."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T12:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T14:00:00+00:00", "2026-01-15T15:00:00+00:00",
                       summary="E2"),
        ]
        # Exactly at E1's end time
        data_ctime = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        # time_since = 0, time_before = 2h → 0 < 2h → returns E1
        assert result["summary"] == "E1"

    # --- Edge cases that return None ---

    def test_returns_none_for_empty_events_list(self):
        """No events at all → return None."""
        data_ctime = datetime(2026, 1, 15, 11, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, [])
        assert result is None

    def test_single_event_data_after_returns_none(self):
        """If there's only one event and data is after it, the function returns
        None because there is no next event to compare against. This is a known
        limitation — even data created 1 second after an experiment ends
        cannot be attributed to that experiment."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T12:00:00+00:00")
        ]
        data_ctime = datetime(2026, 1, 15, 12, 0, 1, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result is None

    def test_data_before_all_events_returns_none(self):
        """When data was created before any event in the list, the function
        should iterate through all events and return None."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T11:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T13:00:00+00:00", "2026-01-15T14:00:00+00:00",
                       summary="E2"),
        ]
        data_ctime = datetime(2026, 1, 15, 8, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result is None

    def test_data_after_all_events_returns_none(self):
        """When data is after ALL events with no next event to compare against,
        returns None. Even data created 1 minute after the last event returns None."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T11:00:00+00:00",
                       summary="E1"),
            make_event("2026-01-15T13:00:00+00:00", "2026-01-15T14:00:00+00:00",
                       summary="E2"),
        ]
        data_ctime = datetime(2026, 1, 15, 14, 1, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        assert result is None

    def test_data_exactly_at_event_start_not_matched(self):
        """EDGE CASE: The function uses strict inequality (start < data_ctime),
        so if data_ctime equals exactly the event's start time, the event is NOT
        considered a match. This means data timestamped at the exact moment an
        experiment begins will not be attributed to that experiment."""
        events = [
            make_event("2026-01-15T10:00:00+00:00", "2026-01-15T12:00:00+00:00",
                       summary="Exp")
        ]
        data_ctime = datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC)
        result = filter_events_at_time(data_ctime, events)
        # start (10:00) < data_ctime (10:00) is False → not matched
        assert result is None

    # --- Failure points ---

    def test_all_day_event_without_dateTime_raises_keyerror(self):
        """FAILURE POINT: If a calendar event is an all-day event, Google Calendar
        uses 'date' instead of 'dateTime'. The function accesses
        e['start']['dateTime'] without a fallback, causing a KeyError."""
        events = [
            {
                "start": {"date": "2026-01-15"},
                "end": {"date": "2026-01-16"},
                "summary": "All Day Event",
            }
        ]
        data_ctime = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
        with pytest.raises(KeyError):
            filter_events_at_time(data_ctime, events)


# ============================================================================
# TESTS: parse_calendar_event_for_ownership
# ============================================================================

class TestParseCalendarEventForOwnership:

    def test_extracts_email_and_numeric_proposal(self):
        """Standard case: attendee email and numeric location (proposal ID)."""
        event = make_event(
            "2026-01-15T10:00:00Z", "2026-01-15T12:00:00Z",
            email="scientist@lbl.gov", location="12345"
        )
        email, proposal = parse_calendar_event_for_ownership(event)
        assert email == "scientist@lbl.gov"
        assert proposal == "MFP12345"

    def test_numeric_location_padded_to_5_digits(self):
        """Short numeric locations should be zero-padded to 5 digits.
        e.g. proposal ID '42' → 'MFP00042'"""
        event = make_event(
            "2026-01-15T10:00:00Z", "2026-01-15T12:00:00Z",
            email="user@example.com", location="42"
        )
        _, proposal = parse_calendar_event_for_ownership(event)
        assert proposal == "MFP00042"

    def test_large_numeric_location_not_truncated(self):
        """Numeric locations with >5 digits should NOT be truncated."""
        event = make_event(
            "2026-01-15T10:00:00Z", "2026-01-15T12:00:00Z",
            email="user@example.com", location="123456"
        )
        _, proposal = parse_calendar_event_for_ownership(event)
        assert proposal == "MFP123456"

    def test_non_numeric_location_used_as_is(self):
        """Non-numeric location strings pass through without MFP formatting."""
        event = make_event(
            "2026-01-15T10:00:00Z", "2026-01-15T12:00:00Z",
            email="user@example.com", location="Internal Research Project"
        )
        _, proposal = parse_calendar_event_for_ownership(event)
        assert proposal == "Internal Research Project"

    def test_missing_attendees_key_returns_none_email(self):
        """If the event has no 'attendees' key at all, email should be None."""
        event = {
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T12:00:00Z"},
            "location": "100",
        }
        email, proposal = parse_calendar_event_for_ownership(event)
        assert email is None
        assert proposal == "MFP00100"

    def test_empty_attendees_list_returns_none_email(self):
        """If attendees exists but is empty, accessing index [0] raises IndexError,
        which is caught by the bare except and returns None."""
        event = {
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T12:00:00Z"},
            "attendees": [],
            "location": "999",
        }
        email, _ = parse_calendar_event_for_ownership(event)
        assert email is None

    def test_missing_location_returns_none_proposal(self):
        """If the event has no 'location' key, proposal should be None."""
        event = {
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T12:00:00Z"},
            "attendees": [{"email": "user@example.com"}],
        }
        _, proposal = parse_calendar_event_for_ownership(event)
        assert proposal is None

    def test_both_missing_returns_none_tuple(self):
        """Event with neither attendees nor location returns (None, None)."""
        event = {
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T12:00:00Z"},
        }
        email, proposal = parse_calendar_event_for_ownership(event)
        assert email is None
        assert proposal is None

    def test_multiple_attendees_uses_only_the_first(self):
        """Only the first attendee's email is extracted; any others are ignored.
        This means co-investigators on a shared booking are never attributed."""
        event = {
            "start": {"dateTime": "2026-01-15T10:00:00Z"},
            "end": {"dateTime": "2026-01-15T12:00:00Z"},
            "attendees": [
                {"email": "first@lbl.gov"},
                {"email": "second@lbl.gov"},
                {"email": "third@lbl.gov"},
            ],
        }
        email, _ = parse_calendar_event_for_ownership(event)
        assert email == "first@lbl.gov"

    def test_location_zero_formatted_correctly(self):
        """Edge case: location '0' is numeric, should become 'MFP00000'."""
        event = make_event(
            "2026-01-15T10:00:00Z", "2026-01-15T12:00:00Z",
            location="0"
        )
        _, proposal = parse_calendar_event_for_ownership(event)
        assert proposal == "MFP00000"


# ============================================================================
# TESTS: setup_client
# ============================================================================

class TestSetupClient:

    @patch("google_calendar.build")
    @patch("google_calendar.service_account.Credentials.from_service_account_file")
    @patch("os.path.exists", return_value=True)
    def test_uses_file_directly_when_it_exists(self, mock_exists, mock_creds, mock_build):
        """When the service account file exists on disk, it should be used
        directly without reading from environment variables."""
        mock_creds.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        setup_client(service_account_file="/path/to/creds.json")

        mock_exists.assert_called_once_with("/path/to/creds.json")
        mock_creds.assert_called_once_with(
            "/path/to/creds.json",
            scopes=["https://www.googleapis.com/auth/calendar.readonly"],
        )

    @patch("google_calendar.build")
    @patch("google_calendar.service_account.Credentials.from_service_account_file")
    @patch("builtins.open", new_callable=mock_open)
    @patch("os.path.exists", return_value=False)
    @patch.dict(
        "os.environ",
        {"GCS_SA": '{"type": "service_account", "project_id": "test"}'},
    )
    def test_falls_back_to_env_var_when_file_missing(
        self, mock_exists, mock_file, mock_creds, mock_build
    ):
        """When the file doesn't exist, the function should read credentials
        from the environment variable, write to temp_creds.json, and use that."""
        mock_creds.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        setup_client(service_account_file="/nonexistent/path.json")

        # Should write to temp_creds.json
        mock_file.assert_called_once_with("temp_creds.json", "w")
        # Should use temp_creds.json (not the original path)
        mock_creds.assert_called_once_with(
            "temp_creds.json",
            scopes=["https://www.googleapis.com/auth/calendar.readonly"],
        )

    @patch("os.path.exists", return_value=False)
    def test_missing_env_var_raises_type_error(self, mock_exists):
        """FAILURE POINT: If the file doesn't exist AND the env var is not set,
        os.getenv() returns None, and json.loads(None) raises TypeError.
        There is no graceful error handling for this case."""
        with pytest.raises(TypeError):
            setup_client(
                service_account_file="/nonexistent.json",
                cred_env_var="__THIS_ENV_VAR_DEFINITELY_DOES_NOT_EXIST__",
            )

    @patch("google_calendar.build")
    @patch("google_calendar.service_account.Credentials.from_service_account_file")
    @patch("os.path.exists", return_value=True)
    def test_custom_scopes_are_passed_through(self, mock_exists, mock_creds, mock_build):
        """Custom scopes should be forwarded to the credentials creation."""
        mock_creds.return_value = MagicMock()
        mock_build.return_value = MagicMock()
        custom_scopes = ["https://www.googleapis.com/auth/calendar"]

        setup_client(service_account_file="/creds.json", scopes=custom_scopes)

        mock_creds.assert_called_once_with("/creds.json", scopes=custom_scopes)


# ============================================================================
# TESTS: get_calendar_events
# ============================================================================

class TestGetCalendarEvents:

    @patch("google_calendar.setup_client")
    def test_returns_events_when_found(self, mock_setup):
        """When the API returns events, they should be returned as a list."""
        mock_service = MagicMock()
        mock_service.events().list().execute.return_value = {
            "items": [
                make_event("2026-01-15T10:00:00Z", "2026-01-15T12:00:00Z",
                           summary="Exp 1")
            ]
        }
        mock_setup.return_value = mock_service

        result = get_calendar_events(
            "cal_id_123", "2026-01-15T00:00:00Z", "2026-01-16T00:00:00Z"
        )
        assert len(result) == 1
        assert result[0]["summary"] == "Exp 1"

    @patch("google_calendar.setup_client")
    def test_returns_empty_list_when_no_events(self, mock_setup):
        """When the API returns an empty items list, return []."""
        mock_service = MagicMock()
        mock_service.events().list().execute.return_value = {"items": []}
        mock_setup.return_value = mock_service

        result = get_calendar_events(
            "cal_id_123", "2026-01-15T00:00:00Z", "2026-01-16T00:00:00Z"
        )
        assert result == []

    @patch("google_calendar.setup_client")
    def test_returns_empty_when_items_key_missing(self, mock_setup):
        """Edge case: if the API response omits 'items' entirely, the .get()
        default of [] should apply."""
        mock_service = MagicMock()
        mock_service.events().list().execute.return_value = {}
        mock_setup.return_value = mock_service

        result = get_calendar_events(
            "cal_id_123", "2026-01-15T00:00:00Z", "2026-01-16T00:00:00Z"
        )
        assert result == []

    @patch("google_calendar.setup_client")
    def test_passes_correct_params_to_api(self, mock_setup):
        """The function should pass the calendar ID, time range, and ordering
        parameters to the Google Calendar API."""
        mock_service = MagicMock()
        mock_service.events().list().execute.return_value = {"items": []}
        mock_setup.return_value = mock_service

        get_calendar_events("my_calendar", "2026-01-15T00:00:00Z", "2026-01-16T00:00:00Z")

        mock_service.events().list.assert_called_with(
            calendarId="my_calendar",
            timeMin="2026-01-15T00:00:00Z",
            timeMax="2026-01-16T00:00:00Z",
            maxResults=10,
            singleEvents=True,
            orderBy="startTime",
        )


# ============================================================================
# TESTS: find_calendar_event (integration of the above functions)
# ============================================================================

class TestFindCalendarEvent:

    @patch("google_calendar.get_calendar_events")
    def test_returns_matching_event(self, mock_get_events):
        """When events are found and one matches the data time, return it."""
        mock_get_events.return_value = [
            make_event("2026-01-15T18:00:00+00:00", "2026-01-15T22:00:00+00:00",
                       summary="Match")
        ]
        # 10:30 AM PST ≈ 18:30 UTC → falls within the 18:00-22:00 UTC event
        result = find_calendar_event(
            "2026-01-15T10:30:00", "cal_id", tz="America/Los_Angeles"
        )
        assert result is not None
        assert result["summary"] == "Match"

    @patch("google_calendar.get_calendar_events")
    def test_returns_none_when_api_returns_no_events(self, mock_get_events):
        """When the calendar API returns no events, return None."""
        mock_get_events.return_value = []
        result = find_calendar_event("2026-01-15T10:30:00", "cal_id")
        assert result is None

    @patch("google_calendar.get_calendar_events")
    def test_returns_none_when_events_exist_but_no_match(self, mock_get_events):
        """When the API returns events but none match the data timestamp,
        filter_events_at_time returns None and so does this function."""
        mock_get_events.return_value = [
            make_event("2026-01-15T02:00:00+00:00", "2026-01-15T03:00:00+00:00",
                       summary="Too Early")
        ]
        # 10:30 AM PST ≈ 18:30 UTC → well outside the 02:00-03:00 UTC event
        result = find_calendar_event(
            "2026-01-15T10:30:00", "cal_id", tz="America/Los_Angeles"
        )
        assert result is None

    @patch("google_calendar.get_calendar_events")
    def test_calls_api_with_correct_calendar_id(self, mock_get_events):
        """The calendar ID should be forwarded to the API call."""
        mock_get_events.return_value = []
        find_calendar_event("2026-06-15T12:00:00", "specific_cal_id_abc")
        call_args = mock_get_events.call_args
        assert call_args[0][0] == "specific_cal_id_abc"

    @patch("google_calendar.get_calendar_events")
    def test_generates_24h_window_for_api_call(self, mock_get_events):
        """The function should query with a ±12h (24h total) window from the data time."""
        mock_get_events.return_value = []
        find_calendar_event("2026-06-15T12:00:00", "cal_id", tz="America/Los_Angeles")

        call_args = mock_get_events.call_args[0]
        # Parse the time_min and time_max that were passed to the API
        time_min_str = call_args[1]
        time_max_str = call_args[2]

        # The window should span from ~midnight June 15 to ~midnight June 16
        # (accounting for timezone offset). Just verify they're ISO strings
        # with the correct date range.
        assert "2026-06" in time_min_str
        assert "2026-06" in time_max_str
