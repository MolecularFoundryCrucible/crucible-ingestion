'''
pip install google-auth \
            google-auth-oauthlib \
            google-auth-httplib2 \
            google-api-python-client

'''

from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pytz
import json
import os


def setup_client(service_account_file = "", cred_env_var = "GCS_SA", scopes = ["https://www.googleapis.com/auth/calendar.readonly"]):
    if os.path.exists(service_account_file):
        print(f"{service_account_file=} was found using os.path.exists")
    else:
        service_account_file = "temp_creds.json"
        J = json.loads(os.getenv(cred_env_var))
        with open("temp_creds.json", "w") as f:
            json.dump(J, f)
            
    credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=scopes)
    service = build("calendar", "v3", credentials=credentials)
    return(service)


def generate_time_range(dt_pst):
    # Compute the 12-hour before and after times
    before_12h = dt_pst - timedelta(hours=12)
    after_12h = dt_pst + timedelta(hours=12)
    return(before_12h, after_12h)


def get_calendar_events(CALENDAR_ID, 
                        time_min, 
                        time_max, 
                        service_account_file = "", 
                        cred_env_var = "GCS_SA", 
                        scopes = ["https://www.googleapis.com/auth/calendar.readonly"]):
    
    """Fetches upcoming events from Google Calendar."""

    service = setup_client(service_account_file, cred_env_var, scopes)
    
    events_result = service.events().list(
        calendarId=CALENDAR_ID,
        timeMin=time_min,  # Set a proper datetime range
        timeMax = time_max,
        maxResults=10,
        singleEvents=True,
        orderBy="startTime",
    ).execute()

    events = events_result.get("items", [])
    if not events:
        print("No upcoming events found.")
        return []
    print(f"Events found between {time_min} and {time_max}:")
    for event in events:
        start = event["start"].get("dateTime", event["start"].get("date"))
        end = event["end"].get("dateTime", event["end"].get("date"))
        print(f"{start} - {end}: {event['summary']}")
        
    return(events)


def filter_events_at_time(data_ctime, events):
    i = 0
    while i < len(events):
        e = events[i]
        start = datetime.fromisoformat(e['start']['dateTime'].replace("Z", "+00:00"))
        end = datetime.fromisoformat(e['end']['dateTime'].replace("Z", "+00:00"))
        
        if start < data_ctime and end > data_ctime:
            return(e)

        if len(events) <= i+1:
            return(None)

        e2 = events[i+1]
        next_start = datetime.fromisoformat(e2['start']['dateTime'].replace("Z", "+00:00"))
        next_end = datetime.fromisoformat(e2['end']['dateTime'].replace("Z", "+00:00"))
        
        time_since_last_event = data_ctime - end
        time_before_next_event = next_start - data_ctime

        # if the calendar event starts before the data was created and ends more closely to when the data was created than the next event; return the earlier event
        if start < data_ctime and time_since_last_event < time_before_next_event:
            return(e)

        # if the data was created between this event and the next one but more close to the next one; return the next event
        elif end < data_ctime and next_start > data_ctime:
            return(e2)

        # otherwise go to the next event
        else:
            i += 1
            
    return(None)


def find_calendar_event(event_time, cal_id, service_account_file = "", tz = "America/Los_Angeles"): 
    # date information
    pst = pytz.timezone(tz)
    dt_pst = datetime.fromisoformat(event_time).replace(tzinfo=pst)
    mintime, maxtime = generate_time_range(dt_pst)
    
    # call gcal api
    events = get_calendar_events(cal_id, mintime.isoformat(), maxtime.isoformat(), service_account_file)
    if len(events) > 0:
        data_event = filter_events_at_time(dt_pst, events)
        return data_event
    else:
        return None

def parse_calendar_event_for_ownership(event):
    # if the event has an email - use it; otherwise none for now
    try:
        email = event['attendees'][0]['email']
    except:
        email = None

    # if the event has a proposal - use it; otherwise unknown
    try:
        if event['location'].isnumeric():
            proposal = f"MFP{int(event['location']):05d}"
        else:
            proposal = event['location']    
    except:
        proposal = None

    return(email, proposal)
































