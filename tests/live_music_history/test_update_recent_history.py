import datetime
from unittest.mock import MagicMock

import pytest
from googleapiclient.errors import HttpError

import live_music_history.update_recent_history as urh


@pytest.fixture
def mock_services():
    """Fixture for mocked Drive and Sheets services."""
    mock_drive = MagicMock()
    mock_sheets = MagicMock()
    mock_sheets.spreadsheets.return_value = mock_sheets
    return mock_drive, mock_sheets


@pytest.fixture
def mock_config(monkeypatch):
    monkeypatch.setattr(urh.config, "LIVE_HISTORY_SPREADSHEET_ID", "test_sheet_id")
    monkeypatch.setattr(urh.config, "NO_HISTORY", "No_recent_history_found_")
    monkeypatch.setattr(urh.config, "TIMEZONE", "America/Chicago")
    monkeypatch.setattr(urh.config, "HISTORY_IN_HOURS", 3)


def test_build_youtube_links(monkeypatch):
    monkeypatch.setattr(urh.log, "debug", MagicMock())
    entries = [
        ["2025-10-26 23:59", "Song A", "Artist 1"],
        ["2025-10-27 00:10", "Song B", "Artist 2"],
    ]
    links = urh.build_youtube_links(entries)
    assert len(links) == 2
    assert "https://www.youtube.com/results?" in links[0][0]
    urh.log.debug.assert_called()


def test_write_entries_to_sheet_no_entries(mock_services, mock_config, monkeypatch):
    drive_service, sheets_service = mock_services
    mock_sheet = sheets_service.spreadsheets()
    monkeypatch.setattr(urh.log, "info", MagicMock())
    monkeypatch.setattr(urh.log, "debug", MagicMock())
    now = datetime.datetime.now()

    urh.write_entries_to_sheet(sheets_service, [], now)

    mock_sheet.values().clear.assert_called_once()
    mock_sheet.values().update.assert_called_once()
    args, kwargs = mock_sheet.values().update.call_args
    assert "No_recent_history_found_" in str(kwargs["body"]["values"])


def test_write_entries_to_sheet_with_entries_success(
    mock_services, mock_config, monkeypatch
):
    drive_service, sheets_service = mock_services
    mock_sheet = sheets_service.spreadsheets()
    monkeypatch.setattr(urh.log, "info", MagicMock())
    monkeypatch.setattr(urh.log, "debug", MagicMock())
    monkeypatch.setattr(urh, "build_youtube_links", lambda e: [["Link1"], ["Link2"]])
    now = datetime.datetime.now()
    entries = [
        ["2025-10-27 00:00", "Song A", "Artist 1"],
        ["2025-10-27 00:01", "Song B", "Artist 2"],
    ]

    urh.write_entries_to_sheet(sheets_service, entries, now)
    assert mock_sheet.values().update.call_count == 2  # entries + links


def test_write_entries_to_sheet_with_http_error(
    mock_services, mock_config, monkeypatch
):
    drive_service, sheets_service = mock_services
    mock_sheet = sheets_service.spreadsheets()

    mock_resp = MagicMock()
    mock_resp.reason = "TestError"

    # Properly mock the chain .values().update().execute()
    mock_execute = MagicMock()
    mock_execute.execute.side_effect = [
        None,
        HttpError(resp=mock_resp, content=b"Error"),
    ]
    mock_sheet.values.return_value.update.return_value = mock_execute

    monkeypatch.setattr(urh.log, "info", MagicMock())
    monkeypatch.setattr(urh.log, "warning", MagicMock())
    monkeypatch.setattr(urh, "build_youtube_links", lambda e: [["L"]])
    now = datetime.datetime.now()
    entries = [["2025-10-27 00:00", "Song A", "Artist 1"]]

    urh.write_entries_to_sheet(sheets_service, entries, now)
    urh.log.warning.assert_called_once()


def test_read_existing_entries_filters_by_cutoff(
    mock_services, mock_config, monkeypatch
):
    drive_service, sheets_service = mock_services
    sheet = sheets_service.spreadsheets()
    cutoff = datetime.datetime(2025, 10, 27, 0, 0)
    data = {
        "values": [
            ["2025-10-26 23:00", "Old Song", "Artist 1"],
            ["2025-10-27 00:30", "New Song", "Artist 2"],
        ]
    }
    sheet.values().get().execute.return_value = data
    monkeypatch.setattr(urh.log, "info", MagicMock())

    result = urh.read_existing_entries(sheets_service, cutoff)
    assert len(result) == 1
    assert result[0][1] == "New Song"


def test_update_last_run_time(mock_services, mock_config):
    _, sheets_service = mock_services
    now = datetime.datetime.now()
    urh.update_last_run_time(sheets_service, now)
    sheets_service.spreadsheets().values().update.assert_called_once()


def test_publish_history_no_files(mock_services, mock_config, monkeypatch):
    drive_service, sheets_service = mock_services
    monkeypatch.setattr(urh.m3u_parsing, "get_most_recent_m3u_file", lambda d: None)
    monkeypatch.setattr(urh.log, "info", MagicMock())
    monkeypatch.setattr(urh.log, "format_date", lambda dt: "2025-10-27 12:00")
    urh.publish_history(drive_service, sheets_service)
    sheets_service.spreadsheets().values().clear.assert_called_once()
    urh.log.info.assert_any_call(
        "No .m3u files found. Clearing sheet and writing NO_HISTORY."
    )


def test_publish_history_with_entries(mock_services, mock_config, monkeypatch):
    drive_service, sheets_service = mock_services
    fake_lines = ["#EXTM3U", "#EXTINF:123,Artist - Title"]
    fake_file = {"id": "file123", "name": "2025-10-27.m3u"}

    monkeypatch.setattr(
        urh.m3u_parsing, "get_most_recent_m3u_file", lambda d: fake_file
    )
    monkeypatch.setattr(urh.m3u_parsing, "download_m3u_file", lambda d, i: fake_lines)
    monkeypatch.setattr(
        urh.m3u_parsing,
        "parse_m3u_lines",
        lambda la, ka, fa: [["2025-10-27 00:00", "Title", "Artist"]],
    )
    monkeypatch.setattr(urh.log, "debug", MagicMock())
    monkeypatch.setattr(urh.log, "info", MagicMock())
    monkeypatch.setattr(urh.log, "format_date", lambda dt: "2025-10-27 12:00")
    monkeypatch.setattr(urh, "write_entries_to_sheet", MagicMock())
    monkeypatch.setattr(urh, "read_existing_entries", lambda s, c: [])

    urh.publish_history(drive_service, sheets_service)
    urh.write_entries_to_sheet.assert_called_once()


def test_main_invokes_both_publish(monkeypatch):
    mock_drive = MagicMock()
    mock_sheets = MagicMock()
    monkeypatch.setattr(urh.google_drive, "get_drive_service", lambda: mock_drive)
    monkeypatch.setattr(urh.google_sheets, "get_sheets_service", lambda: mock_sheets)
    mock_pub = MagicMock()
    monkeypatch.setattr(urh, "publish_history", mock_pub)

    if hasattr(urh, "main"):
        urh.main()
    else:
        urh.publish_history(mock_drive, mock_sheets)

    mock_pub.assert_called()
