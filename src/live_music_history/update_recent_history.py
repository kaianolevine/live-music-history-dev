import datetime
from urllib.parse import urlencode

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as google_drive
import kaiano_common_utils.google_sheets as google_sheets
import kaiano_common_utils.google_sheets.a1_range as a1_range
import kaiano_common_utils.google_sheets.normalize_cell as normalize_cell
import kaiano_common_utils.google_sheets.sheets_clear_values as sheets_clear_values
import kaiano_common_utils.google_sheets.sheets_get_values as sheets_get_values
import kaiano_common_utils.google_sheets.sheets_update_values as sheets_update_values
import kaiano_common_utils.logger as log
import kaiano_common_utils.m3u_parsing as m3u_parsing
import pytz
from googleapiclient.errors import HttpError


def build_dedup_key(row: list[str]) -> str:
    """Build a stable, case-insensitive dedupe key for [datetime, title, artist]."""
    return "||".join(normalize_cell(c).casefold() for c in row[:3])


def build_dedup_keys(rows: list[list[str]]) -> set[str]:
    return {build_dedup_key(r) for r in rows}


def build_youtube_links(entries):
    """Build YouTube search URLs.

    NOTE: We intentionally write raw URLs (not =HYPERLINK formulas) because
    Google Sheets' click/preview redirect layer can be blocked in some
    environments. Raw URLs tend to open directly across more browsers and
    network policies.
    """
    links = []
    for _, title, artist in entries:
        query = urlencode({"search_query": f"{title} {artist}"})
        url = f"https://www.youtube.com/results?{query}"
        log.debug("YouTube link: %s", url)
        links.append([url])
    return links


def write_entries_to_sheet(sheets_service, entries, now):
    log.info("Clearing old entries in sheet range A5:D...")
    sheets_clear_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        a1_range("A", 5, "D"),
    )

    if not entries:
        log.info("No entries to write. Writing NO_HISTORY message.")
        log.debug(
            "Sheet write range: %s, entries: %s",
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
        )
        sheets_update_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
            value_input_option="RAW",
        )
        return

    log.info("Writing %d entries to sheet...", len(entries))
    log.debug(
        "Sheet write range: %s, entries: %s",
        a1_range("A", 5, "C", 5 + len(entries) - 1),
        entries,
    )
    sheets_update_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        a1_range("A", 5, "C", 5 + len(entries) - 1),
        entries,
        value_input_option="RAW",
    )

    try:
        log.info("Writing %d links to sheet...", len(entries))
        links = build_youtube_links(entries)
        log.debug(
            "Link write range: %s, links: %s",
            a1_range("D", 5, "D", 5 + len(links) - 1),
            links,
        )
        sheets_update_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            a1_range("D", 5, "D", 5 + len(links) - 1),
            links,
            value_input_option="RAW",
        )
        log.info("Finished writing entries and links to sheet.")
    except HttpError as e:
        log.warning("Skipping YouTube links due to sheet restriction: %s", e)
        log.debug("Skipping link write due to HttpError.")


# --- SHEET READING AND PUBLISHING HISTORY ---


def _parse_entry_dt(value: str) -> datetime.datetime | None:
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M")
    except Exception:
        return None


def read_existing_entries(sheets_service):
    log.info("Reading existing entries from sheet...")
    values = sheets_get_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        a1_range("A", 5, "C"),
    )

    existing_data: list[list[str]] = []
    for row in values:
        if len(row) >= 2 and row[1] != config.NO_HISTORY:
            dt = _parse_entry_dt(row[0])
            if dt is None:
                continue
            existing_data.append(row[:3])

    log.info("Found %d existing entries.", len(existing_data))
    return existing_data


def update_last_run_time(sheets_service, now):
    log.info("Updating last run time in sheet...")
    sheets_update_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        "A3",
        [[log.format_date(now)]],
        value_input_option="RAW",
    )


def publish_history(drive_service, sheets_service):
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.datetime.now(tz)

    log.info("--- Starting publish_history ---")

    update_last_run_time(sheets_service, now)

    m3u_file = m3u_parsing.get_most_recent_m3u_file(drive_service)
    if not m3u_file:
        log.info("No .m3u files found. Clearing sheet and writing NO_HISTORY.")
        sheets_clear_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            a1_range("A", 5, "D"),
        )
        sheets_update_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
            value_input_option="RAW",
        )
        return

    lines = m3u_parsing.download_m3u_file(drive_service, m3u_file["id"])
    file_date_str = m3u_file["name"].replace(".m3u", "").strip()

    existing_data = read_existing_entries(sheets_service)
    existing_keys = build_dedup_keys(existing_data)

    new_entries = m3u_parsing.parse_m3u_lines(lines, existing_keys, file_date_str)

    max_songs = int(getattr(config, "HISTORY_MAX_SONGS", 200) or 200)
    if max_songs < 1:
        max_songs = 200

    combined = [row[:3] for row in (existing_data + new_entries)]

    # Sort newest -> oldest and keep only the most recent N
    combined.sort(
        key=lambda r: _parse_entry_dt(r[0]) or datetime.datetime.min, reverse=True
    )
    combined = combined[:max_songs]

    log.info(
        "Total combined entries to write (capped to %d): %d", max_songs, len(combined)
    )

    write_entries_to_sheet(sheets_service, combined, now)

    log.info("Script finished. Rows written: %d", len(combined))


if __name__ == "__main__":

    drive_service = google_drive.get_drive_service()
    sheets_service = google_sheets.get_sheets_service()

    publish_history(drive_service, sheets_service)
