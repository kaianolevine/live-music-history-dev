import datetime
from urllib.parse import urlencode

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as google_drive
import kaiano_common_utils.google_sheets as google_sheets
import kaiano_common_utils.logger as log
import kaiano_common_utils.m3u_parsing as m3u_parsing
import pytz
from googleapiclient.errors import HttpError


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
        # Write the raw URL; Sheets will auto-link it.
        links.append([url])
    return links


def write_entries_to_sheet(sheets_service, entries, now):
    sheet = sheets_service.spreadsheets()
    log.info("Clearing old entries in sheet range A5:D...")
    sheet.values().clear(
        spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID, range="A5:D"
    ).execute()

    if not entries:
        log.info("No entries to write. Writing NO_HISTORY message.")
        log.debug(
            "Sheet write range: %s, entries: %s",
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
        )
        sheet.values().update(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
            range="A5:B5",
            valueInputOption="RAW",
            body={"values": [[log.format_date(now), config.NO_HISTORY]]},
        ).execute()
        return

    log.info("Writing %d entries to sheet...", len(entries))
    log.debug("Sheet write range: %s, entries: %s", f"A5:C{5+len(entries)-1}", entries)
    sheet.values().update(
        spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
        range=f"A5:C{5+len(entries)-1}",
        valueInputOption="RAW",
        body={"values": entries},
    ).execute()

    try:
        log.info("Writing %d links to sheet...", len(entries))
        links = build_youtube_links(entries)
        log.debug("Link write range: %s, links: %s", f"D5:D{5+len(links)-1}", links)
        sheet.values().update(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
            range=f"D5:D{5+len(links)-1}",
            valueInputOption="RAW",
            body={"values": links},
        ).execute()
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
    sheet = sheets_service.spreadsheets()
    log.info("Reading existing entries from sheet...")
    result = (
        sheet.values()
        .get(spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID, range="A5:C")
        .execute()
    )
    values = result.get("values", [])

    existing_data = []
    for row in values:
        if len(row) >= 2 and row[1] != config.NO_HISTORY:
            dt = _parse_entry_dt(row[0])
            if dt is None:
                continue
            existing_data.append(row[:3])

    log.info("Found %d existing entries.", len(existing_data))
    return existing_data


def update_last_run_time(sheets_service, now):
    sheet = sheets_service.spreadsheets()
    log.info("Updating last run time in sheet...")
    sheet.values().update(
        spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
        range="A3",
        valueInputOption="RAW",
        body={"values": [[log.format_date(now)]]},
    ).execute()


def publish_history(drive_service, sheets_service):
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.datetime.now(tz)

    log.info("--- Starting publish_history ---")

    update_last_run_time(sheets_service, now)

    m3u_file = m3u_parsing.get_most_recent_m3u_file(drive_service)
    if not m3u_file:
        log.info("No .m3u files found. Clearing sheet and writing NO_HISTORY.")
        sheet = sheets_service.spreadsheets()
        sheet.values().clear(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID, range="A5:D"
        ).execute()
        sheet.values().update(
            spreadsheetId=config.LIVE_HISTORY_SPREADSHEET_ID,
            range="A5:B5",
            valueInputOption="RAW",
            body={"values": [[log.format_date(now), config.NO_HISTORY]]},
        ).execute()
        return

    lines = m3u_parsing.download_m3u_file(drive_service, m3u_file["id"])
    file_date_str = m3u_file["name"].replace(".m3u", "").strip()

    existing_data = read_existing_entries(sheets_service)
    existing_keys = {"||".join(c.strip().lower() for c in r) for r in existing_data}

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
    # import tools.private_history.update_private_history as private_history

    drive_service = google_drive.get_drive_service()
    sheets_service = google_sheets.get_sheets_service()

    publish_history(drive_service, sheets_service)
    # private_history.publish_private_history(drive_service, sheets_service)
