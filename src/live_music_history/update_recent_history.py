import datetime
from urllib.parse import urlencode

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as google_drive
import kaiano_common_utils.google_sheets as google_sheets
import kaiano_common_utils.logger as log
import kaiano_common_utils.m3u_parsing as m3u_parsing
import pytz
from googleapiclient.errors import HttpError


def get_all_m3u_files(drive_service):
    """Return all .m3u files in the configured VirtualDJ history folder.

    The returned list is sorted newest-first by filename (YYYY-MM-DD.m3u).
    """
    # NOTE: This function intentionally reuses the exact Drive query + folder selection
    # logic from get_most_recent_m3u_file.
    files = []
    folder_id = None
    results = (
        drive_service.files()
        .list(
            q="mimeType='application/vnd.google-apps.folder' and name='VirtualDJ History'",
            spaces="drive",
            fields="files(id, name)",
        )
        .execute()
    )
    folders = results.get("files", [])
    if folders:
        folder_id = folders[0]["id"]

    if folder_id:
        query = f"'{folder_id}' in parents and name contains '.m3u' and trashed = false"
        results = (
            drive_service.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                orderBy="name desc",
            )
            .execute()
        )
        files = results.get("files", [])

    files = sorted(files, key=lambda f: f.get("name", ""), reverse=True)
    return files


def build_dedup_key(row: list[str]) -> str:
    """Build a stable, case-insensitive dedupe key for [datetime, title, artist]."""
    return "||".join(google_sheets.normalize_cell(c).casefold() for c in row[:3])


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
    google_sheets.sheets_clear_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        google_sheets.a1_range("A", 5, "D"),
    )

    if not entries:
        log.info("No entries to write. Writing NO_HISTORY message.")
        log.debug(
            "Sheet write range: %s, entries: %s",
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
        )
        google_sheets.sheets_update_values(
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
        google_sheets.a1_range("A", 5, "C", 5 + len(entries) - 1),
        entries,
    )
    google_sheets.sheets_update_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        google_sheets.a1_range("A", 5, "C", 5 + len(entries) - 1),
        entries,
        value_input_option="RAW",
    )

    try:
        log.info("Writing %d links to sheet...", len(entries))
        links = build_youtube_links(entries)
        log.debug(
            "Link write range: %s, links: %s",
            google_sheets.a1_range("D", 5, "D", 5 + len(links) - 1),
            links,
        )
        google_sheets.sheets_update_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            google_sheets.a1_range("D", 5, "D", 5 + len(links) - 1),
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
    values = google_sheets.sheets_get_values(
        sheets_service,
        config.LIVE_HISTORY_SPREADSHEET_ID,
        google_sheets.a1_range("A", 5, "C"),
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
    google_sheets.sheets_update_values(
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

    m3u_files = get_all_m3u_files(drive_service)
    if not m3u_files:
        log.info("No .m3u files found. Clearing sheet and writing NO_HISTORY.")
        google_sheets.sheets_clear_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            google_sheets.a1_range("A", 5, "D"),
        )
        google_sheets.sheets_update_values(
            sheets_service,
            config.LIVE_HISTORY_SPREADSHEET_ID,
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
            value_input_option="RAW",
        )
        return

    existing_data = read_existing_entries(sheets_service)
    # Start with whatever is already in the sheet, and then add new entries from
    # *all* m3u files (newest-first), stopping once we have enough to write.
    max_songs = int(getattr(config, "HISTORY_MAX_SONGS", 200) or 200)
    if max_songs < 1:
        max_songs = 200

    combined: list[list[str]] = [row[:3] for row in existing_data]
    seen_keys: set[str] = build_dedup_keys(combined)

    new_entries: list[list[str]] = []

    # Process files newest-first (already sorted in get_all_m3u_files)
    for m3u_file in m3u_files:
        # If we already have plenty of rows, we can stop early.
        # We still do a final sort+cap later, but this keeps Drive calls bounded.
        if len(combined) + len(new_entries) >= max_songs:
            break

        try:
            lines = m3u_parsing.download_m3u_file(drive_service, m3u_file["id"])
            file_date_str = m3u_file.get("name", "").replace(".m3u", "").strip()
            parsed = m3u_parsing.parse_m3u_lines(lines, seen_keys, file_date_str)

            if parsed:
                new_entries.extend(parsed)
                # Update seen keys so later files (and later parsing) can dedupe
                # against what we've already accepted.
                for row in parsed:
                    seen_keys.add(build_dedup_key(row))

            log.info(
                "Parsed %d new entries from %s", len(parsed or []), m3u_file.get("name")
            )
        except Exception as e:
            log.warning("Failed processing .m3u file %s: %s", m3u_file.get("name"), e)

    combined = [row[:3] for row in (combined + new_entries)]

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
