import datetime
from urllib.parse import urlencode

import kaiano_common_utils.config as config
import kaiano_common_utils.logger as log
import pytz
from googleapiclient.errors import HttpError
from kaiano_common_utils.google import GoogleAPI
from kaiano_common_utils.vdj.m3u.api import M3UToolbox


def normalize_cell(value: str | None) -> str:
    """Normalize a cell value for dedupe comparisons (stable, whitespace-trimmed)."""
    return (value or "").strip()


def build_dedup_key(row: list[str]) -> str:
    """Build a stable, case-insensitive dedupe key for [datetime, title, artist]."""
    return "||".join(normalize_cell(c).casefold() for c in row[:3])


def build_dedup_keys(rows: list[list[str]]) -> set[str]:
    return {build_dedup_key(r) for r in rows}


def build_youtube_links(entries):
    links = []
    for _, title, artist in entries:
        query = urlencode({"search_query": f"{title} {artist}"})
        url = f"https://www.youtube.com/results?{query}"
        log.debug("YouTube link: %s", url)
        links.append([f'=HYPERLINK("{url}", "YouTube Search")'])
    return links


def write_entries_to_sheet(g: GoogleAPI, entries, now):
    log.info("Clearing old entries in sheet range A5:D...")
    g.sheets.clear(
        config.LIVE_HISTORY_SPREADSHEET_ID, g.sheets.get_range_format("A", 5, "D")
    )

    if not entries:
        log.info("No entries to write. Writing NO_HISTORY message.")
        log.debug(
            "Sheet write range: %s, entries: %s",
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
        )
        g.sheets.write_values(
            config.LIVE_HISTORY_SPREADSHEET_ID,
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
            value_input_option="RAW",
        )
        return

    log.info("Writing %d entries to sheet...", len(entries))
    log.debug(
        "Sheet write range: %s, entries: %s",
        g.sheets.get_range_format("A", 5, "C", 5 + len(entries) - 1),
        entries,
    )
    g.sheets.write_values(
        config.LIVE_HISTORY_SPREADSHEET_ID,
        g.sheets.get_range_format("A", 5, "C", 5 + len(entries) - 1),
        entries,
        value_input_option="RAW",
    )

    try:
        log.info("Writing %d links to sheet...", len(entries))
        links = build_youtube_links(entries)
        log.debug(
            "Link write range: %s, links: %s",
            g.sheets.get_range_format("D", 5, "D", 5 + len(links) - 1),
            links,
        )
        g.sheets.write_values(
            config.LIVE_HISTORY_SPREADSHEET_ID,
            g.sheets.get_range_format("D", 5, "D", 5 + len(links) - 1),
            links,
            value_input_option="USER_ENTERED",
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


def read_existing_entries(g: GoogleAPI):
    log.info("Reading existing entries from sheet...")
    values = g.sheets.read_values(
        config.LIVE_HISTORY_SPREADSHEET_ID,
        g.sheets.get_range_format("A", 5, "C"),
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


def update_last_run_time(g: GoogleAPI, now):
    log.info("Updating last run time in sheet...")
    g.sheets.write_values(
        config.LIVE_HISTORY_SPREADSHEET_ID,
        "A1",
        [[log.format_date(now)]],
        value_input_option="RAW",
    )


def publish_history(g: GoogleAPI):
    tz = pytz.timezone(config.TIMEZONE)
    now = datetime.datetime.now(tz)

    log.info("--- Starting publish_history ---")

    update_last_run_time(g, now)
    m3u_tool = M3UToolbox()

    m3u_files = g.drive.get_all_m3u_files()
    if not m3u_files:
        log.info("No .m3u files found. Clearing sheet and writing NO_HISTORY.")
        g.sheets.clear(
            config.LIVE_HISTORY_SPREADSHEET_ID, g.sheets.get_range_format("A", 5, "D")
        )
        g.sheets.write_values(
            config.LIVE_HISTORY_SPREADSHEET_ID,
            "A5:B5",
            [[log.format_date(now), config.NO_HISTORY]],
            value_input_option="RAW",
        )
        return

    existing_data = read_existing_entries(g)
    # Start with whatever is already in the sheet, and then add new entries from
    # *all* m3u files (newest-first), stopping once we have enough to write.
    max_songs = int(getattr(config, "HISTORY_MAX_SONGS", 50) or 50)
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
            lines = g.drive.download_m3u_file_data(m3u_file["id"])
            file_date_str = m3u_file.get("name", "").replace(".m3u", "").strip()
            parsed_entries = m3u_tool.parse.parse_m3u_lines(
                lines, seen_keys, file_date_str
            )
            parsed_rows = [[e.dt, e.title, e.artist] for e in parsed_entries]

            if parsed_rows:
                new_entries.extend(parsed_rows)

            log.info(
                "Parsed %d new entries from %s", len(parsed_rows), m3u_file.get("name")
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

    write_entries_to_sheet(g, combined, now)

    log.info("Script finished. Rows written: %d", len(combined))


if __name__ == "__main__":

    g = GoogleAPI.from_env()

    publish_history(g)
