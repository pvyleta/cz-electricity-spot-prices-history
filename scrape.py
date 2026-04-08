"""
Scraper for OTE-CR Czech electricity day-ahead spot prices.

Resolution strategy per date:
  >= 2025-01-01  try 15min XLSX (available from 2025-10-01), fall back to 1h XLSX
  <  2025-01-01  hit the JSON chart-data API directly (same source, no XLSX needed,
                 works back to 2002)

Output: data/1h/YYYY.csv and data/15min/YYYY.csv

Usage:
    python scrape.py --date 2026-04-08
    python scrape.py --from 2015-01-01 --to 2024-12-31
    python scrape.py --yesterday          # cron entry point

Dependencies:
    pip install openpyxl requests
"""

import argparse
import csv
import io
import logging

from datetime import date, datetime, timedelta
from pathlib import Path

import openpyxl
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.ote-cr.cz/pubweb/attachments/01"
DATA_DIR = Path(__file__).parent / "data"

# Row 22 = header, row 23 = blank, row 24 = first data — true for both XLSX formats.
DATA_START_ROW = 24


def xlsx_url_15min(d: date) -> str:
    return f"{BASE_URL}/{d.year}/month{d.month:02d}/day{d.day:02d}/DT_15MIN_{d.day:02d}_{d.month:02d}_{d.year}_CZ.xlsx"


def xlsx_url_1h(d: date) -> str:
    return f"{BASE_URL}/{d.year}/month{d.month:02d}/day{d.day:02d}/DT_{d.day:02d}_{d.month:02d}_{d.year}_CZ.xlsx"


def download_xlsx(url: str) -> bytes | None:
    """Return raw XLSX bytes, or None on 404 / network error."""
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 500:
            return resp.content
        return None
    except requests.RequestException as e:
        log.warning("Request failed for %s: %s", url, e)
        return None


def parse_15min_xlsx(data: bytes, d: date) -> tuple[list[dict], list[dict]]:
    """Parse the 15min XLSX. Returns (rows_15min, rows_1h).

    The file embeds both resolutions: col C = 15min price, col L = 1h price.
    Extracting both here avoids a second download for the 1h CSV.
    """
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    rows_15min = []
    rows_1h = []
    seen_hours: set[str] = set()  # col L repeats the same 1h price for each slot in the hour

    for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
        period_num = row[0]
        if period_num is None or not isinstance(period_num, (int, float)):
            break  # "Celkem" summary row signals end of data

        time_interval = row[1]  # "HH:MM-HH:MM"
        price_15min = row[2]    # col C — 15min price EUR/MWh
        price_1h = row[11]      # col L — 1h price EUR/MWh

        time_from, time_to = time_interval.split("-")
        rows_15min.append({"date": d.isoformat(), "time_from": time_from,
                           "time_to": time_to, "price_eur_mwh": price_15min})

        hour_start = time_from[:2] + ":00"
        if price_1h is not None and hour_start not in seen_hours:
            seen_hours.add(hour_start)
            hour_end_h = int(time_from[:2]) + 1
            rows_1h.append({
                "date": d.isoformat(),
                "time_from": hour_start,
                "time_to": f"{hour_end_h:02d}:00" if hour_end_h < 24 else "24:00",
                "price_eur_mwh": price_1h,
            })

    wb.close()
    return rows_15min, rows_1h


def parse_1h_xlsx(data: bytes, d: date) -> list[dict]:
    """Parse the 1h-only XLSX (2025-01-01 – 2025-09-30). Col A = hour (1-24), col B = price."""
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    rows = []
    for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
        hour_num = row[0]
        if hour_num is None or not isinstance(hour_num, (int, float)):
            break

        hour = int(hour_num)
        price = row[1]  # EUR/MWh

        rows.append({
            "date": d.isoformat(),
            "time_from": f"{hour - 1:02d}:00",
            "time_to": f"{hour:02d}:00" if hour < 24 else "24:00",
            "price_eur_mwh": price,
        })

    wb.close()
    return rows


JSON_API = "https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/denni-trh/@@chart-data"
CSV_HEADER = ["date", "time_from", "time_to", "price_eur_mwh"]


def scrape_day_json(d: date) -> list[dict]:
    """Fetch 1h prices from OTE's chart-data JSON API.

    Same endpoint the website uses for its charts; available back to 2002.
    The response has multiple dataLine objects; useY2=False identifies the price series.
    """
    params = {"report_date": d.isoformat(), "time_resolution": "PT60M"}
    try:
        resp = requests.get(JSON_API, params=params, timeout=30)
        if resp.status_code != 200:
            return []
        payload = resp.json()
    except Exception as e:
        log.warning("JSON API failed for %s: %s", d, e)
        return []

    price_line = next(
        (dl for dl in payload["data"]["dataLine"] if not dl.get("useY2", True)),
        None,
    )
    if not price_line:
        log.warning("No price dataLine in JSON response for %s", d)
        return []

    rows = []
    for pt in price_line["point"]:
        hour = int(pt["x"])
        rows.append({
            "date": d.isoformat(),
            "time_from": f"{hour - 1:02d}:00",
            "time_to": f"{hour:02d}:00" if hour < 24 else "24:00",
            "price_eur_mwh": pt["y"],
        })
    return rows


def append_to_csv(filepath: Path, rows: list[dict]) -> None:
    """Append rows to CSV, writing the header on first write."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    file_exists = filepath.exists() and filepath.stat().st_size > 0

    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def date_already_scraped(filepath: Path, d: date) -> bool:
    """Return True if the CSV already contains a row for this date."""
    if not filepath.exists():
        return False
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["date"] == d.isoformat():
                return True
    return False


# XLSX files are only published on pubweb from 2025 onwards.
# For earlier dates, skip straight to the JSON API to avoid wasted requests.
XLSX_AVAILABLE_FROM = date(2025, 1, 1)


def scrape_day(d: date) -> bool:
    """Scrape one day. Returns True if rows were written."""
    csv_15min = DATA_DIR / "15min" / f"{d.year}.csv"
    csv_1h    = DATA_DIR / "1h"    / f"{d.year}.csv"

    if date_already_scraped(csv_1h, d):
        log.debug("Already scraped %s, skipping", d)
        return False

    if d >= XLSX_AVAILABLE_FROM:
        # 15min XLSX (available from 2025-10-01) embeds both resolutions.
        data = download_xlsx(xlsx_url_15min(d))
        if data:
            rows_15, rows_1h = parse_15min_xlsx(data, d)
            log.info("%s: %d 15min rows, %d 1h rows (XLSX)", d, len(rows_15), len(rows_1h))
            if rows_15: append_to_csv(csv_15min, rows_15)
            if rows_1h: append_to_csv(csv_1h, rows_1h)
            return True

        # 1h-only XLSX covers 2025-01-01 – 2025-09-30.
        data = download_xlsx(xlsx_url_1h(d))
        if data:
            rows = parse_1h_xlsx(data, d)
            log.info("%s: %d 1h rows (XLSX)", d, len(rows))
            if rows: append_to_csv(csv_1h, rows)
            return True

    # Pre-2025 dates have no XLSX on pubweb; JSON API covers everything back to 2002.
    rows = scrape_day_json(d)
    if rows:
        log.info("%s: %d 1h rows (JSON)", d, len(rows))
        append_to_csv(csv_1h, rows)
        return True

    log.warning("No data for %s", d)
    return False


def scrape_range(start: date, end: date) -> None:
    """Scrape all days in [start, end] inclusive."""
    total_days = (end - start).days + 1
    scraped = 0
    skipped = 0
    failed = 0

    d = start
    while d <= end:
        try:
            if scrape_day(d):
                scraped += 1
            else:
                skipped += 1
        except Exception:
            log.exception("Failed to scrape %s", d)
            failed += 1
        d += timedelta(days=1)

    log.info(
        "Done. %d days total, %d scraped, %d skipped, %d failed",
        total_days, scraped, skipped, failed,
    )


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape CZ electricity spot prices from OTE-CR")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=parse_date, help="Scrape a single date (YYYY-MM-DD)")
    group.add_argument("--yesterday", action="store_true", help="Scrape yesterday's data")
    group.add_argument("--from", dest="from_date", type=parse_date, help="Start of date range (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=parse_date, help="End of date range (YYYY-MM-DD), defaults to today")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.date:
        scrape_day(args.date)
    elif args.yesterday:
        scrape_day(date.today() - timedelta(days=1))
    elif args.from_date:
        end = args.to_date or date.today()
        scrape_range(args.from_date, end)


if __name__ == "__main__":
    main()
