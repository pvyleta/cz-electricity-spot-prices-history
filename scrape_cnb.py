"""
Scraper for CNB (Czech National Bank) daily exchange rates.

Source: https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/
Data is published as pipe-separated text with decimal commas, one full year per URL.
Currencies change over the years (pre-Euro legacy currencies, etc.) and even mid-year
in the early 1990s — the parser handles re-headers within a single year's data.

Output: data/cnb/YYYY.csv — one file per year, decimal points, ISO dates.

Usage:
    python scrape_cnb.py --year 2024
    python scrape_cnb.py --from 1991 --to 2026
    python scrape_cnb.py --current           # re-download current year (cron entry point)

Dependencies: requests
    pip install requests
"""

import argparse
import csv
import logging
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/rok.txt"
DATA_DIR = Path(__file__).parent / "data" / "cnb"
# CNB data starts in 1991 — rok=1990 silently returns the current year.
EARLIEST_YEAR = 1991


def download_year(year: int | None = None) -> str | None:
    """Download one year of CNB exchange rate data. None = current year."""
    params = {"rok": year} if year else {}
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        log.error("Failed to download year %s: %s", year, e)
        return None


def parse_cnb_text(raw: str) -> tuple[list[str], list[dict]]:
    """Parse CNB pipe-separated text into rows.

    Returns (all_columns_sorted, rows) where each row is a dict mapping
    column name to value. Handles mid-year header changes by merging all
    columns seen across the year.

    Decimal commas are converted to decimal points.
    Dates are converted from DD.MM.YYYY to YYYY-MM-DD.
    """
    all_columns: set[str] = set()
    rows: list[dict] = []
    current_header: list[str] = []

    for line in raw.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        if line.startswith("Datum"):
            # Header line — update current column mapping.
            # Format: "Datum|1 AUD|1 EUR|100 JPY|..."
            current_header = line.split("|")[1:]  # drop "Datum"
            all_columns.update(current_header)
            continue

        parts = line.split("|")
        if len(parts) < 2:
            continue

        # Convert DD.MM.YYYY → YYYY-MM-DD
        try:
            date_str = datetime.strptime(parts[0], "%d.%m.%Y").strftime("%Y-%m-%d")
        except ValueError:
            log.warning("Skipping unparseable line: %s", line[:80])
            continue

        row = {"date": date_str}
        for col, val in zip(current_header, parts[1:]):
            # Decimal comma → decimal point
            row[col] = val.replace(",", ".") if val else ""
        rows.append(row)

    sorted_cols = sorted(all_columns)
    return sorted_cols, rows


def write_csv(year: int, columns: list[str], rows: list[dict]) -> Path:
    """Write parsed rows to data/cnb/YYYY.csv."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    filepath = DATA_DIR / f"{year}.csv"

    fieldnames = ["date"] + columns
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            # Fill missing columns with empty string
            out = {col: row.get(col, "") for col in fieldnames}
            writer.writerow(out)

    return filepath


def scrape_year(year: int | None = None) -> bool:
    """Scrape one year. None = current year. Returns True on success."""
    label = year or "current"
    log.info("Downloading CNB data for %s", label)

    raw = download_year(year)
    if not raw:
        return False

    columns, rows = parse_cnb_text(raw)
    if not rows:
        log.warning("No data parsed for %s", label)
        return False

    # Infer actual year from first row's date
    actual_year = int(rows[0]["date"][:4])
    filepath = write_csv(actual_year, columns, rows)
    log.info("  %s: %d rows, %d currencies → %s", label, len(rows), len(columns), filepath)
    return True


def scrape_range(start: int, end: int) -> None:
    ok = 0
    for year in range(start, end + 1):
        if scrape_year(year):
            ok += 1
    log.info("Done. %d/%d years scraped.", ok, end - start + 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape CNB exchange rates")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Scrape a single year")
    group.add_argument("--current", action="store_true", help="Re-download current year")
    group.add_argument("--from", dest="from_year", type=int, help="Start year")
    parser.add_argument("--to", dest="to_year", type=int, help="End year")

    args = parser.parse_args()

    if args.year:
        scrape_year(args.year)
    elif args.current:
        scrape_year(None)
    elif args.from_year:
        end = args.to_year or datetime.now().year
        scrape_range(args.from_year, end)


if __name__ == "__main__":
    main()
