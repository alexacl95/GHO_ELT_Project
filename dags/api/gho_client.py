from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import quote, urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://ghoapi.azureedge.net/api"
DEFAULT_TIMEOUT = (5, 120)  # (connect seconds, read seconds)

log = logging.getLogger(__name__)


def build_session()-> requests.Session:
    """
    Description: Build a session with connection reuse and automatic retries on transient errors.
    Output: requests.Session object
    """
    retry = Retry(
        total=5,
        backoff_factor=1,  # waits seconds between retries: 1, 2, 4, 8, 16
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(
        {"Accept": "application/json", "User-Agent": "gho-pipeline/0.1 (portfolio project)"}
    )
    return session


def _build_url(path: str, odata_filter: str | None = None) -> str:
    """ 
    Description: Build a URL for the GHO API endpoint with optional OData filter.
    Input: 
        1. path: The endpoint path, e.g., "Indicator"
        2. odata_filter: The OData filter string or None
    Output: str
    """
    url = f"{BASE_URL}/{path}"
    if odata_filter:
        url += "?" + urlencode({"$filter": odata_filter}, quote_via=quote)
    return url


def fetch_rows(
    session: requests.Session,
    path: str,
    odata_filter: str | None = None,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    ) -> tuple[list[dict], str]:
    """
    Description: Fetch rows from the GHO API endpoint, handling pagination.
    Input:
        1. session: requests.Session object
        2. path: The endpoint path, e.g., "Indicator"
        3. odata_filter: The OData filter string or None
        4. timeout: Tuple of (connect timeout, read timeout)
    Output:
        1. rows: List of dictionaries representing the fetched rows
        2. first_url: The initial URL used for the request
    """
    first_url = _build_url(path, odata_filter)
    url: str | None = first_url
    rows: list[dict] = []

    while url:
        log.info("GET %s", url)
        resp = session.get(url, timeout=timeout)
        resp.raise_for_status()  
        payload = resp.json()
        rows.extend(payload.get("value", []))
        url = payload.get("@odata.nextLink")

    return rows, first_url

def get_indicator_catalog(session: requests.Session, keyword: str | None = None) -> list[dict]:
    """
    Description: Get the catalog of indicators from the GHO API, optionally filtered by a keyword.
    Input:
        1. session: requests.Session object
        2. keyword: Optional string to filter indicators by name (case-insensitive)
    Output: List of dictionaries with 'IndicatorCode' and 'IndicatorName' keys
    """
    rows, _ = fetch_rows(session, "Indicator", None, None)
    catalog = [
        {"IndicatorCode": r["IndicatorCode"], "IndicatorName": r["IndicatorName"]}
        for r in rows
    ]
    if keyword:
        kw = keyword.lower()
        catalog = [c for c in catalog if kw in c["IndicatorName"].lower()]
    return catalog

def download_indicators(
    session: requests.Session,
    codes: list[str],
    odata_filter: str | None = None,
    pause_seconds: float = 0.5,
    ) -> tuple[dict[str, dict], dict[str, str]]:
    """Download several indicators.

    Returns (results, failures):
      results  = {code: {"source_url": str, "rows": list[dict]}}
      failures = {code: error message}
    One bad indicator does not stop the others.
    """
    results: dict[str, dict] = {}
    failures: dict[str, str] = {}

    for code in codes:
        try:
            rows, source_url = fetch_rows(session, code, odata_filter)
            if not rows:
                raise ValueError("API returned zero rows (wrong code or filter?)")
            results[code] = {"source_url": source_url, "rows": rows}
            log.info("%s: %s rows", code, len(rows))
        except (requests.RequestException, ValueError) as exc:
            log.error("%s failed: %s", code, exc)
            failures[code] = str(exc)
        time.sleep(pause_seconds)  # be polite to a public API

    return results, failures

def _atomic_write_json(payload: dict | list, target: Path) -> None:
    """
    Description: Write JSON to a temp file, then rename, so no half-written file is ever visible.
    Input:
        1. payload: Dictionary or list to be written as JSON
        2. target: Path object representing the target file
    """
    fd, tmp_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise

def save_reference_data(
    run_dir: Path,
    catalog: list[dict],
    countries: list[dict],
) -> None:
    """Save the two dimension sources next to the raw indicator files.

    The "_" prefix keeps them out of the indicator glob ([!_]*.json) that Spark uses.
    """
    if not catalog:
        raise ValueError("Catalog is empty: refusing to overwrite _catalog.json")
    if not countries:
        raise ValueError("Country list is empty: refusing to overwrite _countries.json")

    run_dir.mkdir(parents=True, exist_ok=True)
    # Plain list: matches read_catalog() in transform.py
    _atomic_write_json(catalog, run_dir / "_catalog.json")
    # Wrapped in {"value": [...]}: matches COUNTRY_SCHEMA in transform.py
    _atomic_write_json({"value": countries}, run_dir / "_countries.json")

    log.info("Saved %s indicators and %s countries to %s", len(catalog), len(countries), run_dir)

def save_raw_data(
    results: dict[str, dict],
    out_dir: Path,
    failures: dict[str, str] | None = None,
    run_date: date | None = None,
    ) -> Path:
    """
    Description: Save each indicator as JSON inside <out_dir>/gho_<YYYY-MM-DD>/ and write a manifest.
    Input:
        1. results: Dictionary of indicator data returned by download_indicators
        2. out_dir: Path object representing the output directory
        3. failures: Dictionary of failed indicators returned by download_indicators
        4. run_date: Optional date object representing the run date (defaults to today)
    Output: Path object representing the directory where data was saved
    """
    run_date = run_date or date.today()
    stamp = run_date.isoformat()
    run_dir = out_dir / f"gho_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    extracted_at = datetime.now(UTC).isoformat()

    manifest_items = []
    for code, data in results.items():
        envelope = {
            "indicator_code": code,
            "source_url": data["source_url"],
            "extracted_at": extracted_at,
            "row_count": len(data["rows"]),
            "value": data["rows"],
        }
        filename = f"{code}_{stamp}.json"
        _atomic_write_json(envelope, run_dir / filename)
        manifest_items.append(
            {"indicator_code": code, "file": filename, "row_count": len(data["rows"])}
        )

    manifest = {
        "run_date": stamp,
        "extracted_at": extracted_at,
        "indicators": manifest_items,
        "total_rows": sum(i["row_count"] for i in manifest_items),
        "failures": failures or {},
    }
    _atomic_write_json(manifest, run_dir / "_manifest.json")

    log.info("Saved %s indicators to %s", len(manifest_items), run_dir)
    return run_dir

def get_countries(session: requests.Session) -> list[dict]:
    """
    Description: Fetch the country dimension from the GHO API, including ISO3 code, name, and WHO region.
    Input: session: requests.Session object
    Output: List of dictionaries with 'country_code', 'country_name', 'who_region_code', and 'who_region_name' keys
    """
    rows, _ = fetch_rows(session, "DIMENSION/COUNTRY/DimensionValues")
    return [
        {
            "country_code": r["Code"],
            "country_name": r["Title"],
            "who_region_code": r.get("ParentCode"),
            "who_region_name": r.get("ParentTitle"),
        }
        for r in rows
    ]
