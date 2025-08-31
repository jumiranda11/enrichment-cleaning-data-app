# -*- coding: utf-8 -*-
"""
enrich_contacts.py
Usage:
    python enrich_contacts.py --in path/to/input.csv --out enriched.csv \
        --name-col name --website-col website

What it does:
- Reads CSV or XLSX (auto-detected by extension)
- Normalizes/validates website URLs when present (http/https)
- Crawls homepage + likely contact/about pages
- Extracts emails with a robust regex
- Deduplicates by (business, email) and overall email list
- Exports a clean CSV ready for CRM import
- Logs everything to enrich.log (and INFO to console)

Requirements:
    pip install pandas openpyxl requests beautifulsoup4 tldextract
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

# --------------------------- Logging setup --------------------------- #
LOG_FILE = "enrich.log"
logger = logging.getLogger("enrich")
logger.setLevel(logging.DEBUG)
# File handler (everything)
_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
# Console handler (info+)
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logger.addHandler(_fh)
logger.addHandler(_ch)

# --------------------------- HTTP session --------------------------- #
def build_session(timeout=10):
    session = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; DataEnrichmentBot/1.0; +github.com/you)"
    })
    session.request_timeout = timeout
    return session

SESSION = build_session()

# --------------------------- Helpers --------------------------- #
EMAIL_RE = re.compile(
    r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
)

LIKELY_LINK_HINTS = ("contact", "contato", "about", "sobre", "support", "suporte", "legal", "impressum", "privacy")

def safe_get(url: str) -> str:
    """GET with error handling. Returns HTML text or ''."""
    try:
        resp = SESSION.get(url, timeout=SESSION.request_timeout)
        if resp.status_code == 200 and "text" in resp.headers.get("Content-Type", ""):
            return resp.text
        else:
            logger.debug(f"Non-OK or non-text response for {url} (status={resp.status_code})")
    except Exception as e:
        logger.debug(f"GET failed for {url}: {e}")
    return ""

def normalize_url(raw: str | None) -> str | None:
    """Normalize potential website strings to a usable URL."""
    if not raw or not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw:
        return None

    # If it's an email accidentally, ignore
    if "@" in raw and "." in raw:
        return None

    # Try adding scheme if missing
    parsed = urlparse(raw)
    if not parsed.scheme:
        candidate = f"http://{raw}"
    else:
        candidate = raw

    # Validate netloc
    parsed2 = urlparse(candidate)
    if parsed2.netloc and "." in parsed2.netloc:
        return candidate
    return None

def guess_from_domainish(text: str | None) -> str | None:
    """If there's something that looks like a domain, try to form a URL."""
    if not text or not isinstance(text, str):
        return None
    ext = tldextract.extract(text.lower())
    if ext.domain and ext.suffix:
        return f"http://{ext.domain}.{ext.suffix}"
    return None

def extract_emails_from_html(html: str) -> set[str]:
    if not html:
        return set()
    # Get emails from raw text
    emails = set(EMAIL_RE.findall(html))

    # Basic noise filtering
    bad_suffixes = {".png", ".jpg", ".jpeg", ".gif", ".svg"}
    emails = {e for e in emails if not any(sfx in e.lower() for sfx in bad_suffixes)}
    return emails

def discover_candidate_links(base_url: str, html: str, limit: int = 5) -> list[str]:
    """Find likely 'contact/about' internal links."""
    out = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Make absolute
            abs_url = urljoin(base_url, href)
            # Keep only same-host links
            if urlparse(abs_url).netloc == urlparse(base_url).netloc:
                # Heuristic: hints in path or text
                text = (a.get_text() or "").lower()
                path = urlparse(abs_url).path.lower()
                if any(h in text or h in path for h in LIKELY_LINK_HINTS):
                    out.append(abs_url)
            if len(out) >= limit:
                break
    except Exception as e:
        logger.debug(f"discover_candidate_links error for {base_url}: {e}")
    return out

def crawl_for_emails(url: str, extra_pages: int = 4) -> set[str]:
    """Fetch homepage + a few likely pages and gather emails."""
    emails = set()
    html = safe_get(url)
    emails |= extract_emails_from_html(html)

    for link in discover_candidate_links(url, html, limit=extra_pages):
        emails |= extract_emails_from_html(safe_get(link))

    return emails

# --------------------------- Main pipeline --------------------------- #
def read_input(path: Path) -> pd.DataFrame:
    try:
        if path.suffix.lower() in (".xlsx", ".xls"):
            return pd.read_excel(path)
        return pd.read_csv(path)
    except Exception as e:
        logger.error(f"Failed to read input file '{path}': {e}")
        raise

def dedupe_email_list(emails: set[str]) -> str:
    # Simple normalization: lowercase; sort for determinism
    normed = sorted({e.strip().lower() for e in emails if e and "@" in e})
    return ",".join(normed)

def process_row(row: pd.Series, name_col: str, website_col: str) -> dict:
    """Return a dict with {resolved_url, emails, error} for the row."""
    error = None
    resolved_url = None
    emails = set()

    try:
        # Priority 1: website column
        raw_site = row.get(website_col) if website_col in row else None
        resolved_url = normalize_url(raw_site)

        # If not available, try to guess from any domainish text we can find
        if not resolved_url:
            # Try a couple of plausible fields
            for candidate_col in ("domain", "url", "site", "homepage"):
                if candidate_col in row:
                    resolved_url = normalize_url(row.get(candidate_col))
                    if resolved_url:
                        break

        # Last resort: try to infer from the name (very rough)
        if not resolved_url:
            name_val = row.get(name_col) if name_col in row else None
            resolved_url = guess_from_domainish(name_val)

        if resolved_url:
            emails = crawl_for_emails(resolved_url)
        else:
            error = "No valid website found to crawl"

    except Exception as e:
        error = f"Row processing error: {e}"

    return {
        "resolved_url": resolved_url or "",
        "emails": dedupe_email_list(emails),
        "error": error or ""
    }

def enrich(
    input_path: Path,
    output_path: Path,
    name_col: str = "name",
    website_col: str = "website"
):
    df = read_input(input_path)

    # Ensure required columns exist (soft check; we can still proceed)
    missing = [c for c in [name_col, website_col] if c not in df.columns]
    if missing:
        logger.warning(f"Missing expected columns: {missing}. Will attempt best-effort enrichment.")

    results = []
    for idx, row in df.iterrows():
        try:
            res = process_row(row, name_col=name_col, website_col=website_col)
            results.append(res)
            if res["error"]:
                logger.warning(f"Row {idx}: {res['error']}")
        except Exception as e:
            # Never break the whole run
            results.append({"resolved_url": "", "emails": "", "error": f"Hard failure: {e}"})
            logger.error(f"Row {idx} hard failure: {e}")

    out_df = df.copy()
    out_df["resolved_url"] = [r["resolved_url"] for r in results]
    out_df["emails_raw"]   = [r["emails"] for r in results]
    out_df["error"]        = [r["error"] for r in results]

    # Expand emails into unique rows or keep as comma list; here we keep one row per business
    # but create also a deduped overall "emails_clean" column without duplicates per business.
    def drop_dupes(s: str) -> str:
        return dedupe_email_list(set(s.split(","))) if isinstance(s, str) and s else ""

    out_df["emails_clean"] = out_df["emails_raw"].apply(drop_dupes)

    # Global dedup: if multiple identical rows exist, keep first
    out_df = out_df.drop_duplicates(subset=[name_col, "resolved_url", "emails_clean"], keep="first")

    # Write CSV only (CRM-friendly). If user passed .xlsx, still produce CSV as requested.
    try:
        out_df.to_csv(output_path, index=False)
        logger.info(f"✅ Enriched CSV saved to: {output_path}")
        logger.info(f"📝 Log file saved to: {Path(LOG_FILE).resolve()}")
    except Exception as e:
        logger.error(f"Failed to write output CSV: {e}")
        raise

# --------------------------- CLI --------------------------- #
def parse_args():
    p = argparse.ArgumentParser(description="Enrich business CSV/XLSX with website emails.")
    p.add_argument("--in", dest="input_path", required=True, help="Path to input .csv or .xlsx")
    p.add_argument("--out", dest="output_path", required=True, help="Path to output .csv")
    p.add_argument("--name-col", default="name", help="Column with business name (default: name)")
    p.add_argument("--website-col", default="website", help="Column with website URL if available (default: website)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    try:
        enrich(
            input_path=Path(args.input_path),
            output_path=Path(args.output_path),
            name_col=args.name_col,
            website_col=args.website_col
        )
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
