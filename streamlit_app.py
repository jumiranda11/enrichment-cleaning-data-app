# streamlit_app.py
# ------------------------------------------------------------
# Demo UI for Business Data Enrichment
# Upload CSV/XLSX -> Crawl websites -> Extract emails -> Export
#
# Run locally:
#   streamlit run streamlit_app.py
#
# Deploy:
#   - Push to GitHub
#   - Streamlit Cloud: "New app" -> Select repo/branch/file
# ------------------------------------------------------------
import io
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
import streamlit as st
import tldextract
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

# --------------------- Page config --------------------- #
st.set_page_config(
    page_title="Business Email Enrichment Demo",
    page_icon="📬",
    layout="wide",
)

st.title("📬 Business Email Enrichment — Demo")
st.write(
    "Upload a CSV/XLSX with your business list and I’ll resolve websites, crawl "
    "likely contact pages, extract emails, and export a cleaned CSV ready for CRM."
)

# --------------------- In-memory logger --------------------- #
class MemoryLogger:
    def __init__(self):
        self._buf = io.StringIO()

    def log(self, level: str, msg: str):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        self._buf.write(f"{ts} | {level.upper():<7} | {msg}\n")

    def info(self, msg): self.log("INFO", msg)
    def warn(self, msg): self.log("WARNING", msg)
    def error(self, msg): self.log("ERROR", msg)
    def debug(self, msg): self.log("DEBUG", msg)

    def getvalue(self) -> str:
        return self._buf.getvalue()

memlog = MemoryLogger()

# --------------------- HTTP session --------------------- #
def build_session(timeout=10):
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; EnrichmentDemo/1.0; +github.com/yourusername)"
    })
    session.request_timeout = timeout
    return session

SESSION = build_session()

# --------------------- Helpers --------------------- #
EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b")
LIKELY_LINK_HINTS = ("contact", "contato", "about", "sobre", "support", "suporte", "legal", "impressum", "privacy")

def safe_get(url: str) -> str:
    try:
        r = SESSION.get(url, timeout=SESSION.request_timeout)
        ctype = r.headers.get("Content-Type", "")
        if r.status_code == 200 and "text" in ctype:
            return r.text
        memlog.debug(f"Non-OK or non-text response for {url} (status={r.status_code}, ctype={ctype})")
    except Exception as e:
        memlog.debug(f"GET failed for {url}: {e}")
    return ""

def normalize_url(raw: str | None) -> str | None:
    if not raw or not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw:
        return None
    if "@" in raw and "." in raw:  # looks like an email, not a URL
        return None
    parsed = urlparse(raw)
    candidate = f"http://{raw}" if not parsed.scheme else raw
    parsed2 = urlparse(candidate)
    if parsed2.netloc and "." in parsed2.netloc:
        return candidate
    return None

def guess_from_domainish(text: str | None) -> str | None:
    if not text or not isinstance(text, str):
        return None
    ext = tldextract.extract(text.lower())
    if ext.domain and ext.suffix:
        return f"http://{ext.domain}.{ext.suffix}"
    return None

def extract_emails_from_html(html: str) -> set[str]:
    if not html:
        return set()
    emails = set(EMAIL_RE.findall(html))
    bad_suffixes = {".png", ".jpg", ".jpeg", ".gif", ".svg"}
    return {e for e in emails if not any(sfx in e.lower() for sfx in bad_suffixes)}

def discover_candidate_links(base_url: str, html: str, limit: int = 5) -> list[str]:
    out = []
    if not html:
        return out
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            abs_url = urljoin(base_url, href)
            if urlparse(abs_url).netloc == urlparse(base_url).netloc:
                text = (a.get_text() or "").lower()
                path = urlparse(abs_url).path.lower()
                if any(h in text or h in path for h in LIKELY_LINK_HINTS):
                    out.append(abs_url)
            if len(out) >= limit:
                break
    except Exception as e:
        memlog.debug(f"discover_candidate_links error for {base_url}: {e}")
    return out

def crawl_for_emails(url: str, extra_pages: int = 4) -> set[str]:
    emails = set()
    html = safe_get(url)
    emails |= extract_emails_from_html(html)
    for link in discover_candidate_links(url, html, limit=extra_pages):
        emails |= extract_emails_from_html(safe_get(link))
    return emails

def dedupe_email_list(emails: set[str]) -> str:
    normed = sorted({e.strip().lower() for e in emails if e and "@" in e})
    return ",".join(normed)

def process_row(row: pd.Series, name_col: str, website_col: str, extra_pages: int) -> dict:
    error = None
    resolved_url = None
    emails = set()

    try:
        raw_site = row.get(website_col) if website_col in row else None
        resolved_url = normalize_url(raw_site)

        if not resolved_url:
            for candidate_col in ("domain", "url", "site", "homepage"):
                if candidate_col in row:
                    resolved_url = normalize_url(row.get(candidate_col))
                    if resolved_url:
                        break

        if not resolved_url:
            name_val = row.get(name_col) if name_col in row else None
            resolved_url = guess_from_domainish(name_val)

        if resolved_url:
            emails = crawl_for_emails(resolved_url, extra_pages=extra_pages)
        else:
            error = "No valid website found to crawl"

    except Exception as e:
        error = f"Row processing error: {e}"

    return {
        "resolved_url": resolved_url or "",
        "emails_raw": dedupe_email_list(emails),
        "error": error or ""
    }

def read_any(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".xlsx") or name.endswith(".xls"):
            return pd.read_excel(uploaded_file)
        return pd.read_csv(uploaded_file)
    except Exception as e:
        raise RuntimeError(f"Failed to read file '{uploaded_file.name}': {e}")

# --------------------- Sidebar controls --------------------- #
with st.sidebar:
    st.header("⚙️ Options")
    name_col = st.text_input("Business name column", value="name")
    website_col = st.text_input("Website column (optional)", value="website")
    extra_pages = st.slider("Extra pages to scan per site", min_value=0, max_value=8, value=4, help="Contact/About/Privacy pages on the same domain")
    row_limit = st.number_input("Max rows to process (for demo)", min_value=1, value=250, step=50)
    show_preview_rows = st.number_input("Preview rows (head)", min_value=5, max_value=50, value=15, step=5)
    st.caption("Tip: keep limits modest for a smooth live demo.")

# --------------------- File upload --------------------- #
st.subheader("1) Upload your file")
uploaded = st.file_uploader("Accepts .csv or .xlsx", type=["csv", "xlsx", "xls"])

colA, colB = st.columns([1, 1])
with colA:
    sample_btn = st.button("Download sample CSV")
with colB:
    run_btn = st.button("Run enrichment", type="primary", disabled=uploaded is None)

if sample_btn:
    sample = pd.DataFrame({
        "name": ["Acme Inc", "Beta LLC", "Gamma Studio"],
        "website": ["acme.com", "", "https://gammastudio.example"]
    })
    st.download_button(
        label="📥 Save sample_businesses.csv",
        data=sample.to_csv(index=False).encode("utf-8"),
        file_name="sample_businesses.csv",
        mime="text/csv",
    )

# --------------------- Processing --------------------- #
out_df = None
if run_btn and uploaded is not None:
    try:
        df = read_any(uploaded)
    except Exception as e:
        st.error(str(e))
        st.stop()

    total_rows = len(df)
    if total_rows == 0:
        st.warning("Empty file. Please upload a non-empty CSV/XLSX.")
        st.stop()

    if total_rows > row_limit:
        memlog.warn(f"Input has {total_rows} rows; demo limited to first {row_limit}.")
        df = df.head(row_limit)

    st.subheader("2) Processing")
    progress = st.progress(0, text="Starting...")
    status = st.empty()

    results = []
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        res = process_row(row, name_col=name_col, website_col=website_col, extra_pages=extra_pages)
        results.append(res)
        if res["error"]:
            memlog.warn(f"Row {i}: {res['error']}")
        progress.progress(int(i * 100 / len(df)), text=f"Processed {i}/{len(df)} rows")
        if i % 5 == 0:
            status.write(f"Latest resolved: {res.get('resolved_url', '')}")

    out_df = df.copy()
    out_df["resolved_url"] = [r["resolved_url"] for r in results]
    out_df["emails_raw"]   = [r["emails_raw"] for r in results]
    out_df["error"]        = [r["error"] for r in results]

    # Per-business cleaned email column
    def _clean_list(s: str) -> str:
        return ",".join(sorted({e.strip().lower() for e in s.split(",") if "@" in e})) if isinstance(s, str) and s else ""
    out_df["emails_clean"] = out_df["emails_raw"].apply(_clean_list)

    # Deduplicate rows (name + resolved_url + emails_clean)
    keep_cols = list(out_df.columns)
    subset = [c for c in [name_col, "resolved_url", "emails_clean"] if c in out_df.columns]
    out_df = out_df.drop_duplicates(subset=subset, keep="first")

    st.success("Done! Preview below 👇")

# --------------------- Results & Downloads --------------------- #
if out_df is not None and len(out_df) > 0:
    st.subheader("3) Preview")
    st.dataframe(out_df.head(int(show_preview_rows)), use_container_width=True)

    csv_bytes = out_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download enriched CSV",
        data=csv_bytes,
        file_name="enriched_contacts.csv",
        mime="text/csv",
        type="primary",
    )

    st.subheader("4) Run Log")
    log_text = memlog.getvalue()
    if not log_text:
        log_text = "No warnings/errors recorded."
    st.code(log_text, language="text")
    st.download_button(
        label="📜 Download log",
        data=log_text.encode("utf-8"),
        file_name="enrich.log",
        mime="text/plain",
    )

# --------------------- Footer --------------------- #
st.markdown("---")
st.caption(
    "Demo by Julia · Clean, documented, and extendable. "
    "I’ll adapt this pipeline to your CRM schema, add MX/SMTP verification, "
    "and handle scale + monitoring as needed."
)
