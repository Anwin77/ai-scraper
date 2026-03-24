#!/usr/bin/env python3
"""
COMPANY ENRICHMENT SCRAPER — BASE
===================================
Enriches company data with:
  Company Name | Revenue Size | Revenue Evidence | Industry |
  Employee Size | Region | Company Website

Based on industry_scraper_aimode_v2.py browser/Chrome management.
Scraper-rules 7.md compliant.

Features:
  - Row-by-row append (crash-safe)
  - Resume from progress JSON
  - Random 7-22s delay between searches
  - Auto CAPTCHA handling / browser restart
  - Full CLI: --input, --server-id, --server-count, --log-prefix, --limit, --fresh
"""

import csv
import time
import random
import re
import os
import sys
import json
import hashlib
import argparse
import subprocess
import urllib.parse
import shutil
import logging
from datetime import datetime

# ── SSL fix for macOS Python 3.14 (needed by undetected_chromedriver downloader) ──
import ssl as _ssl
try:
    import certifi as _certifi
    _SSL_CONTEXT = _ssl.create_default_context(cafile=_certifi.where())
    # Monkey-patch urllib so uc's internal HTTPS downloader uses certifi certs
    import urllib.request as _urllib_request
    _orig_urlopen = _urllib_request.urlopen
    def _patched_urlopen(url, data=None, timeout=_ssl._GLOBAL_DEFAULT_TIMEOUT, **kw):  # type: ignore[misc]
        if "context" not in kw:
            kw["context"] = _SSL_CONTEXT
        return _orig_urlopen(url, data, timeout, **kw)
    _urllib_request.urlopen = _patched_urlopen  # type: ignore[assignment]
    print("🔒 SSL patched with certifi CA bundle")
except Exception as _ssl_err:
    print(f"⚠️  SSL patch skipped: {_ssl_err}")

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, SessionNotCreatedException

try:
    import undetected_chromedriver as uc
    HAS_UC = True
except ImportError:
    HAS_UC = False
    print("⚠️  undetected_chromedriver not found. Install: pip install undetected-chromedriver")

try:
    from fake_useragent import UserAgent
    _ua = UserAgent()
    HAS_FAKE_UA = True
except ImportError:
    _ua = None
    HAS_FAKE_UA = False
    print("⚠️  fake_useragent not found. Install: pip install fake-useragent")

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WDM = True
except ImportError:
    ChromeDriverManager = None
    HAS_WDM = False

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

SCRAPER_NAME   = "company_enrichment"
QUEUE_DIR      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queue")
PROCESSING_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processing")
PROCESSED_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processed")
LOGS_DIR       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

# Platform-specific Chrome paths (first existing wins)
_CHROME_PATHS = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",  # macOS
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",        # Windows 64
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",  # Windows 32
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
]
CHROME_PATH = next((p for p in _CHROME_PATHS if os.path.exists(p)), None)


def _get_chrome_version():
    """Detect Chrome major version from installed binary. Returns int or None."""
    for path in _CHROME_PATHS:
        if not os.path.exists(path):
            continue
        try:
            out = subprocess.check_output([path, "--version"], text=True, timeout=5).strip()
            m = re.search(r"(\d+)\.", out)
            if m:
                return int(m.group(1))
        except Exception:
            pass
    return None


def _parse_chrome_version_from_error(exc: BaseException) -> int | None:
    """Extract Chrome major version from SessionNotCreatedException message."""
    msg = str(exc)
    m = re.search(r"Current browser version is (\d+)(?:\.\d+)*", msg, re.I)
    if m:
        return int(m.group(1))
    return None


def _clear_uc_driver_cache():
    """Delete undetected_chromedriver cache to force fresh driver download."""
    try:
        if sys.platform == "win32":
            cache_dir = os.path.join(os.environ.get("APPDATA", ""), "undetected_chromedriver")
        else:
            cache_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "undetected_chromedriver")
        if os.path.isdir(cache_dir):
            shutil.rmtree(cache_dir, ignore_errors=True)
            print(f"    🗑️ Cleared uc driver cache: {cache_dir}")
    except Exception as e:
        print(f"    ⚠️ Could not clear cache: {e}")

OUTPUT_COLUMNS = ["Company Name", "Revenue Size", "Revenue Evidence", "Industry", "Employee Size", "Region", "Company Website", "Company_name_with_duplicates"]


AI_RESPONSE_WAIT_MAX   = 25   # seconds max to wait for AI answer
AI_RESPONSE_WAIT_MIN   = 8    # initial wait before polling
SEARCH_DELAY_MIN       = 7    # seconds
SEARCH_DELAY_MAX       = 22   # seconds

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

class _LogDefaults(logging.Filter):
    """Inject scraper / server_id / row_id into every log record."""
    def __init__(self, scraper: str, server_id: int):
        super().__init__()
        self.scraper = scraper
        self.server_id = server_id

    def filter(self, record):
        record.scraper = getattr(record, "scraper", self.scraper)
        record.server_id = getattr(record, "server_id", self.server_id)
        record.row_id = getattr(record, "row_id", "-")
        return True


def setup_logging(server_id: int = 0, log_prefix: str = ""):
    os.makedirs(LOGS_DIR, exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] [%(scraper)s] [srv=%(server_id)s row=%(row_id)s] %(message)s"
    filt = _LogDefaults(SCRAPER_NAME, server_id)
    formatter = logging.Formatter(fmt)

    # 1. Project-local log file
    tag = f"{log_prefix}_" if log_prefix else ""
    log_file = os.path.join(LOGS_DIR, f"{tag}{SCRAPER_NAME}_srv{server_id}.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.addFilter(filt)

    # 2. Stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.addFilter(filt)

    handlers = [fh, sh]

    # 3. System log for SSH streaming (Windows only)
    if sys.platform == "win32":
        user_profile = os.environ.get("USERPROFILE", "")
        if user_profile:
            system_logs_dir = os.path.join(user_profile, "Documents", "Agent-Scrapy", "logs")
            os.makedirs(system_logs_dir, exist_ok=True)
            system_log_path = os.path.join(system_logs_dir, "system_log.txt")
            sys_fh = logging.FileHandler(system_log_path, encoding="utf-8")
            sys_fh.setFormatter(formatter)
            sys_fh.addFilter(filt)
            handlers.append(sys_fh)

    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
    logging.info("🚀 Logger initialized. Log file: %s", log_file)
    return log_file

# ─────────────────────────────────────────────
# FILE MANAGEMENT
# ─────────────────────────────────────────────

def get_input_file(input_path: str = ""):
    """Use --input path directly, or pick from queue/ → processing/."""
    os.makedirs(PROCESSING_DIR, exist_ok=True)
    os.makedirs(QUEUE_DIR, exist_ok=True)

    if input_path:
        if os.path.isfile(input_path):
            logging.info("📂 Using --input path: %s", input_path)
            return input_path
        logging.error("❌ --input file not found: %s", input_path)
        return None

    # 1. Check queue/ for any CSV file
    queue_csvs = sorted(f for f in os.listdir(QUEUE_DIR) if f.lower().endswith(".csv"))
    if queue_csvs:
        chosen = queue_csvs[0]
        queue_path      = os.path.join(QUEUE_DIR, chosen)
        processing_path = os.path.join(PROCESSING_DIR, chosen)
        logging.info(f"📂 Found '{chosen}' in queue. Moving to processing...")
        if os.path.exists(processing_path):
            old_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = processing_path.replace(".csv", f"_old_{old_ts}.csv")
            os.rename(processing_path, backup)
        shutil.move(queue_path, processing_path)
        logging.info(f"✅ Moved to: {processing_path}")
        return processing_path

    # 2. Check processing/ for any CSV already being worked on
    proc_csvs = sorted(f for f in os.listdir(PROCESSING_DIR) if f.lower().endswith(".csv"))
    if proc_csvs:
        processing_path = os.path.join(PROCESSING_DIR, proc_csvs[0])
        logging.info(f"📂 Found input file already in processing: {processing_path}")
        return processing_path

    logging.error("❌ No CSV files found in queue/ or processing/")
    return None

# ─────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────

class BrowserCrashError(Exception):
    pass

class CaptchaError(Exception):
    pass

# ─────────────────────────────────────────────
# PROMPT BUILDER — BASE (no Shopify)
# ─────────────────────────────────────────────

def build_prompt(company_name: str) -> str:
    """
    Company enrichment prompt: Revenue, Evidence, Industry, Employee Size,
    Region, Company Website. Indian company context, cross-verified data.
    No Shopify field.
    """
    return (
        f'Search ALL available public sources — including the company\'s official website, '
        f'LinkedIn (check "About" and "Company size" field), Crunchbase (funding & ARR), '
        f'X (Twitter), Instagram, news articles, press releases, financial reports, '
        f'MCA (Ministry of Corporate Affairs) filings, and Indian startup databases (e.g. Tracxn, VCCEdge) '
        f'— to give CURRENT and ACCURATE business information about this INDIAN company. '
        f'This is an Indian company registered in India. '
        f'Use only data from 2023 to 2026. Cross-verify all data points across multiple sources before responding.\n\n'
        f'Company: {company_name}\n\n'
        f'IMPORTANT: This is an Indian company. Search specifically for this company in India. '
        f'Do NOT confuse it with any international company that may have a similar name. '
        f'If the company is based in India, the Region MUST start with "India".\n\n'
        f'For Revenue specifically:\n'
        f'- Cross-reference at least 3 sources (e.g. official site + Crunchbase + news + MCA filings)\n'
        f'- Look for: annual revenue, ARR, total funding raised, valuation, or any financial signal\n'
        f'- Revenue Size = ONLY the numeric figure or range (e.g. $2M, $1M-$5M, Rs. 0.5 Crore, Pre-revenue)\n'
        f'- Revenue Evidence = the source/context behind the figure (e.g. "INR 49.5L for FY25", "Series A – $5M raised", "Unfunded below Rs. 0.5 Crore as reported 2024")\n'
        f'- NEVER write "Not publicly disclosed" for Revenue Size if any financial signal (funding, valuation, ARR) exists\n\n'
        f'For Company Website specifically:\n'
        f'- Find the LIVE, WORKING official website of this Indian company\n'
        f'- Must be the primary company domain (not social media profiles)\n'
        f'- Verify the URL is real and currently active\n'
        f'- If no website exists, write "Not available"\n\n'
        f'Give response in the EXACT format below only — no markdown, no extra text, no bullet points:\n\n'
        f'Company Name : {company_name}\n'
        f'Revenue Size : [numeric figure or range ONLY, e.g. $2M, $1M-$5M, Rs. 49.5L, Pre-revenue; no explanation here]\n'
        f'Revenue Evidence : [source or context for the revenue figure, e.g. INR 49.5L for FY25 / Series A ~$5M raised / Unfunded below Rs. 0.5 Crore 2024]\n'
        f'Industry : [primary industry sector, e.g. Artificial Intelligence, HealthTech, EdTech, FinTech, Cybersecurity, etc.]\n'
        f'Employee Size : [number of employees or range, e.g. 50, 200-500, 1000+; source from LinkedIn or official site]\n'
        f'Region : [headquarters state and city in India, e.g. India - Bangalore, Karnataka]\n'
        f'Company Website : [live working official website URL, e.g. https://example.com]\n\n'
        f'Rules:\n'
        f'- This is an INDIAN company — always search with India context\n'
        f'- Revenue Size must be the most recent figure available (FY2024 or FY2025 preferred, FY2023 as fallback)\n'
        f'- Revenue Evidence must cite the source, fiscal year, or funding round name\n'
        f'- Employee Size must come from LinkedIn company page or official website\n'
        f'- Company Website must be a live, working URL — not a social media link\n'
        f'- Region must include Indian city and state\n'
        f'- If the company is a government body, write "Government" for Industry, "Not applicable" for Revenue Size, and "N/A" for Employee Size\n'
        f'- Do NOT skip any field; always fill all seven lines\n'
        f'- Do NOT add any other lines or commentary'
    )

# ─────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────

def load_companies(input_file: str):
    """
    Returns (input_headers, rows) where rows is a list of
    (company_name, company_name_with_duplicates, original_row_dict) tuples.
    Skips rows where Company Name is blank.
    Supports both old format (Company_name column) and DPIIT format.
    """
    companies = []
    input_headers = []
    with open(input_file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        input_headers = list(reader.fieldnames or [])
        for row in reader:
            name = row.get("Company Name", "").strip()
            # Try both column name variants for duplicates tracking
            dup  = row.get("Company_name", "").strip() or name
            if name:
                companies.append((name, dup, dict(row)))
    return input_headers, companies


def ensure_output_file(output_file: str, fieldnames: list | None = None):
    """Create output CSV with header if it doesn't exist."""
    if not os.path.exists(output_file):
        cols = fieldnames or OUTPUT_COLUMNS
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()


def append_row(output_file: str, row_dict: dict, fieldnames: list | None = None):
    """Append a single result row to the output CSV."""
    cols = fieldnames or OUTPUT_COLUMNS
    with open(output_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        writer.writerow(row_dict)

# ─────────────────────────────────────────────
# PROGRESS TRACKER
# ─────────────────────────────────────────────

def _progress_path(input_file: str) -> str:
    """Deterministic tracking JSON: tracking_{input_stem}_{hash8}.json"""
    h = hashlib.md5(input_file.encode()).hexdigest()[:8]
    stem = os.path.splitext(os.path.basename(input_file))[0]
    return os.path.join(LOGS_DIR, f"tracking_{stem}_{h}.json")


def load_progress(input_file: str):
    path = _progress_path(input_file)
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                prog = json.load(f)
            print(f"📂 Resuming from progress file: {path}")
            print(f"   Last completed index : {prog.get('last_completed_index', -1)}")
            print(f"   Companies scraped    : {prog.get('total_processed', 0)}")
            print(f"   Output file          : {prog.get('output_file', '')}")
            return prog
        except Exception as e:
            print(f"⚠️  Could not load progress: {e}")
    return None


def save_progress(input_file: str, last_index: int, output_file: str, total: int,
                  server_id: int = 0, server_count: int = 1, status: str = "running"):
    path = _progress_path(input_file)
    prog = {
        "input_file":            input_file,
        "output_file":           output_file,
        "last_completed_index":  last_index,
        "total_processed":       total,
        "server_id":             server_id,
        "server_count":          server_count,
        "scraper_name":          SCRAPER_NAME,
        "status":                status,
        "timestamp":             datetime.now().isoformat(),
    }
    with open(path, "w") as f:
        json.dump(prog, f, indent=2)


def clear_progress(input_file: str):
    path = _progress_path(input_file)
    if os.path.exists(path):
        done_path = path.replace("tracking_", "done_")
        try:
            os.rename(path, done_path)
        except Exception:
            os.remove(path)

# ─────────────────────────────────────────────
# RESPONSE PARSER — BASE (no Shopify)
# ─────────────────────────────────────────────

def _clean(val: str) -> str:
    """Remove markdown artifacts and strip whitespace."""
    val = re.sub(r'[*`_~]', '', val)
    val = val.strip().strip('"').strip("'")
    return val


def parse_response(response_text: str, company_name: str) -> dict:
    """Extract Revenue Size, Revenue Evidence, Industry, Employee Size, Region, Company Website."""
    result = {
        "Company Name":     company_name,
        "Revenue Size":     "",
        "Revenue Evidence": "",
        "Industry":         "",
        "Employee Size":    "",
        "Region":           "",
        "Company Website":  "",
    }

    if not response_text:
        return result

    response_text = re.sub(r'```.*?```', '', response_text, flags=re.DOTALL)

    for line in response_text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue

        key_raw, _, val = line.partition(":")
        key = key_raw.strip().lower()
        val = _clean(val)

        if not val:
            continue

        if key in ("revenue size", "revenue"):
            result["Revenue Size"] = val
        elif key in ("revenue evidence", "revenue source", "evidence"):
            result["Revenue Evidence"] = val
        elif key == "industry":
            result["Industry"] = val
        elif key in ("employee size", "employees", "employee count", "team size"):
            result["Employee Size"] = val
        elif key == "region":
            result["Region"] = val
        elif key in ("company website", "website", "official website", "company url"):
            result["Company Website"] = val

    # Fallback regex
    if not result["Revenue Size"]:
        m = re.search(r'revenue\s*size\s*[:\-]\s*(.+)', response_text, re.IGNORECASE)
        if m:
            result["Revenue Size"] = _clean(m.group(1).split('\n')[0])
        else:
            m = re.search(r'^revenue\s*[:\-]\s*(.+)', response_text, re.IGNORECASE | re.MULTILINE)
            if m:
                result["Revenue Size"] = _clean(m.group(1).split('\n')[0])

    if not result["Revenue Evidence"]:
        m = re.search(r'revenue\s*evidence\s*[:\-]\s*(.+)', response_text, re.IGNORECASE)
        if m:
            result["Revenue Evidence"] = _clean(m.group(1).split('\n')[0])

    if not result["Industry"]:
        m = re.search(r'industry\s*[:\-]\s*(.+)', response_text, re.IGNORECASE)
        if m:
            result["Industry"] = _clean(m.group(1).split('\n')[0])

    if not result["Employee Size"]:
        m = re.search(r'employee\s*(?:size|count|s)?\s*[:\-]\s*(.+)', response_text, re.IGNORECASE)
        if m:
            result["Employee Size"] = _clean(m.group(1).split('\n')[0])

    if not result["Region"]:
        m = re.search(r'region\s*[:\-]\s*(.+)', response_text, re.IGNORECASE)
        if m:
            result["Region"] = _clean(m.group(1).split('\n')[0])

    if not result["Company Website"]:
        m = re.search(r'(?:company\s*)?website\s*[:\-]\s*(.+)', response_text, re.IGNORECASE)
        if m:
            result["Company Website"] = _clean(m.group(1).split('\n')[0])
        if not result["Company Website"]:
            m = re.search(r'(https?://[^\s,;"\)]+)', response_text, re.IGNORECASE)
            if m:
                url = m.group(1).rstrip('.')
                if 'google.com' not in url and 'gstatic.com' not in url:
                    result["Company Website"] = url

    return result

# ─────────────────────────────────────────────
# SCRAPER CLASS
# ─────────────────────────────────────────────

class CompanyEnrichmentScraper:
    def __init__(self):
        self.driver                = None
        self.request_count         = 0
        self.captcha_count         = 0
        self.consecutive_captchas  = 0
        self.browser_restart_count = 0
        self.current_user_agent    = None
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        os.makedirs(LOGS_DIR,      exist_ok=True)

    # ── Browser Management ─────────────────────

    def _kill_chrome(self):
        """Kill only chromedriver/uc-spawned Chrome — not user's regular Chrome."""
        try:
            if sys.platform == "darwin":
                # Only kill chromedriver processes, not the user's Chrome
                subprocess.run(["pkill", "-f", "chromedriver"], capture_output=True)
                time.sleep(1)
                subprocess.run(["pkill", "-9", "-f", "chromedriver"], capture_output=True)
            elif sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"],      capture_output=True)
                subprocess.run(["taskkill", "/F", "/IM", "chromedriver.exe"], capture_output=True)
            else:
                subprocess.run(["pkill", "-9", "-f", "chromedriver"], capture_output=True)
            print("    🔪 Chromedriver processes killed")
        except Exception as e:
            print(f"    ⚠️  Kill error: {e}")

    def _close_driver(self):
        """Gracefully quit the existing driver and set to None."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
            time.sleep(2)

    def start_browser(self) -> bool:
        try:
            # Step 1: gracefully quit old driver FIRST
            self._close_driver()
            # Step 2: kill any leftover chromedriver orphans
            self._kill_chrome()
            time.sleep(1)

            # Pick user agent
            if HAS_FAKE_UA and _ua:
                self.current_user_agent = _ua.random
            else:
                fallback_uas = [
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                ]
                self.current_user_agent = random.choice(fallback_uas)

            print(f"🌐 Starting Chrome (session #{self.browser_restart_count + 1})...")
            print(f"   UA: {self.current_user_agent[:70]}...")

            if HAS_UC:
                opts = uc.ChromeOptions()
                opts.add_argument(f"--user-agent={self.current_user_agent}")
                opts.add_argument("--disable-blink-features=AutomationControlled")
                opts.add_argument("--no-first-run")
                opts.add_argument("--no-default-browser-check")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--disable-gpu")
                w = random.choice([1366, 1440, 1536, 1400, 1280])
                h = random.choice([768, 900, 864, 800, 720])
                opts.add_argument(f"--window-size={w},{h}")

                chrome_exec = CHROME_PATH
                if chrome_exec:
                    opts.binary_location = chrome_exec
                    print(f"📍 Chrome: {chrome_exec}")

                # Detect actual Chrome version and pin version_main to avoid mismatch
                detected_version = _get_chrome_version()
                if detected_version:
                    print(f"🔍 Detected Chrome version: {detected_version}")

                uc_kwargs = dict(options=opts, use_subprocess=True)
                if chrome_exec:
                    uc_kwargs["browser_executable_path"] = chrome_exec
                if detected_version:
                    uc_kwargs["version_main"] = detected_version

                try:
                    self.driver = uc.Chrome(**uc_kwargs)
                except SessionNotCreatedException as e:
                    parsed_ver = _parse_chrome_version_from_error(e)
                    if parsed_ver:
                        print(f"  ⚠️ Version mismatch detected. Clearing uc cache and retrying with Chrome {parsed_ver}...")
                        _clear_uc_driver_cache()
                        # Kill the orphan Chrome that the failed attempt may have spawned
                        self._kill_chrome()
                        time.sleep(2)
                        opts2 = uc.ChromeOptions()
                        opts2.add_argument(f"--user-agent={self.current_user_agent}")
                        opts2.add_argument("--disable-blink-features=AutomationControlled")
                        opts2.add_argument("--no-first-run")
                        opts2.add_argument("--no-default-browser-check")
                        opts2.add_argument("--disable-dev-shm-usage")
                        opts2.add_argument("--disable-gpu")
                        opts2.add_argument(f"--window-size={w},{h}")
                        if chrome_exec:
                            opts2.binary_location = chrome_exec
                        uc_kwargs2 = dict(options=opts2, use_subprocess=True, version_main=parsed_ver)
                        if chrome_exec:
                            uc_kwargs2["browser_executable_path"] = chrome_exec
                        if HAS_WDM:
                            try:
                                wdm_path = ChromeDriverManager().install()
                                uc_kwargs2["driver_executable_path"] = wdm_path
                                del uc_kwargs2["version_main"]
                                print(f"  📥 Using webdriver-manager driver (bypasses uc cache)")
                            except Exception as wdm_err:
                                print(f"  ⚠️ webdriver-manager failed: {wdm_err}, retrying with version_main only")
                        self.driver = uc.Chrome(**uc_kwargs2)
                    else:
                        raise
            else:
                opts = Options()
                opts.add_argument("--disable-blink-features=AutomationControlled")
                opts.add_argument("--window-size=1400,900")
                self.driver = webdriver.Chrome(options=opts)

            self.driver.set_page_load_timeout(45)
            self.driver.implicitly_wait(3)
            self._apply_stealth()
            self.browser_restart_count += 1
            print(f"✅ Browser ready (session #{self.browser_restart_count})")
            return True

        except Exception as e:
            print(f"❌ Browser start failed: {e}")
            import traceback; traceback.print_exc()
            return False

    def _apply_stealth(self):
        try:
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
        except Exception:
            pass
        try:
            self.driver.execute_script("""
                try {
                  const gp = WebGLRenderingContext.prototype.getParameter;
                  WebGLRenderingContext.prototype.getParameter = function(p) {
                    if (p === 37445) return 'Google Inc.';
                    if (p === 37446) return 'ANGLE (Intel, Intel(R) UHD Graphics Direct3D11 vs_5_0 ps_5_0)';
                    return gp.apply(this, arguments);
                  };
                } catch(e) {}
            """)
        except Exception:
            pass

    # ── AI Mode Navigation ─────────────────────

    def navigate_to_ai_mode(self) -> bool:
        """Navigate to AI Mode and wait for confirmation with retries."""
        try:
            test_url = "https://www.google.com/search?udm=50&q=hello"
            self.driver.get(test_url)
            # Wait with retries for AI mode URL to settle
            for wait_attempt in range(3):
                time.sleep(4)
                current_url = self.driver.current_url.lower()
                if "/sorry" in current_url:
                    print(f"⚠️  /sorry detected during AI Mode check")
                    return False
                if "udm=50" in current_url:
                    print("✅ Google AI Mode accessible")
                    return True
                print(f"⚠️  AI Mode check attempt {wait_attempt + 1}/3: URL={current_url[:80]}")
            # After 3 attempts, proceed anyway — per-search will verify
            print("   Proceeding anyway — will verify per-search")
            return True
        except Exception as e:
            print(f"⚠️  AI Mode nav error: {e}")
            return True  # Try anyway

    # ── CAPTCHA Handling ───────────────────────

    def _detect_captcha(self, page_text: str) -> bool:
        try:
            url = self.driver.current_url.lower()
            if any(x in url for x in ["/sorry", "recaptcha", "consent.google"]):
                return True
        except Exception:
            pass
        if not page_text:
            return False
        low = page_text.lower()
        indicators = [
            "unusual traffic", "are you a robot", "captcha", "verify you're human",
            "verify you are human", "automated queries", "our systems have detected",
            "suspicious activity", "recaptcha", "i'm not a robot", "complete the security check",
            "before you continue to google",
        ]
        return any(x in low for x in indicators)

    def _handle_captcha(self):
        self.captcha_count        += 1
        self.consecutive_captchas += 1
        print(f"\n    🚨 CAPTCHA #{self.captcha_count} (consecutive: {self.consecutive_captchas})")
        print(f"    🔄 Auto-restarting browser with new fingerprint...")

        if self.consecutive_captchas >= 5:
            cooldown = random.uniform(180, 300)
        elif self.consecutive_captchas >= 3:
            cooldown = random.uniform(90, 150)
        else:
            cooldown = random.uniform(30, 60)

        print(f"    ⏳ Cooldown: {cooldown:.0f}s")
        time.sleep(cooldown)

        # Gracefully close driver first, then kill orphans
        self._close_driver()
        self._kill_chrome()
        time.sleep(3)

        for attempt in range(1, 4):
            print(f"    🔄 Restart attempt {attempt}/3...")
            if self.start_browser():
                self.navigate_to_ai_mode()
                time.sleep(random.uniform(3, 6))
                return True
            time.sleep(5)

        raise CaptchaError("Could not restart browser after CAPTCHA — giving up")

    # ── Core Search ────────────────────────────

    def search_ai_mode(self, query: str) -> str | None:
        """Navigate to AI Mode URL with encoded query and return full page text."""
        try:
            encoded = urllib.parse.quote(query)
            url = f"https://www.google.com/search?udm=50&q={encoded}"
            print(f"    🔗 Loading AI Mode URL...")
            self.driver.get(url)

            # Wait for page to settle, then check URL
            time.sleep(3)
            current_url = self.driver.current_url.lower()

            # /sorry = definite CAPTCHA → restart
            if "/sorry" in current_url or "consent.google" in current_url:
                print(f"    🚨 /sorry page detected — restarting with new fingerprint")
                self._handle_captcha()
                return self.search_ai_mode(query)

            # udm=50 missing — wait + retry before restarting (Google may redirect slowly)
            if "udm=50" not in current_url:
                print(f"    ⚠️  AI Mode URL mismatch: {current_url[:80]} — waiting for redirect...")
                for _retry in range(3):
                    time.sleep(3)
                    current_url = self.driver.current_url.lower()
                    if "udm=50" in current_url:
                        print(f"    ✅ AI Mode URL confirmed after wait")
                        break
                    if "/sorry" in current_url:
                        print(f"    🚨 /sorry detected during wait — restarting")
                        self._handle_captcha()
                        return self.search_ai_mode(query)
                else:
                    # Still no udm=50 after retries — restart browser
                    print(f"    ⚠️  AI Mode still not in URL after 3 retries — restarting browser")
                    self._handle_captcha()
                    return self.search_ai_mode(query)

            # Wait for AI response content to appear
            print(f"    ⏳ Waiting {AI_RESPONSE_WAIT_MIN}s for AI response...")
            time.sleep(AI_RESPONSE_WAIT_MIN)

            # Poll until we see meaningful content
            response_text = ""
            waited = AI_RESPONSE_WAIT_MIN
            poll_interval = 2
            content_indicators = [
                "revenue", "industry", "region", "headquarters",
                "founded", "employees", "billion", "million",
                "company", "annual", "sector", "location", "website",
            ]
            while waited < AI_RESPONSE_WAIT_MAX:
                try:
                    page_text = self.driver.find_element(By.TAG_NAME, "body").text
                    low = page_text.lower()
                    if any(ind in low for ind in content_indicators) and len(page_text) > 400:
                        response_text = page_text
                        print(f"    ✅ AI response received ({len(page_text)} chars, waited {waited}s)")
                        break
                except Exception:
                    pass
                time.sleep(poll_interval)
                waited += poll_interval

            if not response_text:
                try:
                    response_text = self.driver.find_element(By.TAG_NAME, "body").text
                    print(f"    ⚠️  Using fallback page text ({len(response_text)} chars)")
                except Exception:
                    response_text = ""

            # Blank page check
            if len(response_text.strip()) < 100:
                print(f"    ⚠️  Blank/minimal page ({len(response_text)} chars) — possible block")
                self._handle_captcha()
                return self.search_ai_mode(query)

            # CAPTCHA / sorry check on final content
            if self._detect_captcha(response_text):
                self._handle_captcha()
                return self.search_ai_mode(query)

            self.consecutive_captchas = 0  # Reset on success
            self.request_count += 1
            return response_text

        except CaptchaError:
            raise
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["no such window", "target window already closed", "web view not found"]):
                raise BrowserCrashError(f"Browser window closed: {e}")
            print(f"    ❌ Search error: {e}")
            import traceback; traceback.print_exc()
            return None

    # ── Main Run Loop ──────────────────────────

    def run(self, companies: list, input_file_path: str, output_file: str,
            start_index: int = 0, total_scrapped_so_far: int = 0,
            server_id: int = 0, server_count: int = 1,
            output_fieldnames: list | None = None):
        total = len(companies)
        scrapped = total_scrapped_so_far

        ensure_output_file(output_file, output_fieldnames)

        logging.info(f"\n{'='*60}")
        logging.info(f"  COMPANY ENRICHMENT SCRAPER — BASE")
        logging.info(f"  Total companies : {total}")
        logging.info(f"  Starting at     : index {start_index}")
        logging.info(f"  Output file     : {output_file}")
        logging.info(f"{'='*60}\n")

        # Start browser
        if not self.start_browser():
            logging.error("❌ Cannot start browser. Exiting.")
            return

        # Navigate to AI Mode
        self.navigate_to_ai_mode()
        time.sleep(random.uniform(2, 4))

        i = start_index
        while i < total:
            company_name, company_name_dup, orig_row = companies[i]
            row_id = orig_row.get("id", i + 1)
            logging.info(f"\n[{i+1}/{total}] 🏢 Company=\"{company_name}\"",
                         extra={"row_id": row_id})

            prompt = build_prompt(company_name)
            response_text = None

            # Attempt with retry on crash
            fatal = False
            for attempt in range(1, 4):
                try:
                    response_text = self.search_ai_mode(prompt)
                    break
                except BrowserCrashError as e:
                    logging.warning(f"    💥 Company=\"{company_name}\" Browser crash (attempt {attempt}/3): {e}",
                                    extra={"row_id": row_id})
                    if attempt < 3:
                        logging.info(f"    🔄 Restarting browser...", extra={"row_id": row_id})
                        self._close_driver()
                        self._kill_chrome()
                        time.sleep(5)
                        self.start_browser()
                        self.navigate_to_ai_mode()
                        time.sleep(3)
                    else:
                        logging.error(f"    ❌ Company=\"{company_name}\" 3 crashes in a row — marking failed",
                                      extra={"row_id": row_id})
                        save_progress(input_file_path, i, output_file, scrapped,
                                      server_id=server_id, server_count=server_count, status="failed")
                        fatal = True
                except CaptchaError as e:
                    logging.error(f"    ❌ Company=\"{company_name}\" CAPTCHA error: {e}",
                                  extra={"row_id": row_id})
                    response_text = None
                    break
                except Exception as e:
                    logging.error(f"    ❌ Company=\"{company_name}\" Unexpected error: {e}",
                                  extra={"row_id": row_id})
                    response_text = None
                    break

            if fatal:
                return

            # §7 rule: skip writing if enrichment returned no data (avoids blank rows)
            if response_text is None:
                logging.warning(f"    ⚠️  Company=\"{company_name}\" No data returned — skipping row (will be retried on resume)",
                                extra={"row_id": row_id})
                i += 1
                continue

            # Parse response and merge with original input row
            enrichment = parse_response(response_text, company_name)
            enrichment["Company_name_with_duplicates"] = company_name_dup
            row = {**orig_row, **enrichment}

            # Write row immediately
            append_row(output_file, row, output_fieldnames)
            scrapped += 1

            logging.info(f"    ✅ Company=\"{company_name}\" Written → Industry: {row['Industry'] or '(empty)'} | "
                         f"Revenue Size: {row['Revenue Size'] or '(empty)'} | "
                         f"Revenue Evidence: {row['Revenue Evidence'] or '(empty)'} | "
                         f"Employees: {row['Employee Size'] or '(empty)'} | "
                         f"Region: {row['Region'] or '(empty)'} | "
                         f"Website: {row['Company Website'] or '(empty)'}",
                         extra={"row_id": row_id})

            # Save progress after every row
            save_progress(input_file_path, i, output_file, scrapped,
                          server_id=server_id, server_count=server_count)

            # Move to next
            i += 1

            # Delay before next search (skip delay after last company)
            if i < total:
                delay = random.uniform(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX)
                logging.info(f"    ⏱️  Waiting {delay:.1f}s before next search...")
                time.sleep(delay)

        logging.info(f"\n{'='*60}")
        logging.info(f"✅ SCRAPING COMPLETE!")
        logging.info(f"   Companies scraped : {scrapped}")
        logging.info(f"   Output file       : {output_file}")
        logging.info(f"{'='*60}\n")

        save_progress(input_file_path, total - 1, output_file, scrapped,
                      server_id=server_id, server_count=server_count, status="done")
        clear_progress(input_file_path)

        # Quit browser
        self._close_driver()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Company Enrichment Scraper — Base (Revenue, Industry, Employees, Region, Website)"
    )
    parser.add_argument("--input", "-i", type=str, default="",
                        help="Absolute path to input CSV (skips queue/ lookup).")
    parser.add_argument("--server-id", type=int, default=0,
                        help="Logical worker ID (1..N).")
    parser.add_argument("--server-count", type=int, default=1,
                        help="Total number of workers.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only scrape first N companies (0 = all).")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore existing progress and start from scratch.")
    parser.add_argument("--log-prefix", type=str, default="",
                        help="Label prepended to log filename for filtering.")
    args = parser.parse_args()

    setup_logging(server_id=args.server_id, log_prefix=args.log_prefix)

    # Get input file (handles --input or queue -> processing move)
    input_file = get_input_file(args.input)
    if not input_file:
        sys.exit(1)

    input_headers, all_companies = load_companies(input_file)
    logging.info(f"📄 Loaded {len(all_companies)} companies from input CSV")

    # Build output fieldnames per scraper-rules 5 §3:
    #   1. id (if present)
    #   2. remaining input headers in original order
    #   3. enrichment columns
    #   4. helper columns (Company_name_with_duplicates)
    output_fieldnames = []
    if "id" in input_headers:
        output_fieldnames.append("id")
    for h in input_headers:
        if h != "id" and h not in output_fieldnames:
            output_fieldnames.append(h)
    for col in OUTPUT_COLUMNS:
        if col not in output_fieldnames:
            output_fieldnames.append(col)
    if "Company_name_with_duplicates" not in output_fieldnames:
        output_fieldnames.append("Company_name_with_duplicates")

    # Apply --limit
    if args.limit > 0:
        all_companies = all_companies[:args.limit]
        logging.info(f"⚡ Limited to first {args.limit} companies (--limit flag)")

    # Check resume
    start_index       = 0
    total_processed   = 0
    output_file       = None

    if not args.fresh:
        prog = load_progress(input_file)
        if prog:
            last_idx          = prog.get("last_completed_index", -1)
            start_index       = last_idx + 1
            total_processed   = prog.get("total_processed", 0)
            output_file       = prog.get("output_file", None)
            # If limit applied, clamp start_index
            if start_index >= len(all_companies):
                logging.info("✅ All companies already scraped per progress file!")
                return

    if not output_file:
        input_stem  = os.path.splitext(os.path.basename(input_file))[0]
        output_file = os.path.join(PROCESSED_DIR, f"{input_stem}_enriched.csv")

    # Run scraper
    scraper = CompanyEnrichmentScraper()
    scraper.run(
        companies             = all_companies,
        input_file_path       = input_file,
        output_file           = output_file,
        start_index           = start_index,
        total_scrapped_so_far = total_processed,
        server_id             = args.server_id,
        server_count          = args.server_count,
        output_fieldnames     = output_fieldnames,
    )


if __name__ == "__main__":
    main()
