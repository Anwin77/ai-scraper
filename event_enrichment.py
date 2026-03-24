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


def _parse_chrome_version_from_error(exc: BaseException) -> int:
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

OUTPUT_COLUMNS = ["Organizer Name", "Contact Name", "Contact Number", "Organizer URL", "Event Name", "Target Industry/Niche", "City", "State", "Venue", "Attendees", "Exhibitors", "Events URL", "Start Date", "End Date", "Company List", "Buyer Profile/Job Titles", "Source URL"]


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

def build_prompt(event_url: str, org_name: str = "", org_url: str = "", event_name: str = "") -> str:
    """
    Event enrichment prompt: Organizer Name, Contact Name, Contact Number, Organizer URL,
    Event Name, Target Industry/Niche, City, State, Venue, Attendees, Exhibitors,
    Events URL, Start Date, End Date, Company List, Buyer Profile/Job Titles, Source URL
    """
    # Build context block from available info
    context_parts = []
    if org_name:
        context_parts.append(f'Organizer: {org_name}')
    if org_url:
        context_parts.append(f'Organizer Website: {org_url}')
    if event_name:
        context_parts.append(f'Event: {event_name}')
    if event_url:
        context_parts.append(f'Event URL: {event_url}')
    context_block = '\n'.join(context_parts)

    return (
        f'{context_block}\n\n'
        f'Use the organizer name and URL along with the event name and event URL to research thoroughly.\n\n'
        f'Organizer Name\n'
        f'Contact Name\n'
        f'Contact Number\n'
        f'Organizer URL\n'
        f'Event Name\n'
        f'Target Industry/Niche\n'
        f'City\n'
        f'State\n'
        f'Venue\n'
        f'Attendees\n'
        f'Exhibitors\n'
        f'Events URL\n'
        f'Start Date\n'
        f'End Date\n'
        f'Company List\n'
        f'Buyer Profile/Job Titles\n'
        f'Source URL\n\n'
        f'find these headers for this event in a proper format . dont hallucinate\n'
        f'give it one below the other, and i need it pointwise .'
    )

# ─────────────────────────────────────────────
# CSV HELPERS
# ─────────────────────────────────────────────

def load_events(input_file: str):
    """
    Returns (input_headers, rows) where rows is a list of
    (event_url, org_name, org_url, event_name, original_row_dict) tuples.

    Supports both formats:
      - Utah-style: Org Name, Org Url, Event Name, Event Url columns
      - Legacy:     single URL column (Root Domain, url, link, domain)
    """
    events = []
    input_headers = []
    with open(input_file, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        input_headers = list(reader.fieldnames or [])
        headers_lower = {h: h.strip().lower() for h in input_headers}

        # Detect column names (case-insensitive, flexible matching)
        col_org_name = ""
        col_org_url = ""
        col_event_name = ""
        col_event_url = ""
        col_legacy_url = ""

        for h, hl in headers_lower.items():
            if hl in ("org name", "organizer_name", "organizer name"):
                col_org_name = h
            elif hl in ("org url", "organizer_url", "organizer url"):
                col_org_url = h
            elif hl in ("event name", "event name "):
                col_event_name = h
            elif hl in ("event url", "event_url"):
                col_event_url = h

        # Legacy fallback: find any URL-like column
        if not col_event_url:
            for h, hl in headers_lower.items():
                if 'domain' in hl or 'url' in hl or 'link' in hl:
                    col_legacy_url = h
                    break
            if not col_legacy_url and input_headers:
                col_legacy_url = input_headers[0]

        for row in reader:
            row_dict = dict(row)
            org_name = row.get(col_org_name, "").strip() if col_org_name else ""
            org_url = row.get(col_org_url, "").strip() if col_org_url else ""
            event_name = row.get(col_event_name, "").strip() if col_event_name else ""
            event_url = row.get(col_event_url, "").strip() if col_event_url else ""

            # Legacy fallback
            if not event_url and col_legacy_url:
                event_url = row.get(col_legacy_url, "").strip()

            # Skip header-like duplicate rows (e.g. mid-file header repeats)
            if org_name.lower() in ("org name", "organizer_name", "organizer name"):
                continue
            if event_url.lower() in ("event url", "event_url"):
                continue

            # Skip completely empty rows
            if not event_url and not org_name and not event_name:
                continue

            # Normalise "Not available" URLs to empty
            if org_url and org_url.lower() in ("not available", "n/a", "na", ""):
                org_url = ""

            # Need at least an event URL or an event name to be useful
            if not event_url and not event_name:
                continue

            events.append((event_url, org_name, org_url, event_name, row_dict))
    return input_headers, events


def ensure_output_file(output_file: str, fieldnames=None):
    """Create output CSV with header if it doesn't exist."""
    if not os.path.exists(output_file):
        cols = fieldnames or OUTPUT_COLUMNS
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()


def append_row(output_file: str, row_dict: dict, fieldnames=None):
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


def parse_response(response_text: str, event_url: str) -> dict:
    """Extract 17 event headers from AI response."""
    result = {
        "Organizer Name": "",
        "Contact Name": "",
        "Contact Number": "",
        "Organizer URL": "",
        "Event Name": "",
        "Target Industry/Niche": "",
        "City": "",
        "State": "",
        "Venue": "",
        "Attendees": "",
        "Exhibitors": "",
        "Events URL": "",
        "Start Date": "",
        "End Date": "",
        "Company List": "",
        "Buyer Profile/Job Titles": "",
        "Source URL": ""
    }

    if not response_text:
        return result

    response_text = re.sub(r'```.*?```', '', response_text, flags=re.DOTALL)

    # Simplified extraction: split by line and match key prefixes
    # Using lowercase matching to be robust against case differences
    keys = list(result.keys())
    for line in response_text.splitlines():
        line = line.strip()
        if not line: continue
        
        # Check standard format: 'Key: Value' or 'Key - Value'
        if ':' in line or '-' in line:
            # We will grab first separator
            sep = ':' if ':' in line else '-'
            key_raw, _, val = line.partition(sep)
            k_low = key_raw.strip().lower()
            val = val.strip().strip('"').strip("'").strip('*')
            
            # Find matching key
            for k in keys:
                if k.lower() in k_low or k_low in k.lower():
                    # Only map if it's currently empty, or keep first found
                    if not result[k]:
                        result[k] = val
                    break
                    
    return result





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

    # ── Human Behavior Simulation ──────────────

    def _simulate_human_behavior(self):
        """Random mouse movements, scrolls, and hovers to look human."""
        try:
            from selenium.webdriver.common.action_chains import ActionChains
            actions = ActionChains(self.driver)

            # Random sequence of 2-4 human-like actions
            num_actions = random.randint(2, 4)
            for _ in range(num_actions):
                action_type = random.choice(["move", "scroll", "hover"])

                if action_type == "move":
                    # Move mouse to random position
                    x = random.randint(-200, 200)
                    y = random.randint(-150, 150)
                    try:
                        actions.move_by_offset(x, y).perform()
                    except Exception:
                        pass
                    time.sleep(random.uniform(0.3, 0.8))

                elif action_type == "scroll":
                    # Scroll up or down randomly
                    scroll_y = random.randint(-300, 500)
                    self.driver.execute_script(f"window.scrollBy(0, {scroll_y})")
                    time.sleep(random.uniform(0.5, 1.5))

                elif action_type == "hover":
                    # Try to hover over a random element on the page
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, "a, span, div, p, h1, h2, h3")
                        if elements:
                            target = random.choice(elements[:15])  # Pick from first 15
                            actions = ActionChains(self.driver)
                            actions.move_to_element(target).perform()
                    except Exception:
                        pass
                    time.sleep(random.uniform(0.3, 1.0))

            # Reset mouse position to avoid accumulation
            try:
                actions = ActionChains(self.driver)
                actions.move_by_offset(0, 0).perform()
            except Exception:
                pass
        except Exception:
            pass  # Non-fatal — don't break the scraper

    # ── AI Mode Navigation ─────────────────────

    def _type_url_in_address_bar(self, url: str):
        """Type a URL in Chrome's address bar physically using pyautogui."""
        import pyautogui
        
        # Move mouse to Chrome's address bar (approximate coordinates for a maximized window)
        # Adjust x, y if your screen resolution is different
        print("    ⌨️  Moving mouse to address bar...")
        pyautogui.moveTo(500, 60, duration=random.uniform(0.5, 1.0))
        pyautogui.click()
        time.sleep(random.uniform(0.2, 0.4))
        
        # Select all text (Ctrl+A) and delete it
        pyautogui.hotkey('ctrl', 'a')
        time.sleep(random.uniform(0.1, 0.3))
        pyautogui.press('backspace')
        time.sleep(random.uniform(0.2, 0.5))

        # Type the URL character by character (slower)
        print(f"    ⌨️  Typing URL visibly...")
        pyautogui.write(url, interval=random.uniform(0.05, 0.15))
        time.sleep(random.uniform(0.3, 0.6))
        
        # Press Enter
        pyautogui.press('enter')
        time.sleep(1)

    def navigate_to_ai_mode(self) -> bool:
        """Navigate to AI Mode by typing URL in address bar and waiting for confirmation."""
        try:
            print("    ⌨️  Typing Google AI Mode URL...")
            self._type_url_in_address_bar("https://www.google.com/search?udm=50&q=hello")
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
        print(f"    🔄 Closing and reopening fresh session...")

        # Close and reopen immediately — no long cooldowns
        self._close_driver()
        self._kill_chrome()
        time.sleep(5)

        for attempt in range(1, 4):
            print(f"    🔄 Restart attempt {attempt}/3...")
            if self.start_browser():
                self.navigate_to_ai_mode()
                time.sleep(random.uniform(3, 6))
                return True
            time.sleep(5)

        raise CaptchaError("Could not restart browser after CAPTCHA — giving up")

    # ── Core Search ────────────────────────────

    def search_ai_mode(self, query: str) -> str:
        """Navigate to AI Mode URL directly with encoded query."""
        try:
            encoded = urllib.parse.quote(query)
            url = f"https://www.google.com/search?udm=50&q={encoded}"
            
            # Simulate human behavior BEFORE the search
            self._simulate_human_behavior()

            # Load the URL directly (bypassing slow typing for speed, since we typed the first one)
            print(f"    🔗 Loading AI Mode URL directly...")
            self.driver.get(url)

            # Wait for page to settle, then check URL
            time.sleep(3)
            current_url = self.driver.current_url.lower()

            # /sorry = definite CAPTCHA → restart
            if "/sorry" in current_url or "consent.google" in current_url:
                print(f"    🚨 /sorry page detected — restarting with new fingerprint")
                self._handle_captcha()
                return self.search_ai_mode(query)

            # udm=50 missing — wait + retry before restarting
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
                # Random scroll while waiting to look human
                try:
                    scroll_y = random.randint(50, 300)
                    self.driver.execute_script(f"window.scrollBy(0, {scroll_y})")
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
            output_fieldnames=None):
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
            event_url, org_name, org_url, event_name, orig_row = companies[i]
            row_id = orig_row.get("id", i + 1)
            display_label = event_name or event_url or org_name
            logging.info(f"\n[{i+1}/{total}] 🏢 Event=\"{display_label}\"",
                         extra={"row_id": row_id})

            prompt = build_prompt(event_url, org_name=org_name, org_url=org_url, event_name=event_name)
            response_text = None

            # Attempt with retry on crash
            fatal = False
            for attempt in range(1, 4):
                try:
                    response_text = self.search_ai_mode(prompt)
                    break
                except BrowserCrashError as e:
                    logging.warning(f"    💥 Event=\"{display_label}\" Browser crash (attempt {attempt}/3): {e}",
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
                        logging.error(f"    ❌ Event=\"{display_label}\" 3 crashes in a row — marking failed",
                                      extra={"row_id": row_id})
                        save_progress(input_file_path, i, output_file, scrapped,
                                      server_id=server_id, server_count=server_count, status="failed")
                        fatal = True
                except CaptchaError as e:
                    logging.error(f"    ❌ Event=\"{display_label}\" CAPTCHA error: {e}",
                                  extra={"row_id": row_id})
                    response_text = None
                    break
                except Exception as e:
                    logging.error(f"    ❌ Event=\"{display_label}\" Unexpected error: {e}",
                                  extra={"row_id": row_id})
                    response_text = None
                    break

            if fatal:
                return

            # §7 rule: skip writing if enrichment returned no data (avoids blank rows)
            if response_text is None:
                logging.warning(f"    ⚠️  Event=\"{display_label}\" No data returned — skipping row (will be retried on resume)",
                                extra={"row_id": row_id})
                i += 1
                continue

            # Parse response and merge with original input row
            enrichment = parse_response(response_text, event_url)
            row = {**orig_row, **enrichment}

            # Write row immediately
            append_row(output_file, row, output_fieldnames)
            scrapped += 1

            logging.info(f"    ✅ Event=\"{display_label}\" Written → "
                         f"Organizer: {row['Organizer Name'] or '(empty)'} | "
                         f"Event Name: {row['Event Name'] or '(empty)'} | "
                         f"Venue: {row['Venue'] or '(empty)'} | "
                         f"City: {row['City'] or '(empty)'} | "
                         f"Date: {row['Start Date'] or '(empty)'}",
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

                # ── Decoy search every 7 successful scrapes ──
                if scrapped > 0 and scrapped % 7 == 0:
                    decoy_queries = [
                        "what is the tastiest fruit in the world",
                        "best hiking trails near salt lake city",
                        "how to make perfect scrambled eggs",
                        "top 10 movies of 2025",
                        "weather forecast this weekend",
                        "best coffee shops in downtown",
                        "how tall is mount everest",
                        "easy pasta recipes for dinner",
                        "best books to read in 2026",
                        "fun things to do on a rainy day",
                        "how to train for a 5k run",
                        "best national parks to visit",
                        "what is the fastest animal on earth",
                        "simple home workout routines",
                        "best budget smartphones 2026",
                        "how to grow tomatoes at home",
                        "most beautiful beaches in the world",
                        "healthy breakfast ideas quick and easy",
                        "top rated TV shows right now",
                        "how does solar energy work",
                    ]
                    decoy = random.choice(decoy_queries)
                    logging.info(f"    🎭 Decoy search #{scrapped // 7}: \"{decoy}\"")
                    try:
                        encoded = urllib.parse.quote(decoy)
                        decoy_url = f"https://www.google.com/search?udm=50&q={encoded}"
                        self.driver.get(decoy_url)
                        decoy_wait = random.uniform(6, 14)
                        logging.info(f"    🎭 Browsing decoy for {decoy_wait:.1f}s...")
                        time.sleep(decoy_wait)
                        # Check for CAPTCHA on decoy too
                        try:
                            page_text = self.driver.find_element(By.TAG_NAME, "body").text
                            if self._detect_captcha(page_text):
                                logging.warning("    🎭 CAPTCHA on decoy — handling...")
                                self._handle_captcha()
                        except Exception:
                            pass
                        # Extra cooldown after decoy to look more natural
                        post_decoy = random.uniform(3, 8)
                        time.sleep(post_decoy)
                    except Exception as e:
                        logging.warning(f"    🎭 Decoy search failed (non-fatal): {e}")

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

    input_headers, all_events = load_events(input_file)
    logging.info(f"📄 Loaded {len(all_events)} companies from input CSV")

    # Build output fieldnames per scraper-rules 5 §3:
    #   1. id (if present)
    #   2. remaining input headers in original order
    #   3. enrichment columns
    output_fieldnames = []
    if "id" in input_headers:
        output_fieldnames.append("id")
    for h in input_headers:
        if h != "id" and h not in output_fieldnames:
            output_fieldnames.append(h)
    for col in OUTPUT_COLUMNS:
        if col not in output_fieldnames:
            output_fieldnames.append(col)

    # Apply --limit
    if args.limit > 0:
        all_events = all_events[:args.limit]
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
            if start_index >= len(all_events):
                logging.info("✅ All companies already scraped per progress file!")
                return

    if not output_file:
        input_stem  = os.path.splitext(os.path.basename(input_file))[0]
        output_file = os.path.join(PROCESSED_DIR, f"{input_stem}_enriched.csv")

    # Run scraper
    scraper = CompanyEnrichmentScraper()
    scraper.run(
        companies             = all_events,
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
