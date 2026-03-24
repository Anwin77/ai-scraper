## Scraper Rules — Web Intel

These rules define the **contract** that every Python scraper must follow so the Web Intel deployment orchestrator can run them in a consistent way across many servers.

They are based on `industry_scraper_aimode_v3.py`, which is the **reference implementation**.

---

## 1. Folder layout and lifecycle

Each scraper lives in its own folder (e.g. `industry/`, `intent/`) and must use the same internal layout:

- `queue/`  
  Inbound CSVs waiting to be processed (typically per‑server chunks copied in by the deployer).

- `processing/`  
  The CSV currently being worked on. When a file is picked from `queue/`, it is moved into `processing/`.

- `processed/`  
  Append‑only output CSVs. For each input file, the scraper writes or appends to a corresponding CSV here. The output file is named `{input_stem}_enriched.csv`, where `input_stem` is the input filename without extension (e.g. `chunk_server_1_of_3` → `chunk_server_1_of_3_enriched.csv`).

- `logs/`  
  Text log files and progress JSON files for each run. The **progress JSON** lives here, inside the scraper directory (e.g. `company-enrichment-scraper/logs/`), **not** in Agent-Scrapy. The DAG polls these JSON files over SSH to track row counts and status.

**Per‑run lifecycle (per server):**

1. **Startup**
   - Create `queue/`, `processing/`, `processed/`, and `logs/` if they do not exist.
   - Determine the input:
     - If `--input` is provided: use that absolute path directly.
     - Else: look for the next CSV in `queue/`, move it to `processing/`, and use that path.
   - Look for a **progress JSON** for that input in `logs/`.
     - If found and `--fresh` is **not** set, resume from the `last_completed_index` stored there.
     - Otherwise, treat this as a fresh run.

2. **Processing**
   - Read rows from the **input CSV in `processing/`**.
   - For each row:
     - Preserve all input columns.
     - Add/update enrichment columns.
     - Append the enriched row to the corresponding output CSV in `processed/`.
     - Update the progress JSON (see Section 4).

3. **Completion**
   - When all rows for that input are processed:
     - Mark progress JSON status as `"done"`.
     - Optionally rename the progress JSON to a `done_*.json` variant for archival.

The deployment layer is free to move completed input files from `processing/` to some `done/` archive if desired, but scrapers do not depend on that.

---

## 2. Unified CLI interface

Every scraper must be a **Python CLI** script callable in the same way (usually via a `.bat` or shell wrapper).

### Required flags

- `--input`, `-i` (string, optional)
  - If provided: absolute path to the CSV chunk to process.
  - If omitted: scraper will auto‑select a CSV from its `queue/` directory as described above.

- `--server-id` (int, required in production)
  - Logical worker ID **1..N** indicating which server/worker this process represents.
  - The scraper must not try to infer IP or hostname; it just uses this value in logs and progress JSON.

- `--server-count` (int, required in production)
  - Total number of servers/workers **N** participating in this job.

### Optional flags

- `--limit` (int, default `0`)
  - If > 0, only process the first N valid rows (useful for testing).

- `--fresh` (flag)
  - Ignore any existing progress JSON for the `--input` file and start from row 0.

- `--log-prefix` (string, optional)
  - A label (e.g. `job_123_industry`) to include at the start of log messages so the backend can filter logs per job.

> **Rule:** All scrapers must use the **same argument names and semantics**, implemented with `argparse`, so the deployment code never needs scraper‑specific branching.

### Windows BAT launcher (run_scraper.bat)

On Windows, the orchestrator launches the scraper via a `.bat` file using **full flag-style arguments**, for example:

```text
run_scraper.bat --input "C:\...\chunk.csv" --server-id 5 --server-count 10 --log-prefix job_123_industry
```

**Critical:** The BAT must **pass all arguments through** to the Python script. Do **not** treat the first argument as a positional value (e.g. SERVER_ID) and then `shift` the rest.

- **Wrong:** `set SERVER_ID=%1` then `shift` then `python script --server-id %SERVER_ID% %*`  
  This breaks when the orchestrator passes `--input` first: `%1` becomes `--input`, the Python script receives invalid args, and you get errors like `argument --server-id: expected one argument`.

- **Right:** Forward everything with `%*`:
  ```bat
  "%PYTHON_EXE%" "%~dp0industry_scraper_aimode_v2.py" %*
  ```
  The Python script already accepts `--input`, `--server-id`, `--server-count`, `--log-prefix` (and optional `--limit`, `--fresh`); the BAT should be a thin pass-through.

Ensure the BAT runs from the scraper directory (`cd /d "%~dp0"`) and that `queue/`, `processing/`, `processed/`, and `logs/` exist before invoking the script. Do **not** create an `output/` directory—output CSVs go in `processed/` only.

---

## 3. CSV contract (ID column + header preservation)

### Input expectations

The deployment layer is responsible for preparing input CSVs. Every chunk passed to a scraper must:

- Contain a unique **`id`** column per row (either 1..M within that chunk, or globally unique).
- Preserve **all original input headers**, for example:
  - `id`, `Company Name`, `Some Other Field`, `Tag`, …

### Scraper behaviour

When processing each row:

- Treat the entire `DictReader` row as the **baseline**.
- Do **not drop** any existing columns – copy them through to the output row.
- Add/overwrite only **enrichment columns** specific to that scraper, e.g. for industry scrapers:
  - `Revenue Size`
  - `Revenue Evidence`
  - `Industry`
  - `Employee Size`
  - `Region`
  - `Company Website`
  - `Shopify`

### Output headers

When constructing the output CSV in `processed/`, the header must be:

1. `id` (if present in input).
2. All other **original input headers** in their original order (excluding `id` to avoid duplication).
3. Enrichment columns (in a stable, scraper‑specific order).
4. Any internal helper column such as `Company_name_with_duplicates` (if used).

**One row in, one row out:**  
For each input row, write exactly one corresponding output row, copying all input columns and appending enrichment fields.

This ensures any downstream consumer can always see both the original request and the enriched data for every `id`.

---

## 4. Progress tracking (JSON)

Each scraper must maintain a **progress JSON** per input CSV in `logs/` **inside the scraper directory** (e.g. `company-enrichment-scraper/logs/`). The file is named deterministically from the input path (for example, via a hash of the filename) and must use the prefix `tracking_`, i.e. `tracking_{input_stem}_{hash8}.json`. The DAG reads these files over SSH for row counts and status—do **not** put progress JSON in Agent-Scrapy.

### Required fields

- `input_file` (string): absolute path to the CSV chunk being processed.
- `output_file` (string): absolute path to the corresponding processed CSV.
- `last_completed_index` (int): **0‑based** index of the last successfully written row.
- `total_processed` (int): number of rows written so far.
- `server_id` (int): the `--server-id` value.
- `server_count` (int): the `--server-count` value.
- `scraper_name` (string): e.g. `"industry_aimode_v3"`.
- `status` (string): `"running" | "done" | "failed"`.
- `timestamp` (string): ISO 8601 timestamp of last update.

### Behaviour

- **On startup**
  - If `--fresh` is not set and a progress JSON exists with `status` in `{"running", "failed"}`:
    - Resume processing at `last_completed_index + 1`.
  - Otherwise start from the first row.

- **During processing**
  - After each row (or every few rows for performance), update:
    - `last_completed_index`, `total_processed`, `timestamp`, `status="running"`.

- **On completion**
  - After the final row is processed, write a final JSON snapshot with `status="done"`.

The deployment layer can read these JSON files over SSH to compute per‑server and overall progress without parsing logs.

---

## 5. Logging rules

Scrapers must log **both** to stdout and to a file in `logs/`, using Python’s `logging` library.

### Format

Use a formatter equivalent to:

```text
%(asctime)s [%(levelname)s] [%(scraper)s] [srv=%(server_id)s row=%(row_id)s] %(message)s
```

- `scraper`: fixed per script, e.g. `industry_aimode_v3`.
- `server_id`: from `--server-id` (or `0`/`-` in dev mode).
- `row_id`: the `id` column for row‑level events, otherwise `-`.

Implementation suggestion:

- Use a `logging.Filter` or `LoggerAdapter` that injects default values (`scraper`, `server_id`, `row_id`) into every record.
- For per‑row logs, pass `extra={"row_id": row_id}` to `logger.info(...)`.

In addition, all **row-related log lines** (success and error) must include the **company name** in the message text, for example:

```text
... [srv=3 row=57] Company="Acme Corp" → ✅ Written → Industry: AI | Revenue: $2M ...
... [srv=3 row=57] Company="Acme Corp" → ⚠️ CAPTCHA detected, restarting browser ...
```

### Log destinations

On Windows scraping servers, scrapers must write logs to **two locations**:

1. A per‑scraper log file inside the project (for local debugging), e.g. `logs/scraper_run_*.log`.
2. A **shared system log** file for SSH streaming. For the scraping stack this lives under the Agent-Scrapy tree:

```text
%USERPROFILE%\Documents\Agent-Scrapy\logs\system_log.txt
```

The system log must:

- Use UTF‑8 or a compatible encoding.
- Be opened in append mode so the backend can safely tail it while the scraper is writing.
- Ensure the Agent-Scrapy log path exists **before** writing:
  - Create the folder: `%USERPROFILE%\Documents\Agent-Scrapy\logs\`
  - Create the file if missing: `%USERPROFILE%\Documents\Agent-Scrapy\logs\system_log.txt`
  
  This should be done in the scraper startup (Python) and/or the launcher (`run_scraper.bat`) so SSH log streaming never fails due to missing paths.

On non‑Windows environments, only the project‑local log file is required.

#### Required implementation (Python)

To make SSH streaming work, the scraper code must **actively append** to the shared system log file above. Logging only to stdout or only to `./logs/` inside the repo will result in “no live logs” in the UI.

Minimum required pattern:

1. Create both log directories on startup:
   - Project log dir: `./logs/`
   - System log dir: `%USERPROFILE%\Documents\Agent-Scrapy\logs\`
2. Configure Python `logging` with **two file handlers**:
   - A per-scraper log file in `./logs/`
   - The shared `system_log.txt` under Agent-Scrapy
3. Ensure writes are **append mode** and encoded as UTF‑8 (flush frequently).

Reference snippet (drop-in):

```python
import logging
import os
from pathlib import Path

def setup_logging(scraper_name: str, server_id: int) -> logging.Logger:
    repo_logs_dir = Path(__file__).resolve().parent / "logs"
    repo_logs_dir.mkdir(parents=True, exist_ok=True)

    user_profile = os.environ.get("USERPROFILE", "")
    system_logs_dir = Path(user_profile) / "Documents" / "Agent-Scrapy" / "logs"
    system_logs_dir.mkdir(parents=True, exist_ok=True)
    system_log_path = system_logs_dir / "system_log.txt"
    system_log_path.touch(exist_ok=True)

    logger = logging.getLogger(scraper_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(scraper)s] [srv=%(server_id)s row=%(row_id)s] %(message)s"
    )

    class Ctx(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            record.scraper = scraper_name
            record.server_id = server_id
            if not hasattr(record, "row_id"):
                record.row_id = "-"
            return True

    logger.addFilter(Ctx())

    # Avoid duplicate handlers if setup_logging() is called twice
    logger.handlers = []

    repo_fh = logging.FileHandler(
        repo_logs_dir / f"{scraper_name}_srv{server_id}.log",
        encoding="utf-8",
    )
    repo_fh.setFormatter(fmt)
    repo_fh.setLevel(logging.INFO)

    sys_fh = logging.FileHandler(system_log_path, encoding="utf-8")
    sys_fh.setFormatter(fmt)
    sys_fh.setLevel(logging.INFO)

    logger.addHandler(repo_fh)
    logger.addHandler(sys_fh)

    return logger
```

If you already have a logger configured, the key requirement is: **add a handler that writes to** `%USERPROFILE%\Documents\Agent-Scrapy\logs\system_log.txt`.

### Required events

- Job start: input file, number of rows, server id/count.
- Per‑row summary: at least one log line when a row is successfully written (can be batched, e.g. every 10 rows).
- Browser and CAPTCHA events: when restarting, when cooldowns are applied.
- Job completion and early exit (including explicit error reason).

These logs are what the Web Intel backend will **stream over SSH** and present live in the dashboard.

---

## 6. Orchestrator expectations

From the scraper’s point of view, the orchestrator guarantees:

- Input CSVs are correctly split and enriched with an `id` column.
- Each worker is launched with:
  - `--input <abs_path_to_chunk.csv>`
  - `--server-id k`
  - `--server-count N`
  - Optional: `--log-prefix job_<job_id>_<scraper_name>`
- Progress JSON and log files are not modified externally while the scraper is running.

From the orchestrator’s point of view, scrapers guarantee:

- One output row per input row, with `id` preserved.
- Progress JSON is kept reasonably up to date.
- Logs follow the standard format and can be tailed and filtered by `server_id` and `row_id`.

---

## 7. Error handling and politeness

While details differ by scraper type (e.g. browser vs HTTP), all scrapers should follow these principles:

- **CAPTCHA / anti‑bot handling**
  - Detect CAPTCHA/`/sorry` pages and back off with increasing cooldowns.
  - Log the event with `level=WARNING` or `ERROR` and row context when applicable.

- **Browser crashes**
  - Treat "connection refused", "max retries exceeded", "target machine actively refused", and similar errors when talking to `localhost` (ChromeDriver port) as a **browser crash**. These occur when Chrome or chromedriver dies—common on some servers due to OOM, antivirus, or process exit. Restart the browser and retry the **same row** instead of continuing with a dead session.
  - On common Selenium/Chrome “session lost” errors (including the above), attempt a bounded number of restarts (e.g. up to 3).
  - After the final failure, mark `status="failed"` in the progress JSON and exit gracefully.

- **Avoid empty row appends on search failure**
  - Do **not** append a row to the output CSV when the enrichment step returned no data (e.g. `response_text is None` after a failed search). On servers where the browser connection dies repeatedly, writing in this case would append many rows with only the company name and blank enrichment columns. Instead: log a warning, skip writing, advance to the next row. Resume on the next run can retry skipped companies.

- **Delay between requests**
  - Use random jittered delays between requests (e.g. 7‑22 seconds for heavy AI Mode scraping) to reduce rate‑limit risk.

---

## 8. Reference implementation

`industry_scraper_aimode_v3.py` is the **canonical example** that implements these rules. When adding or updating other scrapers:

1. Copy the CLI argument pattern (`--input`, `--server-id`, `--server-count`, `--limit`, `--fresh`, `--log-prefix`).
2. Reuse or adapt the common helpers for:
   - Logging setup and filters.
   - Progress JSON handling.
   - CSV header building and row writing.
3. Verify that:
   - All input headers + `id` are preserved in the output.
   - Progress JSON keys match this document.
   - Log lines contain `scraper`, `srv=`, and `row=` segments.

Any new scraper should only be wired into Web Intel’s deployment orchestrator after it passes this contract.
