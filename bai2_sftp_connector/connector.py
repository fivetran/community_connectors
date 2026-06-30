"""
BAI2 SFTP Connector for Fivetran.

Fetches BAI2-format cash management files from an SFTP server and loads
all transactions into a single `bai2_transactions` table.

BAI2 record hierarchy:
  01 - File Header
  02 - Group Header
  03 - Account Identifier & Summary (balance type codes)
  16 - Transaction Detail
  88 - Continuation (merged into parent record)
  49 - Account Trailer     (skipped)
  98 - Group Trailer       (skipped)
  99 - File Trailer        (skipped)

Key behaviours:
  - Incremental: processed filenames tracked in state, never reprocessed
  - Record 88 continuations collected and merged into their parent record
    before parsing — 03 continuations extend the balance group field list;
    16 continuations extend the narrative/description text
  - Balance summary data (Record 03) is denormalized onto every transaction
    row as balance_{type_code} columns (e.g. balance_010, balance_015 …)
  - Structured ACH fields from 88 narrative records are parsed into dedicated
    columns (ach_cust_id, ach_comp_name, ach_sec_code, etc.)
  - Malformed rows are skipped with a warning; file processing continues
  - btid: synthetic auto-incrementing primary key, persisted across syncs
  - SFTP: password authentication with 5-attempt retry logic

See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference
"""

import json
import random
import re
import time
from datetime import datetime

import paramiko

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# ── Constants ──────────────────────────────────────────────────────────────────

_SFTP_MAX_RETRIES = 5
_SFTP_RETRY_DELAY_SEC = 5

_REQUIRED_CONFIG_KEYS = [
    "sftp_host",
    "sftp_username",
    "sftp_password",
    "sftp_remote_path",
]

# BAI2 type code ranges for debit/credit classification
_CREDIT_RANGE = (100, 399)
_DEBIT_RANGE = (400, 699)

# Compiled regex patterns for parsing structured ACH fields from 88 narrative text
_ACH_PATTERNS = {
    "ach_cust_id": re.compile(
        r"Cust\s+ID:\s*(\S+)", re.IGNORECASE
    ),
    "ach_description": re.compile(
        r"Desc:\s*(.+?)(?=Comp\s+Name:|Comp\s+ID:|SEC:|Cust\s+Name:|Date:|\Z)",
        re.IGNORECASE | re.DOTALL,
    ),
    "ach_comp_name": re.compile(
        r"Comp\s+Name:\s*(.+?)(?=Comp\s+ID:|SEC:|Cust\s+Name:|Date:|\Z)",
        re.IGNORECASE | re.DOTALL,
    ),
    "ach_comp_id": re.compile(r"Comp\s+ID:\s*(\S+)", re.IGNORECASE),
    "ach_batch_discr": re.compile(
        r"Batch\s+Discr:\s*(.+?)(?=SEC:|Cust\s+Name:|Date:|\Z)",
        re.IGNORECASE | re.DOTALL,
    ),
    "ach_sec_code": re.compile(r"SEC:\s*(\S+)", re.IGNORECASE),
    "ach_cust_name": re.compile(
        r"Cust\s+Name:\s*(.+?)(?=Date:|\Z)", re.IGNORECASE | re.DOTALL
    ),
    "ach_tran_date": re.compile(r"Date:\s*(\d{2}-\d{2}-\d{2})", re.IGNORECASE),
    "ach_tran_time": re.compile(
        r"Time:\s*(\d{2}:\d{2}\s*(?:AM|PM))", re.IGNORECASE
    ),
    "ach_addenda": re.compile(r"Addenda:\s*(.+?)$", re.IGNORECASE | re.MULTILINE),
}


# ── Configuration validation ───────────────────────────────────────────────────


def validate_configuration(configuration: dict):
    """Raise ValueError if any required configuration key is absent or invalid."""
    missing = [k for k in _REQUIRED_CONFIG_KEYS if k not in configuration]
    if missing:
        raise ValueError(
            f"Missing required configuration key(s): {', '.join(missing)}"
        )
    port_str = configuration.get("sftp_port", "22")
    try:
        port = int(port_str)
        if not (1 <= port <= 65535):
            raise ValueError(f"sftp_port must be between 1 and 65535, got {port}")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid sftp_port configuration: {exc}")
    if not configuration.get("sftp_password", "").strip():
        raise ValueError("sftp_password cannot be empty")
    if not configuration.get("sftp_remote_path", "").strip():
        raise ValueError("sftp_remote_path cannot be empty")
    test_mode = configuration.get("test_mode", "false").strip().lower()
    if test_mode not in ("true", "false"):
        raise ValueError("test_mode must be 'true' or 'false'")
    file_pattern = configuration.get("sftp_file_pattern", "")
    if file_pattern:
        try:
            re.compile(file_pattern)
        except re.error as exc:
            raise ValueError(f"Invalid sftp_file_pattern regex: {exc}")


# ── Parsing helpers ────────────────────────────────────────────────────────────


def parse_bai2_date(yymmdd: str):
    """Convert YYMMDD string to ISO YYYY-MM-DD. Returns None on invalid input."""
    if not yymmdd or len(yymmdd) != 6:
        return None
    try:
        return datetime.strptime(yymmdd, "%y%m%d").date().isoformat()
    except ValueError:
        return None


def parse_amount(raw: str):
    """
    Convert a BAI2 amount string to float (implied 2 decimal places).
      ''      → None  (not reported)
      '000'   → 0.0   (genuine zero)
      '+20921242' → 209212.42
      '-5000'     → -50.0
    """
    if raw is None:
        return None
    raw = raw.strip()
    if raw == "":
        return None
    try:
        return float(raw.lstrip("+")) / 100.0
    except ValueError:
        return None


def debit_credit_mark(type_code: str):
    """Return 'C', 'D', or None based on the BAI2 type code range."""
    try:
        code = int(type_code)
        if _CREDIT_RANGE[0] <= code <= _CREDIT_RANGE[1]:
            return "C"
        if _DEBIT_RANGE[0] <= code <= _DEBIT_RANGE[1]:
            return "D"
    except (ValueError, TypeError):
        pass
    return None


def parse_ach_fields(text: str):
    """
    Mine structured ACH fields from merged 88-record narrative text.
    Returns a dict of ach_* keys; value is None when a field is absent.
    """
    result = {}
    for field, pattern in _ACH_PATTERNS.items():
        m = pattern.search(text)
        result[field] = m.group(1).strip() if m else None
    return result


def parse_balance_groups(fields_str: str):
    """
    Parse repeating (type_code, amount, item_count, funds_type) groups
    from a comma-delimited string.

    Each segment is parsed independently — callers must NOT concatenate
    03 content with 88 content before calling this function, because
    trailing-comma padding in the 03 record creates boundary ambiguity.

    Returns {type_code: amount_float} for every complete group found.
    """
    fields_str = fields_str.strip().rstrip("/").rstrip(",")
    if not fields_str:
        return {}
    fields = [f.strip() for f in fields_str.split(",")]
    result = {}
    i = 0
    while i + 1 < len(fields):
        tc = fields[i].strip()
        amt = fields[i + 1].strip() if i + 1 < len(fields) else ""
        # Only accept valid 3-digit numeric type codes
        if tc and tc.isdigit() and len(tc) == 3:
            result[tc] = parse_amount(amt)
        i += 4  # each group occupies: type_code, amount, item_count, funds_type
    if i < len(fields):
        log.fine(f"Balance group parsing: {len(fields) - i} trailing field(s) ignored (incomplete group)")
    return result


def split_record_line(line: str):
    """
    Split a (potentially fixed-width padded) BAI2 line into
    (record_code, content_str).

    Strips trailing whitespace padding and the record terminator '/'.
    """
    line = line.rstrip()
    comma_idx = line.find(",")
    if comma_idx == -1:
        return line.rstrip("/").strip(), ""
    code = line[:comma_idx].strip()
    content = line[comma_idx + 1:].rstrip()
    if content.endswith("/"):
        content = content[:-1].rstrip()
    return code, content


# ── Record parsers ─────────────────────────────────────────────────────────────


def parse_record_01(content: str):
    """Parse File Header (01). Returns file_context dict."""
    f = [x.strip() for x in content.split(",")]
    return {
        "sender_id": f[0] if len(f) > 0 else None,
        "receiver_id": f[1] if len(f) > 1 else None,
        "file_date": parse_bai2_date(f[2]) if len(f) > 2 else None,
        "file_creation_time": f[3] if len(f) > 3 else None,
    }


def parse_record_02(content: str):
    """Parse Group Header (02). Returns group_context dict."""
    f = [x.strip() for x in content.split(",")]
    return {
        "originator_id": f[1] if len(f) > 1 else None,
        "as_of_date": parse_bai2_date(f[3]) if len(f) > 3 else None,
        "as_of_time": f[4] if len(f) > 4 else None,
        "currency": f[5] if len(f) > 5 else None,
    }


def parse_record_03(content: str, cont_contents: list, group_ctx: dict):
    """
    Parse Account Identifier & Summary (03) plus its 88 continuations.

    The 03 record provides: account_number, currency, and the first batch
    of balance type_code groups. Each 88 continuation provides more groups.

    Segments are parsed independently via parse_balance_groups to avoid
    the trailing-comma concatenation boundary issue inherent in BAI2.
    """
    f = [x.strip() for x in content.split(",")]
    account_number = f[0] if len(f) > 0 else None
    currency = (f[1] if len(f) > 1 and f[1] else None) or group_ctx.get("currency")

    # Parse balance groups from the 03 record itself (fields after account + currency)
    balances = parse_balance_groups(",".join(f[2:]))

    # Parse balance groups from each 88 continuation independently
    for cont in cont_contents:
        balances.update(parse_balance_groups(cont))

    ctx = {"bank_account_number": account_number, "currency": currency}
    for code, amount in balances.items():
        ctx[f"balance_{code}"] = amount
    return ctx


def parse_record_16(
    content: str,
    cont_texts: list,
    file_ctx: dict,
    group_ctx: dict,
    account_ctx: dict,
    btid: int,
):
    """
    Parse Transaction Detail (16) plus its 88 text continuations.

    Structured fields (type_code through customer_reference) are parsed
    positionally from the 16 record. The 88 lines provide the narrative
    description text, which is then mined for structured ACH fields.
    """
    f = [x.strip() for x in content.split(",")]
    idx = 0

    type_code = f[idx] if idx < len(f) else None
    idx += 1
    amount_raw = f[idx] if idx < len(f) else None
    idx += 1
    funds_type = f[idx] if idx < len(f) else None
    idx += 1

    same_day = one_day = two_plus = None
    if funds_type == "S":
        same_day = parse_amount(f[idx]) if idx < len(f) else None
        idx += 1
        one_day = parse_amount(f[idx]) if idx < len(f) else None
        idx += 1
        two_plus = parse_amount(f[idx]) if idx < len(f) else None
        idx += 1
    elif funds_type in ("D", "V"):
        idx += 2  # skip date and amount fields

    bank_ref = (f[idx].strip() if idx < len(f) else "") or None
    idx += 1
    cust_ref = (f[idx].strip() if idx < len(f) else "") or None
    idx += 1

    # Everything after customer_reference on the 16 line is the start of description
    inline_desc = ",".join(f[idx:]).strip() if idx < len(f) else ""

    # Merge 88 text continuations into full description
    parts = [p for p in ([inline_desc] + cont_texts) if p.strip()]
    description = " ".join(parts).strip() or None

    ach = parse_ach_fields(description or "")

    amount = parse_amount(amount_raw)

    row = {
        "btid": btid,
        # File context (Record 01)
        "file_reference": file_ctx.get("file_reference"),
        "file_date": file_ctx.get("file_date"),
        "file_creation_time": file_ctx.get("file_creation_time"),
        "sender_id": file_ctx.get("sender_id"),
        "receiver_id": file_ctx.get("receiver_id"),
        # Group context (Record 02)
        "as_of_date": group_ctx.get("as_of_date"),
        "as_of_time": group_ctx.get("as_of_time"),
        "originator_id": group_ctx.get("originator_id"),
        # Account context (Record 03)
        "bank_account_number": account_ctx.get("bank_account_number"),
        "currency": account_ctx.get("currency") or group_ctx.get("currency"),
        # Transaction fields (Record 16)
        "type_code": type_code,
        "amount": amount,
        "debit_credit_mark": debit_credit_mark(type_code),
        "funds_type": funds_type,
        "same_day_amount": same_day,
        "one_day_amount": one_day,
        "two_plus_day_amount": two_plus,
        "bank_reference": bank_ref,
        "customer_reference": cust_ref,
        "description": description,
        # Structured ACH fields parsed from 88 narrative
        "ach_cust_id": ach.get("ach_cust_id"),
        "ach_description": ach.get("ach_description"),
        "ach_comp_name": ach.get("ach_comp_name"),
        "ach_comp_id": ach.get("ach_comp_id"),
        "ach_batch_discr": ach.get("ach_batch_discr"),
        "ach_sec_code": ach.get("ach_sec_code"),
        "ach_cust_name": ach.get("ach_cust_name"),
        "ach_tran_date": ach.get("ach_tran_date"),
        "ach_tran_time": ach.get("ach_tran_time"),
        "ach_addenda": ach.get("ach_addenda"),
    }

    # Denormalize all balance columns from the account context onto this row.
    # Columns appear dynamically (balance_010, balance_015, …) based on what
    # the bank reports; the SDK auto-detects new columns on first appearance.
    for key, val in account_ctx.items():
        if key.startswith("balance_"):
            row[key] = val

    return row


# ── File processor ─────────────────────────────────────────────────────────────


def process_bai2_file(content: str, filename: str, next_btid: int):
    """
    Parse a complete BAI2 file content string into transaction rows.

    Iterates line-by-line. For each non-88 record, collects all immediately
    following 88 continuation lines before parsing. Context propagates
    downward: file_ctx → group_ctx → account_ctx → transaction row.

    Malformed individual records are skipped with a warning; the rest of
    the file continues processing.

    Returns (rows, next_btid).
    """
    lines = content.splitlines()
    rows = []
    skipped_count = 0
    file_ctx = {"file_reference": filename}
    group_ctx = {}
    account_ctx = {}

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.strip():
            i += 1
            continue

        code, content_str = split_record_line(line)

        # Collect all immediately following 88 continuation lines
        cont_lines = []
        j = i + 1
        while j < len(lines):
            peek = lines[j].rstrip()
            if not peek.strip():
                j += 1
                continue
            peek_code, peek_content = split_record_line(peek)
            if peek_code != "88":
                break
            cont_lines.append(peek_content)
            j += 1

        try:
            if code == "01":
                file_ctx.update(parse_record_01(content_str))

            elif code == "02":
                group_ctx = parse_record_02(content_str)

            elif code == "03":
                account_ctx = parse_record_03(content_str, cont_lines, group_ctx)

            elif code == "16":
                row = parse_record_16(
                    content_str,
                    cont_lines,
                    file_ctx,
                    group_ctx,
                    account_ctx,
                    next_btid,
                )
                rows.append(row)
                next_btid += 1

            # Records 49, 98, 99 (trailers) carry control totals only — skip

        except Exception as exc:
            log.warning(f"Skipping record {code} in {filename}: {exc}")
            skipped_count += 1

        i = j  # advance past this record and all consumed 88 lines

    log.info(f"{filename}: {len(rows)} transaction(s) parsed, {skipped_count} record(s) skipped")
    return rows, next_btid


# ── SFTP connection ────────────────────────────────────────────────────────────


def connect_sftp(configuration: dict):
    """
    Open an SFTP connection using password authentication.

    Returns (SSHClient, SFTPClient). The caller is responsible for closing
    both objects to fully release the SSH transport.

    Retries up to _SFTP_MAX_RETRIES times with _SFTP_RETRY_DELAY_SEC delay.
    Raises RuntimeError after all attempts are exhausted.
    """
    host = configuration["sftp_host"]
    port = int(configuration.get("sftp_port", "22"))
    username = configuration["sftp_username"]
    password = configuration["sftp_password"]

    last_exc = None
    for attempt in range(1, _SFTP_MAX_RETRIES + 1):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            ssh.connect(
                host, port=port, username=username, password=password,
                timeout=30, banner_timeout=30, auth_timeout=30,
            )
            sftp = ssh.open_sftp()
            sftp.get_channel().settimeout(60)
            log.info(f"SFTP connected to {host}:{port} (attempt {attempt})")
            return ssh, sftp

        except Exception as exc:
            last_exc = exc
            log.warning(
                f"SFTP connection attempt {attempt}/{_SFTP_MAX_RETRIES} failed: {exc}"
            )
            if attempt < _SFTP_MAX_RETRIES:
                base_delay = _SFTP_RETRY_DELAY_SEC * (2 ** (attempt - 1))
                delay = min(base_delay + random.uniform(0, base_delay * 0.3), 60)
                log.info(f"Retrying in {delay:.1f}s...")
                time.sleep(delay)

    raise RuntimeError(
        f"Failed to connect to SFTP at {host}:{port} "
        f"after {_SFTP_MAX_RETRIES} attempts: {last_exc}"
    )


# ── SDK entry points ───────────────────────────────────────────────────────────


def schema(configuration: dict):
    """
    Declare the bai2_transactions table.

    Only the table name and primary key are declared here; the SDK
    auto-detects all column types from the first upserted records.
    Balance columns (balance_010, balance_015, …) appear dynamically
    based on the type codes present in each bank's BAI2 files.
    """
    validate_configuration(configuration)
    return [
        {
            "table": "bai2_transactions",
            "primary_key": ["btid"],
            "columns": {
                "amount": "DOUBLE",
                "same_day_amount": "DOUBLE",
                "one_day_amount": "DOUBLE",
                "two_plus_day_amount": "DOUBLE",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Main sync loop.

    1. Lists the SFTP remote directory.
    2. Filters to files not yet processed (tracked in state) that match
       the optional sftp_file_pattern regex.
    3. Sorts new files alphabetically — BAI2 filenames embed timestamps
       (e.g. web.galls.prev.out.20260102.144521) so alpha order is
       chronological order.
    4. Parses each file into rows and upserts them.
    5. Checkpoints after each file so a mid-run failure does not reprocess
       already-completed files.

    When test_mode = "true" in configuration, the sync is capped to 3 files
    per run. SFTP connection is still made normally; use this for initial
    setup validation against the live server.
    """
    validate_configuration(configuration)
    test_mode = configuration.get("test_mode", "false").strip().lower() == "true"
    if test_mode:
        log.info("bai2_sftp_connector: TEST MODE ENABLED — processing up to 3 files only")
    log.info("bai2_sftp_connector: starting sync")

    processed = json.loads(state.get("processed_files", "[]"))
    next_btid = int(state.get("next_btid", "1"))

    remote_path = configuration["sftp_remote_path"].rstrip("/")
    file_pattern = configuration.get("sftp_file_pattern", "")

    ssh_client = None
    sftp = None
    try:
        ssh_client, sftp = connect_sftp(configuration)
        all_files = sftp.listdir(remote_path)

        new_files = sorted(
            fname
            for fname in all_files
            if fname not in processed
            and (not file_pattern or re.search(file_pattern, fname))
        )

        if not new_files:
            log.info("bai2_sftp_connector: no new files to process")
            return

        if test_mode:
            new_files = new_files[:3]
            log.info(f"bai2_sftp_connector: test mode — capped to {len(new_files)} file(s)")
        else:
            log.info(f"bai2_sftp_connector: {len(new_files)} new file(s) to process")

        for fname in new_files:
            remote_filepath = f"{remote_path}/{fname}"
            log.info(f"Processing: {fname}")
            try:
                with sftp.open(remote_filepath, "r") as fh:
                    file_content = fh.read().decode("utf-8", errors="replace")

                rows, next_btid = process_bai2_file(file_content, fname, next_btid)
                log.info(f"  → {len(rows)} transaction(s) parsed")

                for row in rows:
                    op.upsert(table="bai2_transactions", data=row)

                processed.append(fname)
                state["processed_files"] = json.dumps(processed)
                state["next_btid"] = str(next_btid)
                op.checkpoint(state=state)
                log.info(f"  → checkpoint saved")

            except Exception as exc:
                log.warning(f"Failed to process file {fname}: {exc} — skipping")
                failed_files = json.loads(state.get("failed_files", "[]"))
                failed_files.append({
                    "filename": fname,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:200],
                    "timestamp": datetime.utcnow().isoformat(),
                })
                state["failed_files"] = json.dumps(failed_files[-100:])
                op.checkpoint(state=state)
                continue

    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
        if ssh_client is not None:
            try:
                ssh_client.close()
            except Exception:
                pass

    log.info("bai2_sftp_connector: sync complete")


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
