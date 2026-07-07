"""
Fixed-Width SFTP Connector for Fivetran.

Syncs 12 fixed-width files delivered daily via SFTP, organised into subdirectories by provider:
  - sftp_remote_path/ELAN/  — 1 ELAN file  (Fiserv credit-card data, table: fiserv_elan_cpf1582)
  - sftp_remote_path/CUP/   — 1 CUP file   (mortgage data,           table: cup9078501)
  - sftp_remote_path/DFM/   — 10 LPL/DFM files (brokerage/investment data, tables: lpl_*)

Key behaviors
  - Missing file     → abort entire sync with RuntimeError
  - Malformed line   → log.severe + skip that file, continue remaining files
  - SFTP disconnect  → retry up to 5 times with exponential backoff (5, 10, 20, 40 s)
  - Full-refresh tables (10 of 12) include a purge_indicator BOOLEAN column
    for soft-delete: True = row was deleted in source, False = active row
  - Incremental tables (CommissionTransaction, TransactionActivity) append only
  - ELAN implied decimal: integer value / 10^decimal_places; sign applied from companion field
  - CUP implied decimal: integer value / 10^decimal_places; dates MMDDYYYY → ISO conversion
  - DFM decimal: explicit decimal point in file
  - No PII masking in connector; recommend Snowflake Dynamic Data Masking

Optional configuration:
  - sftp_port       — defaults to 22 if omitted
  - sftp_host_key   — base64-encoded server host key for strict host verification;
                       when omitted the connector warns and accepts any host key
  - test_mode       — "true" to validate all 12 files end-to-end but upsert only the
                       first 100 records per file; state and checkpoints are not saved,
                       so the next normal sync re-processes everything cleanly

Authentication: password (sftp_password)

See the Technical Reference documentation:
https://fivetran.com/docs/connectors/connector-sdk/technical-reference
"""

import json
import os
import tempfile
import time
from datetime import datetime

import paramiko

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

from table_specs import FILE_SPECS

# ── Constants ─────────────────────────────────────────────────────────────────

_SFTP_MAX_RETRIES = 5
_SFTP_RETRY_DELAY_SEC = 5
_SFTP_OPERATION_TIMEOUT_SEC = 300  # timeout for individual SFTP file/listing operations

_REQUIRED_CONFIG_KEYS = [
    "sftp_host",
    "sftp_username",
    "sftp_password",
    "sftp_remote_path",
]

# Composite PK separator used when joining multiple PK values into a single state key
_PK_SEP = "|"

# Maximum records upserted per file when test_mode is enabled
_TEST_MODE_RECORD_LIMIT = 100


# ── Configuration validation ──────────────────────────────────────────────────


def validate_configuration(configuration: dict):
    """Raise ValueError if any required configuration key is absent or invalid."""
    missing = [k for k in _REQUIRED_CONFIG_KEYS if k not in configuration]
    if missing:
        raise ValueError(f"Missing required configuration key(s): {', '.join(missing)}")

    if not configuration.get("sftp_host", "").strip():
        raise ValueError("sftp_host cannot be empty")
    if not configuration.get("sftp_username", "").strip():
        raise ValueError("sftp_username cannot be empty")
    remote_path = configuration.get("sftp_remote_path", "").strip()
    if not remote_path:
        raise ValueError("sftp_remote_path cannot be empty")
    if any(ch in remote_path for ch in ("*", "?", "[", "]")):
        raise ValueError("sftp_remote_path cannot contain wildcard characters (*, ?, [, ])")

    if "sftp_port" in configuration:
        try:
            port = int(configuration["sftp_port"])
            if not 1 <= port <= 65535:
                raise ValueError("sftp_port must be between 1 and 65535")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"sftp_port must be a valid integer: {exc}")

    if not configuration.get("sftp_password", "").strip():
        raise ValueError("sftp_password cannot be empty")

    if configuration.get("sftp_host_key", ""):
        try:
            import base64

            key_bytes = base64.b64decode(configuration["sftp_host_key"])
            paramiko.RSAKey(data=key_bytes)
        except Exception as exc:
            raise ValueError(f"Invalid sftp_host_key: {exc}")

    if "test_mode" in configuration:
        if configuration["test_mode"].strip().lower() not in ("true", "false"):
            raise ValueError("test_mode must be 'true' or 'false'")


# ── Schema ────────────────────────────────────────────────────────────────────


def schema(configuration: dict):
    """
    Return the Fivetran schema for all 12 tables.

    Each entry contains only the table name and primary_key list.
    The SDK auto-detects column data types from the upserted records.
    Tables with no primary keys (TransactionActivity) receive a
    Fivetran-generated _fivetran_id.

    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    """
    validate_configuration(configuration)

    schema_list = []
    for spec in FILE_SPECS:
        entry = {"table": spec["table"]}
        if spec["primary_keys"]:
            entry["primary_key"] = spec["primary_keys"]
        schema_list.append(entry)

    return schema_list


# ── Update ────────────────────────────────────────────────────────────────────


def update(configuration: dict, state: dict):
    """
    Main sync loop.

    For each of the 12 file specs:
      1. Locate the most-recent file on SFTP that matches the file pattern.
      2. Download to a local temp file.
      3. Validate header/trailer counts.
      4. Parse all fixed-width records.
      5. For full-refresh tables: compute soft-deletes (purge_indicator).
      6. Upsert all records.
      7. Checkpoint after each file.

    If any file is missing → entire sync aborts (RuntimeError bubbles up).
    If a line is malformed → that file is skipped, remaining files continue.

    See: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    """
    validate_configuration(configuration)

    test_mode = configuration.get("test_mode", "").strip().lower() == "true"
    if test_mode:
        log.warning(
            f"TEST MODE ENABLED: all 12 files will be validated and parsed, "
            f"but only the first {_TEST_MODE_RECORD_LIMIT} records per file will be "
            f"upserted. State and checkpoints will NOT be saved — the next normal "
            f"sync will re-process all files from scratch."
        )
    log.info("fixed_width_sftp_connector: starting sync")

    state.setdefault("processed", {})
    state.setdefault("pks", {})

    ssh_client = None
    sftp = None
    try:
        ssh_client, sftp = connect_sftp(configuration)
        remote_path = configuration["sftp_remote_path"].rstrip("/")

        # One SFTP directory listing per unique subdirectory (ELAN, CUP, DFM)
        subdir_listings = {}
        for spec in FILE_SPECS:
            subdir = spec["subdirectory"]
            if subdir not in subdir_listings:
                subdir_path = f"{remote_path}/{subdir}"
                subdir_listings[subdir] = sftp.listdir_attr(subdir_path)
                log.info(
                    f"SFTP subdirectory '{subdir}' contains "
                    f"{len(subdir_listings[subdir])} file(s)"
                )

        for spec in FILE_SPECS:
            pattern = spec["file_pattern"]
            table = spec["table"]
            subdir = spec["subdirectory"]

            # ── Locate most-recent file matching this pattern ──────────────
            filename = find_latest_file(subdir_listings[subdir], pattern, state, test_mode)
            remote_filepath = f"{remote_path}/{subdir}/{filename}"
            log.info(f"[{table}] Processing file: {filename}")

            # ── Download to temp file ──────────────────────────────────────
            # tmp_path initialised to None so the finally block is safe even
            # if NamedTemporaryFile() or tmp.close() raises before assignment.
            tmp_path = None
            lines = None
            file_skipped = False
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".fw.tmp")
                tmp_path = tmp.name
                tmp.close()

                sftp.get(remote_filepath, tmp_path)

                # ── Validate and read lines ────────────────────────────────
                lines = read_and_validate_file(tmp_path, spec, filename)

                # ── Stream parse → upsert, track purge PKs in one pass ────
                result = stream_and_upsert_records(lines, spec, state, test_mode)
                if result is None:
                    # Malformed line detected — skip this file, continue others
                    log.warning(
                        f"[{table}] File skipped due to malformed line(s); "
                        f"remaining files will still be processed."
                    )
                    file_skipped = True
                else:
                    record_count, current_pks = result
                    log.info(f"[{table}] Processed {record_count} record(s) from {filename}")

                    # ── Update state (skipped in test mode) ───────────────
                    if not test_mode:
                        state["processed"][pattern] = filename
                        if spec["purge"] and spec["primary_keys"]:
                            state["pks"][table] = current_pks

            finally:
                # Close the generator before deleting the temp file.
                # On Windows an open file handle prevents os.unlink(); closing
                # the generator triggers GeneratorExit which closes the
                # underlying file object inside the generator's with-block.
                if lines is not None:
                    try:
                        lines.close()
                    except Exception:
                        pass
                if tmp_path and os.path.exists(tmp_path):
                    os.unlink(tmp_path)

            # ── Checkpoint after each file (skipped in test mode) ─────────
            if test_mode:
                log.info(f"[{table}] Test mode: checkpoint skipped")
            else:
                # Checkpointing on skip persists state from files already processed
                # earlier in this sync run, so a crash between files doesn't force
                # re-processing of all previously-completed files.
                op.checkpoint(state)
                if file_skipped:
                    log.info(f"[{table}] Checkpoint saved (file skipped due to malformed lines)")
                    continue
                log.info(f"[{table}] Checkpoint saved after processing {filename}")

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

    if test_mode:
        log.warning(
            "fixed_width_sftp_connector: TEST MODE complete — "
            "state was not modified; run again without test_mode for full production sync"
        )
    else:
        log.info("fixed_width_sftp_connector: sync complete")


# ── SFTP connection ───────────────────────────────────────────────────────────


def connect_sftp(configuration: dict):
    """
    Open an SFTP connection using password authentication.

    Returns a (SSHClient, SFTPClient) tuple — the caller is responsible
    for closing both in order to fully release the SSH transport.

    When sftp_host_key is present in configuration it is used for strict
    host-key verification (recommended for production).  When absent the
    connector falls back to AutoAddPolicy and emits a warning.

    Retries up to _SFTP_MAX_RETRIES times with exponential backoff.
    Raises RuntimeError after all retries are exhausted.
    """
    host = configuration["sftp_host"]
    port = int(configuration.get("sftp_port", 22))
    username = configuration["sftp_username"]
    password = configuration["sftp_password"]
    host_key_b64 = configuration.get("sftp_host_key", "")

    last_exc = None
    for attempt in range(1, _SFTP_MAX_RETRIES + 1):
        sftp = None
        try:
            ssh = paramiko.SSHClient()

            if host_key_b64:
                # Strict host-key verification: decode the base64 host key and
                # add it to the known-hosts store, then use RejectPolicy.
                import base64

                host_key_bytes = base64.b64decode(host_key_b64)
                host_key = paramiko.RSAKey(data=host_key_bytes)
                ssh.get_host_keys().add(host, "ssh-rsa", host_key)
                ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
            else:
                log.warning(
                    "sftp_host_key not configured — host key verification is disabled. "
                    "Set sftp_host_key in configuration for production use."
                )
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            ssh.connect(host, port=port, username=username, password=password, timeout=30)
            sftp = ssh.open_sftp()
            sftp.get_channel().settimeout(_SFTP_OPERATION_TIMEOUT_SEC)
            log.info(f"SFTP connected to {host}:{port} (attempt {attempt})")
            return ssh, sftp

        except Exception as exc:
            last_exc = exc
            # Close sftp first (if open_sftp() succeeded but settimeout() raised),
            # then close ssh to fully release the transport.
            if sftp is not None:
                try:
                    sftp.close()
                except Exception:
                    pass
            try:
                ssh.close()
            except Exception:
                pass
            log.warning(f"SFTP connection attempt {attempt}/{_SFTP_MAX_RETRIES} failed: {exc}")
            if attempt < _SFTP_MAX_RETRIES:
                delay = _SFTP_RETRY_DELAY_SEC * (2 ** (attempt - 1))  # 5, 10, 20, 40 s
                log.warning(f"Retrying in {delay}s...")
                time.sleep(delay)

    raise RuntimeError(
        f"Failed to connect to SFTP at {host}:{port} after {_SFTP_MAX_RETRIES} attempts: {last_exc}"
    )


# ── File discovery ────────────────────────────────────────────────────────────


def find_latest_file(remote_files: list, pattern: str, state: dict, test_mode: bool = False):
    """
    Return the filename of the most-recently modified file whose name
    contains *pattern*.

    Raises RuntimeError if:
      - No file matches the pattern (file is missing → abort sync).
      - The most-recent match is the same file already processed in the
        previous sync (no new file available → abort sync).

    In test_mode the already-processed check is skipped so the same files
    can be re-tested without waiting for the provider to drop new ones.
    """
    matches = [f for f in remote_files if pattern in f.filename]
    if not matches:
        raise RuntimeError(
            f"Sync aborted: expected file matching '{pattern}' not found in SFTP directory"
        )

    # Most-recently modified file first
    matches.sort(key=lambda f: f.st_mtime, reverse=True)
    latest_filename = matches[0].filename

    if not test_mode:
        already_processed = state["processed"].get(pattern)
        if latest_filename == already_processed:
            raise RuntimeError(
                f"Sync aborted: no new file for pattern '{pattern}' — "
                f"most recent file '{latest_filename}' was already processed in the previous sync"
            )

    return latest_filename


# ── File reading and validation ───────────────────────────────────────────────


def read_and_validate_file(file_path: str, spec: dict, filename: str):
    """
    Validate a fixed-width file and return an iterable of detail lines.

    Uses a two-pass strategy to avoid buffering the entire file in memory:

    Pass 1 — counts non-empty lines and captures the final line (trailer).
             Only two values are held in memory: an integer count and one string.
    Pass 2 — re-opens the file and returns a generator that yields detail lines
             one at a time, skipping the header and trailer for header/trailer
             files.

    Raises RuntimeError on any validation failure.
    """
    if not spec["has_header_trailer"]:
        # No validation needed — return a generator over all non-empty lines
        def _all_lines():
            with open(file_path, "r", encoding="cp1252") as fh:
                for raw in fh:
                    stripped = raw.rstrip("\r\n")
                    if stripped.strip():
                        yield stripped

        return _all_lines()

    # ── Pass 1: count lines and capture trailer ────────────────────────────
    line_count = 0
    trailer_line = None
    with open(file_path, "r", encoding="cp1252") as fh:
        for raw in fh:
            stripped = raw.rstrip("\r\n")
            if stripped.strip():
                line_count += 1
                trailer_line = stripped  # updated on every non-empty line → ends as trailer

    if line_count < 3:
        raise RuntimeError(
            f"Sync aborted: {spec['table']} file '{filename}' has {line_count} line(s) "
            f"— expected at least 3 (header + 1 detail + trailer)"
        )

    count_start = spec.get("trailer_count_start")
    count_length = spec.get("trailer_count_length")

    if count_start is not None and count_length is not None:
        raw_count = trailer_line[count_start - 1 : count_start - 1 + count_length].strip()
        try:
            trailer_count = int(raw_count)
        except ValueError:
            raise RuntimeError(
                f"Sync aborted: {spec['table']} trailer record-count field is not an integer "
                f"(got '{raw_count}'). File: {filename}"
            )

        # Trailer count = total lines (incl. header+trailer) or detail lines only
        expected_count = line_count if spec["trailer_includes_header"] else line_count - 2

        if trailer_count != expected_count:
            raise RuntimeError(
                f"Sync aborted: {spec['table']} trailer record-count {trailer_count} "
                f"does not match actual line count {expected_count}. File: {filename}"
            )
    else:
        log.warning(
            f"[{spec['table']}] Trailer record-count position not configured — "
            f"skipping count validation for file '{filename}'"
        )

    # ── Pass 2: yield detail lines (skip header=first, skip trailer=last) ──
    def _detail_lines():
        with open(file_path, "r", encoding="cp1252") as fh:
            lines_iter = (ln.rstrip("\r\n") for ln in fh if ln.strip())
            next(lines_iter, None)  # skip header
            prev = next(lines_iter, None)
            for line in lines_iter:
                yield prev  # prev is a confirmed detail line (not the trailer)
                prev = line
            # prev is now the trailer — do not yield it

    return _detail_lines()


# ── Record parsing ────────────────────────────────────────────────────────────


def stream_and_upsert_records(lines: list, spec: dict, state: dict, test_mode: bool = False):
    """
    Parse each detail line and upsert it immediately, one record at a time,
    to avoid holding the entire file in memory as a list of dicts.

    For full-refresh tables (spec["purge"] is True):
      - Adds purge_indicator=False to every active record.
      - Collects composite PKs during the pass.
      - After the pass, emits purge_indicator=True records for any PKs present
        in the previous sync but absent in this file.

    Incremental tables (spec["purge"] is False) never receive a purge_indicator.

    In test_mode:
      - Upserts at most _TEST_MODE_RECORD_LIMIT records then stops.
      - Purge logic is skipped entirely (a partial record set would generate
        false soft-deletes against the previous full PK set in state).

    Returns (record_count, current_pks) on success, or None if any line is
    malformed (some records may already have been upserted — they will be
    re-processed correctly on the next sync since no checkpoint is saved).
    """
    implied = spec.get("implied_decimal", False)
    is_elan = spec["subdirectory"] == "ELAN"
    table = spec["table"]
    pk_columns = spec["primary_keys"]
    do_purge = spec["purge"] and pk_columns and not test_mode

    current_pks = []
    record_count = 0

    for line_num, line in enumerate(lines, start=1):
        if test_mode and record_count >= _TEST_MODE_RECORD_LIMIT:
            log.info(
                f"[{table}] Test mode: reached {_TEST_MODE_RECORD_LIMIT}-record limit, "
                f"stopping early"
            )
            break

        try:
            record = parse_line(line, spec["fields"], implied)
        except Exception as exc:
            log.severe(f"Malformed line {line_num} in {table}: {exc}. " f"Aborting this file.")
            return None

        if is_elan:
            _apply_elan_signs(record)

        if do_purge:
            record["purge_indicator"] = False
            current_pks.append(_build_composite_pk(record, pk_columns))

        op.upsert(table=table, data=record)
        record_count += 1

    # Emit soft-delete records for PKs absent from this sync (normal mode only)
    if do_purge:
        previous_pks = set(state["pks"].get(table, []))
        missing_pks = previous_pks - set(current_pks)
        if missing_pks:
            log.info(
                f"[{table}] {len(missing_pks)} record(s) no longer present — "
                f"emitting purge_indicator=True"
            )
        for composite_pk in missing_pks:
            op.upsert(table=table, data=_build_purge_record(composite_pk, pk_columns))

    return record_count, current_pks


def parse_line(line: str, fields: list, implied_decimal: bool):
    """
    Extract each field from a fixed-width *line* using 1-indexed positions.

    Raises ValueError / any conversion exception on bad data (caller logs
    the error and aborts the file).
    """
    record = {}
    for field in fields:
        start = field["start"] - 1  # convert to 0-indexed
        end = start + field["length"]
        raw = line[start:end]
        record[field["name"]] = convert_value(raw, field, implied_decimal)
    return record


def convert_value(raw: str, field: dict, implied_decimal: bool):
    """
    Convert a raw fixed-width substring to the appropriate Python type.

    Type rules
    ----------
    STRING      → str.strip() or None if blank
    INT         → int(stripped) or None if blank/all-zeros
    LONG        → int(stripped) or None if blank/all-zeros
    FLOAT (explicit) → float(stripped) or None if blank
    FLOAT (implied)  → int(stripped) / 10**decimal_places or None if blank
    NAIVE_DATE (YYYYMMDD) → "YYYY-MM-DD" ISO string or None if blank/zeros
    NAIVE_DATE (MMDDYYYY) → "YYYY-MM-DD" ISO string or None if blank/zeros
    """
    stripped = raw.strip()
    ftype = field["parse_as"]

    if ftype == "STRING":
        return stripped if stripped else None

    if ftype in ("INT", "LONG"):
        if not stripped or stripped.lstrip("0") == "":
            return None
        return int(stripped)

    if ftype == "FLOAT":
        if not stripped:
            return None
        dec_places = field.get("decimal_places")
        if implied_decimal and dec_places is not None:
            # COBOL implied decimal: integer digits / 10^dec_places
            return int(stripped) / (10**dec_places)
        else:
            return float(stripped)

    if ftype == "NAIVE_DATE":
        if not stripped or stripped.replace("0", "") == "":
            return None
        date_fmt = field.get("date_format")
        if date_fmt == "MMDDYYYY":
            # CUP: MMDDYYYY → parse → emit ISO
            dt = datetime.strptime(stripped, "%m%d%Y")
        else:
            # Default: YYYYMMDD
            dt = datetime.strptime(stripped, "%Y%m%d")
        return dt.date().isoformat()

    # Fallback: return stripped string
    return stripped if stripped else None


def _apply_elan_signs(record: dict):
    """
    Mutate *record* in place: multiply signed ELAN float fields by -1 when
    the corresponding sign indicator field contains "-".
    """
    _sign_pairs = [
        ("BALANCE_SIGN", "BALANCE"),
        ("RWD_POINTS_AVAILABLE_SIGN", "RWD_POINTS_AVAILABLE"),
    ]
    for sign_field, value_field in _sign_pairs:
        if sign_field in record and value_field in record:
            if record[sign_field] == "-" and record[value_field] is not None:
                record[value_field] = -record[value_field]


# ── Purge helpers ─────────────────────────────────────────────────────────────


def _build_composite_pk(record: dict, pk_columns: list):
    """Join PK column values with _PK_SEP into a single string key."""
    return _PK_SEP.join(str(record.get(col, "")) for col in pk_columns)


def _build_purge_record(composite_pk: str, pk_columns: list):
    """
    Reconstruct a minimal record dict from a composite PK string.
    Sets purge_indicator = True and all non-PK fields to None.
    """
    pk_values = composite_pk.split(_PK_SEP)
    record = {col: val for col, val in zip(pk_columns, pk_values)}
    record["purge_indicator"] = True
    return record


# ── Connector entry point ─────────────────────────────────────────────────────

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    connector.debug(configuration=configuration)
