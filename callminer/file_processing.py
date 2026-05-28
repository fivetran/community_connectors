"""
File download and processing functions for CallMiner exports.
"""

import requests
import zipfile
import gzip
import json
import csv
import io
import os
import tempfile
import threading
from typing import Dict, Any, Tuple, List
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from fivetran_connector_sdk import Operations as op, Logging as log
from auth import retry_on_500_error

# Thread-safe error statistics tracking
_ERROR_STATS = defaultdict(int)
_ERROR_STATS_LOCK = threading.Lock()


def _cleanup_materialized_csv_files(materialized_files: List[Dict[str, Any]]) -> None:
    """
    Remove temporary CSV files created by worker threads.

    Args:
        materialized_files: List of materialized CSV metadata dictionaries.
    """
    for materialized_file in materialized_files:
        file_path = materialized_file.get("file_path")
        if not file_path:
            continue

        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as e:
            log.warning(f"Unable to remove temporary file {file_path}: {e}")


def log_error_statistics() -> None:
    """
    Log comprehensive error statistics collected during file processing.
    This helps with monitoring and debugging connector behavior.
    """
    if _ERROR_STATS:
        log.warning("File Processing Error Statistics:")
        with _ERROR_STATS_LOCK:
            for error_type, count in _ERROR_STATS.items():
                log.warning(f"  {error_type}: {count}")
    else:
        log.info("No file processing errors encountered")


def reset_error_statistics() -> None:
    """
    Reset error statistics. Called at the start of each job processing.
    """
    with _ERROR_STATS_LOCK:
        _ERROR_STATS.clear()


@retry_on_500_error(max_retries=3, initial_delay=1, backoff_factor=2)
def download_and_stream_file(download_endpoint: str, bearer_token: str) -> io.BytesIO:
    """
    Download a file from CallMiner Bulk Export API and return as stream.

    Args:
        download_endpoint: The full download endpoint URL or download ID
        bearer_token: Bearer token for authentication

    Returns:
        io.BytesIO: File content as a stream
    """
    # If just an ID is provided, construct the full URL
    if not download_endpoint.startswith("http"):
        url = f"https://api.callminer.net/bulkexport/api/download/" f"{download_endpoint}"
    else:
        url = download_endpoint

    headers = {"Authorization": f"Bearer {bearer_token}"}

    log.info(f"Downloading file from: {url}")

    try:
        # Longer timeout for file downloads (30s connect, 10min read)
        response = requests.get(url, headers=headers, stream=True, timeout=(30, 600))
        response.raise_for_status()

        try:
            # Stream directly into BytesIO
            file_stream = io.BytesIO()
            total_size = 0

            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file_stream.write(chunk)
                    total_size += len(chunk)

            file_stream.seek(0)  # Reset to beginning for reading
            log.info(f"Successfully downloaded file, size: {total_size} bytes")
            return file_stream
        finally:
            response.close()

    except requests.exceptions.RequestException as e:
        log.error(f"Error downloading file: {e}")
        raise


def parse_table_name_from_filename(filename: str) -> str:
    """
    Extract and normalize table name from filename.

    Handles formats like:
    - UUID_TableName.csv.gz -> "tablename"
    - UUID_TableName.csv.zip -> "tablename"
    - TableName.csv -> "tablename"

    Args:
        filename: Filename to parse (may include UUID prefix and extensions)

    Returns:
        Normalized table name in lowercase with underscores
    """
    # Remove all possible extensions
    base = (
        filename.replace(".csv.gz", "")
        .replace(".csv.zip", "")
        .replace(".gz", "")
        .replace(".zip", "")
        .replace(".csv", "")
    )

    # Split by underscore and skip UUID if present
    parts = base.split("_")
    if len(parts) > 1 and "-" in parts[0]:
        # UUID_TableName format - skip UUID (first part)
        return "_".join(parts[1:]).lower()
    else:
        # Simple filename - use as is
        return base.lower().replace(" ", "_")


def process_csv_stream(
    csv_reader: csv.DictReader, table_name: str, max_records: int = None
) -> int:
    """
    Process CSV stream and upsert records.

    Args:
        csv_reader: CSV DictReader instance
        table_name: Target table name for upserting
        max_records: Maximum records to process (None for unlimited)

    Returns:
        Number of records processed
    """
    record_count = 0
    log_interval = 1000000  # Log every million records

    for row in csv_reader:
        if max_records and record_count >= max_records:
            log.info(f"Reached limit of {max_records} records for {table_name}")
            break

        op.upsert(table=table_name, data=row)
        record_count += 1

        # Log progress every million records
        if record_count % log_interval == 0:
            log.info(f"Processing {table_name}: {record_count:,} records processed")

    return record_count


def materialize_csv_stream(
    csv_reader: csv.DictReader,
    table_name: str,
    source_name: str,
    order_key: Tuple[int, int],
    max_records: int = None,
) -> Dict[str, Any]:
    """
    Write parsed CSV rows to a temporary CSV file without calling SDK operations.

    Args:
        csv_reader: CSV DictReader instance
        table_name: Target table name for later upserting
        source_name: Source file name for logging
        order_key: Stable sort key for main-thread upsert ordering
        max_records: Maximum records to materialize (None for unlimited)

    Returns:
        Dictionary with temporary file metadata and record count
    """
    temp_file = tempfile.NamedTemporaryFile(
        mode="w",
        newline="",
        encoding="utf-8",
        delete=False,
        prefix="callminer_",
        suffix=".csv",
    )
    record_count = 0

    try:
        fieldnames = csv_reader.fieldnames
        if not fieldnames:
            log.warning(f"No CSV headers found in {source_name}")
            return {
                "file_path": temp_file.name,
                "table_name": table_name,
                "source_name": source_name,
                "record_count": record_count,
                "order_key": order_key,
            }

        writer = csv.DictWriter(temp_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for row in csv_reader:
            if max_records and record_count >= max_records:
                log.info(f"Reached limit of {max_records} records for {table_name}")
                break

            writer.writerow(row)
            record_count += 1

        return {
            "file_path": temp_file.name,
            "table_name": table_name,
            "source_name": source_name,
            "record_count": record_count,
            "order_key": order_key,
        }
    except Exception:
        temp_file.close()
        _cleanup_materialized_csv_files([{"file_path": temp_file.name}])
        raise
    finally:
        if not temp_file.closed:
            temp_file.close()


def upsert_materialized_csv_file(materialized_file: Dict[str, Any]) -> int:
    """
    Upsert rows from a materialized CSV file on the main thread.

    Args:
        materialized_file: Temporary CSV metadata dictionary

    Returns:
        Number of records upserted
    """
    table_name = materialized_file["table_name"]
    file_path = materialized_file["file_path"]

    with open(file_path, "r", newline="", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        return process_csv_stream(csv_reader, table_name)


def process_single_nested_file(
    file_data: bytes,
    filename: str,
    table_name: str,
    file_order: int,
    max_records: int = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Extract a single gzip or zip file to temporary CSV files.

    Args:
        file_data: Raw bytes of the file
        filename: Filename for logging
        table_name: Target table name
        file_order: Stable order of the nested file in the outer zip
        max_records: Maximum records to process (None for no limit)

    Returns:
        Tuple of (filename, materialized CSV metadata list)
    """
    materialized_files = []

    try:
        thread_name = threading.current_thread().name
        log.info(f"[{thread_name}] Processing file: {filename} -> {table_name}")

        file_stream = io.BytesIO(file_data)

        # Handle .gz files
        if filename.endswith(".gz"):
            with gzip.open(file_stream, "rt", encoding="utf-8") as gzf:
                csv_reader = csv.DictReader(gzf)
                materialized_files.append(
                    materialize_csv_stream(
                        csv_reader,
                        table_name,
                        filename,
                        (file_order, 0),
                        max_records,
                    )
                )

        # Handle .zip files
        elif filename.endswith(".zip"):
            with zipfile.ZipFile(file_stream) as inner_zip:
                inner_files = inner_zip.namelist()

                # Process all CSV files in the nested zip
                for inner_file_order, inner_file in enumerate(inner_files):
                    if not inner_file.endswith(".csv"):
                        continue

                    csv_table_name = parse_table_name_from_filename(inner_file)

                    with inner_zip.open(inner_file) as csvf:
                        with io.TextIOWrapper(csvf, encoding="utf-8") as text_stream:
                            csv_reader = csv.DictReader(text_stream)
                            materialized_files.append(
                                materialize_csv_stream(
                                    csv_reader,
                                    csv_table_name,
                                    inner_file,
                                    (file_order, inner_file_order),
                                    max_records,
                                )
                            )

        thread_name = threading.current_thread().name
        record_count = sum(file_info["record_count"] for file_info in materialized_files)
        log.info(f"[{thread_name}] Completed {filename}: {record_count} records")
        return filename, materialized_files

    except gzip.BadGzipFile as e:
        thread_name = threading.current_thread().name
        _cleanup_materialized_csv_files(materialized_files)
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["gzip_decompression_errors"] += 1
        log.error(f"[{thread_name}] Gzip decompression error in {filename}: {e}")
        raise
    except zipfile.BadZipFile as e:
        thread_name = threading.current_thread().name
        _cleanup_materialized_csv_files(materialized_files)
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["zip_decompression_errors"] += 1
        log.error(f"[{thread_name}] Zip decompression error in {filename}: {e}")
        raise
    except csv.Error as e:
        thread_name = threading.current_thread().name
        _cleanup_materialized_csv_files(materialized_files)
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["csv_parsing_errors"] += 1
        log.error(f"[{thread_name}] CSV parsing error in {filename}: {e}")
        raise
    except UnicodeDecodeError as e:
        thread_name = threading.current_thread().name
        _cleanup_materialized_csv_files(materialized_files)
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["encoding_errors"] += 1
        log.error(f"[{thread_name}] Encoding error in {filename}: {e}")
        raise
    except Exception as e:
        thread_name = threading.current_thread().name
        _cleanup_materialized_csv_files(materialized_files)
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["unexpected_file_errors"] += 1
        log.error(f"[{thread_name}] Unexpected error processing {filename}: {e}")
        raise


def process_multi_type_zip_file(
    zip_stream: io.BytesIO,
    download_id: str,
    data_types_str: str,
    state: Dict[str, Any],
    max_records: int = None,
    max_threads: int = 8,
) -> None:
    """
    Process a zip file that may contain data for multiple data types.
    Automatically detects table names from file names.
    Uses multi-threading to extract nested files, then upserts on the main thread.

    Args:
        zip_stream: Zip file content as BytesIO stream
        download_id: ID of the downloaded file for logging
        data_types_str: Comma-separated string of data types
        state: State dictionary for checkpointing
        max_records: Maximum records to process per file (None for no limit)
        max_threads: Maximum number of threads for parallel extraction
            (default: 8)
    """
    # Reset error statistics for this job
    reset_error_statistics()

    try:
        # Open the outer zip file
        with zipfile.ZipFile(zip_stream) as outer_zip:
            file_list = outer_zip.namelist()
            log.info(f"Files in outer zip: {file_list}")

            # Find JSON metadata file
            json_file = None
            nested_files = []

            for filename in file_list:
                if filename.endswith(".json"):
                    json_file = filename
                elif filename.endswith(".zip") or filename.endswith(".gz"):
                    nested_files.append(filename)

            # Read metadata (optional)
            if json_file:
                with outer_zip.open(json_file) as jf:
                    metadata = json.load(jf)
                    if isinstance(metadata, dict):
                        metadata_keys = ", ".join(metadata.keys())
                        log.info(f"Metadata loaded for file: {json_file} ({metadata_keys})")
                    else:
                        log.info(f"Metadata loaded for file: {json_file}")

            # If no nested files found, log error
            if not nested_files:
                log.error(f"No nested compressed files found in {download_id}")
                return

            log.info(
                f"Extracting {len(nested_files)} files with up to " f"{max_threads} worker threads"
            )

            # Extract all nested files into memory first
            # (ZipFile objects aren't thread-safe)
            file_tasks = []
            for file_order, nested_file in enumerate(nested_files):
                table_name = parse_table_name_from_filename(nested_file)
                with outer_zip.open(nested_file) as ncf:
                    file_data = ncf.read()
                    file_tasks.append(
                        {
                            "file_data": file_data,
                            "filename": nested_file,
                            "table_name": table_name,
                            "file_order": file_order,
                            "file_size": len(file_data),
                        }
                    )

            # Sort by file size (largest first) to process large files early
            # This prevents large files from blocking after small files finish
            file_tasks.sort(key=lambda task: task["file_size"], reverse=True)

            if file_tasks:
                largest_mb = file_tasks[0]["file_size"] / (1024 * 1024)
                smallest_mb = file_tasks[-1]["file_size"] / (1024 * 1024)
                log.info(
                    f"Extracting {len(file_tasks)} files using up to "
                    f"{max_threads} threads (largest: {largest_mb:.1f} MB, "
                    f"smallest: {smallest_mb:.1f} MB)"
                )
            else:
                log.info(
                    f"Extracting {len(file_tasks)} files using up to " f"{max_threads} threads"
                )

            # Extract/decompress files in parallel, but keep SDK operations on the main thread.
            materialized_files = []
            total_records = 0
            errors = []

            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit all tasks
                future_to_file = {
                    executor.submit(
                        process_single_nested_file,
                        task["file_data"],
                        task["filename"],
                        task["table_name"],
                        task["file_order"],
                        max_records,
                    ): task["filename"]
                    for task in file_tasks
                }

                # Collect results as they complete
                for future in as_completed(future_to_file):
                    filename = future_to_file[future]
                    try:
                        result_filename, result_files = future.result()
                        materialized_files.extend(result_files)
                        extracted_records = sum(
                            file_info["record_count"] for file_info in result_files
                        )
                        log.info(
                            f"Completed extraction for {result_filename}: "
                            f"{extracted_records} records"
                        )

                    except Exception as e:
                        error_msg = f"Failed to process {filename}: {e}"
                        log.error(error_msg)
                        errors.append(error_msg)

            if errors:
                _cleanup_materialized_csv_files(materialized_files)
                log.error(
                    f"Completed with {len(errors)} error(s). "
                    f"Total records processed: {total_records}"
                )
                raise RuntimeError(f"Errors during parallel processing: {errors}")

            try:
                for materialized_file in sorted(
                    materialized_files, key=lambda file_info: file_info["order_key"]
                ):
                    record_count = upsert_materialized_csv_file(materialized_file)
                    total_records += record_count
                    log.info(
                        f"Completed upsert for {materialized_file['source_name']}: "
                        f"{record_count} records"
                    )
            finally:
                _cleanup_materialized_csv_files(materialized_files)

            log.info(f"Successfully processed all files. " f"Total records: {total_records}")

    except zipfile.BadZipFile as e:
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["outer_zip_errors"] += 1
        log.error(f"Invalid outer zip file: {e}")
        raise
    except Exception as e:
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["outer_processing_errors"] += 1
        log.error(f"Error processing zip file: {e}")
        raise
    finally:
        # Always log error statistics at the end
        log_error_statistics()
