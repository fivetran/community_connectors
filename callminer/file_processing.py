"""
File download and processing functions for CallMiner exports.
"""

import requests
import zipfile
import gzip
import json
import csv
import io
import threading
from typing import Dict, Any, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from fivetran_connector_sdk import Operations as op, Logging as log
from auth import retry_on_500_error

# Thread-safe error statistics tracking
_ERROR_STATS = defaultdict(int)
_ERROR_STATS_LOCK = threading.Lock()


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


def process_single_nested_file(
    file_data: bytes, filename: str, table_name: str, max_records: int = None
) -> Tuple[str, int]:
    """
    Process a single gzip or zip file in a thread-safe manner.

    Args:
        file_data: Raw bytes of the file
        filename: Filename for logging
        table_name: Target table name
        max_records: Maximum records to process (None for no limit)

    Returns:
        Tuple of (filename, record_count)
    """
    try:
        thread_name = threading.current_thread().name
        log.info(f"[{thread_name}] Processing file: {filename} -> {table_name}")

        file_stream = io.BytesIO(file_data)

        # Handle .gz files
        if filename.endswith(".gz"):
            with gzip.open(file_stream, "rt", encoding="utf-8") as gzf:
                csv_reader = csv.DictReader(gzf)
                record_count = process_csv_stream(csv_reader, table_name, max_records)

        # Handle .zip files
        elif filename.endswith(".zip"):
            record_count = 0
            with zipfile.ZipFile(file_stream) as inner_zip:
                inner_files = inner_zip.namelist()

                # Process all CSV files in the nested zip
                for inner_file in inner_files:
                    if not inner_file.endswith(".csv"):
                        continue

                    csv_table_name = parse_table_name_from_filename(inner_file)

                    with inner_zip.open(inner_file) as csvf:
                        text_stream = io.TextIOWrapper(csvf, encoding="utf-8")
                        try:
                            csv_reader = csv.DictReader(text_stream)
                            count = process_csv_stream(csv_reader, csv_table_name, max_records)
                            record_count += count
                        finally:
                            text_stream.detach()
        else:
            record_count = 0

        thread_name = threading.current_thread().name
        log.info(f"[{thread_name}] Completed {filename}: {record_count} records")
        return filename, record_count

    except gzip.BadGzipFile as e:
        thread_name = threading.current_thread().name
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["gzip_decompression_errors"] += 1
        log.error(f"[{thread_name}] Gzip decompression error in {filename}: {e}")
        raise
    except zipfile.BadZipFile as e:
        thread_name = threading.current_thread().name
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["zip_decompression_errors"] += 1
        log.error(f"[{thread_name}] Zip decompression error in {filename}: {e}")
        raise
    except csv.Error as e:
        thread_name = threading.current_thread().name
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["csv_parsing_errors"] += 1
        log.error(f"[{thread_name}] CSV parsing error in {filename}: {e}")
        raise
    except UnicodeDecodeError as e:
        thread_name = threading.current_thread().name
        with _ERROR_STATS_LOCK:
            _ERROR_STATS["encoding_errors"] += 1
        log.error(f"[{thread_name}] Encoding error in {filename}: {e}")
        raise
    except Exception as e:
        thread_name = threading.current_thread().name
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
    Uses multi-threading to process nested files in parallel.

    Args:
        zip_stream: Zip file content as BytesIO stream
        download_id: ID of the downloaded file for logging
        data_types_str: Comma-separated string of data types
        state: State dictionary for checkpointing
        max_records: Maximum records to process per file (None for no limit)
        max_threads: Maximum number of threads for parallel processing
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
                f"Extracting {len(nested_files)} files for parallel "
                f"processing (max {max_threads} threads)"
            )

            # Extract all nested files into memory first
            # (ZipFile objects aren't thread-safe)
            file_tasks = []
            for nested_file in nested_files:
                table_name = parse_table_name_from_filename(nested_file)
                with outer_zip.open(nested_file) as ncf:
                    file_data = ncf.read()
                    file_tasks.append((file_data, nested_file, table_name))

            # Sort by file size (largest first) to process large files early
            # This prevents large files from blocking after small files finish
            file_tasks.sort(key=lambda x: len(x[0]), reverse=True)

            if file_tasks:
                largest_mb = len(file_tasks[0][0]) / (1024 * 1024)
                smallest_mb = len(file_tasks[-1][0]) / (1024 * 1024)
                log.info(
                    f"Processing {len(file_tasks)} files using up to "
                    f"{max_threads} threads (largest: {largest_mb:.1f} MB, "
                    f"smallest: {smallest_mb:.1f} MB)"
                )
            else:
                log.info(
                    f"Processing {len(file_tasks)} files using up to " f"{max_threads} threads"
                )

            # Process files in parallel
            total_records = 0
            errors = []

            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Submit all tasks
                future_to_file = {
                    executor.submit(
                        process_single_nested_file, file_data, filename, table_name, max_records
                    ): filename
                    for file_data, filename, table_name in file_tasks
                }

                # Collect results as they complete
                for future in as_completed(future_to_file):
                    filename = future_to_file[future]
                    try:
                        result_filename, record_count = future.result()
                        total_records += record_count
                        log.info(
                            f"Completed processing {result_filename}: " f"{record_count} records"
                        )

                    except Exception as e:
                        error_msg = f"Failed to process {filename}: {e}"
                        log.error(error_msg)
                        errors.append(error_msg)

            # Report results
            if errors:
                log.error(
                    f"Completed with {len(errors)} error(s). "
                    f"Total records processed: {total_records}"
                )
                raise RuntimeError(f"Errors during parallel processing: {errors}")
            else:
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
