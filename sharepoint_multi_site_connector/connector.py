"""
This is an example for how to work with the fivetran_connector_sdk module.
It defines a connector that syncs CSV and Excel file data from multiple SharePoint Online sites
using the Microsoft Graph API with incremental processing and deletion handling.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# Import standard libraries
import csv
import io
import json
import time
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

# Used to parse Excel files (.xlsx, .xlsm)
import openpyxl
import requests

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like upsert(), update(), delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

__GRAPH_BASE = "https://graph.microsoft.com/v1.0"
__SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xlsm"}

# Maximum file size to download (50 MB); larger files are skipped with a warning.
__MAX_FILE_BYTES = 50 * 1024 * 1024

__ACCESS_TOKEN = ""
__TOKEN_EXPIRY = 0.0


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def get_access_token(configuration: dict) -> str:
    """
    Get an access token for Microsoft Graph API using client credentials flow.
    Caches the token in memory until it expires to avoid unnecessary requests.
    Args:
        configuration: a dictionary containing the configuration settings for the connector, including tenant_id, client_id, and client_secret.
    Returns:
        A string representing the access token for Microsoft Graph API.
    """
    global __ACCESS_TOKEN, __TOKEN_EXPIRY

    if __ACCESS_TOKEN and time.time() < __TOKEN_EXPIRY - 60:
        return __ACCESS_TOKEN

    response = requests.post(
        f"https://login.microsoftonline.com/{configuration['tenant_id']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": configuration["client_id"],
            "client_secret": configuration["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
        },
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    __ACCESS_TOKEN = payload["access_token"]
    __TOKEN_EXPIRY = time.time() + payload.get("expires_in", 3600)

    log.info("Access token obtained/refreshed")
    return __ACCESS_TOKEN


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def graph_get(configuration: dict, url: str, params: dict = None) -> dict:
    """
    Perform a GET request to the Microsoft Graph API with retries for token expiration, rate limiting, and service unavailability.
    Args:
        configuration: a dictionary containing the configuration settings for the connector, including tenant_id, client_id, and client_secret.
        url: the URL to send the GET request to.
        params: optional dictionary of query parameters to include in the request.
    Returns:
        A dictionary containing the JSON response from the Microsoft Graph API.
    """
    global __TOKEN_EXPIRY

    for _ in range(4):
        token = get_access_token(configuration)
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=60,
        )

        if response.status_code == 401:
            __TOKEN_EXPIRY = 0
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            log.warning(f"Rate limited; retrying in {retry_after}s")
            time.sleep(retry_after)
            continue

        if response.status_code in (503, 504):
            log.warning(f"Service unavailable ({response.status_code}); retrying in 30s")
            time.sleep(30)
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError(f"Failed after retries: GET {url}")


def graph_download(configuration: dict, drive_id: str, item_id: str) -> bytes:
    """
    Stream-download a file from SharePoint via Microsoft Graph.
    Enforces a __MAX_FILE_BYTES size limit to avoid unbounded memory usage.
    Args:
        configuration: a dictionary containing the configuration settings for the connector, including tenant_id, client_id, and client_secret.
        drive_id: the ID of the SharePoint drive containing the file.
        item_id: the ID of the file item to download.
    Returns:
        A bytes object containing the downloaded file content.
    """
    global __TOKEN_EXPIRY
    url = f"{__GRAPH_BASE}/drives/{drive_id}/items/{item_id}/content"

    for _ in range(4):
        token = get_access_token(configuration)
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
            allow_redirects=True,
            stream=True,
        )

        if response.status_code == 401:
            __TOKEN_EXPIRY = 0
            continue

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 30))
            log.warning(f"Rate limited during file download; retrying in {retry_after}s")
            time.sleep(retry_after)
            continue

        if response.status_code in (503, 504):
            log.warning(f"File download unavailable ({response.status_code}); retrying in 30s")
            time.sleep(30)
            continue

        response.raise_for_status()

        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            total += len(chunk)
            if total > __MAX_FILE_BYTES:
                raise RuntimeError(
                    f"File size exceeds {__MAX_FILE_BYTES // (1024 * 1024)} MB limit; "
                    f"drive_id={drive_id}, item_id={item_id}"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    raise RuntimeError(f"Failed after retries: download drive_id={drive_id}, item_id={item_id}")


def paginate(configuration: dict, url: str, params: dict = None) -> Iterator[dict]:
    """
    Generator to paginate through Microsoft Graph API results using @odata.nextLink.
    Args:
        configuration: a dictionary containing the configuration settings for the connector, including tenant_id, client_id, and client_secret.
        url: the initial URL to fetch results from.
        params: optional dictionary of query parameters to include in the request.
    Returns:
        An iterator yielding each item from the paginated results.
    """
    while url:
        payload = graph_get(configuration, url, params)
        params = None
        for item in payload.get("value", []):
            yield item
        url = payload.get("@odata.nextLink")


# ---------------------------------------------------------------------------
# Config + site helpers
# ---------------------------------------------------------------------------


def validate_configuration(configuration: dict) -> None:
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or site targeting is absent.
    """
    required = ["tenant_id", "client_id", "client_secret"]
    missing = [k for k in required if not configuration.get(k, "").strip()]
    if missing:
        raise ValueError(f"Missing required configuration key(s): {', '.join(missing)}")

    not_site_ids = not configuration.get("site_ids", "").strip()
    not_site_urls = not configuration.get("site_urls", "").strip()

    if not_site_ids and not_site_urls:
        raise ValueError("Provide at least one of: site_ids or site_urls")


def resolve_sites(configuration: dict) -> List[Tuple[str, str]]:
    """
    Resolve the list of SharePoint sites to sync based on site IDs or site URLs provided in the configuration.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector, including site_ids and site_urls.
    Returns:
        A list of tuples, each containing the site ID and site name for each resolved SharePoint site.
    """
    site_ids_raw = configuration.get("site_ids", "").strip()
    site_urls_raw = configuration.get("site_urls", "").strip()

    if site_ids_raw:
        sites = []
        for site_id in [x.strip() for x in site_ids_raw.split(",") if x.strip()]:
            payload = graph_get(configuration, f"{__GRAPH_BASE}/sites/{site_id}")
            sites.append(
                (payload["id"], payload.get("displayName") or payload.get("name") or site_id)
            )
        return sites

    sites = []
    for raw_url in [x.strip() for x in site_urls_raw.split(",") if x.strip()]:
        parsed = urlparse(raw_url)
        hostname = parsed.netloc
        path = parsed.path.rstrip("/")
        payload = graph_get(configuration, f"{__GRAPH_BASE}/sites/{hostname}:{path}")
        sites.append((payload["id"], payload.get("displayName") or payload.get("name") or raw_url))
    return sites


def get_default_drive(configuration: dict, site_id: str) -> dict:
    """
    Get the default document library (drive) for a given SharePoint site.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector, including tenant_id, client_id, and client_secret.
        site_id: the ID of the SharePoint site to retrieve the default drive for.
    Returns:
        A dictionary containing the default drive information for the specified SharePoint site.
    """
    return graph_get(configuration, f"{__GRAPH_BASE}/sites/{site_id}/drive")


def get_children_url(drive_id: str, folder_path: str) -> str:
    """
    Construct the Microsoft Graph API URL to list children of a folder in a SharePoint drive.
    Args:
        drive_id: The ID of the SharePoint drive containing the folder.
        folder_path: The path of the folder within the drive (relative to the root).
    Returns:
        A string representing the URL to list the children of the specified folder in the SharePoint drive.
    """
    clean_path = folder_path.strip("/")
    if clean_path:
        return f"{__GRAPH_BASE}/drives/{drive_id}/root:/{clean_path}:/children"
    return f"{__GRAPH_BASE}/drives/{drive_id}/root/children"


def get_extension(file_name: str) -> str:
    """
    Get the file extension from a file name, if it is one of the supported extensions.
    Args:
        file_name: The name of the file to check for a supported extension.
    Returns:
        A string representing the file extension (including the dot) if it is supported, or an empty string if not.
    """
    file_name = (file_name or "").lower()
    for ext in __SUPPORTED_EXTENSIONS:
        if file_name.endswith(ext):
            return ext
    return ""


def file_matches(item: dict, file_pattern: Optional[str]) -> bool:
    """
    Match a file item against the specified file pattern, if provided. Only files with supported extensions are considered.
    Args:
        item: A dictionary representing a file item from the Microsoft Graph API, which may include keys like "name", "file", and "folder".
        file_pattern: An optional string pattern to match against the file name. If None, all files with supported extensions are considered matches.
    Returns:
        True if the file item matches the specified pattern and has a supported extension, False otherwise.
    """
    if "folder" in item:
        return False
    if not item.get("file"):
        return False
    if get_extension(item.get("name", "")) == "":
        return False

    if not file_pattern:
        return True

    return file_pattern.lower() in item.get("name", "").lower()


def list_files_in_folder(
    configuration: dict,
    drive_id: str,
    folder_path: str,
    recurse: bool,
    file_pattern: Optional[str],
) -> List[dict]:
    """
    List files in a SharePoint folder, optionally filtering by file pattern and recursing into subfolders.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector, including tenant_id, client_id, and client_secret.
        drive_id: The ID of the SharePoint drive containing the folder to list files from.
        folder_path: The path of the folder within the drive (relative to the root) to list files from.
        recurse: A boolean indicating whether to recursively list files in subfolders.
        file_pattern: An optional string pattern to filter files by name. If None, all files with supported extensions are listed.
    Returns:
        A list of dictionaries, each representing a file item that matches the specified criteria.
    """
    start_url = get_children_url(drive_id, folder_path)
    files: List[dict] = []

    def walk(url: str) -> None:
        for item in paginate(configuration, url):
            if "folder" in item:
                if recurse:
                    child_url = f"{__GRAPH_BASE}/drives/{drive_id}/items/{item['id']}/children"
                    walk(child_url)
            elif file_matches(item, file_pattern):
                files.append(item)

    walk(start_url)
    return files


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def parse_csv_rows(
    content_bytes: bytes, delimiter: Optional[str]
) -> Iterator[Tuple[Optional[str], int, Dict]]:
    """
    Parse CSV content from bytes, yielding each row as a dictionary with cleaned keys.
    Args:
        content_bytes: The CSV content in bytes to be parsed.
        delimiter: An optional string specifying the delimiter to use for parsing the CSV. If None, the delimiter will be auto-detected.
    Returns:
        An iterator yielding tuples of (sheet_name, row_number, row_data), where sheet_name is None for CSV files, row_number is the line number in the CSV,
        and row_data is a dictionary of cleaned key-value pairs for each row.
    """
    text = content_bytes.decode("utf-8-sig")
    stream = io.StringIO(text)

    if delimiter:
        reader = csv.DictReader(stream, delimiter=delimiter)
    else:
        sample = text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            reader = csv.DictReader(stream, dialect=dialect)
        except csv.Error:
            stream.seek(0)
            reader = csv.DictReader(stream)

    for row_number, row in enumerate(reader, start=1):
        cleaned = {}
        for key, value in row.items():
            if key is None:
                continue
            key = str(key).strip()
            if not key:
                continue
            cleaned[key] = value
        yield None, row_number, cleaned


def parse_excel_rows(
    content_bytes: bytes, skip_rows: int
) -> Iterator[Tuple[Optional[str], int, Dict]]:
    """
    Parse Excel content from bytes, yielding each row as a dictionary with cleaned keys.
    Args:
        content_bytes: The Excel content in bytes to be parsed.
        skip_rows: An integer specifying the number of initial rows to skip before reading the header row.
    Returns:
        An iterator yielding tuples of (sheet_name, row_number, row_data), where sheet_name is the name of the active worksheet,
    """
    workbook = openpyxl.load_workbook(
        io.BytesIO(content_bytes),
        read_only=True,
        data_only=True,
    )

    try:
        worksheet = workbook.active
        row_iter = worksheet.iter_rows(values_only=True)

        for _ in range(skip_rows):
            next(row_iter, None)

        header_row = next(row_iter, None)
        if not header_row:
            return

        headers: List[str] = []
        for index, value in enumerate(header_row, start=1):
            if value is None or str(value).strip() == "":
                headers.append(f"col_{index}")
            else:
                headers.append(str(value).strip())

        for row_number, raw_row in enumerate(row_iter, start=1):
            record = {}
            for header, value in zip(headers, raw_row):
                record[header] = None if value is None else str(value)
            yield worksheet.title, row_number, record
    finally:
        workbook.close()


def parse_file_rows(
    file_name: str,
    content_bytes: bytes,
    delimiter: Optional[str],
    skip_rows: int,
) -> Iterator[Tuple[Optional[str], int, Dict]]:
    """
    Parse file content based on its extension, yielding each row as a dictionary with cleaned keys.
    Args:
        file_name: The name of the file to be parsed, used to determine the file type based on its extension.
        content_bytes: The content of the file in bytes to be parsed.
        delimiter: An optional string specifying the delimiter to use for parsing CSV files. If None, the delimiter will be auto-detected.
        skip_rows: An integer specifying the number of initial rows to skip before reading the header row for Excel files.
    Returns:
        An iterator yielding tuples of (sheet_name, row_number, row_data), where sheet_name is the name of the active worksheet for Excel files or None for CSV files,
        row_number is the line number in the file, and row_data is a dictionary of cleaned key-value pairs for each row.
    """
    ext = get_extension(file_name)

    if ext == ".csv":
        yield from parse_csv_rows(content_bytes, delimiter)
        return

    if ext in {".xlsx", ".xlsm"}:
        yield from parse_excel_rows(content_bytes, skip_rows)
        return


# ---------------------------------------------------------------------------
# Row sync helpers
# ---------------------------------------------------------------------------


def build_row_id(file_id: str, sheet_name: Optional[str], source_row_number: int) -> str:
    """
    Build a unique row ID for a file row based on the file ID, sheet name (if applicable), and source row number.
    """
    if sheet_name:
        return f"{file_id}::{sheet_name}::{source_row_number}"
    return f"{file_id}::{source_row_number}"


def _sheet_key(sheet_name: Optional[str]) -> str:
    """
    Return a JSON-safe dict key for a sheet name (None for CSV files).
    """
    return "__csv__" if sheet_name is None else sheet_name


def flatten_file_record(item: dict, drive_id: str, site_id: str, site_name: str) -> dict:
    """
    Flatten a file item from Microsoft Graph into a dictionary suitable for upsert into the destination table.
    """
    parent_ref = item.get("parentReference", {})
    return {
        "file_id": item.get("id"),
        "drive_id": drive_id,
        "site_id": site_id,
        "site_name": site_name,
        "file_name": item.get("name"),
        "web_url": item.get("webUrl"),
        "size_bytes": item.get("size"),
        "mime_type": item.get("file", {}).get("mimeType"),
        "parent_id": parent_ref.get("id"),
        "parent_path": parent_ref.get("path"),
        "created_date_time": item.get("createdDateTime"),
        "last_modified_date_time": item.get("lastModifiedDateTime"),
        "etag": item.get("eTag"),
    }


def sync_one_file(
    configuration: dict,
    state: dict,
    site_id: str,
    site_name: str,
    drive_id: str,
    item: dict,
):
    """
    Sync a single file: upsert metadata and row data, then delete orphaned rows.
    State tracks the maximum row number per sheet rather than the full row-ID list
    to keep state size bounded regardless of file size.
    """
    file_states = state.setdefault("file_states", {})
    state_key = f"{site_id}:{item['id']}"
    previous = file_states.get(state_key, {})

    last_modified = item.get("lastModifiedDateTime", "")
    if previous.get("last_modified") == last_modified:
        log.info(f"Skipping unchanged file: {item.get('name')}")
        return

    # The 'upsert' operation is used to insert or update file metadata in the destination table.
    # The first argument is the name of the destination table.
    # The second argument is a dictionary containing the record to be upserted.
    op.upsert("files", flatten_file_record(item, drive_id, site_id, site_name))

    content_bytes = graph_download(configuration, drive_id, item["id"])
    delimiter = configuration.get("delimiter", "").strip() or None
    skip_rows = int(configuration.get("skip_rows", "0") or "0")

    new_sheet_row_counts: Dict[str, int] = {}
    row_count = 0

    for sheet_name, source_row_number, row_data in parse_file_rows(
        item["name"], content_bytes, delimiter, skip_rows
    ):
        sk = _sheet_key(sheet_name)
        new_sheet_row_counts[sk] = source_row_number
        row_count += 1

        row_id = build_row_id(item["id"], sheet_name, source_row_number)

        # The 'upsert' operation is used to insert or update row-level data in the destination table.
        # Each row of a parsed file becomes an individual record keyed by row_id.
        op.upsert(
            "file_rows",
            {
                "row_id": row_id,
                "file_id": item["id"],
                "drive_id": drive_id,
                "site_id": site_id,
                "site_name": site_name,
                "file_name": item["name"],
                "sheet_name": sheet_name,
                "source_row_number": source_row_number,
                "data": row_data,
                "last_modified_date_time": last_modified,
            },
        )

    # Delete rows beyond the new maximum for sheets that shrank, and all rows for removed sheets.
    prev_sheet_row_counts = previous.get("sheet_row_counts", {})
    for sk, prev_max in prev_sheet_row_counts.items():
        sheet_name = None if sk == "__csv__" else sk
        new_max = new_sheet_row_counts.get(sk, 0)
        for row_num in range(new_max + 1, prev_max + 1):
            row_id = build_row_id(item["id"], sheet_name, row_num)
            # The 'delete' operation removes a row that no longer exists in the source file.
            op.delete("file_rows", {"row_id": row_id})

    file_states[state_key] = {
        "last_modified": last_modified,
        "sheet_row_counts": new_sheet_row_counts,
        "file_id": item["id"],
        "drive_id": drive_id,
    }

    log.info(f"Synced {row_count} row(s) from file '{item['name']}' in site '{site_name}'")


def handle_deleted_files_for_site(site_id: str, current_file_ids: set, state: dict):
    """
    For any file previously tracked in state that is no longer present in the site,
    delete its rows and metadata record from the destination.
    """
    file_states = state.setdefault("file_states", {})
    delete_keys = []

    for state_key, file_state in file_states.items():
        if not state_key.startswith(f"{site_id}:"):
            continue

        _, file_id = state_key.split(":", 1)
        if file_id not in current_file_ids:
            delete_keys.append(state_key)

    for state_key in delete_keys:
        file_state = file_states.pop(state_key)

        for sk, max_row in file_state.get("sheet_row_counts", {}).items():
            sheet_name = None if sk == "__csv__" else sk
            for row_num in range(1, max_row + 1):
                row_id = build_row_id(file_state["file_id"], sheet_name, row_num)
                # The 'delete' operation removes a row belonging to a file deleted from SharePoint.
                op.delete("file_rows", {"row_id": row_id})

        # The 'delete' operation removes the file metadata record for the deleted file.
        op.delete(
            "files",
            {
                "file_id": file_state["file_id"],
                "drive_id": file_state["drive_id"],
            },
        )


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "files",
            "primary_key": ["file_id", "drive_id"],
            "columns": {
                "file_id": "STRING",
                "drive_id": "STRING",
                "site_id": "STRING",
                "site_name": "STRING",
                "file_name": "STRING",
                "web_url": "STRING",
                "size_bytes": "LONG",
                "mime_type": "STRING",
                "parent_id": "STRING",
                "parent_path": "STRING",
                "created_date_time": "UTC_DATETIME",
                "last_modified_date_time": "UTC_DATETIME",
                "etag": "STRING",
            },
        },
        {
            "table": "file_rows",
            "primary_key": ["row_id"],
            "columns": {
                "row_id": "STRING",
                "file_id": "STRING",
                "drive_id": "STRING",
                "site_id": "STRING",
                "site_name": "STRING",
                "file_name": "STRING",
                "sheet_name": "STRING",
                "source_row_number": "LONG",
                "data": "JSON",
                "last_modified_date_time": "UTC_DATETIME",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
        state: a dictionary containing the state checkpointed during the prior sync.
               The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.warning("Example: Source Example - SharePoint Multi-Site Connector")

    validate_configuration(configuration)

    sites = resolve_sites(configuration)
    folder_path = configuration.get("folder_path", "").strip()
    recurse = configuration.get("sync_subfolders", "false").lower() == "true"
    file_pattern = configuration.get("file_pattern", "").strip() or None

    log.info(f"Starting sync for {len(sites)} site(s)")

    for index, (site_id, site_name) in enumerate(sites, start=1):
        log.info(f"Syncing site {index}/{len(sites)}: {site_name}")

        drive = get_default_drive(configuration, site_id)
        drive_id = drive["id"]

        files = list_files_in_folder(
            configuration=configuration,
            drive_id=drive_id,
            folder_path=folder_path,
            recurse=recurse,
            file_pattern=file_pattern,
        )

        files.sort(key=lambda item: item.get("lastModifiedDateTime", ""))

        current_file_ids = set()

        for item in files:
            current_file_ids.add(item["id"])
            sync_one_file(
                configuration=configuration,
                state=state,
                site_id=site_id,
                site_name=site_name,
                drive_id=drive_id,
                item=item,
            )

        handle_deleted_files_for_site(site_id, current_file_ids, state)

        # Save the progress by checkpointing the state after each site. This is important for ensuring
        # that the sync process can resume from the correct position in case of interruptions.
        op.checkpoint(state=state)
        log.info(f"Completed site {index}/{len(sites)}: {site_name}")

    log.info("Sync complete")


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development.
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
