---
name: "USDA Boxed Beef"
description: "Sync USDA AMS wholesale boxed beef prices from the Livestock Mandatory Reporting DataMart (the daily Choice and Select cutout, primal values, negotiated cut, ground beef and trimmings prices, and weekly Prime cut prices), picking up corrections by published date, with a context pack ([usda_boxed_beef/context_pack](https://github.com/fivetran/community_connectors/tree/main/usda_boxed_beef/context_pack)) that gives AI agents tested dbt v2 tables on DuckDB and agent instructions with checked questions."
category: "AI and Connector SDK"
---

# USDA Boxed Beef Connector Example

## Connector overview

This connector syncs wholesale boxed beef prices published by the USDA Agricultural Marketing Service (AMS) under Livestock Mandatory Reporting (LMR). It reads the public [LMR DataMart API](https://mpr.datamart.ams.usda.gov/), which needs no key, and syncs two reports:

- Report 2453, National Daily Boxed Beef Cutout and Boxed Beef Cuts, Negotiated Sales, Afternoon (LM_XB403): the Choice and Select cutout values, the composite primal values, the volume, and the negotiated price of each boxed beef cut, ground beef blend and trimming, every business day from April 2001.
- Report 2460, National Weekly Boxed Beef Cuts for Prime Product, Negotiated Sales (LM_XB456): the negotiated price of each Prime cut, weekly from June 2002.

Prices are FOB plant, in U.S. dollars per 100 pounds, for boxed beef delivered within 0 to 21 days. They are wholesale market prices, not what a particular buyer pays. Use cases include tracking wholesale cost movement for menu items made from a given cut, comparing Prime, Choice and Select prices for the same cut, and watching the Choice and Select spread.

## Accreditation

This example was contributed by [Kelly Kohlleffel](https://github.com/kellykohlleffel).

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template usda_boxed_beef
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

This template creates `connector/` and the optional `context_pack/` side by side. Run `fivetran debug` and `fivetran deploy` from `connector/`.

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

Every setting in this connector's `configuration.json` is optional, and the shipped placeholders run with the defaults.

## Features

- Syncs seven tables from two LMR reports, with every key declared and every price, weight and count written as `DOUBLE`.
- Backfills by report date in windows of up to 180 days, the longest range the API allows in one request, and checkpoints after every window.
- After the backfill, reads by published date. AMS corrects a report by republishing the same report date with a new published time, so each correction is read by the next sync with no fixed lookback.
- Splits each cut description into its IMPS number, fat limitation and cut name.
- Writes a price only on a row with trades, and never as zero: a no-trade day's `.00` prices are written as null.
- Waits at least `request_delay_seconds` between requests, because AMS blocks addresses that send high-frequency automated requests.
- Provides a setup form with one field per configuration key and a setup test that reads one week of the first report.

## Configuration file

```json
{
  "report_ids": "<REPORT_IDS>",
  "start_date": "<START_DATE_YYYY_MM_DD>",
  "max_window_days": "<MAX_WINDOW_DAYS>",
  "max_windows_per_sync": "<MAX_WINDOWS_PER_SYNC>",
  "request_delay_seconds": "<REQUEST_DELAY_SECONDS>"
}
```

- `report_ids` (optional): comma-separated report ids to sync, `2453`, `2460` or both. Defaults to `2453,2460`.
- `start_date` (optional): the first report date the initial sync reads, as `YYYY-MM-DD`, no earlier than `2001-04-03`. Defaults to `2024-01-01`. Moving it earlier on a running connection starts the backfill again from the new date; moving it later keeps the rows already loaded.
- `max_window_days` (optional): report dates covered by one request, from 1 to 180. Defaults to `180`.
- `max_windows_per_sync` (optional): the most date windows one sync reads for each report, from 1 to 500. A longer backfill continues on the next sync. Defaults to `50`.
- `request_delay_seconds` (optional): the least time between two requests, from 0.5 to 30 seconds. Defaults to `1.0`.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not have any dependencies beyond the packages pre-installed in the Connector SDK runtime.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The LMR DataMart API is public and needs no key or account. The connector sends a descriptive `User-Agent` header with every request.

## Pagination

The API returns a whole section for a date range in one response, so there is no page cursor. The connector pages by date instead (refer to `def sync_report(client, slug_id, settings, today, state, issues, counts)`):

- The initial sync reads `report_date` windows from `start_date` through today in US Central time, the time zone the API uses. Each window covers at most `max_window_days` days and both ends are inclusive, so adjacent windows share no day.
- Once the backfill reaches today, each sync reads `published_date` from three days before its saved position through today, so a report published a little late is still read. Re-reading those days upserts the same rows.
- A sync that runs the backfill does not also read by published date. The first incremental sync starts from the day the backfill began, so a correction published during the backfill is read then.

The API caps a response at 100,000 records. A 180-day window of the largest section holds fewer than 8,000, and the connector fails the sync rather than load a response the API reports as truncated.

## Data handling

Each report section maps to one table (refer to `REPORTS` in `tables.py` and the builders in `transforms.py`):

- Numbers arrive as strings with thousands separators (`"1,382.45"`) and without a leading zero (`"-.77"`). They are parsed to `DOUBLE`. A value that is not a finite number is written as null and counted.
- An item with no trades that day is still returned, with its numbers null, empty, or `.00`. A price is kept only when the row's trade count is above zero and the price is not zero, so `.00` is written as null and so is a price on a row whose trade count is missing or unreadable. Zero pounds on a no-trade row are written as null too. AMS sometimes reports trades for an item but withholds its prices, so a row can have trades and null prices.
- Report dates (`MM/DD/YYYY`) become `NAIVE_DATE`. Published times are US Central and are converted to UTC per timestamp, so times on both sides of a daylight saving change are correct.
- Cut descriptions carry the IMPS number and the fat limitation, either at the end (`Rib, ribeye, lip-on, bn-in (109E  1)`) or at the start (`121D 4  Plate, Inside Skirt`). The connector keeps the description as the key and splits it into `imps_code`, `fat_limitation` and `cut_name`.
- The Current Cutout Values and Change From Prior Day sections are merged into one `cutout_values` row per report date.
- A correction is read by upsert, so it replaces every row it republishes. The connector does not delete: if a correction ever drops an item, the earlier row for that item and date stays. In the backfill from 2024-01-02 to 2026-11-09, each of the 18 corrected daily reports republished every row of its date.
- Current Volume becomes one `volume` row per volume class.

## Error handling

Refer to `def fetch_section(client, slug_id, section_name, field, start, end)` and `def get_json_with_retry(client, url, params, label)` in `usda_client.py`:

- HTTP 429 and 5xx responses, connection errors and timeouts are retried three times with exponential backoff, honouring a `Retry-After` header in seconds up to 120 seconds. Other request errors and 4xx responses fail the sync.
- For a date range with no report, such as a weekend or a federal holiday, the API returns HTTP 200 with the string `"No Results Found. "`. That string and an empty `results` list are the only responses read as no rows. Any other string, such as the message the API returns for an unknown section, fails the sync, as does a body that is not a report section, a result that is not a row, or a response the API reports as truncated.
- Every row the API sends today carries every field of its section. When the incremental sync reads a row missing a field the connector reads, the sync fails with the field's name, because reading it as null would overwrite a value already loaded (refer to `class ReportLayoutError` in `transforms.py`). The backfill writes report dates for the first time, in older layouts this connector has not been checked against, so there a missing field is written as null and named in an `op.warning`.
- State is saved after every window, so a failed sync resumes from the last window it finished.
- Values that could not be used (a number or published time that does not parse, a price on a row without a trade count above zero, a row without a report date or item) are counted and reported once per sync with `op.warning`, refer to `def report_issues(issues)`.
- The backfill does not read a report date again, so a gap of 14 or more days between two report dates, or between the last report date and today, is named once in an `op.warning`. The last report date is saved with the progress, so a gap that spans windows or syncs is found too. Moving `start_date` earlier reads the dates again (refer to `def report_empty_dates(slug_id, first, last)`).

## Tables created

All prices are U.S. dollars per 100 pounds. `slug_id` is the report id, `2453` or `2460`.

### reports

One row per report and report date, from the Summary section. Primary key: `slug_id`, `report_date`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date | Yes |
| `report_title` | STRING | Report title, with the LMR report code | No |
| `published_at` | UTC_DATETIME | When AMS published this version of the report | No |
| `is_correction` | BOOLEAN | Whether this version corrects an earlier one | No |
| `narrative` | STRING | Notice published with the report, such as a holiday schedule | No |

### cutout_values

The estimated boxed beef cutout value for 600 to 900 pound carcasses, report 2453. Primary key: `slug_id`, `report_date`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date | Yes |
| `choice_cutout` | DOUBLE | Choice cutout value | No |
| `select_cutout` | DOUBLE | Select cutout value | No |
| `choice_change` | DOUBLE | Change in the Choice cutout from the prior report day | No |
| `select_change` | DOUBLE | Change in the Select cutout from the prior report day | No |
| `published_at` | UTC_DATETIME | When AMS published the report | No |

### primal_values

The composite value of each primal (rib, chuck, round, loin, brisket, plate, flank), report 2453. Primary key: `slug_id`, `report_date`, `primal`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date | Yes |
| `primal` | STRING | Primal name, such as `Primal Rib` | Yes |
| `choice_value` | DOUBLE | Choice composite primal value | No |
| `select_value` | DOUBLE | Select composite primal value | No |
| `published_at` | UTC_DATETIME | When AMS published the report | No |

### cut_prices

The negotiated price of each boxed beef cut. `grade` is `Choice`, `Select` or `Choice, Select and Ungraded` from report 2453, or `Prime` from report 2460. Primary key: `slug_id`, `report_date`, `grade`, `item_description`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date; for report 2460, the date of the weekly report | Yes |
| `grade` | STRING | USDA quality grade of the section | Yes |
| `item_description` | STRING | The cut as the report lists it, with its IMPS number and fat limitation | Yes |
| `imps_code` | STRING | Institutional Meat Purchase Specifications number, null when the report leaves it blank | No |
| `fat_limitation` | STRING | Fat limitation, `1` to `6` | No |
| `cut_name` | STRING | The cut without its codes | No |
| `number_trades` | DOUBLE | Number of trades | No |
| `total_pounds` | DOUBLE | Pounds traded | No |
| `price_range_low` | DOUBLE | Lowest price | No |
| `price_range_high` | DOUBLE | Highest price | No |
| `weighted_average` | DOUBLE | Weighted average price | No |
| `published_at` | UTC_DATETIME | When AMS published the report | No |

### ground_beef_prices

The negotiated price of each ground beef and blended ground beef item, report 2453. Primary key: `slug_id`, `report_date`, `item_description`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date | Yes |
| `item_description` | STRING | Item, such as `Ground Beef 81%` | Yes |
| `section` | STRING | `Ground Beef` or `Blended Ground Beef` | No |
| `number_trades` | DOUBLE | Number of trades | No |
| `total_pounds` | DOUBLE | Pounds traded | No |
| `price_range_low` | DOUBLE | Lowest price | No |
| `price_range_high` | DOUBLE | Highest price | No |
| `weighted_average` | DOUBLE | Weighted average price | No |
| `published_at` | UTC_DATETIME | When AMS published the report | No |

### trimmings_prices

The negotiated price of fresh and frozen 50% lean trimmings, report 2453. Primary key: `slug_id`, `report_date`, `item_description`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date | Yes |
| `item_description` | STRING | Item, such as `Fresh 50% lean trimmings` | Yes |
| `number_trades` | DOUBLE | Number of trades | No |
| `total_pounds` | DOUBLE | Pounds traded | No |
| `price_range_low` | DOUBLE | Lowest price | No |
| `price_range_high` | DOUBLE | Highest price | No |
| `weighted_average` | DOUBLE | Weighted average price | No |
| `published_at` | UTC_DATETIME | When AMS published the report | No |

### volume

Volume traded, by class. Report 2453 reports `choice`, `select`, `trimmings` and `coarse` (ground beef); report 2460 reports one `total`. One load is 40,000 pounds. Primary key: `slug_id`, `report_date`, `volume_class`.

| Column | Type | Description | Primary key |
|---|---|---|---|
| `slug_id` | STRING | Report id | Yes |
| `report_date` | NAIVE_DATE | Report date | Yes |
| `volume_class` | STRING | `choice`, `select`, `trimmings`, `coarse` or `total` | Yes |
| `loads` | DOUBLE | Loads traded | No |
| `pounds` | DOUBLE | Pounds traded | No |
| `published_at` | UTC_DATETIME | When AMS published the report | No |

## Additional files

- **`tables.py`** – The destination tables, and the sections of each report with the table and row builder each one uses.
- **`transforms.py`** – Row building: number, date and published-time parsing, and the IMPS and fat limitation split. It makes no HTTP calls.
- **`usda_client.py`** – The LMR DataMart client: request pacing, retries with backoff, and the rules for which responses mean no rows.
- **`setup_form.py`** – The setup form Fivetran renders in the dashboard: one field per configuration key, all optional. The connector supplies the defaults and the setup test.

The optional context pack in [`../context_pack/`](../context_pack/) gives an AI agent what it needs to answer questions from this connector's tables without the mistakes the raw tables invite. USDA prices are per 100 pounds, so a per-pound answer read straight from `weighted_average` is 100 times too high. A no-trade cut has a null price, which an average must skip and never count as zero. Prime is a weekly report and Choice and Select are daily, so the two cannot share one trend line. A corrected report replaces its date in place. The pack has no Python and makes no LLM call. It holds:

- **`../context_pack/dbt/`** – A dbt v2 project with a `local` target that attaches `../connector/files/warehouse.db` read-only. It builds staging views over the connector's tables in the `usda_staging` schema and the tables the agent reads in the `usda` schema, both in `usda_local.duckdb`. The tables put each price in dollars per 100 pounds and in dollars per pound side by side, flag no-trade rows, mark the weekly Prime report apart from the daily one, carry each report's correction flag, and give 7- and 28-day averages over report dates only. Its data tests ship. Every test on the synced data runs on the staging views: no report loaded, a zero price, a priced no-trade row, a Prime row outside the weekly report, a second Prime report in one week, a repeated report date, or a report date with only some of its rows fails the build. The tests on the agent's tables check their SQL, such as the per-pound conversion, the Choice-Select spread and the trend windows. Rows Fivetran marks deleted are left out.
- **`../context_pack/agent/AGENTS.md`** – Instructions for the agent: the session setup, ten rules every query follows, the tables and their grain, USDA's definitions with links, and five checked questions with their SQL and the answers they returned on 2026-11-10, on the debug warehouse and on a Managed Data Lake copy.
- **`../context_pack/evidence/`** – The USDA definitions as quoted, with their sources.

To use it, sync the connector with `fivetran debug`, then build from `context_pack/dbt/` in two steps, the staging views and their tests first and the agent's tables only when those pass:

```bash
dbt build --profiles-dir . --select tag:staging && dbt build --profiles-dir . --select tag:marts
```

When a test on the synced data fails, the agent's tables keep the last build that passed. A single `dbt build` would not always hold them back: with dbt 2.0.11, a failed test that reads two models, such as the zero-price test, skipped nothing downstream. The second step reads the warehouse again, so do not run a sync between the two steps. Rebuild after every sync.

The project was built with dbt 2.0.11 and DuckDB 1.5.5, the version `fivetran debug` writes `warehouse.db` with; a DuckDB client on an older storage version cannot open the file. Then give an agent `../context_pack/agent/AGENTS.md` as its instructions and a tool that opens `usda_local.duckdb` read-only and runs DuckDB SQL, and have it run the setup statement in that file first. The agent's session does not open the connector's warehouse, so it does not block the next sync, but it does hold `usda_local.duckdb`: close it before you rebuild.

To run the pack on a Fivetran Managed Data Lake instead, read the connection's tables from the destination's Iceberg REST catalog in DuckDB and copy them into a DuckDB file with a `tester` schema, the shape `fivetran debug` writes. The catalog endpoint, catalog name, client ID and client secret are on the destination's page in the Fivetran dashboard; keep the secret out of every tracked file and pass it to DuckDB as a parameter:

```sql
install iceberg; load iceberg; install httpfs; load httpfs;
create temporary secret lake_catalog (type iceberg, client_id ?, client_secret ?,
  oauth2_scope 'PRINCIPAL_ROLE:ALL', oauth2_server_uri '<catalog endpoint>/v1/oauth/tokens');
attach '<catalog name>' as lake (type iceberg, secret lake_catalog, endpoint '<catalog endpoint>',
  default_region '<region>', read_only);
attach 'lake_snapshot.duckdb' as snap;
create schema snap.tester;
create table snap.tester.cut_prices as select * from lake.<connection schema>.cut_prices;
```

Repeat the last statement for `reports`, `cutout_values`, `primal_values`, `ground_beef_prices`, `trimmings_prices` and `volume`. Then run the two build steps with `USDA_WAREHOUSE` set to the copy's full path. The agent's setup does not change. The pack's tests and checked questions return the same results on a lake copy as on the debug warehouse.

## Additional considerations

The data is published by USDA AMS Livestock, Poultry and Grain Market News. Cut, ground beef and trimmings prices summarize negotiated sales reported under Livestock Mandatory Reporting, and the cutout and primal values are AMS estimates built from those prices and average industry cutting yields. A buyer's contract price differs.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
