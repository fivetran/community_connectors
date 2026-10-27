---
name: Snowflake Cortex NHTSA Safety Intelligence
description: Sync NHTSA recalls, complaints, and vehicle specs, then use Snowflake Cortex Agent for vehicle safety discovery and analysis. A fixed-list mode feeds a context pack ([snowflake_cortex/snowflake-cortex-code-nhtsa-safety-intelligence/context_pack](https://github.com/fivetran/community_connectors/tree/main/snowflake_cortex/snowflake-cortex-code-nhtsa-safety-intelligence/context_pack)) with dbt Semantic Layer metrics over the recalls, a publish of the dbt project's model, column and metric definitions into Snowflake with [Agents Schema](https://github.com/dbt-labs/agents_schema), and a Cortex Agent that reads both.
category: AI and Connector SDK
---

# Snowflake Cortex Code NHTSA Safety Intelligence Connector Example

## Connector overview

This connector syncs vehicle recall campaigns, consumer safety complaints, and vehicle specifications from NHTSA (National Highway Traffic Safety Administration) public APIs, then uses Snowflake Cortex Agent to analyze the data and autonomously discover related vehicles to investigate. This is the agent-driven discovery pattern: the AI analyzes initial safety data and decides which additional vehicles to fetch next based on shared components, platform similarities, and cross-manufacturer safety trends.

Built entirely with Snowflake Cortex Code and the Fivetran Connector Builder Skill.

The connector has two modes. Seed mode, the default, starts from one seed vehicle and can hand the next steps to Cortex Agent. Fixed-list mode, on when `models` is set, syncs the recalls for every make, model and model year in a configured list, keeps the campaigns whose report received date falls in a configured window, and makes no AI calls. Fixed-list mode also writes a `configured_vehicle` table with one row per configured vehicle, so a vehicle with no recalls in the window is recorded as checked rather than missing.

Use cases:
- Automotive safety intelligence - Analyze recall and complaint patterns across vehicle makes, models, and years to identify systemic safety issues
- Agent-driven discovery - Let Snowflake Cortex Agent autonomously decide which related vehicles to investigate based on component failure patterns
- Cross-vehicle synthesis - Generate fleet-wide safety grades and component risk rankings across multiple vehicles
- Regulatory monitoring - Track NHTSA recall campaigns and consumer complaints for specific vehicles or entire manufacturer fleets
- Fleet or portfolio recall tracking - Keep the recalls for a fixed list of vehicles current, with the configured names your other data uses

APIs used:
- [NHTSA Recalls API](https://api.nhtsa.gov) - Vehicle recall campaigns by make/model/year
- [NHTSA Complaints API](https://api.nhtsa.gov) - Consumer safety complaints by make/model/year
- [NHTSA vPIC API](https://vpic.nhtsa.dot.gov/api/) - Vehicle product information catalog (models for a make)
- [Snowflake Cortex Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-rest-api) - Discovery analysis and cross-vehicle synthesis via REST API

Key capabilities:
- Three-phase sync: seed vehicle fetch, agent-driven discovery, cross-vehicle synthesis
- Agent autonomy: Cortex Agent recommends which related vehicles to investigate next
- Real-time AI enrichment during data ingestion
- Component failure pattern analysis across vehicle platforms
- Cost control with configurable discovery and enrichment limits
- Graceful degradation: works without Cortex (data-only mode) when AI is disabled
- Fixed-list mode: a configured model list and report-date window, with deletes so the `recalls` and `configured_vehicle` tables always equal the filter

The context pack in `../context_pack/` is built on the fixed-list tables. It has a dbt project with dbt Semantic Layer metrics over `recalls` and `configured_vehicle`, and a script that publishes the project's model, column, lineage and metric definitions into an `AGENTS` schema in Snowflake with [Agents Schema](https://github.com/dbt-labs/agents_schema). Its Cortex Agent gets every count from those metrics through the dbt MCP server, and reads what a model, column or metric means from the `AGENTS` tables. See the [context pack README](../context_pack/README.md).


## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- Snowflake account with Cortex enabled (for Agent-Driven Discovery phases in seed mode)
- Snowflake Personal Access Token (PAT) for Cortex API access (seed mode with Cortex enabled only)


## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template snowflake_cortex/snowflake-cortex-code-nhtsa-safety-intelligence/connector
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.


## Features

- Exponential backoff retry logic for transient API failures and rate limiting
- Automatic PascalCase-to-snake_case field normalization for warehouse compatibility
- Nested structure flattening and list-to-JSON serialization


## Configuration file

The connector requires the following configuration parameters in `configuration.json`:

```json
{
    "seed_make": "<VEHICLE_MAKE>",
    "seed_model": "<VEHICLE_MODEL>",
    "seed_year": "<MODEL_YEAR>",
    "discovery_depth": "<DISCOVERY_DEPTH_1_TO_3>",
    "max_discoveries": "<MAX_VEHICLES_PER_DISCOVERY>",
    "enable_cortex": "<TRUE_OR_FALSE>",
    "snowflake_account": "<YOUR_SNOWFLAKE_ACCOUNT_HOSTNAME>",
    "snowflake_pat_token": "<YOUR_SNOWFLAKE_PAT>",
    "cortex_model": "<CORTEX_MODEL_NAME>",
    "max_enrichments": "<MAX_CORTEX_ENRICHMENTS>",
    "models": "<MODELS_JSON_ARRAY_OR_EMPTY_FOR_SEED_MODE>",
    "report_date_from": "<REPORT_DATE_FROM_YYYY_MM_DD>",
    "report_date_to": "<REPORT_DATE_TO_YYYY_MM_DD>",
    "request_pause_seconds": "<REQUEST_PAUSE_SECONDS>"
}
```

The connector runs in fixed-list mode when `models` holds a value. When `models` is unset, empty or a placeholder, it runs in seed mode and ignores `report_date_from`, `report_date_to` and `request_pause_seconds`. Fixed-list mode ignores the seed and Cortex parameters.

Seed mode parameters:
- `seed_make` (required) - Vehicle manufacturer to start investigation (e.g., "Tesla", "Toyota", "Ford")
- `seed_model` (required) - Vehicle model to start investigation (e.g., "Model 3", "Camry", "F-150")
- `seed_year` (required) - Model year to start investigation (e.g., "2023")
- `discovery_depth` (optional) - Discovery phase depth: 0=data only, 1=discovery only, 2=discovery+synthesis. Defaults to "2"
- `max_discoveries` (optional) - Maximum vehicles the agent can discover per sync. Defaults to "3"
- `enable_cortex` (optional) - Enable Snowflake Cortex Agent for discovery and synthesis phases. Defaults to "false"
- `snowflake_account` (conditional) - Snowflake account URL (e.g., "orgname-acctname.snowflakecomputing.com"), required if Cortex is enabled
- `snowflake_pat_token` (conditional) - Snowflake Personal Access Token, required if Cortex is enabled
- `cortex_model` (optional) - Cortex LLM model for analysis. Defaults to "claude-sonnet-4-6"
- `max_enrichments` (optional) - Maximum Cortex API calls per sync for cost control. Defaults to "10"

Fixed-list mode parameters:
- `models` (required for fixed-list mode) - A JSON array, encoded as a string, of objects with `make`, `model`, `nhtsa_model`, `year_from` and `year_to`. `make` and `model` are the names written to the tables. `nhtsa_model` is the spelling the API matches, because some marketing names return nothing (a Lexus ES 350 is `ES`, a Mercedes-Benz C-Class is `C 300`, a Ford Mustang Mach-E is `MUSTANG MACH E`). Model years run from `year_from` to `year_to` inclusive. At most 200 models, and each make and model pair appears once, compared case-insensitively because the API matches names that way. A make is spelled the same way in every entry (`Toyota` twice, not `Toyota` and `toyota`), because the tables group and filter on it. Two entries cannot read the same once make and model are joined by a space (`Alfa` with `Romeo Giulia` and `Alfa Romeo` with `Giulia`), because the context pack labels each model that way.
- `report_date_from` (required for fixed-list mode) - The first NHTSA report received date kept, inclusive, as `YYYY-MM-DD`
- `report_date_to` (required for fixed-list mode) - The last NHTSA report received date kept, inclusive, as `YYYY-MM-DD`
- `request_pause_seconds` (optional) - The pause after each API call, between 0 and 10. Defaults to "0.2"

A filled-in fixed-list example with one model:

```json
{
  "models": "[{\"make\":\"Toyota\",\"model\":\"Camry\",\"nhtsa_model\":\"CAMRY\",\"year_from\":2016,\"year_to\":2025}]",
  "report_date_from": "2025-01-01",
  "report_date_to": "2026-09-30",
  "request_pause_seconds": "0.2"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Authentication

NHTSA APIs:
- No authentication required for any NHTSA endpoint
- All three APIs (Recalls, Complaints, vPIC) are free and public
- Seed mode sends a descriptive User-Agent header as a courtesy. Fixed-list mode keeps the `requests` default, because the recalls API's edge answers HTTP 403 to some custom User-Agent strings

Snowflake Cortex Agent (seed mode with `enable_cortex` set to `true` only) uses [Personal Access Token (PAT) authentication](https://docs.snowflake.com/en/en/user-guide/programmatic-access-tokens#label-pat-generate). The token is sent as a Bearer token in the Authorization header. To get a token:

1. Sign in to Snowsight as the user the connector runs as.
2. Go to **Profile** > **Account Settings** > **Security** > **Personal Access Tokens**.
3. Generate a token and copy it. Snowflake shows it once, in the form `ver:1-hint:abc123...`.
4. Set it as `snowflake_pat_token` in `configuration.json`, and set `snowflake_account` to your account hostname.


## Pagination

NHTSA APIs return all results for a given make/model/year in a single response. No pagination is required.

In seed mode, the connector processes discovered vehicles sequentially with rate limiting delays (0.5 seconds between API calls) to respect NHTSA server capacity. In fixed-list mode, it pauses `request_pause_seconds` after each call.


## Data handling

In seed mode, the connector processes data through a three-phase pipeline, implemented in `def update(configuration, state)`:

Phase 1, seed vehicle fetch:
1. Fetch recalls from NHTSA Recalls API via `def fetch_recalls(session, make, model, year)`
2. Fetch complaints from NHTSA Complaints API via `def fetch_complaints(session, make, model, year)`
3. Fetch vehicle specs from NHTSA vPIC API via `def fetch_vehicle_specs(session, make)`
4. Normalize PascalCase API field names to snake_case and upsert records

Phase 2, agent-driven discovery (requires Cortex):
1. Build analysis prompt with component failure summaries via `def build_discovery_prompt(make, model, year, recalls, complaints)`
2. Call Cortex Agent for pattern analysis via `def call_cortex_agent(configuration, prompt, cortex_session=None)`
3. Agent recommends related vehicles based on shared components and safety patterns
4. Fetch recalls, complaints, and specs for each discovered vehicle
5. Upsert discovery insights with component risk rankings

Phase 3, cross-vehicle synthesis (requires Cortex and multiple vehicles):
1. Build synthesis prompt with pre-computed aggregate data via `def build_synthesis_prompt(vehicles_investigated, aggregates)` using component frequency counts instead of full in-memory record lists
2. Call Cortex Agent for fleet-wide analysis
3. Upsert safety analysis with component risk rankings and fleet safety grade

Data transformations:
- PascalCase API field names normalized to snake_case (e.g., `NHTSACampaignNumber` becomes `nhtsa_campaign_number`)
- Nested dictionaries flattened using underscore separators via `def flatten_dict(d, parent_key, sep)`
- Lists serialized to JSON strings for warehouse compatibility
- `ReportReceivedDate` arrives as `DD/MM/YYYY` and is written as a date (`report_received_date`); an unparseable value is written as null and the recall is kept
- State checkpointed after each phase and each discovered vehicle

Fixed-list mode is implemented in `def sync_fixed_list(configuration, state)`:
1. For each configured model, call the NHTSA Recalls API once per model year with the `nhtsa_model` spelling, via `def collect_model_rows(session, model, date_from, date_to, pause_seconds)` in `fixed_list.py`
2. Keep the campaigns whose report received date falls in the window, and de-duplicate a campaign the API lists more than once for one vehicle
3. Upsert the `recalls` rows with the configured `make` and `model`, and NHTSA's spelling in `nhtsa_make` and `nhtsa_model`
4. Upsert one `configured_vehicle` row per model year, with the number of campaigns the `recalls` table holds for it, so a zero means the API was called and no campaign is held for it in the window
5. Delete the rows that model no longer returns, for example a campaign whose report date was amended out of the window, and the model years removed from its range. A tracked campaign is kept instead when a skipped result could stand for it, or when NHTSA answers no recalls for its model year to the query it was tracked under (see Error handling)
6. Checkpoint after each model. The state records each model this pass has finished, with its `nhtsa_model`, year range and report-date window, so an interrupted sync resumes with every model the pass has not finished. A model added to the list, or a finished model whose entry or window changed, is synced on the resume, not skipped

After a full pass, the connector deletes every row of a model removed from `models`. A sync that starts with no fixed-list state truncates both tables first. That covers the first sync, a full re-sync, and the first sync after a connection switches from seed mode to fixed-list mode, including a switch back after an earlier fixed-list period: seed mode drops the fixed-list state keys, so the next fixed-list sync always starts with the truncate. A re-sync clears state but keeps every destination row, and seed mode leaves its own `recalls` rows, so without the truncate those rows would never be deleted. The seed-only tables (`complaints`, `vehicle_specs`, `discovery_insights`, `safety_analysis`) are not part of the fixed-list schema and keep their last seed-mode rows; use a new connection for fixed-list mode if they should not be there.

A result with no campaign number or an unparseable report date is skipped and counted, and each model's count is logged and reported to the dashboard with `op.warning` before that model's checkpoint, so an interrupted sync that resumes past the model has already reported it. A skipped result is unknown, not absent, so it never causes a delete: a tracked campaign whose number the skipped result carries is kept, and a skipped result with no readable campaign number keeps every tracked campaign of that model year until a later sync returns a clean response.


## Error handling

The connector implements comprehensive error handling - refer to `def fetch_data_with_retry(session, url, params, headers)`:

Retry logic:
- Exponential backoff for transient failures (429, 500, 502, 503, 504 status codes)
- Up to 3 attempts in all, with delays of 1s and 2s between them
- Connection errors and timeouts are retried with the same backoff pattern
- Non-retryable errors (401, 403) are logged and raised immediately

Fixed-list mode handling via `def fetch_vehicle_recalls(session, make, nhtsa_model, model_year)` in `fixed_list.py`:
- HTTP 400 with NHTSA's no-recalls body (`{"Count": 0, "results": []}`), or a 200 with a zero `Count`, is treated as no campaigns. NHTSA sends it both for a vehicle year with no recalls and for a vehicle it does not know, so a misspelled `make` or `nhtsa_model` cannot be told from a model with no recalls. When every configured year of a model gets this answer, the sync also sends an `op.warning` naming the model, so a misspelling does not pass unnoticed; a correctly spelled model with no recalls in its configured years gets the same warning. When a model year gets this answer but the destination holds campaigns tracked for it, the connector keeps those campaigns instead of deleting them, because NHTSA dropping every campaign of a model year it listed before is more likely a fault than a withdrawal. It does so only when they were tracked with the same `nhtsa_model`, in any case (NHTSA ignores case), and under a report window the current one contains, since the window is applied by the connector and is not sent to NHTSA. It sends an `op.warning` naming the model years and how many campaigns it kept, and a full re-sync of the connection clears them. After a respelling of `nhtsa_model` or a narrower window, the answer says nothing about the campaigns tracked before it, so they are deleted as usual, and so are campaigns carried over such a change because a skipped result could stand for them. A change to `make` or `model` makes a new model entry, whose old rows are deleted. A 400 with any other body, such as an edge error page, fails the sync naming the vehicle, before anything is written for that model
- HTTP 429 and 5xx, connection errors and timeouts are tried up to three times in all, with exponential backoff (2 and 4 seconds), then the sync fails naming the vehicle
- Any other non-success status, a body that is not JSON, a JSON body that is not an object, or a `results` field that is not a list fails the sync immediately naming the vehicle, before anything is written for that model, so a malformed response cannot delete tracked campaigns. Models synced earlier in the same sync keep what they wrote and checkpointed, and the next sync resumes after them
- A missing or null `results` field is treated as no campaigns only when `Count` is 0. Without a zero `Count`, as in a proxy's JSON error object, the sync fails naming the vehicle. A `results` list is accepted only when `Count` is an integer equal to its length, so a short list, or an empty list without a zero `Count`, from something that is not NHTSA fails the sync instead of deleting tracked campaigns. An empty list with a zero `Count` is the no-recalls answer above

Cortex Agent handling via `def call_cortex_agent(configuration, prompt, cortex_session=None)`:
- Retry logic with exponential backoff for transient failures (429, 500, 502, 503, 504)
- Authentication errors (401, 403) are logged and not retried
- Agent timeouts are retried up to 3 times; sync continues without AI enrichment if all fail
- Invalid JSON responses from the agent are logged and treated as empty results
- Empty agent responses result in skipped discovery/synthesis phases
- Markdown code fences in agent responses are automatically stripped before JSON parsing
- LLM output is type-validated before use (e.g., recommended_vehicles must be a list of dicts)
- `max_enrichments` budget is enforced with a counter before each Cortex call
- Cortex calls use a dedicated `requests.Session` via `def _create_cortex_session(configuration)` for TCP connection pooling across discovery and synthesis phases
- Streaming responses are explicitly closed via `response.close()` in a finally block

Validation:
- Configuration validation occurs before sync starts - refer to `def validate_configuration(configuration)`, which checks the fixed-list parameters with `def validate_fixed_list_configuration(configuration)` when `models` is set
- Records without primary keys are logged as warnings and skipped
- Snowflake account URL format validated when Cortex is enabled


## Tables created

Seed mode creates five tables covering NHTSA safety data and AI-generated insights: `recalls`, `complaints`, `vehicle_specs`, `discovery_insights` and `safety_analysis`. Fixed-list mode creates two: `recalls` and `configured_vehicle`.

### recalls

Vehicle recall campaign records from the NHTSA Recalls API. The Mode column shows which mode writes each column.

Primary key: `nhtsa_campaign_number`, `make`, `model`, `model_year`

| Column | Type | Description | Mode |
|--------|------|-------------|------|
| `nhtsa_campaign_number` | STRING | NHTSA recall campaign identifier | Both |
| `manufacturer` | STRING | Vehicle manufacturer name | Both |
| `make` | STRING | Vehicle make: as NHTSA returned it in seed mode, as configured in fixed-list mode | Both |
| `model` | STRING | Vehicle model: as NHTSA returned it in seed mode, as configured (the marketing name) in fixed-list mode | Both |
| `model_year` | STRING in seed mode, INT in fixed-list mode | Model year | Both |
| `nhtsa_make` | STRING | Make as NHTSA returned it | Fixed list |
| `nhtsa_model` | STRING | Model as NHTSA returned it | Fixed list |
| `component` | STRING | Affected component (e.g., "ELECTRICAL SYSTEM", "AIR BAGS") | Both |
| `summary` | STRING | Recall description and affected vehicles | Both |
| `consequence` | STRING | Safety consequence of the defect | Both |
| `remedy` | STRING | Manufacturer remedy and owner notification details | Both |
| `notes` | STRING | Additional notes including NHTSA hotline information | Both |
| `report_received_date` | NAIVE_DATE | Date NHTSA received the recall report | Both |
| `park_it` | BOOLEAN | Whether the vehicle should be parked immediately | Both |
| `park_outside` | BOOLEAN | Whether the vehicle should be parked outside | Both |
| `over_the_air_update` | BOOLEAN | Whether the recall can be resolved via OTA software update | Both |
| `nhtsa_action_number` | STRING | Related NHTSA enforcement action number | Seed |
| `source` | STRING | Record source: "seed" or "discovered" | Seed |

### configured_vehicle

One row per configured make, model and model year, written in fixed-list mode only.

Primary key: `make`, `model`, `model_year`

| Column | Type | Description |
|--------|------|-------------|
| `make` | STRING | Make as configured |
| `model` | STRING | Model as configured (the marketing name) |
| `model_year` | INT | Model year the API was called for |
| `nhtsa_model` | STRING | Model spelling the API was called with |
| `campaigns_returned` | INT | Campaign rows the `recalls` table holds for this vehicle after the sync: the campaigns in the report-date window, plus any tracked campaign kept because its result was skipped or because NHTSA answered no recalls for its model year. Zero when there are none |
| `last_synced_at` | UTC_DATETIME | Start of the sync that last called the API for this vehicle |

### complaints

Consumer safety complaint records from the NHTSA Complaints API.

Primary key: `odi_number`

| Column | Type | Description |
|--------|------|-------------|
| `odi_number` | INTEGER | NHTSA Office of Defects Investigation number |
| `manufacturer` | STRING | Vehicle manufacturer name |
| `crash` | BOOLEAN | Whether a crash was involved |
| `fire` | BOOLEAN | Whether a fire was involved |
| `number_of_injuries` | INTEGER | Number of injuries reported |
| `number_of_deaths` | INTEGER | Number of deaths reported |
| `date_of_incident` | STRING | Date of the safety incident (MM/DD/YYYY) |
| `date_complaint_filed` | STRING | Date the complaint was filed (MM/DD/YYYY) |
| `vin` | STRING | Partial vehicle identification number |
| `components` | STRING | Affected vehicle components |
| `summary` | STRING | Consumer complaint narrative |
| `products` | STRING | JSON-serialized array of affected product details |
| `source` | STRING | Record source: "seed" or "discovered" |

### vehicle_specs

Vehicle model specifications from the NHTSA vPIC API.

Primary key: `make_id`, `model_id`

| Column | Type | Description |
|--------|------|-------------|
| `make_id` | INTEGER | NHTSA make identifier |
| `make_name` | STRING | Vehicle manufacturer name |
| `model_id` | INTEGER | NHTSA model identifier |
| `model_name` | STRING | Vehicle model name |
| `source` | STRING | Record source: "seed" or "discovered" |

### discovery_insights

AI-generated discovery analysis from the Cortex Agent (Phase 2).

Primary key: `insight_id`

| Column | Type | Description |
|--------|------|-------------|
| `insight_id` | STRING | Unique insight identifier |
| `seed_make` | STRING | Seed vehicle make |
| `seed_model` | STRING | Seed vehicle model |
| `seed_year` | STRING | Seed vehicle year |
| `top_components` | STRING | JSON-serialized array of top failing components with severity |
| `severity_score` | INTEGER | Overall severity score (1-10) |
| `crash_count` | INTEGER | Number of crash incidents identified |
| `fire_count` | INTEGER | Number of fire incidents identified |
| `recommended_vehicles` | STRING | JSON-serialized array of vehicles the agent recommends investigating |
| `analysis_summary` | STRING | Agent-generated analysis narrative |

### safety_analysis

AI-generated cross-vehicle synthesis from the Cortex Agent (Phase 3).

Primary key: `analysis_id`

| Column | Type | Description |
|--------|------|-------------|
| `analysis_id` | STRING | Unique analysis identifier |
| `seed_make` | STRING | Seed vehicle make |
| `seed_model` | STRING | Seed vehicle model |
| `seed_year` | STRING | Seed vehicle year |
| `vehicles_analyzed` | INTEGER | Number of vehicles included in the analysis |
| `total_recalls_analyzed` | INTEGER | Total recall records analyzed |
| `total_complaints_analyzed` | INTEGER | Total complaint records analyzed |
| `component_risk_rankings` | STRING | JSON-serialized array of component risk rankings across all vehicles |
| `manufacturer_response_score` | INTEGER | Manufacturer response quality score (1-10) |
| `systemic_issues` | STRING | JSON-serialized array of systemic industry issues identified |
| `fleet_safety_grade` | STRING | Overall fleet safety grade (A-F) |
| `executive_summary` | STRING | Agent-generated executive summary |


## Cost considerations

Cortex Agent enrichment costs scale with the discovery depth and number of discovered vehicles:

Example cost projections (using claude-sonnet-4-6 model):
- Data-only mode (enable_cortex=false) and fixed-list mode: $0.00 per sync (NHTSA APIs are free, and neither makes a Cortex call)
- Discovery only (discovery_depth=1, max_discoveries=3): approximately $0.002 per sync (1 agent call)
- Full pipeline (discovery_depth=2, max_discoveries=3): approximately $0.004 per sync (2 agent calls)

Use the `discovery_depth` and `max_discoveries` configuration parameters to control costs during testing and production.


## Additional files

- **`fixed_list.py`** – Fixed-list mode helpers: report-date parsing, row shaping, the per-vehicle recalls fetch and the checkpoint state. It makes no destination writes; `connector.py` owns every upsert, delete, truncate and checkpoint.
- **`models_config.py`** – Fixed-list mode configuration parsing: placeholder detection, the report dates, the pause and the validation of every `models` entry.


## Additional considerations

This example was contributed by [Kelly Kohlleffel](https://github.com/kellykohlleffel).

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.

Agent-driven discovery pattern:
- The agent analyzes component failure patterns and recommends related vehicles based on shared platforms, competitor vehicles in the same class, and adjacent model years
- Discovery is bounded by `max_discoveries` to prevent runaway API usage
- The agent returns structured JSON that the connector parses to determine next fetch targets

Sync behavior:
- This connector performs full refresh on each sync
- In fixed-list mode, every sync re-reads all configured vehicles, because the recalls API has no change filter, and deletes the rows that left the filter
- NHTSA data changes infrequently (new recalls and complaints are added periodically)
- Recommended sync frequency: every 24 hours for monitoring, weekly for general analysis
- The connector checkpoints after each phase and each discovered vehicle in seed mode, and after each model in fixed-list mode, for safe resumption
- Earlier versions of this example wrote `report_received_date` in `recalls` as text in `DD/MM/YYYY` form. It is now a date in both modes. An existing seed-mode connection rewrites only the rows it fetches again, so rows of vehicles it does not rediscover keep the text value; use a new connection when the column must hold dates only
