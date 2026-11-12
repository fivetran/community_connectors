---
name: Oura Ring
description: Sync daily activity, sleep, readiness, stress, heart rate, and detailed sleep period data from the Oura API v2.
category: SaaS & APIs
---

# Oura Ring Connector Example

## Connector overview

This connector syncs health and wellness data from the Oura API v2 into your Fivetran destination. It covers the daily activity, sleep, readiness, and stress summaries, continuous heart rate, and the detailed `sleep` table, which holds one row per sleep period with durations, HRV, and heart rate that the daily sleep score does not include.

It authenticates with Oura's OAuth2 authorization-code flow, because Oura retired personal access tokens in December 2025. Oura's refresh tokens are single-use, so each sync spends one, keeps the replacement encrypted in state, and checkpoints it before reading any data. Each table keeps its own date cursor, and every sync re-reads the last 7 days because days can reach Oura late, after the ring syncs to the app.

## Accreditation

This example was contributed by [Kelly Kohlleffel](https://github.com/kellykohlleffel).

## Requirements

- [Supported Python versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)
- An Oura account and a registered Oura API application

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/getting-started) to get started.

To initialize a new Connector SDK project using this connector as a starting point, run:

```bash
fivetran init --template oura_ring
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Six tables, including per-period sleep detail
- Per-table date cursors, so a failure in one table does not re-read the others
- A 7-day overlap on every sync to pick up days that reach Oura late, after the ring syncs to the app
- `next_token` pagination, with a guard that stops the sync if the API repeats a token
- Heart rate requests split into 29-day windows, within Oura's 30-day limit
- Retries with exponential backoff that wait longer when a `Retry-After` header in seconds asks for it
- A setup form with a setup test that validates the configuration

## Configuration file

The `configuration.json` file contains the parameters needed to connect to the Oura API:

```json
{
    "client_id": "<YOUR_OURA_CLIENT_ID>",
    "client_secret": "<YOUR_OURA_CLIENT_SECRET>",
    "refresh_token": "<YOUR_OURA_REFRESH_TOKEN>",
    "fernet_key": "<YOUR_FERNET_KEY>",
    "lookback_days": "<LOOKBACK_DAYS>"
}
```

- `client_id` (required): Client ID of your Oura API application
- `client_secret` (required): Client secret of the same application
- `refresh_token` (required): Refresh token from the authorization-code consent, with the `daily` and `heartrate` scopes. It seeds the first sync. After that the connector uses the rotated token it keeps in state.
- `fernet_key` (required): A Fernet key (32 url-safe base64-encoded bytes) that encrypts the rotated refresh token in state. Generate one with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
- `lookback_days` (optional): Number of days to read on a table's first sync, from 1 to 3650, defaults to 90

> Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

The `requirements.txt` file declares `cryptography`, which the connector uses to encrypt the rotated refresh token in state with the `fernet_key`. The version matches the one `fivetran_connector_sdk` pins, so the two do not conflict:

```
cryptography==49.0.0
```

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The connector uses Oura's OAuth2 authorization-code (server-side) flow. To set it up:

1. Register an API application at https://developer.ouraring.com/applications with a redirect URI, for example `http://localhost:8080/callback`, and the `daily` and `heartrate` scopes.
2. Send the user to `https://cloud.ouraring.com/oauth/authorize` with `response_type=code`, your `client_id`, the redirect URI, the scopes, and a random `state` value.
3. After consent, Oura redirects to the redirect URI with a `code`. Exchange it within a few minutes at `https://api.ouraring.com/oauth/token` with `grant_type=authorization_code`, the code, the redirect URI, and your client credentials.
4. Add the `client_id`, `client_secret`, the returned `refresh_token`, and a new `fernet_key` to your `configuration.json` file.

At the start of every sync, the `def refresh_access_token(configuration, state, user_agent)` function in `oura_auth.py` exchanges the refresh token for a new access token. Oura invalidates a refresh token once it is used and returns a new one, so the connector encrypts the new refresh token with the `fernet_key`, stores it in state, and checkpoints that state before it requests any data. After the first sync the connector uses the token in state, and the configured `refresh_token` is spent. State records a SHA-256 fingerprint of the configured token, so a new consent with a different `refresh_token` replaces the token in state.

Because the configured `refresh_token` works only once, two situations need a new consent:

- A full re-sync, or any reset of the connector's state, removes the token in state. The next sync falls back to the configured `refresh_token`, which is already spent, and stops with an error that says so. Complete the consent again and set the new `refresh_token` before the next sync.
- A local `fivetran debug` run spends the configured `refresh_token` into the local state. Complete the consent again after testing locally and before you deploy, so the deployed connection starts with an unspent token.

To rotate the `fernet_key`, run the consent again and set the new `refresh_token` and the new `fernet_key` together. The first sync after that encrypts the next token with the new key. If only the key changes, the connector cannot decrypt the token in state and falls back to the configured `refresh_token`, which fails if it was already spent.

The setup test (refer to `def verify_configuration(configuration)`) checks that every value is present and that the `fernet_key` is valid. It does not call Oura, because a setup test cannot save a rotated token and would spend the only valid one.

## Pagination

The Oura API v2 returns a `next_token` field when more records remain. The `def sync_window(session, table_name, table_config, window_start, window_end, state)` function passes it back until the API returns none. A `next_token` is never saved to state, because it is only valid for the window that produced it. An interrupted window is read again from its start, which upserts the same primary keys.

## Data handling

The `def update(configuration, state)` function walks the six tables in turn, and the `def sync_table(session, table_name, table_config, start_date, today, state)` function reads each one through today. State holds a `<table>_synced_through` date per table. The next sync starts 7 days before that date, and the date filters are inclusive, so the overlap re-reads those days and upserts the same primary keys. The saved date follows the clock rather than the records returned, so a day that reaches Oura more than 7 days late, for example from a ring that stayed out of range of the app for longer than that, is not read again. Each sync therefore reads about 9 days of every table (the 7-day overlap through tomorrow), including the heart rate samples for those days.

State saved by the earlier version of this connector, which used personal access tokens, holds one `last_sync_date` for all tables. On the first sync after the upgrade, `daily_activity`, `daily_sleep`, `daily_readiness` and `heart_rate` resume from that date, re-reading the 7 days before it as every incremental sync does. `daily_stress` starts at the same point or at `lookback_days` before today, whichever is earlier. The earlier version advanced its date even when it skipped stress, so the lookback picks up stress days it skipped within that range, and the old date covers the days after it. Stress days it skipped before both of those points are not read. `sleep`, which the earlier version did not have, starts from `lookback_days`. The old `last_sync_date` and `<table>_next_token` keys are then removed.

The query runs through tomorrow in UTC because Oura dates records in the wearer's time zone, which can be ahead of UTC. The saved date never passes today.

The `def flatten_record(record, columns)` function flattens nested objects, so `{"contributors": {"deep_sleep": 99}}` becomes `{"contributors_deep_sleep": 99}`. Lists are serialized to JSON strings. Every row holds all of the table's declared columns. A nested object such as `readiness` arrives as an explicit null when it is missing, and that null is left out so it does not add a stray column, while a field Oura adds later passes through once it has a value.

Daily endpoints use `start_date` and `end_date` in YYYY-MM-DD format. The heart rate endpoint uses `start_datetime` and `end_datetime` in ISO 8601 format, handled by the `def build_date_params(table_config, window_start, window_end)` function.

## Error handling

The `def fetch_data_with_retry(session, url, params)` function retries HTTP 429, 500, 502, 503, and 504 responses, connection errors, and timeouts, up to 3 attempts with exponential backoff. When a retried response carries a `Retry-After` header in plain seconds, it waits for the longer of that header and the backoff, capped at 300 seconds. Any other form of the header, such as an HTTP date, falls back to the backoff.

A token refresh rejected with HTTP 400 or 401 stops the sync before any data request. When Oura's error code is `invalid_grant`, the message says the refresh token was already used or revoked; otherwise it asks you to check `client_id` and `client_secret`. The token request is retried on a connection error, an HTTP 429, or a 5xx response, because those usually mean Oura did not complete it. It reads `Retry-After` the same way as data requests, with the same 300-second cap, and backs off from 2 seconds over up to 3 retries. A gateway 502 or 504 can still arrive after Oura rotated the token, and then the next sync reports `invalid_grant`. A read timeout is not retried, because Oura may already have rotated the token.

An HTTP 401 on a data request stops the sync at once. An HTTP 403 means a missing scope or a lapsed subscription, and an HTTP 426 means the account needs an upgrade. On `daily_stress` the connector skips the table for either one and reports it with `op.warning`, because stress data depends on the subscription. On any other table a 403 stops the sync with a message that names the scope, and a 426 stops it with the upgrade message. Records with no primary key are skipped and counted in one `op.warning` summary.

Error messages carry the status and the URL only. Response bodies can hold health data, so they are never logged. Token errors add only the OAuth error code, such as `invalid_grant`, and never the token or the client secret.

## Tables created

### daily_activity

The `DAILY_ACTIVITY` table consists of the following columns:

- `id` (STRING, primary key): Unique identifier of the record
- `active_calories` (INT): Active calories expended in kilocalories
- `average_met_minutes` (DOUBLE): Average MET minutes
- `class_5_min` (STRING): 5-minute activity classification string
- `contributors_meet_daily_targets` (INT): Contribution of meeting previous 7-day daily activity targets in range [1, 100]
- `contributors_move_every_hour` (INT): Contribution of previous 24-hour inactivity alerts in range [1, 100]
- `contributors_recovery_time` (INT): Contribution of previous 7-day recovery time in range [1, 100]
- `contributors_stay_active` (INT): Contribution of previous 24-hour activity in range [1, 100]
- `contributors_training_frequency` (INT): Contribution of previous 7-day exercise frequency in range [1, 100]
- `contributors_training_volume` (INT): Contribution of previous 7-day exercise volume in range [1, 100]
- `day` (STRING): Date in YYYY-MM-DD format, in the wearer's time zone
- `equivalent_walking_distance` (INT): Equivalent walking distance of energe expenditure in meters
- `high_activity_met_minutes` (INT): The total METs of each minute classified as high activity
- `high_activity_time` (INT): The total time in seconds of each minute classified as high activity
- `inactivity_alerts` (INT): Number of inactivity alerts received
- `low_activity_met_minutes` (INT): The total METs of each minute classified as low activity
- `low_activity_time` (INT): The total time in seconds of each minute classified as low activity
- `medium_activity_met_minutes` (INT): The total METs of each minute classified as medium activity
- `medium_activity_time` (INT): The total time in seconds of each minute classified as medium activity
- `met_interval` (DOUBLE): Interval in seconds between the sampled items
- `met_items` (STRING): JSON-serialized array of MET samples
- `met_timestamp` (STRING): ISO 8601 timestamp of the first MET sample
- `meters_to_target` (INT): Meters remaining to target
- `non_wear_time` (INT): Ring non-wear time in seconds
- `resting_time` (INT): Resting time in seconds
- `score` (INT): Activity score in range [1, 100]
- `sedentary_met_minutes` (INT): Sedentary MET minutes
- `sedentary_time` (INT): Sedentary time in seconds
- `steps` (INT): Total number of steps taken
- `target_calories` (INT): Daily activity target in kilocalories
- `target_meters` (INT): Daily activity target in meters
- `timestamp` (STRING): ISO 8601 timestamp
- `total_calories` (INT): Total calories expended in kilocalories

### daily_sleep

The `DAILY_SLEEP` table consists of the following columns:

- `id` (STRING, primary key): Unique identifier of the record
- `contributors_deep_sleep` (INT): Contribution of deep sleep in range [1, 100]
- `contributors_efficiency` (INT): Contribution of sleep efficiency in range [1, 100]
- `contributors_latency` (INT): Contribution of sleep latency in range [1, 100]
- `contributors_rem_sleep` (INT): Contribution of REM sleep in range [1, 100]
- `contributors_restfulness` (INT): Contribution of sleep restfulness in range [1, 100]
- `contributors_timing` (INT): Contribution of sleep timing in range [1, 100]
- `contributors_total_sleep` (INT): Contribution of total sleep in range [1, 100]
- `day` (STRING): Date in YYYY-MM-DD format, in the wearer's time zone
- `score` (INT): Daily sleep score
- `timestamp` (STRING): ISO 8601 timestamp

### daily_readiness

The `DAILY_READINESS` table consists of the following columns:

- `id` (STRING, primary key): Unique identifier of the record
- `contributors_activity_balance` (INT): Contribution of cumulative activity balance in range [1, 100]
- `contributors_body_temperature` (INT): Contribution of body temperature in range [1, 100]
- `contributors_hrv_balance` (INT): Contribution of heart rate variability balance in range [1, 100]
- `contributors_previous_day_activity` (INT): Contribution of previous day's activity in range [1, 100]
- `contributors_previous_night` (INT): Contribution of previous night's sleep in range [1, 100]
- `contributors_recovery_index` (INT): Contribution of recovery index in range [1, 100]
- `contributors_resting_heart_rate` (INT): Contribution of resting heart rate in range [1, 100]
- `contributors_sleep_balance` (INT): Contribution of sleep balance in range [1, 100]
- `contributors_sleep_regularity` (INT): Contribution of sleep regularity in range [1, 100]
- `day` (STRING): Date in YYYY-MM-DD format, in the wearer's time zone
- `score` (INT): Daily readiness score
- `temperature_deviation` (DOUBLE): Temperature deviation in degrees Celsius
- `temperature_trend_deviation` (DOUBLE): Temperature trend deviation in degrees Celsius
- `timestamp` (STRING): ISO 8601 timestamp

### daily_stress

The `DAILY_STRESS` table consists of the following columns:

- `id` (STRING, primary key): Unique identifier of the record
- `day` (STRING): Date in YYYY-MM-DD format, in the wearer's time zone
- `day_summary` (STRING): Stress summary of full day
- `recovery_high` (INT): Time spent in a high recovery zone (bottom quartile data) in seconds
- `stress_high` (INT): Time spent in a high stress zone (top quartile of data) in seconds

### heart_rate

The `HEART_RATE` table consists of the following columns:

- `timestamp` (STRING, primary key): ISO 8601 timestamp of the reading
- `timestamp_unix` (LONG): Timestamp of the discrete sample as unix time in milliseconds
- `bpm` (INT): Heart rate as beats per minute
- `source` (STRING): Context of the reading: awake, rest, sleep, session, live, or workout
- `producer_timestamp` (STRING): Timestamp the reading was produced, which Oura returns although its API specification does not list it

### sleep

The `SLEEP` table consists of the following columns:

- `id` (STRING, primary key): Unique identifier of the record
- `average_breath` (DOUBLE): Average breathing rate during sleep as breaths/minute
- `average_heart_rate` (DOUBLE): Average heart rate during sleep in beats per minute
- `average_hrv` (INT): Average heart rate variability during sleep
- `awake_time` (INT): Duration spent awake in seconds
- `bedtime_end` (STRING): ISO 8601 end of the sleep period
- `bedtime_start` (STRING): ISO 8601 start of the sleep period
- `day` (STRING): Date in YYYY-MM-DD format, in the wearer's time zone
- `deep_sleep_duration` (INT): Duration spent in deep sleep in seconds
- `efficiency` (INT): Sleep efficiency rating in range [1, 100]
- `heart_rate_interval` (DOUBLE): Interval in seconds between the sampled items
- `heart_rate_items` (STRING): JSON-serialized array of heart rate samples
- `heart_rate_timestamp` (STRING): ISO 8601 timestamp of the first heart rate sample
- `hrv_interval` (DOUBLE): Interval in seconds between the sampled items
- `hrv_items` (STRING): JSON-serialized array of HRV samples
- `hrv_timestamp` (STRING): ISO 8601 timestamp of the first HRV sample
- `latency` (INT): Time taken to fall asleep in seconds
- `light_sleep_duration` (INT): Duration spent in light sleep in seconds
- `low_battery_alert` (BOOLEAN): Flag indicating if a low battery alert occurred
- `lowest_heart_rate` (INT): Lowest heart rate during sleep
- `movement_30_sec` (STRING): 30-second movement classification string
- `period` (INT): ECore sleep period identifier
- `readiness_contributors_activity_balance` (INT): Contribution of cumulative activity balance in range [1, 100]
- `readiness_contributors_body_temperature` (INT): Contribution of body temperature in range [1, 100]
- `readiness_contributors_hrv_balance` (INT): Contribution of heart rate variability balance in range [1, 100]
- `readiness_contributors_previous_day_activity` (INT): Contribution of previous day's activity in range [1, 100]
- `readiness_contributors_previous_night` (INT): Contribution of previous night's sleep in range [1, 100]
- `readiness_contributors_recovery_index` (INT): Contribution of recovery index in range [1, 100]
- `readiness_contributors_resting_heart_rate` (INT): Contribution of resting heart rate in range [1, 100]
- `readiness_contributors_sleep_balance` (INT): Contribution of sleep balance in range [1, 100]
- `readiness_contributors_sleep_regularity` (INT): Contribution of sleep regularity in range [1, 100]
- `readiness_score` (INT): Readiness score in range [1, 100]
- `readiness_temperature_deviation` (DOUBLE): Temperature deviation in degrees Celsius
- `readiness_temperature_trend_deviation` (DOUBLE): Temperature trend deviation in degrees Celsius
- `readiness_score_delta` (INT): Effect on readiness score caused by this sleep period
- `rem_sleep_duration` (INT): Duration spent in REM sleep in seconds
- `restless_periods` (INT): Number of restless periods during sleep
- `sleep_algorithm_version` (STRING): Version of the sleep algorithm used to calculate the sleep data
- `sleep_analysis_reason` (STRING): The reason for the creation or update of the latest version of this sleep
- `sleep_phase_30_sec` (STRING): 30-second sleep phase classification string
- `sleep_phase_5_min` (STRING): 5-minute sleep phase classification string
- `sleep_score_delta` (INT): Effect on sleep score caused by this sleep period
- `time_in_bed` (INT): Duration spent in bed in seconds
- `total_sleep_duration` (INT): Total sleep duration in seconds
- `type` (STRING): Type of period: deleted, sleep, long_sleep, late_nap, or rest. Oura uses `deleted` for a period the user deleted and `rest` for one it rejected as a false detection, and the connector syncs these rows like any other, so filter them out before you total sleep durations
- `ring_id` (STRING): Identifier of the ring; deprecated by Oura and usually absent
- `app_sleep_phase_5_min` (STRING): 5-minute sleep phase classification aligned with the Oura app

## Additional files

- **`tables.py`** – The endpoint, date parameters, and column types of every table.
- **`oura_auth.py`** – The OAuth2 token refresh, and the Fernet encryption of the rotated refresh token kept in state.
- **`setup_form.py`** – The setup form Fivetran renders in the dashboard: one field per configuration key, with the client secret, refresh token, and Fernet key masked. The connector supplies the default and the setup test.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
