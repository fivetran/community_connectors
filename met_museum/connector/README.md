---
name: "Met Museum"
description: "Syncs public-domain objects, artists and curator tags from The Metropolitan Museum of Art Collection API, with optional Snowflake Cortex vision enrichment."
category: "SaaS & APIs"
---

# Met Museum Connector Example

## Connector overview

This connector syncs public-domain objects from [The Metropolitan Museum of Art Collection API](https://metmuseum.github.io/) into a Fivetran destination. For each object in the departments you choose, it writes the catalog record (title, artist, date, medium, dimensions, place of origin, image links, credit line), its constituents (artists, makers and publishers with their roles) and the subject tags the Met's curators assigned. The API needs no key, and every object it calls public domain is released under Creative Commons Zero, so the data can be reused without restriction.

The connector can also enrich each object's image with a vision model through the Snowflake Cortex REST API. The model classifies the subject, scene, style period, colors and materials into fixed sets of values, lists the things it can see, and writes a short alt-text description. The connector then measures how many of the curator's tags the model's list of visible things agrees with. Enrichment is off by default, so a data-only sync needs no Snowflake account.

The connector runs unchanged under `fivetran debug` and under `fivetran run`, which Connector SDK 3.0 adds to sync into an Iceberg REST catalog instead of a local DuckDB file (refer to [Writing to an Iceberg catalog with `fivetran run`](#writing-to-an-iceberg-catalog-with-fivetran-run)).

The context pack in [`../context_pack/`](../context_pack/README.md) connects an AI agent to the synced data. It has a dbt project with Semantic Layer metrics, instructions for the agent, a check that Snowflake and DuckDB return the same results for the models and the checked questions over the same Managed Data Lake tables, and a script that publishes that context to an `AGENTS` schema in Snowflake. An agent can also query the metrics through a dbt MCP server.

Typical uses are collection search and recommendation over open-access art, image datasets with curated subject labels, and comparing a model's reading of an image with a curator's.

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
fivetran init --template met_museum/connector
```

`fivetran init` initializes a new Connector SDK project by setting up the project structure, configuration files, and a connector you can run immediately with `fivetran debug`. For more information on `fivetran init`, refer to the [Connector SDK `init` documentation](https://fivetran.com/docs/connector-sdk/getting-started/connector-sdk-commands#fivetraninit).

> Note: Ensure you have updated the `configuration.json` file with the necessary parameters before running `fivetran debug`. See the [Configuration file](#configuration-file) section for details on the required configuration parameters.

## Features

- Syncs the objects of one or more Met departments, keeping only public-domain objects.
- Historical sync of every object in the configured departments, then incremental syncs of only the objects the Met has changed since (refer to `def run_walk(context, state, counts)`).
- Deletes the row of an object the API no longer has, or that is no longer public domain (refer to `def process_object(context, department_id, object_id, state, counts)`).
- Resumes a capped sync at the next object id and a failed one after its last checkpoint, with a checkpoint every 25 objects.
- Spaces requests to 75 a minute, below the rate at which the API's CDN starts blocking clients.
- Optional Snowflake Cortex vision enrichment, validated against fixed value sets, with a curator-tag agreement score (refer to `cortex.py`).
- An object is enriched again only when its image URL, catalog text, tags, model or prompt version change.

## Configuration file

The connector reads the following keys from `configuration.json`:

```json
{
  "department_ids": "<DEPARTMENT_IDS>",
  "max_objects_per_sync": "<MAX_OBJECTS_PER_SYNC>",
  "enable_cortex": "<ENABLE_CORTEX>",
  "snowflake_account": "<SNOWFLAKE_ACCOUNT>",
  "snowflake_pat_token": "<SNOWFLAKE_PAT_TOKEN>",
  "cortex_model": "<CORTEX_MODEL>",
  "max_enrichments_per_sync": "<MAX_ENRICHMENTS_PER_SYNC>"
}
```

| Key | Required | Description |
| --- | --- | --- |
| `department_ids` | Yes | Comma-separated Met department ids, for example `11` or `11, 21`. The `department` table lists every id. Department 11, European Paintings, holds about 2,650 objects. Reading them all takes about 35 minutes, which the default `max_objects_per_sync` of 1000 spreads over three syncs. |
| `max_objects_per_sync` | No | Object detail requests per sync, from 1 to 100000. Defaults to 1000. A capped sync continues from the next object id on the next sync. |
| `enable_cortex` | No | `true` to enrich each object's image with Snowflake Cortex. Defaults to `false`. |
| `snowflake_account` | When `enable_cortex` is `true` | The Snowflake account host, for example `abc12345.us-east-1.snowflakecomputing.com`, without `https://`. |
| `snowflake_pat_token` | When `enable_cortex` is `true` | A Snowflake programmatic access token whose role can use Cortex. |
| `cortex_model` | No | The Cortex model, which must accept images. Defaults to `claude-sonnet-5`. |
| `max_enrichments_per_sync` | No | Cortex calls per sync, from 1 to 5000. Defaults to 25. While enrichment is on, a sync stops when this budget is used and the next sync continues from the next object. |

Any key left as its `<...>` placeholder takes its default, so a data-only sync needs only `department_ids`.

> Note: When submitting connector code as a community connector in the open-source [Community Connector repository](https://github.com/fivetran/community_connectors/tree/main), ensure the `configuration.json` file has placeholder values. When adding the connector to your production repository, ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file

This connector does not require a `requirements.txt` file: it uses only `requests` and the Python standard library.

> Note: [Some packages](https://fivetran.com/docs/connector-sdk/technical-reference#preinstalledpackages) are pre-installed in the Connector SDK runtime environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

The Collection API needs no authentication. The connector sends a `User-Agent` that identifies it as this example.

Cortex enrichment authenticates with a Snowflake programmatic access token, sent as a bearer token (refer to `def create_session(snowflake_pat_token)` in `cortex.py`). To create one:

1. In Snowsight, go to **Settings** > **Authentication**.
2. In the Programmatic access tokens section, generate a new token for a role that has the `SNOWFLAKE.CORTEX_USER` database role.
3. Copy the token into `snowflake_pat_token`.

## Pagination

The API has no paged object list. `GET /objects?departmentIds=<id>` returns every object id in a department in one response, unsorted, and adding `metadataDate=<YYYY-MM-DD>` returns only the ids changed on or after that date. The connector sorts the ids and requests each object's detail from `GET /objects/<id>`, walking departments and ids in ascending order (refer to `def walk_department(context, department_id, state, counts)`). State records the department and the last object id processed, so a sync stopped by `max_objects_per_sync` or `max_enrichments_per_sync` resumes at the next id, and a failed one at the id after its last checkpoint.

## Data handling

- Historical and incremental syncs – the first sync, and any sync after `department_ids` changes, reads every object in the configured departments. When every department is finished, the cursor moves to the day before the walk's first sync started, and later syncs read only the ids `metadataDate` returns for it. The API accepts a date, not a time, so each incremental sync reads a day of changes again, and the upsert makes that harmless.
- Public-domain filter – the object list includes objects that are not public domain, so the connector reads each object's `isPublicDomain` and deletes the row of any object where it is `false`, including one that was public domain at an earlier sync. The connector skips an object whose detail has no `isPublicDomain`, or a value other than `true` or `false`, and counts it in the sync's dashboard warning. Its row is neither written nor deleted. The connector keeps the id in state and reads the object again in each later sync that finishes its walk, within `max_objects_per_sync`, until its detail has a `true` or `false` `isPublicDomain`. An object the API answers with `404 ObjectID not found` is deleted too.
- Child tables – `object_constituent` and `object_tag` rows carry the object's `metadata_date` in `object_metadata_date`. The API returns only an object's current constituents and tags, so when the Met removes one, its row stays in the destination with an older stamp. A child row is current when `object_metadata_date` equals the `metadata_date` of a live `object` row. The `object_depicted_element` rows follow the same rule with `enrichment_hash`, a stamp that is unique to each enrichment.
- Field names – API fields are renamed to snake_case, and `state` becomes `state_province`. Empty strings, which the API sends for most unset fields, are written as null. `additionalImages` and `measurements` are not synced. The row keeps the count of additional images.
- Enrichment (refer to `def enrich_object(context, obj, department_id, object_id, state, counts)`) – the connector downloads `primaryImageSmall` and sends it, with the title, artist, date, medium and classification, to the Cortex Chat Completions endpoint as a base64 data URL. The prompt lists the allowed values for each categorical field, and the validator accepts only those values: anything else is stored as null, and a row is written only when at least one field was accepted. The connector keeps a hash of each object's enrichment inputs in state and skips the Cortex call while the hash is unchanged. The `enrichment_hash` column holds a hash of that input hash and `enriched_at`. If an object's inputs change and then change back, its input hash repeats, but the new enrichment has a new `enriched_at` and a new `enrichment_hash`. Element rows from the earlier enrichment keep the old value and no longer match. When an object loses its image, the connector deletes its `object_visual_attribute` row and keeps its `object` row. Turning enrichment on, including turning it back on with the same model after syncs with it off, or changing `cortex_model`, starts the historical walk again so existing objects are enriched.
- Curator agreement – `curator_tag_agreement` is the share of the object's curator tags that match a depicted element, ignoring case and regular plural endings. A tag matches when the words of one are all in the other, so the tag `Trees` matches `cypress trees` and the tag `Cypresses` matches `cypress`. Irregular plurals such as `Women` do not match their singular.

### Writing to an Iceberg catalog with `fivetran run`

Connector SDK 3.0 adds `fivetran run`, which syncs into an Iceberg REST catalog instead of the local DuckDB file `fivetran debug` writes, and the connector runs under it unchanged. Describe the catalog and its storage in a `destination.json` file (the catalog `uri`, `warehouse`, `namespace`, OAuth `client_id`, `client_secret` and `scope`, and the `s3.*` storage settings), then run:

```bash
fivetran reset
fivetran run --configuration configuration.json --destination-file destination.json
```

`fivetran run` and `fivetran debug` share `files/state.json`, so run `fivetran reset` before the first sync into a new namespace. To read the tables back in DuckDB from a catalog that does not vend storage credentials, such as a local Apache Polaris catalog over MinIO, create an S3 secret for the storage and an Iceberg secret for the catalog, and attach it with `ACCESS_DELEGATION_MODE 'none'`. Without that option DuckDB asks the catalog for credentials and the catalog rejects the request:

```sql
ATTACH '<warehouse>' AS lake (
  TYPE ICEBERG,
  SECRET lake_catalog,
  ENDPOINT '<catalog uri>',
  ACCESS_DELEGATION_MODE 'none'
);
select title, artist_display_name from lake.met_museum.object limit 10;
```

## Error handling

- Rate limiting – in testing, requests spaced 0.35 seconds apart drew HTTP 403 from the API's CDN, and each block lasted about a minute. The connector spaces requests 0.8 seconds apart, 75 a minute, and retries a 403 after 60 seconds (refer to `def send_request(client, url, params, not_found_ok, accept, error_status_ok, stream)`).
- Transient failures – HTTP 429, 500, 502, 503 and 504, connection errors and timeouts are tried up to five times in all, with exponential backoff and jitter between attempts. Any other status, and any other request error, fails the sync with the status and the start of the response body. An error status from the image host is handled differently. After the retries for a retryable one, it leaves that object unenriched and pending, and the sync continues. A connection error or timeout on every attempt still fails the sync. The connector reads an image in 64 KiB chunks and drops it once it passes 10 MiB. When the `Content-Length` header is over 10 MiB, it drops the image without reading it. A download that breaks off leaves the object pending.
- Deletes – an object row is deleted only when the API returns `404` with the body `{"message": "ObjectID not found"}`, or the object detail has `isPublicDomain` set to `false`. A 404 with any other body, or an object list without the documented `total` and `objectIDs` fields, fails the sync and deletes nothing. The sync also fails on an object list with fewer ids than its `total` and on an id in the list that is not a whole number. The walk checkpoints past every id it reads, and skipping one of these cases would let it checkpoint past an object it never read. An object detail whose `objectID` differs from the id requested is skipped like a detail with no `isPublicDomain`, and counted the same way. Once a sync's walk is finished, the connector reads the skipped objects again with what is left of `max_objects_per_sync` (refer to `def reread_malformed_details(context, state, counts)`).
- Configuration – `def validate_configuration(configuration)` rejects a missing or malformed `department_ids`, a count of zero or above its ceiling, a value of `enable_cortex` other than true or false, and, when enrichment is on, a missing token, a `snowflake_account` with a scheme, a path, a host outside `snowflakecomputing.com` or any character other than letters, digits, hyphens, underscores and the dots between labels, or a `cortex_model` with characters other than lowercase letters, digits, dots and hyphens. A department id the API does not list fails the sync with the list of valid ids.
- Cortex failures – HTTP 400, 401, 403, 404 and any other permanent error from Cortex stop the sync with a message naming the settings to check. HTTP 429, 500, 502, 503 and 504, timeouts, connection errors and a body that is not valid JSON are tried four times in all, and then the object is left unenriched for this sync. The connector keeps the id of each object whose image could not be downloaded or whose enrichment failed in state. Once a sync's walk is finished, the connector reads those objects again and enriches them with what is left of that sync's `max_objects_per_sync` and `max_enrichments_per_sync` limits. Each retry pass starts after the id the previous pass tried last, wraps around to the first, and checkpoints every 25 objects like the walk. An object the walk read in the same sync waits for the next one. An id is removed once its object is enriched, deleted, no longer public domain, or has no image (refer to `def retry_pending_enrichments(context, state, counts)`). Objects without an image, images that could not be downloaded, enrichments that returned no valid field, and objects written with no readable `metadataDate` are counted and reported in one warning at the end of the sync (refer to `def report_counts(counts)`).

## Tables created

The connector creates six tables (refer to `def schema(configuration)`). `object_visual_attribute` and `object_depicted_element` stay empty unless `enable_cortex` is `true`.

### department

Every department of the Met. Primary key `department_id`.

| Column | Type | Description |
| --- | --- | --- |
| `department_id` | INT | The department id. Primary key |
| `display_name` | STRING | The department name, for example `European Paintings` |

### object

One row per public-domain object. Primary key `object_id`.

| Column | Type | Description |
| --- | --- | --- |
| `object_id` | INT | The object id. Primary key. From the API field `objectID` |
| `department_id` | INT | The configured department whose object list returned the object |
| `department` | STRING | The department name on the object record |
| `is_public_domain` | BOOLEAN | Always true for a live row |
| `is_highlight` | BOOLEAN | Whether the Met lists the object as a highlight of its collection |
| `is_timeline_work` | BOOLEAN | Whether the object appears in the Heilbrunn Timeline of Art History |
| `accession_number` | STRING | The Met's identifying number for the object |
| `accession_year` | STRING | The year the object entered the collection |
| `primary_image` | STRING | URL of the full-size image |
| `primary_image_small` | STRING | URL of the web-size image, the one sent to Cortex |
| `additional_image_count` | INT | The number of additional images |
| `object_name` | STRING | The type of object, for example `Painting` |
| `title` | STRING | The title |
| `culture` | STRING | The culture the object comes from |
| `period` | STRING | The period the object comes from |
| `dynasty` | STRING | The dynasty the object comes from |
| `reign` | STRING | The reign the object comes from |
| `portfolio` | STRING | The portfolio or series the object belongs to |
| `artist_role` | STRING | The primary artist's role |
| `artist_prefix` | STRING | Text before the artist's name, for example `Workshop of` |
| `artist_display_name` | STRING | The primary artist's name |
| `artist_display_bio` | STRING | The primary artist's nationality and life dates |
| `artist_suffix` | STRING | Text after the artist's name |
| `artist_alpha_sort` | STRING | The artist's name in sort order |
| `artist_nationality` | STRING | The artist's nationality |
| `artist_begin_date` | STRING | The artist's birth year |
| `artist_end_date` | STRING | The artist's death year |
| `artist_gender` | STRING | The artist's gender, when the Met records it |
| `artist_wikidata_url` | STRING | The artist's Wikidata URL |
| `artist_ulan_url` | STRING | The artist's Getty ULAN URL |
| `object_date` | STRING | The date as the Met writes it, for example `1889` or `ca. 1665` |
| `object_begin_date` | INT | The earliest year the object could date from |
| `object_end_date` | INT | The latest year the object could date from |
| `medium` | STRING | The materials, for example `Oil on canvas` |
| `dimensions` | STRING | The dimensions as text |
| `credit_line` | STRING | How the object was acquired |
| `geography_type` | STRING | How the place fields relate to the object, for example `Made in` |
| `city` | STRING | The city |
| `state_province` | STRING | The state or province. From the API field `state` |
| `county` | STRING | The county |
| `country` | STRING | The country |
| `region` | STRING | The region |
| `subregion` | STRING | The subregion |
| `locale` | STRING | The locale |
| `locus` | STRING | The locus |
| `excavation` | STRING | The excavation the object comes from |
| `river` | STRING | The river |
| `classification` | STRING | The Met's classification, for example `Paintings` |
| `rights_and_reproduction` | STRING | The rights holder, when there is one |
| `link_resource` | STRING | A related URL |
| `repository` | STRING | The holding institution |
| `object_url` | STRING | The object's page on metmuseum.org |
| `object_wikidata_url` | STRING | The object's Wikidata URL |
| `gallery_number` | STRING | The gallery where the object is on view |
| `metadata_date` | UTC_DATETIME | When the Met last updated the record |

### object_constituent

The people and organizations credited on an object. Primary key `object_id`, `constituent_id`, `role`.

| Column | Type | Description |
| --- | --- | --- |
| `object_id` | INT | The object id. Primary key |
| `constituent_id` | INT | The Met's constituent id. Primary key |
| `role` | STRING | The constituent's role, for example `Artist`, or empty when the Met records none. Primary key |
| `name` | STRING | The constituent's name |
| `gender` | STRING | The constituent's gender, when the Met records it |
| `constituent_ulan_url` | STRING | The constituent's Getty ULAN URL |
| `constituent_wikidata_url` | STRING | The constituent's Wikidata URL |
| `object_metadata_date` | UTC_DATETIME | The object's `metadata_date` when this row was written |

### object_tag

The subject tags the Met's curators assigned to an object. Primary key `object_id`, `term`.

| Column | Type | Description |
| --- | --- | --- |
| `object_id` | INT | The object id. Primary key |
| `term` | STRING | The tag, for example `Landscapes`. Primary key |
| `aat_url` | STRING | The tag's Getty Art and Architecture Thesaurus URL |
| `wikidata_url` | STRING | The tag's Wikidata URL |
| `object_metadata_date` | UTC_DATETIME | The object's `metadata_date` when this row was written |

### object_visual_attribute

The Cortex vision enrichment of an object's image. Primary key `object_id`.

| Column | Type | Description |
| --- | --- | --- |
| `object_id` | INT | The object id. Primary key |
| `subject_category` | STRING | One of `landscape`, `portrait`, `religious`, `mythological`, `history`, `genre_scene`, `still_life`, `animal`, `architecture`, `marine`, `abstract`, `other` |
| `scene_type` | STRING | One of `outdoor`, `indoor`, `mixed`, `none` |
| `style_period` | STRING | One of `medieval`, `renaissance`, `baroque`, `rococo`, `neoclassical`, `romantic`, `realist`, `impressionist`, `post_impressionist`, `symbolist`, `modern`, `other` |
| `dominant_colors` | JSON | A list of colors from a fixed set of thirteen. The prompt asks for up to five. The connector keeps every allowed color the model returns, and a row can hold more than five |
| `materials_visible` | JSON | A list of materials from a fixed set of seventeen |
| `visual_description` | STRING | Two or three sentences describing what the image shows, written as alt text |
| `depicted_element_count` | INT | The number of `object_depicted_element` rows written with this enrichment |
| `curator_tag_agreement` | DOUBLE | The share of curator tags that match a depicted element, from 0 to 1, or null when the object has no tags |
| `curator_tags_matched` | INT | The number of curator tags that match a depicted element |
| `curator_tags_missed` | INT | The number of curator tags that match none |
| `enrichment_model` | STRING | The Cortex model that produced the row |
| `enrichment_hash` | STRING | A hash of the enrichment inputs' hash and `enriched_at`, different for each enrichment |
| `enriched_at` | UTC_DATETIME | When the enrichment ran |

### object_depicted_element

The things the model saw in an object's image, one row each. Primary key `object_id`, `element`.

| Column | Type | Description |
| --- | --- | --- |
| `object_id` | INT | The object id. Primary key |
| `element` | STRING | A lowercase noun phrase, for example `cypress`. Primary key |
| `enrichment_hash` | STRING | The `enrichment_hash` of the enrichment that wrote this row |

## Additional files

- **`transform.py`** – Maps Collection API responses to destination rows, with no requests, configuration or state, so the mapping can be tested on its own.
- **`cortex.py`** – The optional Snowflake Cortex vision enrichment: settings validation, the prompt and the allowed values it shares with the validator, the REST call and its retries, and the curator-tag agreement score.

## Additional considerations

An object removed from a department's object list without a change to its record, or moved to a department you did not configure, keeps its row, because the connector cannot see it any more. Removing a department from `department_ids` also keeps that department's rows.

With enrichment on, the state holds one enrichment hash per enriched object and is never trimmed, so it grows by about 30 bytes for each object enriched. With European Paintings (department 11), 2,365 hashes come to about 70 KB. Configure many departments and the state, which is saved at every checkpoint, grows with them.

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
