"""Module-level constants for the ZoomInfo connector.

Tunable settings (page size, worker pool size, retry policy, checkpoint
cadence) and fixed values (endpoint paths, default output-field lists, state
keys) shared across the connector's modules live here. Imported by name from
client.py, config.py, transforms.py, sync.py, and connector.py.
"""

TOKEN_URL = "https://api.zoominfo.com/gtm/oauth/v1/token"
SEARCH_BASE = "https://api.zoominfo.com"

# Search endpoints (free — no credits, records/requests counted)
ENDPOINT_CONTACTS = "/gtm/data/v1/contacts/search"
ENDPOINT_COMPANIES = "/gtm/data/v1/companies/search"
ENDPOINT_SCOOPS = "/gtm/data/v1/scoops/search"
ENDPOINT_INTENT = "/gtm/data/v1/intent/search"
ENDPOINT_NEWS = "/gtm/data/v1/news/search"

# Enrich endpoints (cost credits per company/contact)
ENDPOINT_CONTACTS_ENRICH = "/gtm/data/v1/contacts/enrich"
ENDPOINT_COMPANIES_ENRICH = "/gtm/data/v1/companies/enrich"
ENDPOINT_SCOOPS_ENRICH = "/gtm/data/v1/scoops/enrich"
ENDPOINT_TECHNOLOGIES_ENRICH = "/gtm/data/v1/companies/technologies/enrich"
ENDPOINT_CORP_HIER_ENRICH = "/gtm/data/v1/companies/corporate-hierarchy/enrich"

# Usage endpoint (free)
ENDPOINT_USAGE = "/gtm/data/v1/users/usage"

# JSON:API type name per endpoint
JSONAPI_TYPE = {
    ENDPOINT_CONTACTS: "ContactSearch",
    ENDPOINT_COMPANIES: "CompanySearch",
    ENDPOINT_SCOOPS: "ScoopSearch",
    ENDPOINT_INTENT: "IntentSearch",
    ENDPOINT_NEWS: "NewsSearch",
    ENDPOINT_CONTACTS_ENRICH: "ContactEnrich",
    ENDPOINT_COMPANIES_ENRICH: "CompanyEnrich",
    ENDPOINT_SCOOPS_ENRICH: "ScoopEnrich",
    ENDPOINT_TECHNOLOGIES_ENRICH: "TechnologyEnrich",
    ENDPOINT_CORP_HIER_ENRICH: "CorporateHierarchyEnrich",
}

# Pagination via URL query params: page[size] and page[number]
PAGE_SIZE = 100

# Safety cap on pages per endpoint per sync (client-side backstop). At
# PAGE_SIZE=100 this is 1M records. See SEARCH_RESULT_CEILING below for the
# server-side limit that is reached first in practice.
MAX_PAGES = 10000

# Server-side ceiling on a single ZoomInfo Search query. The Search API returns
# at most 100 pages (meta.page.total caps at 100), i.e. PAGE_SIZE * 100 = 10,000
# records, regardless of how large meta.totalResults is. A query whose universe
# exceeds this returns only the first 10,000 records — the remainder are NOT
# retrievable by paging further. The connector cannot page past this; the only
# way to capture more is to narrow the query (e.g. a tighter `countries` filter)
# so each query's universe stays under the ceiling. paginate() logs a WARNING
# when totalResults exceeds this so silent truncation is visible. See the
# "Known limitations" section of the README.
SEARCH_RESULT_CEILING = PAGE_SIZE * 100  # = 10,000

# Enrich API accepts up to 25 records per request (contacts and companies)
ENRICH_BATCH_SIZE = 25

# Worker pool size for per-company enrich loops (scoops, technologies).
# Lowered from 5 to 3 in 2026-05-28 — Fivetran cloud OOM'd with 5 workers
# accumulating per-company row lists in memory. Streaming via queue also added;
# see _stream_per_company_enrich. Don't raise above 3 without re-validating
# memory under the 1 GB cloud limit.
ENRICH_WORKERS = 3

# Bounded queue size for the per-company streaming pattern. Caps peak memory
# at roughly QUEUE_MAX × avg_row_size bytes. At ~5 KB/row → ~10 MB ceiling.
# Workers block on put() when queue is full, providing backpressure.
ENRICH_QUEUE_MAX = 2000

# Intra-table checkpoint cadence for high-volume streams. Fivetran best
# practice is ~1K rows or ~10 min per checkpoint — without intermediate
# checkpoints the entire table buffers in flight and OOMs the runtime.
# See https://fivetran.com/docs/connector-sdk/best-practices
CHECKPOINT_EVERY_N_ROWS = 1000

# Default country filter when the customer leaves `countries` blank.
DEFAULT_COUNTRY = "United States"

# Default output fields for Contact Enrichment
# Note: linkedInUrl is NOT a valid field — LinkedIn data is not available via this API.
# Note: managementLevel returns a list (e.g. ["C-Level"]) — stored as JSON string.
# Note: companyName is nested under company.name in the response, not a top-level field.
DEFAULT_CONTACT_ENRICH_FIELDS = [
    "firstName",
    "lastName",
    "email",
    "phone",
    "mobilePhone",
    "jobTitle",
    "jobFunction",
    "managementLevel",
    "companyId",
    "companyName",
    "companyWebsite",
    "companyRevenue",
    "companyEmployeeCount",
    "companyPrimaryIndustry",
    "city",
    "region",
    "state",
    "country",
    "directPhoneDoNotCall",
    "mobilePhoneDoNotCall",
    "lastUpdatedDate",
    "validDate",
    "contactAccuracyScore",
]

# Default output fields for Company Enrichment
# Confirmed valid via:
#   GET /gtm/data/v1/lookup/enrich?filter[entity]=company&filter[fieldType]=output
# Invalid names we fixed: "industry" -> "primaryIndustry", "companyType" -> "type"
DEFAULT_COMPANY_ENRICH_FIELDS = [
    "name",
    "website",
    "phone",
    "revenue",
    "revenueRange",
    "employeeCount",
    "employeeRange",
    "employeeGrowth",
    "primaryIndustry",
    "industries",
    "city",
    "state",
    "country",
    "street",
    "zipCode",
    "metroArea",
    "description",
    "ticker",
    "type",
    "naicsCodes",
    "sicCodes",
    "foundedYear",
    "companyStatus",
    "parentId",
    "parentName",
    "ultimateParentId",
    "ultimateParentName",
    "totalFundingAmount",
    "recentFundingAmount",
    "recentFundingDate",
    "socialMediaUrls",
    "logo",
    "lastUpdatedDate",
    "isDefunct",
]

# Default output fields for Corporate Hierarchy Enrichment
# Confirmed valid via:
#   GET /gtm/data/v1/lookup/enrich?filter[entity]=corporate-hierarchy&filter[fieldType]=output
# Only 4 valid fields exist: id, companyId, parentage, familyTree
# Note: id is the primary key (auto-returned), companyId is the ZoomInfo company ID,
#       parentage lists companies higher up in the hierarchy,
#       familyTree lists all companies and locations in the family tree.
# No standard company fields (name, website, city, etc.) are valid here —
# use companies_enriched table for those details.
DEFAULT_CORP_HIER_FIELDS = [
    "companyId",
    "parentage",
    "familyTree",
]

# Retry settings for transient HTTP failures
MAX_RETRIES = 5
RETRY_BASE_WAIT = 2  # seconds — doubles each retry (exponential backoff)
RETRY_MAX_WAIT = 60  # seconds — cap on any single wait

# HTTP status codes that should trigger a retry (429 + transient 5xx).
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# (connect, read) timeout for every outbound request. Without this, a hung
# ZoomInfo connection would stall the sync forever.
REQUEST_TIMEOUT = (10, 60)

# State keys persisted via op.checkpoint between syncs. Centralized so a typo
# at one call-site can't silently break incremental sync (a wrong key reads as
# "no prior state" and re-pulls the full universe).
STATE_CONTACTS_LAST_UPDATED = "contacts_last_updated"
STATE_COMPANIES_LAST_UPDATED = "companies_last_updated"
STATE_SCOOPS_LAST_UPDATED = "scoops_last_updated"
STATE_INTENT_LAST_UPDATED = "intent_last_updated"
STATE_NEWS_LAST_UPDATED = "news_last_updated"
