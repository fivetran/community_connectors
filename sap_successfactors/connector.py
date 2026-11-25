"""Fivetran Connector SDK connector for SAP SuccessFactors OData v2.

Design: every entity below is synced with ALL of its real, viewable fields,
including every field declared in metadata even when a given record omits it
(SAP commonly omits null/unused optional properties from the JSON payload;
those are emitted as NULL so every metadata-declared column always exists in
the destination table) and every navigation-link property (emitted as the
unexpanded link URI, since expanding them would need a separate API call per
record per link). All date/time values are normalized from SAP's raw
`/Date(milliseconds)/` wire format to standard UTC ISO 8601 strings. Primary
keys, pagination order, and effective-dating behavior are derived at runtime
from the tenant's authenticated OData `$metadata` (never hand-typed), which
also protects against silent primary-key drift as the SuccessFactors instance
evolves. Effective-dated entities are queried across the full history window
(`fromDate`/`toDate`) instead of SAP's implicit `asOfDate=today` default, so
every historical and future-dated record is extracted rather than only the
slice effective right now.
"""

from datetime import (
    datetime,
    timedelta,
    timezone,
)  # For UTC datetime arithmetic and ISO 8601 formatting
import base64  # For encoding the SAP OAuth SAML2 assertion payload
import binascii  # For detecting invalid base64 in the private key
import json  # For serialising inline navigation expansions stored as JSON columns
import re  # For the compiled OData field-name normalisation patterns
import threading  # For the token-refresh lock and rate-limiter lock across parallel workers
import time  # For rate-limiter timing, checkpoint cadence, and backoff sleeps
import urllib.parse  # For URL-encoding OData query parameters
from collections import deque  # For the bounded prefetch queue in parallel page fetching
from concurrent.futures import (
    ThreadPoolExecutor,
)  # For parallel entity page and form page fetching
import xml.etree.ElementTree as ET  # For parsing the OData $metadata XML document

import requests  # For HTTP sessions, retries, and OAuth token requests

from fivetran_connector_sdk import (
    Connector,
    Logging as log,
    Operations as op,
)  # Fivetran Connector SDK

BASE_URL = ""  # Set at runtime from configuration["sf_api_url"]; see build_session()
LAST_MODIFIED_FIELD = "lastModifiedDateTime"
LAST_MODIFIED_COLUMN = "last_modified_date_time"
INCREMENTAL_OVERLAP_SECONDS = 60
# Above this share of entities failing in one sync, the run is reported as failed
# instead of partially succeeding. Isolated entity failures are expected and
# tolerated; a broad failure means credentials, the source, or the connector is
# broken, and a "successful" sync would hide that the destination went stale.
MAX_FAILED_ENTITY_RATIO = 0.25
# Destination table recording per-entity sync failures, so they are queryable in the
# warehouse instead of only visible in the Fivetran dashboard log.
SYNC_ISSUE_TABLE = "connector_sync_issues"
# A full pass over every entity runs for hours, and a single large entity can page for
# hours on its own, so progress is checkpointed on a time cadence *while paging* rather
# than only between entities. Fivetran's guidance is to checkpoint long operations
# roughly every 10 minutes and never more than once a minute.
CHECKPOINT_INTERVAL_SECONDS = 300
# SuccessFactors applies an implicit `asOfDate=today` to every effective-dated
# entity when no date parameter is supplied, so an unqualified query silently
# returns only the slice of each record effective right now (e.g. EmpJob
# returns one current row per employment instead of its full job history).
# Sending a standalone, maximal `fromDate`/`toDate` window instead makes SAP
# return every past, current, and future-dated record. These are standalone
# query parameters, not `$filter` expressions, and they combine with `$filter`,
# `$orderby`, and `__next` pagination.
HISTORY_FROM_DATE = "1900-01-01"
HISTORY_TO_DATE = "9999-12-31"
# Effective-dated entities are identified generically from live `$metadata`:
# SuccessFactors models effective dating by putting an effective-start date of
# type `Edm.DateTime` in the entity's key. SAP uses three names for it across
# EC (`startDate`), MDF (`effectiveStartDate`), and system-effective MDF
# (`mdfSystemEffectiveStartDate`) entities, plus parent-qualified forms on
# child entities (e.g. `PaymentInformationV3_effectiveStartDate`). Matching
# these platform-wide field names - never a hand-maintained list of entity
# names - keeps detection correct as entities are added or reconfigured.
EFFECTIVE_START_DATE_KEY_FIELDS = frozenset(
    {"startDate", "effectiveStartDate", "mdfSystemEffectiveStartDate"}
)
# v5: bumped from v4 to force a one-time full historical resync after fixing
# effective-dated extraction. Records that were only ever visible outside SAP's
# implicit `asOfDate=today` window were never synced at all, and their
# `lastModifiedDateTime` is usually older than the saved v4 cursor, so an
# incremental sync alone would never pick them up.
#
# v4: bumped from v3 to force a one-time full historical resync after adding
# full metadata-field completeness (None-filled missing fields), navigation-
# link columns, and UTC ISO 8601 date normalization to build_row(). Without
# this bump, the ~640K rows already synced under v3 would keep their old raw
# `/Date(...)/` values and missing nav columns until each row is next updated
# upstream, since incremental syncs only touch changed records.
STATE_VERSION = "v5"
# Full-field (no $select) pages return substantially more bytes per row than
# the old curated allowlists, so requests need more time to complete.
REQUEST_TIMEOUT_SECONDS = 180
LARGE_ENTITY_PAGE_SIZE = 1000
# Sync all configured entities while retaining the Review/360 form hierarchy handling.
ACTIVE_SOURCE_PREFIXES = None
# Entities the business owner confirmed are not required. They stay in ENTITIES so
# metadata coverage remains complete, but are neither declared nor synced.
NOT_REQUIRED_ENTITIES = {
    source: "Not required (PMGM owner, 2026-10-05): Career Development Planning is not enabled"
    for source in (
        "Achievement",
        "AchievementDevGoalDetail",
        "AchievementGoalDetail",
        "Activity",
        "ActivityFeedback",
        "ActivityStatus",
        "ContinuousPerformanceUserPermission",
        "DevGoalAchievements",
        "DevGoalAchievementsList",
        "DevGoalDetail",
    )
}
# Entities SAP will not serve usefully to this integration, verified live on 2026-10-05
# (see EXCLUSIONS.md and entity_failures_2026-10-05.xlsx). Requesting them only adds a
# guaranteed HTTP 400 to connector_sync_issues every run.
_SAP_NOT_QUERYABLE = "SAP does not support querying this entity"
_CPM_UNSUPPORTED = "SAP: not supported by this version of Continuous Performance Management"
_OWN_USER_ONLY = "Requires a per-key parameter and only returns the API user's own settings"
UNSUPPORTED_ENTITIES = {
    **{
        source: _SAP_NOT_QUERYABLE
        for source in (
            "InlineResult",
            "InnerMessage",
            "MessageDetail",
            "UpsertResult",
            "DGExpression",
            "DGFieldOperator",
            "DGFieldValue",
            "DGFilter",
            "DGPeoplePool",
            "MDFLocalizedValue",
            "ThemeTemplate",
        )
    },
    **{
        source: _CPM_UNSUPPORTED
        for source in (
            "FeedbackFlag",
            "GoalAchievements",
            "GoalAchievementsList",
            "GoalDetail",
        )
    },
    **{f"GoalPermission_{suffix}": _OWN_USER_ONLY for suffix in (*range(1, 14), 1000)},
    "GoalPlanState": _OWN_USER_ONLY,
    "WorkflowAllowedActionList": "Requires wfRequestId per call (444,595 workflow requests)",
    "GoalWeight": "Requires businessKey per goal; Goal_1 holds no goals",
    "MDFEnumValue": "Requires key eq '<enum>' per picklist; PickListValueV2 is synced instead",
    "WfRequestUIData": "Only reachable by expanding all 444,595 WfRequest rows; UI payload",
    "UserPermissions": "Only reachable via User/Permissions expand; permission metadata",
    "PersonKey": "Only reachable via User navigation; duplicates PerPerson keys",
    "SuccessStoreContentBlob": "Only reachable via SuccessStoreContent expand; binary SAP content",
    "ONB2ActivityNudgeDetails": "Requires activityId; the parent onboarding activity entity does not exist",
    "SuccessStoreContent": "Requires an undocumented query parameter; SAP content store, not HR data",
}
# Entities SAP can serve, but whose cost or sensitivity needs a decision before syncing.
DEFERRED_ENTITIES = {
    "EmpCompensationCalculated": (
        "Expand-only from EmpCompensation; measured ~80 min per sync as a full reload"
    ),
    "EmpCompensationGroupSumCalculated": (
        "Expand-only from EmpCompensation; measured ~37 min per sync as a full reload"
    ),
    "ComplianceFormDataFieldValue": (
        "Holds compliance form answers (disability self-ID, tax worksheets); needs "
        "data-owner approval and a composite key (its id repeats across forms)"
    ),
    "EmpTimeAccountBalance": (
        "Requires userId and timeAccountType per call (employees x 78 account types)"
    ),
}
SKIPPED_ENTITIES = {**NOT_REQUIRED_ENTITIES, **UNSUPPORTED_ENTITIES, **DEFERRED_ENTITIES}
# SAP lists lastModifiedDateTime for these but rejects it in $filter and $orderby
# ("not filterable"/"not sortable"), so every incremental request failed with HTTP 400.
# They are small and reload in full each sync instead.
NO_CURSOR_ENTITIES = frozenset(
    {
        "CalibrationSession",
        "CalibrationSessionSubject",
        "CalibrationSubjectComment",
        "CalibrationTemplate",
    }
)
FORM_EXPANSION_PAGE_SIZE = 10
# Measured live: ~1.1 s/form sequentially vs ~0.35 s/form with 4 concurrent pages,
# with no errors. Kept modest to avoid overloading the SAP API.
FORM_FETCH_WORKERS = 4
FORM_PAGE_MAX_ATTEMPTS = 4
FORM_TYPES = {
    "Review": "pmReviewContentDetail",
    "360": "form360ReviewContentDetail",
}
FORM_EXPANSION_EXCLUSIONS = {
    "FormRaterListSection": (
        "SAP returned 'Rater list is not visible' for the integration user; "
        "the 360 rater-list expansion is omitted."
    ),
}
# Navigations expanded in addition to the breadth-first tree. The tree reaches every
# entity once, so FormUserRatingComment arrives via a competency section and the overall
# form ratings ("Highly Effective", "1.8") are never read. These carry the reported
# ratings, so they are expanded explicitly and kept inline on the parent row as JSON.
FORM_SUPPLEMENTARY_NAVIGATIONS = {
    "FormSummarySection": (
        "overallFormRating",
        "calculatedFormRating",
        "overallAdjustedRating",
        "selfRatingComment",
    ),
    "FormObjCompSummarySection": (
        "overallFormRating",
        "calculatedFormRating",
        "overallAdjustedRating",
    ),
    "FormPerfPotSummarySection": (
        "overallFormRating",
        "calculatedFormRating",
        "overallAdjustedRating",
    ),
    "FormCompetency": ("officialRating",),
}
# Expanded payloads worth keeping on the parent row. BigQuery stores these as JSON, so
# the rating stays queryable next to its section instead of collapsing to a link URI.
FORM_INLINE_EXPANSIONS = frozenset(
    navigation
    for navigations in FORM_SUPPLEMENTARY_NAVIGATIONS.values()
    for navigation in navigations
)
FORM_TYPE_ONLY_ENTITIES = {
    "Review": frozenset(
        {
            "Form360Participant",
            "Form360ParticipantCategory",
            "Form360ParticipantColumn",
            "Form360ParticipantConfig",
            "Form360ParticipantDetail",
            "Form360ParticipantSection",
            "Form360Rater",
            "Form360RaterSection",
            "Form360ReviewContentDetail",
            "Form360SummaryViewCategory",
            "Form360SummaryViewRater",
            "Form360SummaryViewSection",
            "FormJobDescSection",
            "FormJobRole",
            "FormReviewInfoSection",
            "FormReviewerInfoSection",
        }
    ),
    "360": frozenset(
        {
            "FormCustomizedWeightedRatingSection",
            "FormObjCompSummarySection",
            "FormPMReviewContentDetail",
            "FormPerfPotSummarySection",
        }
    ),
}
OAUTH_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:saml2-bearer"
OAUTH_FIELDS = ("sf_client_id", "sf_company_id", "sf_user_id", "sf_private_key")
# SAP reuses a token until it lapses; refuse one this close to expiry and wait it out.
OAUTH_MIN_TOKEN_LIFETIME_SECONDS = 300
OAUTH_REFRESH_ATTEMPTS = 4
# Rate limit matching the native SuccessFactors connector: 40 requests per second.
SAP_RATE_LIMIT_RPS = 40
# Parallel page prefetch workers for large entities (matches form phase worker count).
ENTITY_FETCH_WORKERS = 4
# Entities where 30-day windowed time-slice fetch replaces the single-cursor stream.
# The native connector uses this pattern; it turns one massive sequential read (e.g.
# 1.6 M EmployeeTime rows) into ~300 small targeted requests, each checkpointed.
WINDOWED_FETCH_ENTITIES = frozenset(
    {
        "EmployeeTime",
        "EmployeeTimeSheet",
        "EmployeeTimeSheetEntry",
        "EmployeeTimeValuationResult",
        "TimeAccount",
        "TimeAccountDetail",
        "TimeAccountSnapshot",
        "TimeAccountSummary",
    }
)
WINDOW_INITIAL_VALUE = "2001-01-01T00:00:00Z"
WINDOW_STEP_DAYS = 30

# Source entity -> metadata fields that must be left out of the request.
#
# Requesting these fields makes SAP silently drop records from the response rather
# than returning them with a null value, so asking for them costs rows. Because
# OData v2 has no "exclude" operator, the only way to omit a field is to switch
# that entity to an explicit `$select` of every other property.
#
# Photo: measured at `$top=200&$skip=50000`, SAP returned 93 records without
# `photoHash` but only 61 with it -- roughly a third of the page suppressed by a
# field that is null on 99% of the rows it does return. `personUUID` is listed for
# a different reason: SAP rejects it inside `$select` outright
# (COE_BAD_PROPERTY_EXPRESSION), and it never appears in the payload anyway, so
# naming it would fail the request without costing any real data.
EXCLUDED_FIELDS = {
    "Photo": frozenset({"photoHash", "personUUID"}),
}

# Source entity -> a mandatory `$filter` predicate applied to every request.
#
# User: SAP's User entity silently returns **only active users** unless the query
# carries a status predicate. That default hid 11,237 of 14,343 users (78%), so
# `EmpJob` and every other historical entity referencing a terminated employee had
# no matching user row to join to. Filtering on an impossible status value is SAP's
# documented idiom for "all statuses": the tenant's real values are `t`, `f`, `T`,
# `active_external_suite`, and `inactive_external_suite`, and an
# `active`/`inactive` predicate would still drop the 206 external-suite users.
# The sentinel is deliberately not a plausible status string.
ENTITY_FILTERS = {
    "User": "status ne 'fivetran_all_statuses_sentinel'",
}

# Source entity -> scope tag, for entities whose *query scope* widened after rows had
# already been synced.
#
# A widened scope exposes rows that were always there but invisible, so their
# `lastModifiedDateTime` is old and an incremental run filtering on the stored cursor
# would never fetch them. Changing the tag abandons that entity's cursor once, forcing
# a single full re-read. Bumping the global STATE_VERSION would do the same thing for
# all entities, which is a multi-hour resync for a two-entity change.
ENTITY_SCOPE_VERSION = {
    "User": "allstatus",  # now returns inactive and external-suite users, not just active
    "Photo": "nohash",  # photoHash removed from the projection, un-suppressing previously omitted rows
    # The 2026-09-28 initial read landed only 21 of ~31,030 headers, and the saved
    # formLastModifiedDate cursor means older forms (e.g. formDataId 6467, last
    # modified 2021) would never be re-requested. Forces one full re-read.
    "FormHeader": "fullreread1",
}

# Source entity -> modification timestamp to use when SAP does not expose the standard
# `lastModifiedDateTime` property.
#
# Without an entry here these entities have no cursor at all, so every sync truncates
# and reloads the whole table. Each field below was verified against live SAP: the
# filter is accepted, a far-future bound returns zero rows, a mid-range bound returns a
# genuine subset, and no landed row has a null value in the field. Acceptance alone is
# not sufficient -- a field SAP accepts but ignores would silently return everything and
# make the sync look incremental while reloading the table.
ALTERNATIVE_CURSOR_FIELDS = {
    "Background_CompStatements": "lastModifiedDate",
    "Background_CustomPerfHist": "lastModifiedDate",
    "Background_PotentialForNextRole": "lastModifiedDate",
    "Background_VarPayEmpHistDataSWI": "lastModifiedDate",
    "Background_VarPayEmpHistDataSWI2022": "lastModifiedDate",
    "Background_VarPayEmpHistDataSWI2024": "lastModifiedDate",
    "DynamicGroup": "lastModifiedDate",
    "FormContent": "lastModifiedDate",
    "FormHeader": "formLastModifiedDate",
    "GoalComment_1": "lastModified",
    "GoalComment_2": "lastModified",
    "GoalComment_3": "lastModified",
    "GoalComment_4": "lastModified",
    "GoalComment_5": "lastModified",
    "GoalComment_6": "lastModified",
    "GoalComment_7": "lastModified",
    "GoalComment_8": "lastModified",
    "GoalComment_9": "lastModified",
    "GoalComment_10": "lastModified",
    "GoalComment_11": "lastModified",
    "GoalComment_12": "lastModified",
    "GoalComment_13": "lastModified",
    "GoalComment_1000": "lastModified",
    "TrendData_SysOverallPerformance": "lastModified",
    "TrendData_SysOverallPotential": "lastModified",
}

# Source entity -> destination table. Fields, primary keys, and pagination
# order are all discovered from live metadata in discover_entities(); do not
# add per-entity field allowlists here.
ENTITIES = [
    # Custom MDF objects.
    {"source": "cust_GeoZone", "table": "cust_geo_zone"},
    {"source": "cust_GeoZone_Location", "table": "cust_geo_zone_location"},
    {"source": "cust_CashRetentionAward", "table": "cust_cash_retention_award"},
    {"source": "cust_CashSettledRSU", "table": "cust_cash_settled_rsu"},
    {"source": "cust_LTI_Award", "table": "cust_lti_award"},
    {"source": "cust_PPUPIU", "table": "cust_ppupiu"},
    {"source": "cust_salesQuota", "table": "cust_sales_quota"},
    {"source": "cust_salesQuotaMon", "table": "cust_sales_quota_mon"},
    {"source": "cust_salesQuotaYrQtr", "table": "cust_sales_quota_yr_qtr"},
    # Core standard entities.
    {"source": "EmpEmployment", "table": "emp_employment"},
    {"source": "EmpJob", "table": "emp_job"},
    {"source": "PerPerson", "table": "per_person"},
    {
        "source": "PersonEmpTerminationInfo",
        "table": "person_emp_termination_info",
        "expand_parent": "PerPerson",
        "expand_navigation": "personEmpTerminationInfoNav",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "FOCompany", "table": "fo_company"},
    {"source": "FOBusinessUnit", "table": "fo_business_unit"},
    {"source": "FODivision", "table": "fo_division"},
    {"source": "FODepartment", "table": "fo_department"},
    {"source": "FOCostCenter", "table": "fo_cost_center"},
    {"source": "FOLocation", "table": "fo_location"},
    {"source": "FOJobCode", "table": "fo_job_code"},
    {"source": "Position", "table": "position"},
    {"source": "FOPayComponent", "table": "fo_pay_component"},
    {"source": "FOPayComponentGroup", "table": "fo_pay_component_group"},
    {"source": "User", "table": "users"},
    {"source": "ExternalUser", "table": "external_users"},
    # Geozone foundation object.
    {"source": "FOGeozone", "table": "fo_geozone"},
    # Structural/reference pay tables (no employee-level pay amounts).
    {"source": "FOPayGrade", "table": "fo_pay_grade"},
    {"source": "FOPayGroup", "table": "fo_pay_group"},
    {"source": "FOPayRange", "table": "fo_pay_range"},
    {"source": "PayScaleArea", "table": "pay_scale_area"},
    {"source": "PayScaleGroup", "table": "pay_scale_group"},
    {"source": "PayScaleLevel", "table": "pay_scale_level"},
    {"source": "PayScaleType", "table": "pay_scale_type"},
    {"source": "PayScalePayComponent", "table": "pay_scale_pay_component"},
    # Additional approved HR entities.
    # See EXCLUSIONS.md for the entities deliberately left out
    # (system/admin/UI-config tables, slow time-account tables, and a
    # small number of entities that are structurally unqueryable or
    # gated behind SF modules/features not enabled on this tenant).
    {
        "source": "CompetencyRating",
        "table": "competency_rating",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "PerAddressDEFLT",
        "table": "per_address_deflt",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "PerGlobalInfoARE", "table": "per_global_info_are"},
    {"source": "PerGlobalInfoARG", "table": "per_global_info_arg"},
    {"source": "PerGlobalInfoAUS", "table": "per_global_info_aus"},
    {"source": "PerGlobalInfoAUT", "table": "per_global_info_aut"},
    {"source": "PerGlobalInfoBEL", "table": "per_global_info_bel"},
    {"source": "PerGlobalInfoBGD", "table": "per_global_info_bgd"},
    {"source": "PerGlobalInfoBGR", "table": "per_global_info_bgr"},
    {"source": "PerGlobalInfoBLR", "table": "per_global_info_blr"},
    {"source": "PerGlobalInfoBRA", "table": "per_global_info_bra"},
    {"source": "PerGlobalInfoBWA", "table": "per_global_info_bwa"},
    {"source": "PerGlobalInfoCAN", "table": "per_global_info_can"},
    {"source": "PerGlobalInfoCHE", "table": "per_global_info_che"},
    {"source": "PerGlobalInfoCHL", "table": "per_global_info_chl"},
    {"source": "PerGlobalInfoCHN", "table": "per_global_info_chn"},
    {"source": "PerGlobalInfoCOL", "table": "per_global_info_col"},
    {"source": "PerGlobalInfoCRI", "table": "per_global_info_cri"},
    {"source": "PerGlobalInfoCZE", "table": "per_global_info_cze"},
    {"source": "PerGlobalInfoDEU", "table": "per_global_info_deu"},
    {"source": "PerGlobalInfoDNK", "table": "per_global_info_dnk"},
    {"source": "PerGlobalInfoDOM", "table": "per_global_info_dom"},
    {"source": "PerGlobalInfoECU", "table": "per_global_info_ecu"},
    {"source": "PerGlobalInfoEGY", "table": "per_global_info_egy"},
    {"source": "PerGlobalInfoESP", "table": "per_global_info_esp"},
    {"source": "PerGlobalInfoEST", "table": "per_global_info_est"},
    {"source": "PerGlobalInfoFIN", "table": "per_global_info_fin"},
    {"source": "PerGlobalInfoFRA", "table": "per_global_info_fra"},
    {"source": "PerGlobalInfoGBR", "table": "per_global_info_gbr"},
    {"source": "PerGlobalInfoGRC", "table": "per_global_info_grc"},
    {"source": "PerGlobalInfoGTM", "table": "per_global_info_gtm"},
    {"source": "PerGlobalInfoHKG", "table": "per_global_info_hkg"},
    {"source": "PerGlobalInfoHND", "table": "per_global_info_hnd"},
    {"source": "PerGlobalInfoHRV", "table": "per_global_info_hrv"},
    {"source": "PerGlobalInfoHUN", "table": "per_global_info_hun"},
    {"source": "PerGlobalInfoIDN", "table": "per_global_info_idn"},
    {"source": "PerGlobalInfoIND", "table": "per_global_info_ind"},
    {"source": "PerGlobalInfoIRL", "table": "per_global_info_irl"},
    {"source": "PerGlobalInfoIRQ", "table": "per_global_info_irq"},
    {"source": "PerGlobalInfoISR", "table": "per_global_info_isr"},
    {"source": "PerGlobalInfoITA", "table": "per_global_info_ita"},
    {"source": "PerGlobalInfoJOR", "table": "per_global_info_jor"},
    {"source": "PerGlobalInfoJPN", "table": "per_global_info_jpn"},
    {"source": "PerGlobalInfoKAZ", "table": "per_global_info_kaz"},
    {"source": "PerGlobalInfoKEN", "table": "per_global_info_ken"},
    {"source": "PerGlobalInfoKHM", "table": "per_global_info_khm"},
    {"source": "PerGlobalInfoKOR", "table": "per_global_info_kor"},
    {"source": "PerGlobalInfoKWT", "table": "per_global_info_kwt"},
    {"source": "PerGlobalInfoLBN", "table": "per_global_info_lbn"},
    {"source": "PerGlobalInfoLKA", "table": "per_global_info_lka"},
    {"source": "PerGlobalInfoLTU", "table": "per_global_info_ltu"},
    {"source": "PerGlobalInfoLUX", "table": "per_global_info_lux"},
    {"source": "PerGlobalInfoLVA", "table": "per_global_info_lva"},
    {"source": "PerGlobalInfoMAR", "table": "per_global_info_mar"},
    {"source": "PerGlobalInfoMEX", "table": "per_global_info_mex"},
    {"source": "PerGlobalInfoMLT", "table": "per_global_info_mlt"},
    {"source": "PerGlobalInfoMMR", "table": "per_global_info_mmr"},
    {"source": "PerGlobalInfoMYS", "table": "per_global_info_mys"},
    {"source": "PerGlobalInfoNAM", "table": "per_global_info_nam"},
    {"source": "PerGlobalInfoNGA", "table": "per_global_info_nga"},
    {"source": "PerGlobalInfoNIC", "table": "per_global_info_nic"},
    {"source": "PerGlobalInfoNLD", "table": "per_global_info_nld"},
    {"source": "PerGlobalInfoNOR", "table": "per_global_info_nor"},
    {"source": "PerGlobalInfoNZL", "table": "per_global_info_nzl"},
    {"source": "PerGlobalInfoOMN", "table": "per_global_info_omn"},
    {"source": "PerGlobalInfoPAK", "table": "per_global_info_pak"},
    {"source": "PerGlobalInfoPAN", "table": "per_global_info_pan"},
    {"source": "PerGlobalInfoPER", "table": "per_global_info_per"},
    {"source": "PerGlobalInfoPHL", "table": "per_global_info_phl"},
    {"source": "PerGlobalInfoPOL", "table": "per_global_info_pol"},
    {"source": "PerGlobalInfoPRI", "table": "per_global_info_pri"},
    {"source": "PerGlobalInfoPRT", "table": "per_global_info_prt"},
    {"source": "PerGlobalInfoQAT", "table": "per_global_info_qat"},
    {"source": "PerGlobalInfoROU", "table": "per_global_info_rou"},
    {"source": "PerGlobalInfoRUS", "table": "per_global_info_rus"},
    {"source": "PerGlobalInfoSAU", "table": "per_global_info_sau"},
    {"source": "PerGlobalInfoSGP", "table": "per_global_info_sgp"},
    {"source": "PerGlobalInfoSLV", "table": "per_global_info_slv"},
    {"source": "PerGlobalInfoSRB", "table": "per_global_info_srb"},
    {"source": "PerGlobalInfoSSD", "table": "per_global_info_ssd"},
    {"source": "PerGlobalInfoSVK", "table": "per_global_info_svk"},
    {"source": "PerGlobalInfoSVN", "table": "per_global_info_svn"},
    {"source": "PerGlobalInfoSWE", "table": "per_global_info_swe"},
    {"source": "PerGlobalInfoTHA", "table": "per_global_info_tha"},
    {"source": "PerGlobalInfoTUR", "table": "per_global_info_tur"},
    {"source": "PerGlobalInfoTWN", "table": "per_global_info_twn"},
    {"source": "PerGlobalInfoTZA", "table": "per_global_info_tza"},
    {"source": "PerGlobalInfoUGA", "table": "per_global_info_uga"},
    {"source": "PerGlobalInfoUKR", "table": "per_global_info_ukr"},
    {"source": "PerGlobalInfoURY", "table": "per_global_info_ury"},
    {"source": "PerGlobalInfoUSA", "table": "per_global_info_usa"},
    {"source": "PerGlobalInfoVEN", "table": "per_global_info_ven"},
    {"source": "PerGlobalInfoVNM", "table": "per_global_info_vnm"},
    {"source": "PerGlobalInfoXKX", "table": "per_global_info_xkx"},
    {"source": "PerGlobalInfoZAF", "table": "per_global_info_zaf"},
    {"source": "CurrencyExchangeRate", "table": "currency_exchange_rate"},
    {"source": "HireDateChange", "table": "hire_date_change"},
    {"source": "DepartmentApprenticeDetail", "table": "department_apprentice_detail"},
    {"source": "ApprenticeEventType", "table": "apprentice_event_type"},
    {"source": "ApprenticeGroup", "table": "apprentice_group"},
    {"source": "Apprentice", "table": "apprentice"},
    {"source": "ApprenticeInternalTrainingEvent", "table": "apprentice_internal_training_event"},
    {"source": "ApprenticePracticalTrainingEvent", "table": "apprentice_practical_training_event"},
    {"source": "ApprenticeSchool", "table": "apprentice_school"},
    {"source": "ApprenticeSchoolEvent", "table": "apprentice_school_event"},
    {"source": "SecondaryAssignments", "table": "secondary_assignments"},
    {"source": "SecondaryAssignmentsItem", "table": "secondary_assignments_item"},
    {"source": "Background_Awards", "table": "background_awards"},
    {"source": "Background_Benefitselection", "table": "background_benefitselection"},
    {"source": "Background_Certificates", "table": "background_certificates"},
    {"source": "Background_Community", "table": "background_community"},
    {"source": "Background_Courses", "table": "background_courses"},
    {"source": "Background_CustomPerfHist", "table": "background_custom_perf_hist"},
    {"source": "Background_Documents", "table": "background_documents"},
    {"source": "Background_Education", "table": "background_education"},
    {"source": "Background_Fsaelection", "table": "background_fsaelection"},
    {"source": "Background_FuncExperience", "table": "background_func_experience"},
    {"source": "Background_Googledocs", "table": "background_googledocs"},
    {"source": "Background_InsideWorkExperience", "table": "background_inside_work_experience"},
    {"source": "Background_Languages", "table": "background_languages"},
    {"source": "Background_LeadExperience", "table": "background_lead_experience"},
    {"source": "Background_Memberships", "table": "background_memberships"},
    {"source": "Background_Mobility", "table": "background_mobility"},
    {"source": "Background_OutsideWorkExperience", "table": "background_outside_work_experience"},
    {"source": "Background_PIPSWO", "table": "background_pipswo"},
    {"source": "Background_PotentialForNextRole", "table": "background_potential_for_next_role"},
    {"source": "Background_PreferredNextMove", "table": "background_preferred_next_move"},
    {"source": "Background_Promotability", "table": "background_promotability"},
    {"source": "Background_SpecialAssign", "table": "background_special_assign"},
    {
        "source": "Background_SysScoreCardDevelopmentObjectivesPortlet",
        "table": "background_sys_score_card_development_objectives_portlet",
    },
    {"source": "EmployeeProfileBlockContent", "table": "employee_profile_block_content"},
    {"source": "EmployeeProfileBlockLink", "table": "employee_profile_block_link"},
    {"source": "EmployeeProfileFieldConfig", "table": "employee_profile_field_config"},
    {"source": "EmployeeProfileHeaderConfig", "table": "employee_profile_header_config"},
    {"source": "EmployeeProfilePageConfig", "table": "employee_profile_page_config"},
    {"source": "EmployeeProfileSectionConfig", "table": "employee_profile_section_config"},
    {"source": "EmployeeProfileSubSectionConfig", "table": "employee_profile_sub_section_config"},
    {"source": "EPCustomBackgroundPortlet", "table": "ep_custom_background_portlet"},
    {"source": "TrendData_SysOverallCompetency", "table": "trend_data_sys_overall_competency"},
    {"source": "TrendData_SysOverallObjective", "table": "trend_data_sys_overall_objective"},
    {"source": "TrendData_SysOverallPerformance", "table": "trend_data_sys_overall_performance"},
    {"source": "TrendData_SysOverallPotential", "table": "trend_data_sys_overall_potential"},
    {"source": "EmpEmploymentTermination", "table": "emp_employment_termination"},
    {"source": "EmpJobRelationships", "table": "emp_job_relationships"},
    {"source": "EmpWorkPermit", "table": "emp_work_permit"},
    {"source": "BudgetGroup", "table": "budget_group"},
    {"source": "CompanyContactDetails", "table": "company_contact_details"},
    {"source": "FODynamicRole", "table": "fo_dynamic_role"},
    {"source": "FOEventReason", "table": "fo_event_reason"},
    {"source": "FOCorporateAddressDEFLT", "table": "fo_corporate_address_deflt"},
    {"source": "FOJobClassLocalDEFLT", "table": "fo_job_class_local_deflt"},
    {"source": "FOLegalEntityLocalDEFLT", "table": "fo_legal_entity_local_deflt"},
    {"source": "FOFrequency", "table": "fo_frequency"},
    {"source": "FOJobClassLocalAUS", "table": "fo_job_class_local_aus"},
    {"source": "JobClassificationAUS", "table": "job_classification_aus"},
    {"source": "JobClassificationBOL", "table": "job_classification_bol"},
    {"source": "FOJobClassLocalBRA", "table": "fo_job_class_local_bra"},
    {"source": "JobClassificationBRA", "table": "job_classification_bra"},
    {"source": "FOJobClassLocalCAN", "table": "fo_job_class_local_can"},
    {"source": "JobClassificationCAN", "table": "job_classification_can"},
    {"source": "JobClassificationCHL", "table": "job_classification_chl"},
    {"source": "JobClassificationCountry", "table": "job_classification_country"},
    {"source": "FOJobClassLocalFRA", "table": "fo_job_class_local_fra"},
    {"source": "JobClassificationFRA", "table": "job_classification_fra"},
    {"source": "FOJobClassLocalITA", "table": "fo_job_class_local_ita"},
    {"source": "JobClassificationITA", "table": "job_classification_ita"},
    {"source": "JobClassificationZAF", "table": "job_classification_zaf"},
    {"source": "FOJobClassLocalGBR", "table": "fo_job_class_local_gbr"},
    {"source": "JobClassificationGBR", "table": "job_classification_gbr"},
    {"source": "FOJobClassLocalUSA", "table": "fo_job_class_local_usa"},
    {"source": "JobClassificationUSA", "table": "job_classification_usa"},
    {"source": "FOJobFunction", "table": "fo_job_function"},
    {"source": "JobClassificationBGR", "table": "job_classification_bgr"},
    {"source": "FOLegalEntityLocalARG", "table": "fo_legal_entity_local_arg"},
    {"source": "LegalEntityARG", "table": "legal_entity_arg"},
    {"source": "LegalEntityBLR", "table": "legal_entity_blr"},
    {"source": "LegalEntityBOL", "table": "legal_entity_bol"},
    {"source": "LegalEntityCAN", "table": "legal_entity_can"},
    {"source": "FOLegalEntityLocalFRA", "table": "fo_legal_entity_local_fra"},
    {"source": "LegalEntityFRA", "table": "legal_entity_fra"},
    {"source": "FOLegalEntityLocalDEU", "table": "fo_legal_entity_local_deu"},
    {"source": "LegalEntityDEU", "table": "legal_entity_deu"},
    {"source": "LegalEntityPRY", "table": "legal_entity_pry"},
    {"source": "LegalEntityRUS", "table": "legal_entity_rus"},
    {"source": "LegalEntitySAU", "table": "legal_entity_sau"},
    {"source": "LegalEntitySGP", "table": "legal_entity_sgp"},
    {"source": "LegalEntitySVN", "table": "legal_entity_svn"},
    {"source": "FOLegalEntityLocalESP", "table": "fo_legal_entity_local_esp"},
    {"source": "LegalEntityESP", "table": "legal_entity_esp"},
    {"source": "LegalEntityTHA", "table": "legal_entity_tha"},
    {"source": "LegalEntityTUN", "table": "legal_entity_tun"},
    {"source": "FOLegalEntityLocalUSA", "table": "fo_legal_entity_local_usa"},
    {"source": "LegalEntityUSA", "table": "legal_entity_usa"},
    {"source": "FOLocationGroup", "table": "fo_location_group"},
    {"source": "PayCalendar", "table": "pay_calendar"},
    {"source": "PayPeriod", "table": "pay_period"},
    {"source": "NAReportingEntity", "table": "na_reporting_entity"},
    {"source": "Territory", "table": "territory"},
    {"source": "EmpEmploymentHigherDuty", "table": "emp_employment_higher_duty"},
    {"source": "Allowance", "table": "allowance"},
    {"source": "AllowanceRecording", "table": "allowance_recording"},
    {"source": "AllowanceType", "table": "allowance_type"},
    {"source": "AvailableAllowanceType", "table": "available_allowance_type"},
    # Deferred (too slow for a full sync at current scale) — see PHASE2_EXCLUSIONS.md.
    {"source": "ExternalAllowance", "table": "external_allowance"},
    {"source": "ExternalTimeData", "table": "external_time_data"},
    {"source": "ExternalTimeRecord", "table": "external_time_record"},
    {"source": "ExternalTimeSegment", "table": "external_time_segment"},
    {"source": "TimeCollector", "table": "time_collector"},
    {"source": "TimeContainer", "table": "time_container"},
    {"source": "TimeRecording", "table": "time_recording"},
    # Deferred (too slow for a full sync at current scale) — see PHASE2_EXCLUSIONS.md.
    {"source": "PerPersonRelationship", "table": "per_person_relationship"},
    {"source": "PerEmail", "table": "per_email"},
    {"source": "NameFormatGO", "table": "name_format_go"},
    {"source": "NameElementGO", "table": "name_element_go"},
    {"source": "PerNationalId", "table": "per_national_id"},
    {"source": "PerPersonal", "table": "per_personal"},
    {"source": "PerPhone", "table": "per_phone"},
    {"source": "PerEmergencyContacts", "table": "per_emergency_contacts"},
    {"source": "PerSocialAccount", "table": "per_social_account"},
    {"source": "PositionMatrixRelationship", "table": "position_matrix_relationship"},
    {"source": "PositionRightToReturn", "table": "position_right_to_return"},
    {"source": "AbsenceCountingMethod", "table": "absence_counting_method"},
    {"source": "AvailableTimeType", "table": "available_time_type"},
    {"source": "EmployeeTimeAUS", "table": "employee_time_aus"},
    {"source": "EmployeeTimeCAN", "table": "employee_time_can"},
    {"source": "EmployeeTimeCOL", "table": "employee_time_col"},
    {"source": "EmployeeTimeDEU", "table": "employee_time_deu"},
    {"source": "EmployeeTimeESP", "table": "employee_time_esp"},
    {"source": "EmployeeTimeITA", "table": "employee_time_ita"},
    {"source": "EmployeeTimeMEX", "table": "employee_time_mex"},
    {"source": "EmployeeTimeNLD", "table": "employee_time_nld"},
    {"source": "EmployeeTimeNOR", "table": "employee_time_nor"},
    {"source": "EmployeeTimePOL", "table": "employee_time_pol"},
    {"source": "EmployeeTimeSWE", "table": "employee_time_swe"},
    {"source": "EmployeeTimeUSA", "table": "employee_time_usa"},
    {"source": "EmployeeTimeGroup", "table": "employee_time_group"},
    {"source": "EmployeeTimeGroupItem", "table": "employee_time_group_item"},
    {
        "source": "ElectronicSicknessCertificateExclusionReasonDEU",
        "table": "electronic_sickness_certificate_exclusion_reason_deu",
    },
    {"source": "Holiday", "table": "holiday"},
    {"source": "HolidayAssignment", "table": "holiday_assignment"},
    {"source": "HolidayCalendar", "table": "holiday_calendar"},
    {"source": "HolidayCategory", "table": "holiday_category"},
    {"source": "ShiftClassification", "table": "shift_classification"},
    {"source": "TemporaryTimeInformation", "table": "temporary_time_information"},
    {"source": "TimeManagementAlert", "table": "time_management_alert"},
    {"source": "TimeManagementAlertMessage", "table": "time_management_alert_message"},
    {
        "source": "TimeManagementTerminationEndHandlingConfiguration",
        "table": "time_management_termination_end_handling_configuration",
    },
    {
        "source": "TimeManagementTerminationEndHandlingExcludedEventReason",
        "table": "time_management_termination_end_handling_excluded_event_reason",
    },
    {
        "source": "TimeManagementTerminationEndHandlingExcludedTimeAccountType",
        "table": "time_management_termination_end_handling_excluded_time_account_type",
    },
    {
        "source": "TimeManagementTerminationEndHandlingExclusion",
        "table": "time_management_termination_end_handling_exclusion",
    },
    {
        "source": "TimeManagementTerminationEndHandlingLegalEntityConfiguration",
        "table": "time_management_termination_end_handling_legal_entity_configuration",
    },
    {"source": "TimeTypeProfile", "table": "time_type_profile"},
    {"source": "TimeType", "table": "time_type"},
    {"source": "TimeTypeAUS", "table": "time_type_aus"},
    {"source": "TimeTypeCAN", "table": "time_type_can"},
    {"source": "TimeTypeDEU", "table": "time_type_deu"},
    {"source": "TimeTypeITA", "table": "time_type_ita"},
    {"source": "TimeTypeNLD", "table": "time_type_nld"},
    {"source": "TimeTypeNOR", "table": "time_type_nor"},
    {"source": "TimeTypeSWE", "table": "time_type_swe"},
    {"source": "TimeTypeUSA", "table": "time_type_usa"},
    {"source": "WorkSchedule", "table": "work_schedule"},
    {"source": "WorkScheduleDay", "table": "work_schedule_day"},
    {"source": "WorkScheduleDayModel", "table": "work_schedule_day_model"},
    {"source": "WorkScheduleDayModelAssignment", "table": "work_schedule_day_model_assignment"},
    {
        "source": "WorkScheduleDayModelAssignmentSegment",
        "table": "work_schedule_day_model_assignment_segment",
    },
    {"source": "WorkScheduleDayModelSegment", "table": "work_schedule_day_model_segment"},
    {
        "source": "WorkScheduleDayModelVariantAssignment",
        "table": "work_schedule_day_model_variant_assignment",
    },
    {
        "source": "WorkScheduleDayModelVariantIdentifier",
        "table": "work_schedule_day_model_variant_identifier",
    },
    {"source": "AlertMessage", "table": "alert_message"},
    {"source": "ExtAddressInfo", "table": "ext_address_info"},
    {"source": "Country", "table": "country"},
    {"source": "Currency", "table": "currency"},
    {"source": "ExtEmailInfo", "table": "ext_email_info"},
    {"source": "ExtPersonalInfo", "table": "ext_personal_info"},
    {"source": "ExtPhoneInfo", "table": "ext_phone_info"},
    {"source": "TimeZone", "table": "time_zone"},
    {"source": "VendorInfo", "table": "vendor_info"},
    {"source": "cust_additionalEmployeeData", "table": "cust_additional_employee_data"},
    {"source": "cust_ONB_AMERICAS_PoliciesList", "table": "cust_onb_americas_policies_list"},
    {"source": "cust_ONB_APAC_PoliciesList", "table": "cust_onb_apac_policies_list"},
    {"source": "cust_SalesBusinessUnit", "table": "cust_sales_business_unit"},
    {"source": "cust_CareerLevel", "table": "cust_career_level"},
    {"source": "cust_CaseManagement_ER_PIP", "table": "cust_case_management_er_pip"},
    {"source": "cust_Category", "table": "cust_category"},
    {"source": "cust_CertificateOfEmployment", "table": "cust_certificate_of_employment"},
    {"source": "cust_ConsentDisclaimer", "table": "cust_consent_disclaimer"},
    {"source": "cust_DirectDeposit", "table": "cust_direct_deposit"},
    {"source": "cust_notification_6months", "table": "cust_notification_6months"},
    {"source": "cust_OffboardingGuide", "table": "cust_offboarding_guide"},
    {
        "source": "cust_ONB_CZE_PersonalQuestionnaire",
        "table": "cust_onb_cze_personal_questionnaire",
    },
    {"source": "cust_ONB_GBR_BUPAHealthCare", "table": "cust_onb_gbr_bupa_health_care"},
    {
        "source": "cust_ONB_GBR_GLDBenefitsNomination",
        "table": "cust_onb_gbr_gld_benefits_nomination",
    },
    {"source": "cust_ONB_GBR_GPPNomination", "table": "cust_onb_gbr_gpp_nomination"},
    {"source": "cust_ONB_GBR_P46", "table": "cust_onb_gbr_p46"},
    {"source": "cust_ONB_GPPRetirementPlan", "table": "cust_onb_gpp_retirement_plan"},
    {"source": "cust_ONB_IND_Upload_Documents", "table": "cust_onb_ind_upload_documents"},
    {"source": "cust_ONB_IRL_WishesForm", "table": "cust_onb_irl_wishes_form"},
    {"source": "cust_ONB_Medicash_HeathCare", "table": "cust_onb_medicash_heath_care"},
    {"source": "cust_ONB_POL_ChildrenDeclaration", "table": "cust_onb_pol_children_declaration"},
    {"source": "cust_ONB_POL_Questionnaire", "table": "cust_onb_pol_questionnaire"},
    {
        "source": "cust_ONB_POL_SocialSecuritiesBasisStatement",
        "table": "cust_onb_pol_social_securities_basis_statement",
    },
    {"source": "cust_ONB_POL_TaxStatement", "table": "cust_onb_pol_tax_statement"},
    {"source": "cust_ONB_POL_ZUS_EnrollmentForm", "table": "cust_onb_pol_zus_enrollment_form"},
    {"source": "cust_ONB_PoliciesList", "table": "cust_onb_policies_list"},
    {"source": "cust_ONB_PolicyForms", "table": "cust_onb_policy_forms"},
    {"source": "cust_ONB_SGP_ALLIANZ", "table": "cust_onb_sgp_allianz"},
    {"source": "cust_ONB_USA_401K", "table": "cust_onb_usa_401_k"},
    {"source": "cust_RTO_Report", "table": "cust_rto_report"},
    {"source": "cust_SW_VehicleRegistration", "table": "cust_sw_vehicle_registration"},
    {
        "source": "DependentGlobalInfoConfigurationImpl",
        "table": "dependent_global_info_configuration_impl",
    },
    {
        "source": "DependentGlobalInfoFieldConfigurationImpl",
        "table": "dependent_global_info_field_configuration_impl",
    },
    {"source": "cust_DependentMDF", "table": "cust_dependent_mdf"},
    {"source": "DomainEventAlert", "table": "domain_event_alert"},
    {"source": "cust_ONB_EMEA_PoliciesList", "table": "cust_onb_emea_policies_list"},
    {"source": "cust_ONB_GLOBAL_PoliciesList", "table": "cust_onb_global_policies_list"},
    {"source": "cust_JobTDCRanges", "table": "cust_job_tdc_ranges"},
    {"source": "cust_JobGrade", "table": "cust_job_grade"},
    {"source": "cust_SubFunction", "table": "cust_sub_function"},
    {
        "source": "cust_JPNAccrualLookupBySeniority",
        "table": "cust_jpn_accrual_lookup_by_seniority",
    },
    {
        "source": "cust_PerformanceRatingIntegrationObjectV2",
        "table": "cust_performance_rating_integration_object_v2",
    },
    {"source": "cust_PersDataHist", "table": "cust_pers_data_hist"},
    {"source": "cust_PHLWorkingTimezoneLookup", "table": "cust_phl_working_timezone_lookup"},
    {"source": "cust_PHL_OE_Dependent", "table": "cust_phl_oe_dependent"},
    {"source": "cust_PHL_OE_Header", "table": "cust_phl_oe_header"},
    {"source": "cust_RTO", "table": "cust_rto"},
    {"source": "cust_RTO_MANAGER", "table": "cust_rto_manager"},
    {"source": "cust_SalesRegion", "table": "cust_sales_region"},
    {"source": "cust_TR_Benefits", "table": "cust_tr_benefits"},
    {"source": "cust_TR_Perks", "table": "cust_tr_perks"},
    {"source": "PersonType", "table": "person_type"},
    {"source": "PersonTypeUsage", "table": "person_type_usage"},
    # SAP's own OData metadata declares this entity's key as (payComponentCode,
    # payDate, userId), but SAP SuccessFactors data can contain distinct records
    # that collide on that triple — two different pay amounts sharing one key.
    # The API also returns a `sequenceNumber` field that is not part of the
    # declared key but does distinguish them. This is the one confirmed exception
    # to "primary keys are always derived purely from metadata" — see key_override.
    {
        "source": "EmpPayCompNonRecurring",
        "table": "emp_pay_comp_non_recurring",
        "key_override": ["payComponentCode", "payDate", "userId", "sequenceNumber"],
    },
    {"source": "EmpPayCompRecurring", "table": "emp_pay_comp_recurring"},
    {"source": "EmpCompensation", "table": "emp_compensation"},
    {"source": "EmployeePayrollRunResults", "table": "employee_payroll_run_results"},
    {"source": "EmployeePayrollRunResultsItems", "table": "employee_payroll_run_results_items"},
    {"source": "Background_Compensation", "table": "background_compensation"},
    {"source": "Background_CompStatements", "table": "background_comp_statements"},
    {"source": "Background_VarPayEmpHistData", "table": "background_var_pay_emp_hist_data"},
    {"source": "Background_VarPayEmpHistDataSWI", "table": "background_var_pay_emp_hist_data_swi"},
    {
        "source": "Background_VarPayEmpHistDataSWI2022",
        "table": "background_var_pay_emp_hist_data_swi2022",
    },
    {
        "source": "Background_VarPayEmpHistDataSWI2024",
        "table": "background_var_pay_emp_hist_data_swi2024",
    },
    {"source": "Bank", "table": "bank"},
    {"source": "CustomPayType", "table": "custom_pay_type"},
    {"source": "CustomPayTypeAssignment", "table": "custom_pay_type_assignment"},
    {"source": "PaymentInformationV3", "table": "payment_information_v3"},
    {"source": "PaymentInformationDetailV3", "table": "payment_information_detail_v3"},
    {"source": "PaymentInformationDetailV3ARG", "table": "payment_information_detail_v3_arg"},
    {"source": "PaymentInformationDetailV3BLR", "table": "payment_information_detail_v3_blr"},
    {"source": "PaymentInformationDetailV3BOL", "table": "payment_information_detail_v3_bol"},
    {"source": "PaymentInformationDetailV3BRA", "table": "payment_information_detail_v3_bra"},
    {"source": "PaymentInformationDetailV3CHL", "table": "payment_information_detail_v3_chl"},
    {"source": "PaymentInformationDetailV3COL", "table": "payment_information_detail_v3_col"},
    {"source": "PaymentInformationDetailV3CZE", "table": "payment_information_detail_v3_cze"},
    {"source": "PaymentInformationDetailV3ECU", "table": "payment_information_detail_v3_ecu"},
    {"source": "PaymentInformationDetailV3ESP", "table": "payment_information_detail_v3_esp"},
    {"source": "PaymentInformationDetailV3FRA", "table": "payment_information_detail_v3_fra"},
    {"source": "PaymentInformationDetailV3GBR", "table": "payment_information_detail_v3_gbr"},
    {"source": "PaymentInformationDetailV3GHA", "table": "payment_information_detail_v3_gha"},
    {"source": "PaymentInformationDetailV3IRQ", "table": "payment_information_detail_v3_irq"},
    {"source": "PaymentInformationDetailV3ISR", "table": "payment_information_detail_v3_isr"},
    {"source": "PaymentInformationDetailV3ITA", "table": "payment_information_detail_v3_ita"},
    {"source": "PaymentInformationDetailV3JPN", "table": "payment_information_detail_v3_jpn"},
    {"source": "PaymentInformationDetailV3KEN", "table": "payment_information_detail_v3_ken"},
    {"source": "PaymentInformationDetailV3MEX", "table": "payment_information_detail_v3_mex"},
    {"source": "PaymentInformationDetailV3MKD", "table": "payment_information_detail_v3_mkd"},
    {"source": "PaymentInformationDetailV3MMR", "table": "payment_information_detail_v3_mmr"},
    {"source": "PaymentInformationDetailV3MOZ", "table": "payment_information_detail_v3_moz"},
    {"source": "PaymentInformationDetailV3MWI", "table": "payment_information_detail_v3_mwi"},
    {"source": "PaymentInformationDetailV3NAM", "table": "payment_information_detail_v3_nam"},
    {"source": "PaymentInformationDetailV3NGA", "table": "payment_information_detail_v3_nga"},
    {"source": "PaymentInformationDetailV3NZL", "table": "payment_information_detail_v3_nzl"},
    {"source": "PaymentInformationDetailV3PER", "table": "payment_information_detail_v3_per"},
    {"source": "PaymentInformationDetailV3PRY", "table": "payment_information_detail_v3_pry"},
    {"source": "PaymentInformationDetailV3SUR", "table": "payment_information_detail_v3_sur"},
    {"source": "PaymentInformationDetailV3SVK", "table": "payment_information_detail_v3_svk"},
    {"source": "PaymentInformationDetailV3SVN", "table": "payment_information_detail_v3_svn"},
    {"source": "PaymentInformationDetailV3TUN", "table": "payment_information_detail_v3_tun"},
    {"source": "PaymentInformationDetailV3USA", "table": "payment_information_detail_v3_usa"},
    {"source": "PaymentInformationDetailV3VEN", "table": "payment_information_detail_v3_ven"},
    {"source": "PaymentInformationDetailV3ZAF", "table": "payment_information_detail_v3_zaf"},
    {"source": "PaymentInformationDetailV3ZWE", "table": "payment_information_detail_v3_zwe"},
    {"source": "PaymentMethodV3", "table": "payment_method_v3"},
    {"source": "PaymentMethodAssignmentV3", "table": "payment_method_assignment_v3"},
    {"source": "cust_EquityAnnualGrant", "table": "cust_equity_annual_grant"},
    {"source": "cust_RSUUnvestGrant", "table": "cust_rsu_unvest_grant"},
    {"source": "cust_BonusAllowances", "table": "cust_bonus_allowances"},
    {"source": "cust_CompensationData", "table": "cust_compensation_data"},
    {"source": "cust_EquityDiscretionaryGrant", "table": "cust_equity_discretionary_grant"},
    {"source": "cust_EquityHireGrantDetail", "table": "cust_equity_hire_grant_detail"},
    {"source": "cust_LastComp", "table": "cust_last_comp"},
    {"source": "cust_RSU", "table": "cust_rsu"},
    {"source": "cust_Total_Comp_Report", "table": "cust_total_comp_report"},
    {"source": "cust_TR_Compensation", "table": "cust_tr_compensation"},
    # Newly added entities from the latest live metadata probe.
    {"source": "AssignedComplianceForm", "table": "assigned_compliance_form"},
    {"source": "AutoDelegateConfig", "table": "auto_delegate_config"},
    {"source": "AutoDelegateDetail", "table": "auto_delegate_detail"},
    {"source": "ComplianceDocumentFlow", "table": "compliance_document_flow"},
    {"source": "ComplianceFormData", "table": "compliance_form_data"},
    {"source": "ComplianceFormSignature", "table": "compliance_form_signature"},
    {"source": "ComplianceProcess", "table": "compliance_process"},
    {"source": "ComplianceProcessTask", "table": "compliance_process_task"},
    {"source": "ComplianceUserFormData", "table": "compliance_user_form_data"},
    {"source": "DigitalSupportIncident", "table": "digital_support_incident"},
    {"source": "DigitalSupportSystemInformation", "table": "digital_support_system_information"},
    {"source": "EMMonitoredProcess", "table": "em_monitored_process"},
    {"source": "EmpWfRequest", "table": "emp_wf_request"},
    {"source": "FormAuditTrail", "table": "form_audit_trail"},
    {"source": "FormBehaviorRatingComment", "table": "form_behavior_rating_comment"},
    {"source": "FormCompetencyBehavior", "table": "form_competency_behavior"},
    {"source": "FormContent", "table": "form_content"},
    {
        "source": "FormCustomizedWeightedRatingSection",
        "table": "form_customized_weighted_rating_section",
    },
    {"source": "FormFolder", "table": "form_folder"},
    {"source": "FormHeader", "table": "form_header"},
    {"source": "FormIntroductionSection", "table": "form_introduction_section"},
    {"source": "FormObjCompSummarySection", "table": "form_obj_comp_summary_section"},
    {"source": "FormObjective", "table": "form_objective"},
    {"source": "FormObjectiveComment", "table": "form_objective_comment"},
    {"source": "FormObjectiveDetails", "table": "form_objective_details"},
    {"source": "FormObjectiveOtherDetails", "table": "form_objective_other_details"},
    {"source": "FormObjectiveOtherDetailsItem", "table": "form_objective_other_details_item"},
    {
        "source": "FormObjectiveOtherDetailsItemCol",
        "table": "form_objective_other_details_item_col",
    },
    {
        "source": "FormObjectiveOtherDetailsItemValueCell",
        "table": "form_objective_other_details_item_value_cell",
    },
    {"source": "FormObjectiveSection", "table": "form_objective_section"},
    {"source": "FormPerfPotSummarySection", "table": "form_perf_pot_summary_section"},
    {"source": "FormRatingScale", "table": "form_rating_scale"},
    {"source": "FormRatingScaleValue", "table": "form_rating_scale_value"},
    {"source": "FormReviewFeedback", "table": "form_review_feedback"},
    {"source": "FormReviewFeedbackList", "table": "form_review_feedback_list"},
    {"source": "FormRouteMap", "table": "form_route_map"},
    {"source": "FormRouteStep", "table": "form_route_step"},
    {"source": "FormRouteSubStep", "table": "form_route_sub_step"},
    {"source": "FormSignature", "table": "form_signature"},
    {"source": "FormSummarySection", "table": "form_summary_section"},
    {"source": "FormTemplate", "table": "form_template"},
    {"source": "FormUserInformationSection", "table": "form_user_information_section"},
    {"source": "FormUserRatingComment", "table": "form_user_rating_comment"},
    # SAP does not scope GoalComment_<templateId> by template: every GoalComment_N
    # endpoint returns the same tenant-wide comment set (verified identical across
    # multiple endpoints). Only this one is configured, and it is named `goal_comment`
    # rather than `goal_comment_1` because its contents are not specific to template 1.
    {"source": "GoalComment_1", "table": "goal_comment"},
    {"source": "GoalEnum", "table": "goal_enum"},
    {"source": "GoalPlanTemplate", "table": "goal_plan_template"},
    {"source": "Goal_1", "table": "goal_1"},
    {"source": "Goal_10", "table": "goal_10"},
    {"source": "Goal_11", "table": "goal_11"},
    {"source": "Goal_12", "table": "goal_12"},
    {"source": "Goal_13", "table": "goal_13"},
    {"source": "Goal_2", "table": "goal_2"},
    {"source": "Goal_4", "table": "goal_4"},
    {"source": "Goal_5", "table": "goal_5"},
    {"source": "Goal_6", "table": "goal_6"},
    {"source": "Goal_7", "table": "goal_7"},
    {"source": "Goal_8", "table": "goal_8"},
    {"source": "Goal_9", "table": "goal_9"},
    {"source": "JobLocation", "table": "job_location"},
    {"source": "JobLocationLongFormat", "table": "job_location_long_format"},
    {"source": "JobLocationLongFormatConfigGO", "table": "job_location_long_format_config_go"},
    {"source": "JobLocationShortFormat", "table": "job_location_short_format"},
    {"source": "JobLocationShortFormatConfigGO", "table": "job_location_short_format_config_go"},
    {"source": "MarketingBrand", "table": "marketing_brand"},
    {"source": "MentoringProgram", "table": "mentoring_program"},
    {
        "source": "MentoringProgramMatchedParticipant",
        "table": "mentoring_program_matched_participant",
    },
    {"source": "MyPendingWorkflow", "table": "my_pending_workflow"},
    {"source": "ONB2BuddyActivity", "table": "onb2_buddy_activity"},
    {"source": "ONB2BuddyActivityResponsible", "table": "onb2_buddy_activity_responsible"},
    {"source": "ONB2DataCollectionUserConfig", "table": "onb2_data_collection_user_config"},
    {"source": "ONB2EquipmentActivity", "table": "onb2_equipment_activity"},
    {"source": "ONB2EquipmentActivityResponsible", "table": "onb2_equipment_activity_responsible"},
    {"source": "ONB2EquipmentType", "table": "onb2_equipment_type"},
    {"source": "ONB2EquipmentTypeValue", "table": "onb2_equipment_type_value"},
    {"source": "ONB2Process", "table": "onb2_process"},
    {"source": "ONB2ProcessResponsible", "table": "onb2_process_responsible"},
    {"source": "ONB2ProcessTask", "table": "onb2_process_task"},
    {"source": "ONB2ProcessTrigger", "table": "onb2_process_trigger"},
    {"source": "ONB2ProcessUserTask", "table": "onb2_process_user_task"},
    {"source": "ONB2ProcessVariant", "table": "onb2_process_variant"},
    {"source": "OnboardingCandidateInfo", "table": "onboarding_candidate_info"},
    {"source": "OnboardingEquipment", "table": "onboarding_equipment"},
    {"source": "OnboardingEquipmentActivity", "table": "onboarding_equipment_activity"},
    {"source": "OnboardingEquipmentType", "table": "onboarding_equipment_type"},
    {"source": "OnboardingEquipmentTypeValue", "table": "onboarding_equipment_type_value"},
    {"source": "OnboardingGoal", "table": "onboarding_goal"},
    {"source": "OnboardingGoalActivity", "table": "onboarding_goal_activity"},
    {"source": "OnboardingGoalCategory", "table": "onboarding_goal_category"},
    {"source": "OnboardingInfo", "table": "onboarding_info"},
    {"source": "OnboardingMeetingActivity", "table": "onboarding_meeting_activity"},
    {"source": "OnboardingMeetingEvent", "table": "onboarding_meeting_event"},
    {"source": "OnboardingNewHireActivitiesStep", "table": "onboarding_new_hire_activities_step"},
    {"source": "OnboardingProcess", "table": "onboarding_process"},
    {"source": "Photo", "table": "photo"},
    {"source": "PickListV2", "table": "pick_list_v2"},
    {"source": "PickListValueV2", "table": "pick_list_value_v2"},
    {"source": "SimpleGoal", "table": "simple_goal"},
    {"source": "StateProvince", "table": "state_province"},
    {"source": "TodoEntryV2", "table": "todo_entry_v2"},
    {"source": "UserAccount", "table": "user_account"},
    {"source": "UserBadges", "table": "user_badges"},
    {"source": "WorkOrder", "table": "work_order"},
    {"source": "cust_Focus", "table": "cust_focus"},
    {"source": "cust_InvestmentPillar", "table": "cust_investment_pillar"},
    {"source": "cust_NationalIDAlertItems", "table": "cust_national_id_alert_items"},
    {"source": "cust_NationalIDAlerts", "table": "cust_national_id_alerts"},
    {"source": "cust_ONB_Payroll_Form", "table": "cust_onb_payroll_form"},
    {"source": "cust_Payroll_Forms", "table": "cust_payroll_forms"},
    {"source": "cust_Severance", "table": "cust_severance"},
    {"source": "cust_SeverancePaymentDetails", "table": "cust_severance_payment_details"},
    {"source": "cust_SubCategory", "table": "cust_sub_category"},
    {
        "source": "cust_costCenterSubCategoryMapping",
        "table": "cust_cost_center_sub_category_mapping",
    },
    # All remaining non-time entity sets from live SAP metadata.
    {"source": "Achievement", "table": "achievement", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "AchievementDevGoalDetail",
        "table": "achievement_dev_goal_detail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "AchievementGoalDetail",
        "table": "achievement_goal_detail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "Activity", "table": "activity", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "ActivityFeedback",
        "table": "activity_feedback",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "ActivityStatus", "table": "activity_status", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Attachment", "table": "attachment", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "BadgeTemplates", "table": "badge_templates", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Challenge", "table": "challenge", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "ChallengeInvitation",
        "table": "challenge_invitation",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CompanyProvisioner",
        "table": "company_provisioner",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ComplianceFormDataFieldValue",
        "table": "compliance_form_data_field_value",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ContinuousPerformanceUserPermission",
        "table": "continuous_performance_user_permission",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "CustomNav", "table": "custom_nav", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "DGExpression", "table": "dg_expression", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "DGField", "table": "dg_field", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "DGFieldOperator",
        "table": "dg_field_operator",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "DGFieldValue", "table": "dg_field_value", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "DGFilter", "table": "dg_filter", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "DGPeoplePool", "table": "dg_people_pool", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "DRTMPurgeFreeze",
        "table": "drtm_purge_freeze",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "DevGoalAchievements",
        "table": "dev_goal_achievements",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "DevGoalAchievementsList",
        "table": "dev_goal_achievements_list",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "DevGoalDetail", "table": "dev_goal_detail", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "DynamicGroup", "table": "dynamic_group", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "EMEvent", "table": "em_event", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "EMEventAttribute",
        "table": "em_event_attribute",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "EMEventPayload", "table": "em_event_payload", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "EmpCompensationCalculated",
        "table": "emp_compensation_calculated",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "EmpCompensationGroupSumCalculated",
        "table": "emp_compensation_group_sum_calculated",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ExternalLearner",
        "table": "external_learner",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ExternalLearnerAddressInfo",
        "table": "external_learner_address_info",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ExternalLearnerEmailInfo",
        "table": "external_learner_email_info",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ExternalLearnerPersonalInfo",
        "table": "external_learner_personal_info",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ExternalLearnerPhoneInfo",
        "table": "external_learner_phone_info",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "FOWfConfig", "table": "fo_wf_config", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "FOWfConfigStepApprover",
        "table": "fo_wf_config_step_approver",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "FeedbackFlag", "table": "feedback_flag", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "FoTranslation", "table": "fo_translation", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "Form360Participant",
        "table": "form360_participant",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360ParticipantCategory",
        "table": "form360_participant_category",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360ParticipantColumn",
        "table": "form360_participant_column",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360ParticipantConfig",
        "table": "form360_participant_config",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360ParticipantDetail",
        "table": "form360_participant_detail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360ParticipantSection",
        "table": "form360_participant_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "Form360Rater", "table": "form360_rater", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "Form360RaterSection",
        "table": "form360_rater_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360ReviewContentDetail",
        "table": "form360_review_content_detail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360SummaryViewCategory",
        "table": "form360_summary_view_category",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360SummaryViewRater",
        "table": "form360_summary_view_rater",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "Form360SummaryViewSection",
        "table": "form360_summary_view_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "FormCompetency", "table": "form_competency", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "FormCompetencySection",
        "table": "form_competency_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormCustomElement",
        "table": "form_custom_element",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormCustomElementListValue",
        "table": "form_custom_element_list_value",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormCustomSection",
        "table": "form_custom_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormItemConfiguration",
        "table": "form_item_configuration",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormJobDescSection",
        "table": "form_job_desc_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "FormJobRole", "table": "form_job_role", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "FormPMReviewContentDetail",
        "table": "form_pm_review_content_detail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormRaterListSection",
        "table": "form_rater_list_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormReviewInfoSection",
        "table": "form_review_info_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormReviewerInfoSection",
        "table": "form_reviewer_info_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormSectionConfiguration",
        "table": "form_section_configuration",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "FormSignatureSection",
        "table": "form_signature_section",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalAchievements",
        "table": "goal_achievements",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalAchievementsList",
        "table": "goal_achievements_list",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "GoalDetail", "table": "goal_detail", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "GoalPermission_1",
        "table": "goal_permission_1",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_10",
        "table": "goal_permission_10",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_1000",
        "table": "goal_permission_1000",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_11",
        "table": "goal_permission_11",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_12",
        "table": "goal_permission_12",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_13",
        "table": "goal_permission_13",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_2",
        "table": "goal_permission_2",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_3",
        "table": "goal_permission_3",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_4",
        "table": "goal_permission_4",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_5",
        "table": "goal_permission_5",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_6",
        "table": "goal_permission_6",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_7",
        "table": "goal_permission_7",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_8",
        "table": "goal_permission_8",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "GoalPermission_9",
        "table": "goal_permission_9",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "GoalPlanState", "table": "goal_plan_state", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalWeight", "table": "goal_weight", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Goal_1000", "table": "goal_1000", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Goal_3", "table": "goal_3", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "InitiativeAlignmentBean",
        "table": "initiative_alignment_bean",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationOnboardingData",
        "table": "job_application_onboarding_data",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationOnboardingStatus",
        "table": "job_application_onboarding_status",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "LocalizedData", "table": "localized_data", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "MDFBlockConfig", "table": "mdf_block_config", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "MDFEnumValue", "table": "mdf_enum_value", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "MDFLocalizedValue",
        "table": "mdf_localized_value",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "MDFTenantPreferredTimeZone",
        "table": "mdf_tenant_preferred_time_zone",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "ONB2ActivityNudgeDetails",
        "table": "onb2_activity_nudge_details",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "PPXOIDCConfig", "table": "ppxoidc_config", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "PersonKey", "table": "person_key", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Picklist", "table": "picklist", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "PicklistLabel", "table": "picklist_label", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "PicklistOption", "table": "picklist_option", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "RBPBasicPermission",
        "table": "rbp_basic_permission",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "RBPRole", "table": "rbp_role", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "RBPRule", "table": "rbp_rule", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "SAPSystemConfiguration",
        "table": "sap_system_configuration",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "SuccessStoreContent",
        "table": "success_store_content",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "SuccessStoreContentBlob",
        "table": "success_store_content_blob",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteAnonymizationConfig",
        "table": "tr_bizx_suite_anonymization_config",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteAnonymizationFieldConfig",
        "table": "tr_bizx_suite_anonymization_field_config",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteExecutionParamValueSource",
        "table": "tr_bizx_suite_execution_param_value_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteExecutionParamsSource",
        "table": "tr_bizx_suite_execution_params_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteExecutionSource",
        "table": "tr_bizx_suite_execution_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteExecutionTarget",
        "table": "tr_bizx_suite_execution_target",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteLifeCycleStageEventForServiceErrorSource",
        "table": "tr_bizx_suite_life_cycle_stage_event_for_service_error_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteLifeCycleStageEventForServiceSource",
        "table": "tr_bizx_suite_life_cycle_stage_event_for_service_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteLifeCycleStageEventForServiceWarningSource",
        "table": "tr_bizx_suite_life_cycle_stage_event_for_service_warning_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteLifeCycleStageEventForServiceWarningSourceArguments",
        "table": "tr_bizx_suite_life_cycle_stage_event_for_service_warning_source_arguments",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteStageExecutionSource",
        "table": "tr_bizx_suite_stage_execution_source",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteStageExecutionSourceError",
        "table": "tr_bizx_suite_stage_execution_source_error",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteStageExecutionSourceWarning",
        "table": "tr_bizx_suite_stage_execution_source_warning",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteStageExecutionSourceWarningsArgument",
        "table": "tr_bizx_suite_stage_execution_source_warnings_argument",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TRBizxSuiteStageExecutionTarget",
        "table": "tr_bizx_suite_stage_execution_target",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "ThemeConfig", "table": "theme_config", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "ThemeInfo", "table": "theme_info", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "ThemeTemplate", "table": "theme_template", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Todo", "table": "todo", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "UserPermissions",
        "table": "user_permissions",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "UserRewardInfo", "table": "user_reward_info", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "WfRequest", "table": "wf_request", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "WfRequestComments",
        "table": "wf_request_comments",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "WfRequestParticipator",
        "table": "wf_request_participator",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "WfRequestStep", "table": "wf_request_step", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "WfRequestUIData",
        "table": "wf_request_ui_data",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "WorkflowAllowedActionList",
        "table": "workflow_allowed_action_list",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "cust_CSFJobAtt", "table": "cust_csf_job_att", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "cust_TR_TotalRewards",
        "table": "cust_tr_total_rewards",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    # Remaining metadata entity sets. Time tracking and time accounts were previously
    # deferred for sync duration; they are now in scope.
    #
    # The highest-volume members here (EmployeeTime, EmployeeTimeSheetEntry,
    # EmployeeTimeValuationResult, TimeAccountDetail, EmployeeTimeSheet) all expose
    # the standard `lastModifiedDateTime`, so only the first sync pays full cost;
    # later runs are incremental.
    {
        "source": "AccrualCalculationBase",
        "table": "accrual_calculation_base",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "EmpTimeAccountBalance",
        "table": "emp_time_account_balance",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "EmployeeTime", "table": "employee_time", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "EmployeeTimeCalendar",
        "table": "employee_time_calendar",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "EmployeeTimeSheet",
        "table": "employee_time_sheet",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "EmployeeTimeSheetEntry",
        "table": "employee_time_sheet_entry",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "EmployeeTimeValuationResult",
        "table": "employee_time_valuation_result",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "PeriodicTimeAccountUpdateProfile",
        "table": "periodic_time_account_update_profile",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "RecalculationBasedField",
        "table": "recalculation_based_field",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "TimeAccount", "table": "time_account", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "TimeAccountDetail",
        "table": "time_account_detail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountPayout",
        "table": "time_account_payout",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountPayoutProfile",
        "table": "time_account_payout_profile",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountPostingRule",
        "table": "time_account_posting_rule",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountPurchaseProfile",
        "table": "time_account_purchase_profile",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountPurchaseProfilePayComponentAssignment",
        "table": "time_account_purchase_profile_pay_component_assignment",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountSnapshot",
        "table": "time_account_snapshot",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountSummary",
        "table": "time_account_summary",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountType",
        "table": "time_account_type",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "TimeAccountTypeAUS",
        "table": "time_account_type_aus",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    # Calibration.
    {
        "source": "CalibrationSession",
        "table": "calibration_session",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CalibrationSessionParticipantsInfo",
        "table": "calibration_session_participants_info",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CalibrationSessionReviewer",
        "table": "calibration_session_reviewer",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CalibrationSessionSubject",
        "table": "calibration_session_subject",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CalibrationSubjectComment",
        "table": "calibration_subject_comment",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CalibrationSubjectRank",
        "table": "calibration_subject_rank",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "CalibrationTemplate",
        "table": "calibration_template",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    # Recruiting and onboarding; most are gated on Admin Center export switches.
    {
        "source": "CandidateComments",
        "table": "candidate_comments",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationAssessmentOrder",
        "table": "job_application_assessment_order",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationAssessmentReport",
        "table": "job_application_assessment_report",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationBackgroundCheckRequest",
        "table": "job_application_background_check_request",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationBackgroundCheckResult",
        "table": "job_application_background_check_result",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationStatusAuditTrail",
        "table": "job_application_status_audit_trail",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobApplicationStatusLabel",
        "table": "job_application_status_label",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "JobReqQuestion", "table": "job_req_question", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "JobReqScreeningQuestionChoice",
        "table": "job_req_screening_question_choice",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobRequisitionAssessment",
        "table": "job_requisition_assessment",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobRequisitionLocale",
        "table": "job_requisition_locale",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "JobRequisitionLocaleFieldControls",
        "table": "job_requisition_locale_field_controls",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "RcmCompetency", "table": "rcm_competency", "page_size": LARGE_ENTITY_PAGE_SIZE},
    # Succession and talent.
    {
        "source": "NominationTarget",
        "table": "nomination_target",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "NomineeHistory", "table": "nominee_history", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "Successor", "table": "successor", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "TalentGraphicOption",
        "table": "talent_graphic_option",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    # TalentRatings has no timestamp property at all (14 fields, none date-like), so
    # it cannot sync incrementally and reloads all ~724K rows every run.
    {"source": "TalentRatings", "table": "talent_ratings", "page_size": LARGE_ENTITY_PAGE_SIZE},
    # Remaining singletons.
    {
        "source": "CurrencyConversion",
        "table": "currency_conversion",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {"source": "Entity", "table": "entity", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "LegacyPositionEntity",
        "table": "legacy_position_entity",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "MentoringProgramParticipantInfo",
        "table": "mentoring_program_participant_info",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    {
        "source": "cust_JobHistory",
        "table": "cust_job_history",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
    # SAP operation-response types. `$top=1` returns
    # `COE_UNSUPPORTED_FEATURE: Unsupported feature: queryable`, so these can never
    # return rows. Declared only so the connector covers all metadata entity sets;
    # they are expected permanent members of `connector_sync_issues`.
    {"source": "InlineResult", "table": "inline_result", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "InnerMessage", "table": "inner_message", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "MessageDetail", "table": "message_detail", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "UpsertResult", "table": "upsert_result", "page_size": LARGE_ENTITY_PAGE_SIZE},
    # Every GoalComment_<templateId> endpoint returns the same tenant-wide comment set
    # (verified identical across multiple endpoints). They are declared for complete
    # metadata entity coverage; `goal_comment` above remains the copy to query.
    {"source": "GoalComment_2", "table": "goal_comment_2", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_3", "table": "goal_comment_3", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_4", "table": "goal_comment_4", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_5", "table": "goal_comment_5", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_6", "table": "goal_comment_6", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_7", "table": "goal_comment_7", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_8", "table": "goal_comment_8", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_9", "table": "goal_comment_9", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_10", "table": "goal_comment_10", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_11", "table": "goal_comment_11", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_12", "table": "goal_comment_12", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {"source": "GoalComment_13", "table": "goal_comment_13", "page_size": LARGE_ENTITY_PAGE_SIZE},
    {
        "source": "GoalComment_1000",
        "table": "goal_comment_1000",
        "page_size": LARGE_ENTITY_PAGE_SIZE,
    },
]

_NON_IDENTIFIER_CHARACTER = re.compile(r"[^0-9a-zA-Z_]+")
_LOWER_OR_DIGIT_TO_UPPER = re.compile(r"([a-z0-9])([A-Z])")
_ACRONYM_TO_WORD = re.compile(r"([A-Z]+)([A-Z][a-z])")
_REPEATED_UNDERSCORES = re.compile(r"_+")
_SAP_DATE = re.compile(r"^/Date\((-?\d+)([+-]\d{4})?\)/$")


def normalize_field_name(field_name):
    """Convert SAP OData field names into stable destination column names."""
    value = _NON_IDENTIFIER_CHARACTER.sub("_", field_name)
    value = _ACRONYM_TO_WORD.sub(r"\1_\2", value)
    value = _LOWER_OR_DIGIT_TO_UPPER.sub(r"\1_\2", value)
    value = _REPEATED_UNDERSCORES.sub("_", value).strip("_").lower()
    return value or "unnamed_field"


_UNIX_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_successfactors_datetime(value):
    """Parse SAP OData timestamps into UTC datetimes.

    Uses timedelta arithmetic from the epoch rather than datetime.fromtimestamp(), since the
    latter raises OSError on Windows for out-of-range values (e.g. SAP's negative-millisecond
    sentinel dates for "no date set", which land before 1970 or outside the platform's C library
    range).
    """
    if not isinstance(value, str):
        raise ValueError(f"Unsupported timestamp value: {value!r}")
    match = _SAP_DATE.match(value)
    if match:
        return _UNIX_EPOCH_UTC + timedelta(milliseconds=int(match.group(1)))
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).astimezone(
        timezone.utc
    )


def format_successfactors_filter_datetime(value):
    """Apply a small overlap window to a stored UTC cursor."""
    parsed = parse_successfactors_datetime(value) - timedelta(seconds=INCREMENTAL_OVERLAP_SECONDS)
    return parsed.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_date_value(value):
    """Convert SAP's raw `/Date(milliseconds[+-offset])/` wire format to a standard UTC ISO 8601 string.

    Values that don't match that exact pattern (including plain strings that merely look
    date-like) are returned unchanged, so only genuine SAP date/time fields are touched. A
    handful of SAP tenants use out-of-range sentinel values (e.g. for "no date set") that fall
    outside Python's datetime range; those are left in their raw wire format rather than
    failing the whole record.
    """
    if not isinstance(value, str) or not _SAP_DATE.match(value):
        return value
    try:
        return parse_successfactors_datetime(value).isoformat().replace("+00:00", "Z")
    except (OverflowError, ValueError):
        return value


def resolve_cursor_field(source_entity, metadata_fields):
    """Return the modification-timestamp property to use as this entity's cursor.

    SAP's standard `lastModifiedDateTime` wins when present. Otherwise an entity may
    name a verified substitute in ALTERNATIVE_CURSOR_FIELDS, which is what lets
    entities that would otherwise truncate-and-reload sync incrementally. Returning
    None means no reliable cursor exists and the table falls back to a full snapshot.
    """
    if source_entity in NO_CURSOR_ENTITIES:
        return None
    if LAST_MODIFIED_FIELD in metadata_fields:
        return LAST_MODIFIED_FIELD
    alternative = ALTERNATIVE_CURSOR_FIELDS.get(source_entity)
    if alternative and alternative not in metadata_fields:
        # Treated as fatal rather than ignored: silently dropping to a full snapshot
        # would look like a working sync while quietly reloading the table forever.
        raise RuntimeError(
            f"ALTERNATIVE_CURSOR_FIELDS names '{alternative}' for {source_entity}, "
            "which is absent from live metadata"
        )
    return alternative


def state_key(entity):
    """Return the per-entity cursor key.

    An entity listed in ENTITY_SCOPE_VERSION gets an extra suffix so it abandons its
    old cursor exactly once, without forcing the other 742 entities to resync.
    """
    scope = ENTITY_SCOPE_VERSION.get(entity.get("source"))
    scope_suffix = f"_{scope}" if scope else ""
    return f"{entity['table']}_{STATE_VERSION}{scope_suffix}_last_modified_date_time"


def build_row(record, entity):
    """Build a row with every metadata-declared field, real or navigation-link.

    - Scalar values have SAP's raw `/Date(...)/ ` timestamps normalized to UTC ISO 8601.
    - Dict-valued fields are unexpanded navigation-link stubs (`{"__deferred": {"uri": ...}}`);
      the link URI is kept as the column's value so the navigation column itself always
      exists (resolving the link would need a separate API call per record per link).
    - Any metadata-declared field absent from this particular record (SAP commonly omits
      null/unused optional properties from the JSON payload) is filled with None so every
      destination column exists from the very first sync.
    """
    row = {}
    destination_by_source = entity.get("destination_by_source", {})
    for source_key, value in record.items():
        if source_key == "__metadata":
            continue
        if isinstance(value, dict):
            value = (
                value.get("__deferred", {}).get("uri")
                if "__deferred" in value
                else json.dumps(value)
            )
        else:
            value = normalize_date_value(value)
        destination_key = destination_by_source.get(source_key)
        if destination_key is None:
            destination_key = normalize_field_name(source_key)
        if destination_key in row:
            raise RuntimeError(
                f"Duplicate destination field '{destination_key}' for {entity['source']}"
            )
        row[destination_key] = value
    for destination_key in entity["all_destination_fields"]:
        row.setdefault(destination_key, None)
    missing_keys = [key for key in entity["primary_key"] if row.get(key) is None]
    if missing_keys:
        raise RuntimeError(
            f"{entity['source']} record is missing key fields: {', '.join(missing_keys)}"
        )
    return row


def raise_for_error(response, source_entity):
    """Raise a sanitized error for a failed SuccessFactors request."""
    if response.status_code == 200:
        return
    try:
        error = response.json().get("error", {})
        code = error.get("code", "")
        message = error.get("message", {}).get("value", "")
    except ValueError:
        code, message = "", ""
    if response.status_code == 403 and "COE_GENERAL_FORBIDDEN" in code:
        log.error(
            f"SAP SuccessFactors denied access to {source_entity} (COE_GENERAL_FORBIDDEN). "
            "The integration user lacks permission for this entity or field."
        )
        raise RuntimeError(
            f"Permission denied by SAP SuccessFactors for {source_entity} (COE_GENERAL_FORBIDDEN)"
        )
    log.error(
        f"SAP SuccessFactors returned HTTP {response.status_code} for {source_entity}: {code} {message}".strip()
    )
    raise RuntimeError(
        f"SAP SuccessFactors request failed for {source_entity} (HTTP {response.status_code})"
    )


def build_entity_url(
    source_entity,
    cursor,
    order_by,
    effective_dated=False,
    page_size=None,
    skip=0,
    select_fields=None,
    cursor_field=LAST_MODIFIED_FIELD,
):
    """Build a bounded OData v2 query with deterministic pagination.

    Fields are normally left unrestricted so every metadata property is synced. An
    entity listed in EXCLUDED_FIELDS instead passes an explicit `select_fields` list,
    which is the only way OData v2 can omit a row-suppressing property.

    Effective-dated entities get a standalone maximal `fromDate`/`toDate` window so SAP
    returns every historical and future-dated record instead of applying its implicit
    `asOfDate=today` default. The window is applied to initial (no cursor) and incremental
    (cursor) requests alike: the cursor `$filter` selects *which records changed recently*,
    while `fromDate`/`toDate` decides *which effective-dated slices of those records are
    visible at all*, so omitting it on incremental requests would silently drop the
    historical slices of every recently-changed record.

    An entity in ENTITY_FILTERS contributes a mandatory predicate that is ANDed with
    the cursor filter, so incremental runs keep both conditions.

    `cursor_field` is the property the incremental filter is applied to. It defaults to
    SAP's standard `lastModifiedDateTime` and is overridden per entity via
    ALTERNATIVE_CURSOR_FIELDS. SAP accepts a `datetimeoffset` literal for both
    `Edm.DateTime` and `Edm.DateTimeOffset` properties (verified per entity), so one
    literal form covers every configured cursor.
    """
    query = {"$format": "json", "$paging": "snapshot"}
    if select_fields:
        query["$select"] = ",".join(select_fields)
    if order_by:
        query["$orderby"] = ",".join(order_by)
    if effective_dated:
        query["fromDate"] = HISTORY_FROM_DATE
        query["toDate"] = HISTORY_TO_DATE
    predicates = []
    entity_filter = ENTITY_FILTERS.get(source_entity)
    if entity_filter:
        predicates.append(entity_filter)
    if cursor:
        predicates.append(
            f"{cursor_field} ge datetimeoffset'{format_successfactors_filter_datetime(cursor)}'"
        )
    if predicates:
        query["$filter"] = (
            " and ".join(f"({predicate})" for predicate in predicates)
            if len(predicates) > 1
            else predicates[0]
        )
    if page_size:
        query["$top"] = page_size
        query["$inlinecount"] = "allpages"
        if skip:
            query["$skip"] = skip
    return f"{BASE_URL}/{source_entity}?{urllib.parse.urlencode(query)}"


def build_expand_url(parent_source, navigation_property, parent_keys, page_size, skip=0):
    """Build a deterministic parent query for a navigation-only entity."""
    query = {
        "$format": "json",
        "$paging": "snapshot",
        "$select": ",".join([*parent_keys, navigation_property]),
        "$expand": navigation_property,
        "$orderby": ",".join(parent_keys),
        "$top": page_size,
        "$inlinecount": "allpages",
    }
    if skip:
        query["$skip"] = skip
    return f"{BASE_URL}/{parent_source}?{urllib.parse.urlencode(query)}"


def is_effective_dated(key_fields, field_types):
    """True if live metadata shows this entity is effective-dated.

    SuccessFactors models effective dating by including an effective-start date of type
    `Edm.DateTime` in the entity key: `startDate` (EC), `effectiveStartDate` (MDF), or
    `mdfSystemEffectiveStartDate` (system-effective MDF). Child entities of an
    effective-dated parent carry the parent-qualified form of the same field
    (e.g. `PaymentInformationV3_effectiveStartDate`), so the segment after the last
    underscore is what gets matched. This is a platform-wide SAP convention, which is why
    detection needs no hand-maintained list of entity names.
    """
    return any(
        field_types.get(key) == "Edm.DateTime"
        and key.rsplit("_", 1)[-1] in EFFECTIVE_START_DATE_KEY_FIELDS
        for key in key_fields
    )


def is_unsupported_date_window_error(response):
    """True if SuccessFactors rejected the standalone `fromDate`/`toDate` window itself.

    Metadata-based detection is the primary signal, but a tenant/entity combination can
    still refuse the window (an entity that looks effective-dated but is not queryable
    that way). Rather than hand-maintaining exceptions, `iter_records()` treats a rejected
    parameter as a signal to retry the same request without the window.
    """
    try:
        error = response.json().get("error", {})
    except ValueError:
        return False
    message = error.get("message", {}).get("value", "")
    return "fromDate" in message or "toDate" in message or "asOfDate" in message


def is_not_sortable_error(response):
    """True if SuccessFactors rejected the request because a key field can't be used in $orderby."""
    try:
        error = response.json().get("error", {})
    except ValueError:
        return False
    message = error.get("message", {}).get("value", "")
    return error.get("code") == "COE_BAD_PROPERTY_EXPRESSION" and "is not sortable" in message


def build_select_fields(source_entity, metadata_fields):
    """Return the explicit `$select` list for an entity in EXCLUDED_FIELDS, else None.

    Returning None keeps the default unrestricted request, so only the handful of
    entities with a known row-suppressing field pay the cost of pinning a field list.
    Keys are never excludable, so they are always present in the result.
    """
    excluded = EXCLUDED_FIELDS.get(source_entity)
    if not excluded:
        return None
    unknown = sorted(excluded - metadata_fields)
    if unknown:
        # A renamed or removed property would otherwise turn the exclusion into a
        # silent no-op and quietly bring the suppressed rows back.
        log.warning(
            f"{source_entity} EXCLUDED_FIELDS names field(s) absent from live metadata: {', '.join(unknown)}"
        )
    selected = sorted(metadata_fields - excluded)
    if not selected:
        raise RuntimeError(f"EXCLUDED_FIELDS leaves no selectable fields for {source_entity}")
    return selected


def iter_records(
    session,
    source_entity,
    cursor,
    order_by,
    effective_dated=False,
    page_size=None,
    select_fields=None,
    cursor_field=LAST_MODIFIED_FIELD,
):
    """Yield every field of every record from an OData v2 entity, following server pagination.

    `effective_dated` adds the full `fromDate`/`toDate` history window (see
    build_entity_url()). SAP echoes every standalone parameter into the `__next` link it
    returns, so the window and the cursor filter stay applied across all pages.

    `select_fields` restricts the request to an explicit property list for entities in
    EXCLUDED_FIELDS; it is None for every other entity, which keeps the full field set.
    """
    skip = 0
    url = build_entity_url(
        source_entity,
        cursor,
        order_by,
        effective_dated,
        page_size,
        skip,
        select_fields,
        cursor_field,
    )
    first_request = True
    while url:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if (
            first_request
            and response.status_code == 400
            and effective_dated
            and is_unsupported_date_window_error(response)
        ):
            # The entity's metadata key looks effective-dated but this SuccessFactors
            # service refuses the standalone date window. Retry once without it: the
            # entity then falls back to SAP's default view rather than failing the sync.
            log.warning(
                f"{source_entity} rejected the fromDate/toDate history window; retrying without it"
            )
            effective_dated = False
            url = build_entity_url(
                source_entity,
                cursor,
                order_by,
                effective_dated,
                page_size,
                skip,
                select_fields,
                cursor_field,
            )
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        if (
            first_request
            and response.status_code == 400
            and order_by
            and is_not_sortable_error(response)
        ):
            # A handful of entities declare a primary key field that SAP's own OData
            # service refuses to sort by (e.g. backgroundElementId, country, seqNumber).
            # Retry without $orderby -- and also without $top, because unordered
            # $top/$skip offset paging is precisely the pattern SAP documents as losing
            # and duplicating records: with no stable sort order, a record can move
            # between pages as the offset advances, so it is read twice or never.
            # Dropping $top makes SAP emit a __next link and drive pagination itself,
            # which stays internally consistent without requiring an $orderby.
            log.warning(
                f"{source_entity} key field(s) are not sortable; retrying without $orderby "
                "using server-driven pagination"
            )
            order_by = []
            page_size = None
            url = build_entity_url(
                source_entity,
                cursor,
                order_by,
                effective_dated,
                page_size,
                skip,
                select_fields,
                cursor_field,
            )
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        first_request = False
        raise_for_error(response, source_entity)
        try:
            payload = response.json()["d"]
            results = payload["results"]
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError(f"Invalid OData response for {source_entity}") from error
        if not isinstance(results, list):
            raise RuntimeError(f"Invalid OData results for {source_entity}")
        for record in results:
            if not isinstance(record, dict):
                raise RuntimeError(f"Non-object record returned for {source_entity}")
            yield record
        url = payload.get("__next")
        if not url and page_size:
            skip += len(results)
            try:
                total_count = int(payload["__count"])
            except (KeyError, TypeError, ValueError):
                total_count = None
            if results and (
                (total_count is None and len(results) == page_size)
                or (total_count is not None and skip < total_count)
            ):
                url = build_entity_url(
                    source_entity,
                    cursor,
                    order_by,
                    effective_dated,
                    page_size,
                    skip,
                    select_fields,
                    cursor_field,
                )


def iter_records_parallel(
    session,
    source_entity,
    cursor,
    order_by,
    effective_dated=False,
    page_size=None,
    select_fields=None,
    cursor_field=LAST_MODIFIED_FIELD,
    workers=ENTITY_FETCH_WORKERS,
):
    """Parallel page-prefetch variant of iter_records().

    HTTP fetch+parse runs in a thread pool; records are yielded from the calling thread
    so op.upsert() / op.checkpoint() stay on the main thread (SDK requirement). Up to
    `workers * 2` pages are queued ahead to keep workers busy without unbounded memory.

    The first page is fetched synchronously so the same 400-error fallbacks as
    iter_records() apply (unsupported date window, unsortable key). Falls back to
    sequential iter_records() when workers <= 1, page_size is None, or a sortable-key
    error forces server-driven pagination.
    """
    if workers <= 1 or page_size is None:
        yield from iter_records(
            session,
            source_entity,
            cursor,
            order_by,
            effective_dated,
            page_size,
            select_fields,
            cursor_field,
        )
        return

    # First page: synchronous, with the same 400-error handling as iter_records().
    first_url = build_entity_url(
        source_entity,
        cursor,
        order_by,
        effective_dated,
        page_size,
        0,
        select_fields,
        cursor_field,
    )
    first_response = session.get(first_url, timeout=REQUEST_TIMEOUT_SECONDS)
    if (
        first_response.status_code == 400
        and effective_dated
        and is_unsupported_date_window_error(first_response)
    ):
        log.warning(
            f"{source_entity} rejected the fromDate/toDate history window; retrying without it"
        )
        effective_dated = False
        first_url = build_entity_url(
            source_entity,
            cursor,
            order_by,
            False,
            page_size,
            0,
            select_fields,
            cursor_field,
        )
        first_response = session.get(first_url, timeout=REQUEST_TIMEOUT_SECONDS)
    if first_response.status_code == 400 and order_by and is_not_sortable_error(first_response):
        log.warning(
            f"{source_entity} key field(s) are not sortable; retrying without $orderby "
            "using server-driven pagination"
        )
        yield from iter_records(
            session,
            source_entity,
            cursor,
            [],
            effective_dated,
            None,
            select_fields,
            cursor_field,
        )
        return
    raise_for_error(first_response, source_entity)
    try:
        first_payload = first_response.json()["d"]
        first_results = first_payload["results"]
    except (ValueError, KeyError, TypeError) as error:
        raise RuntimeError(f"Invalid OData response for {source_entity}") from error
    if not isinstance(first_results, list):
        raise RuntimeError(f"Invalid OData results for {source_entity}")
    try:
        total_count = int(first_payload["__count"])
    except (KeyError, TypeError, ValueError):
        total_count = None
    for record in first_results:
        if not isinstance(record, dict):
            raise RuntimeError(f"Non-object record returned for {source_entity}")
        yield record
    if not first_results or len(first_results) < page_size:
        return
    if total_count is not None and page_size >= total_count:
        return

    def fetch_page(skip):
        url = build_entity_url(
            source_entity,
            cursor,
            order_by,
            effective_dated,
            page_size,
            skip,
            select_fields,
            cursor_field,
        )
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        raise_for_error(response, source_entity)
        try:
            results = response.json()["d"]["results"]
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError(f"Invalid OData response for {source_entity}") from error
        if not isinstance(results, list):
            raise RuntimeError(f"Invalid OData results for {source_entity}")
        return results

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = deque()
        nxt = page_size  # first page already consumed
        done = False
        while not done:
            while len(pending) < workers * 2:
                pending.append((nxt, pool.submit(fetch_page, nxt)))
                nxt += page_size
            skip_pos, fut = pending.popleft()
            results = fut.result()
            for record in results:
                if not isinstance(record, dict):
                    raise RuntimeError(f"Non-object record returned for {source_entity}")
                yield record
            rows_so_far = skip_pos + len(results)
            at_end = (
                not results
                or len(results) < page_size
                or (total_count is not None and rows_so_far >= total_count)
            )
            if at_end:
                done = True
                for _, remaining in pending:
                    remaining.cancel()


def build_windowed_url(
    source_entity, window_from, window_to, order_by, page_size, skip=0, select_fields=None
):
    """Build a URL that fetches only records whose lastModifiedDateTime falls in [from, to]."""
    from_str = format_successfactors_filter_datetime(window_from.isoformat())
    to_str = format_successfactors_filter_datetime(window_to.isoformat())
    window_filter = (
        f"datetimeoffset'{from_str}' le {LAST_MODIFIED_FIELD} "
        f"and datetimeoffset'{to_str}' ge {LAST_MODIFIED_FIELD}"
    )
    predicates = []
    entity_filter = ENTITY_FILTERS.get(source_entity)
    if entity_filter:
        predicates.append(entity_filter)
    predicates.append(window_filter)
    query = {
        "$format": "json",
        "$paging": "snapshot",
        "$filter": (
            " and ".join(f"({p})" for p in predicates) if len(predicates) > 1 else predicates[0]
        ),
        "$orderby": ",".join(order_by),
        "$top": page_size,
        "$inlinecount": "allpages",
    }
    if select_fields:
        query["$select"] = ",".join(select_fields)
    if skip:
        query["$skip"] = skip
    return f"{BASE_URL}/{source_entity}?{urllib.parse.urlencode(query)}"


def iter_records_in_window(
    session, source_entity, window_from, window_to, order_by, page_size, select_fields=None
):
    """Yield all records whose lastModifiedDateTime falls within [window_from, window_to).

    Uses explicit $skip pagination so windows can be fetched independently. Designed for
    use by sync_entity_windowed(); not for general-purpose use.
    """
    skip = 0
    while True:
        url = build_windowed_url(
            source_entity, window_from, window_to, order_by, page_size, skip, select_fields
        )
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        raise_for_error(response, source_entity)
        try:
            payload = response.json()["d"]
            results = payload["results"]
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError(f"Invalid OData response for {source_entity} (windowed)") from error
        if not isinstance(results, list):
            raise RuntimeError(f"Invalid OData results for {source_entity} (windowed)")
        for record in results:
            if not isinstance(record, dict):
                raise RuntimeError(f"Non-object record returned for {source_entity} (windowed)")
            yield record
        skip += len(results)
        try:
            total_count = int(payload["__count"])
        except (KeyError, TypeError, ValueError):
            total_count = None
        at_end = (
            not results
            or len(results) < page_size
            or (total_count is not None and skip >= total_count)
        )
        if at_end:
            return


def iter_expanded_records(session, entity):
    """Yield a navigation-only entity through deterministic, bounded parent pages."""
    skip = 0
    while True:
        url = build_expand_url(
            entity["expand_parent"],
            entity["expand_navigation"],
            entity["parent_metadata_keys"],
            entity["page_size"],
            skip,
        )
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        raise_for_error(response, f"{entity['expand_parent']}/{entity['expand_navigation']}")
        try:
            payload = response.json()["d"]
            parents = payload["results"]
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError(
                f"Invalid OData response for {entity['source']} expansion"
            ) from error
        if not isinstance(parents, list):
            raise RuntimeError(f"Invalid OData results for {entity['source']} expansion")
        for parent in parents:
            expanded = parent.get(entity["expand_navigation"])
            if expanded is None:
                continue
            records = (
                expanded.get("results")
                if isinstance(expanded, dict) and "results" in expanded
                else [expanded]
            )
            if not isinstance(records, list):
                raise RuntimeError(f"Invalid expanded records for {entity['source']}")
            for record in records:
                if not isinstance(record, dict) or "__deferred" in record:
                    raise RuntimeError(f"Invalid expanded record for {entity['source']}")
                yield record
        skip += len(parents)
        try:
            total_count = int(payload["__count"])
        except (KeyError, TypeError, ValueError):
            total_count = None
        if (
            not parents
            or (total_count is None and len(parents) < entity["page_size"])
            or (total_count is not None and skip >= total_count)
        ):
            break


def build_form_expansion_paths(entities, form_type):
    """Resolve configured form tables to their navigation paths from FormHeader.

    Each entity maps to a tuple of paths. The breadth-first walk reaches every entity
    once, which is enough for structural tables, but a few navigations carry the data
    stakeholders actually report on and share a destination table with an earlier path
    (FormUserRatingComment is reached first through a competency section). Those are
    listed in FORM_SUPPLEMENTARY_NAVIGATIONS and expanded in addition to the tree.
    """
    if form_type not in FORM_TYPES:
        raise ValueError(f"Unsupported SuccessFactors form type: {form_type}")
    entity_by_source = {entity["source"]: entity for entity in entities}
    metadata_targets = {
        source: entity.get("navigation_targets", {}) for source, entity in entity_by_source.items()
    }
    required = {
        entity["source"]
        for entity in entities
        if entity["source"].startswith("Form")
        and entity["source"] not in FORM_EXPANSION_EXCLUSIONS
        and entity["source"] not in FORM_TYPE_ONLY_ENTITIES[form_type]
        and entity["source"] != "FormHeader"
    }
    paths = {"FormHeader": ()}
    pending = ["FormHeader"]
    while pending:
        source = pending.pop(0)
        for navigation, target in metadata_targets.get(source, {}).items():
            if target not in required or target in paths:
                continue
            if navigation in FORM_TYPES.values() and navigation != FORM_TYPES[form_type]:
                continue
            paths[target] = (*paths[source], navigation)
            pending.append(target)
    missing = sorted(required - paths.keys())
    if missing:
        raise RuntimeError(
            f"FormHeader metadata has no {form_type} expansion path for: {', '.join(missing)}"
        )
    resolved = {source: [path] for source, path in paths.items()}
    for parent, navigations in FORM_SUPPLEMENTARY_NAVIGATIONS.items():
        if parent not in paths:
            continue
        for navigation in navigations:
            target = metadata_targets.get(parent, {}).get(navigation)
            if target is None or target not in required:
                continue
            candidate = (*paths[parent], navigation)
            if candidate not in resolved[target]:
                resolved[target].append(candidate)
    return {source: tuple(found) for source, found in resolved.items()}


def build_form_hierarchy_url(form_type, parent_keys, expansion_paths, form_ids):
    """Expand exactly the form IDs returned by a keyset page."""
    if not form_ids:
        raise ValueError("Cannot expand an empty form page")
    query = {
        "$format": "json",
        "$filter": (
            f"formTemplateType eq '{form_type}' and ("
            + " or ".join(f"formDataId eq {int(form_id)}" for form_id in form_ids)
            + ")"
        ),
        "$orderby": ",".join(parent_keys),
        "$expand": ",".join(expansion_paths),
        "$top": len(form_ids),
    }
    return f"{BASE_URL}/FormHeader?{urllib.parse.urlencode(query)}"


class TransientFormPageError(RuntimeError):
    """SAP kept failing a form page with a server/throttling error or a network error."""


class FormBatchMismatchError(RuntimeError):
    """SAP's expanded batch omitted (or added) forms relative to the IDs requested.

    Observed for formDataId 82534: listed by the ID page and expandable on its own,
    but silently dropped from a ten-form expanded batch. Recovered per form.
    """


class FormAccessDeniedError(RuntimeError):
    """SAP refuses the integration user access to at least one form in the request.

    SAP rejects the *whole* request with HTTP 400 [COE0018] when any single form in
    it is off limits, so this is isolated per form rather than failing the sync.
    """


class FormExpansionUnsupportedError(RuntimeError):
    """SAP crashed expanding a navigation SAP itself advertises in its metadata.

    Triggered by the `metricLookupTables` subtree on forms whose objective data SAP
    cannot load (`IObjectiveSubDetailSupportDataVO.getSubObjectiveList()` NPE). It is
    deterministic per form, so retrying is pointless; the request is retried without
    the fragile paths instead.
    """


FORM_ACCESS_DENIED_MARKER = "COE0018"
FORM_SERVER_BUG_MARKER = "ServerErrorException"
# Expanding this navigation makes SAP throw for many forms; see FormExpansionUnsupportedError.
FORM_FRAGILE_EXPANSION_SEGMENT = "metricLookupTables"
# The entities reached only through that navigation, and so incomplete when it is dropped.
FORM_FRAGILE_ENTITIES = (
    "FormObjectiveDetails",
    "FormObjectiveOtherDetailsItem",
    "FormObjectiveOtherDetailsItemCol",
    "FormObjectiveOtherDetailsItemValueCell",
)
# Off since 2026-10-02: SAP failed 1,904 batched pages on this navigation, yet every
# form succeeded individually and returned no rows. Requesting it only cost ~19k
# extra requests per sync. Set True to resume syncing the entities above.
FORM_REQUEST_FRAGILE_EXPANSIONS = False

# Incremental form loading. Each sync re-fetches only forms whose FormHeader or
# FormContent date moved since the last successful form phase (minus an overlap for
# SAP save lag), plus forms that could not be retrieved last time. A full pass over
# every form runs when the last one is older than the interval, or on demand via
# the `force_full_form_pass` configuration value. Neither mode truncates: removed
# child rows are identified by their FORM_FETCHED_AT_COLUMN stamp instead.
FORM_STATE_CURSOR_KEY = "form_hierarchy_v1_phase_started_at"
FORM_STATE_LAST_FULL_KEY = "form_hierarchy_v1_last_full_pass_started_at"
FORM_STATE_RETRY_KEY = "form_hierarchy_v1_retry_form_ids"
FORM_INCREMENTAL_OVERLAP_SECONDS = 2 * 60 * 60
FORM_FULL_PASS_INTERVAL_DAYS = 7
FORM_RETRY_ID_LIMIT = 5000
FORM_ID_LOOKUP_CHUNK = 500
FORM_CHANGE_PROBE_PAGE_SIZE = 1000
FORM_FETCHED_AT_COLUMN = "form_fetched_at"
FORM_FETCHED_ID_COLUMN = "fetched_form_data_id"
FORM_FETCH_LOG_TABLE = "form_fetch_log"
FORM_SYNC_RUNS_TABLE = "form_sync_runs"


def summarise_omitted_expansions(form_issues):
    """Roll degraded pages up per form type so one SAP defect writes one issue row."""
    summary = {}
    for issue in form_issues:
        if issue["error_type"] != "ExpansionOmitted":
            continue
        entry = summary.setdefault(
            issue["form_type"],
            {
                "forms": 0,
                "requests": 0,
                "omitted_paths": issue["omitted_paths"],
                "omitted_entities": list(FORM_FRAGILE_ENTITIES),
                "reason": issue["error"],
            },
        )
        entry["forms"] += issue["forms"]
        entry["requests"] += 1
    return summary


def is_form_access_denied(response):
    """True when SAP refused the request because a form in it is not permitted."""
    if response.status_code != 400:
        return False
    body = response.text or ""
    return FORM_ACCESS_DENIED_MARKER in body or "permission to access the form data" in body


def is_form_expansion_unsupported(response):
    """True when SAP crashed on an expansion it advertises but cannot serve."""
    return response.status_code == 500 and FORM_SERVER_BUG_MARKER in (response.text or "")


def drop_fragile_expansion_paths(expansion_paths):
    """Return the expansion paths SAP can serve for every form, and the ones dropped."""
    safe, fragile = [], []
    for path in expansion_paths:
        (fragile if FORM_FRAGILE_EXPANSION_SEGMENT in path.split("/") else safe).append(path)
    return safe, fragile


class FormRequestTiming:
    """Aggregate request and JSON parsing time across concurrent form fetches."""

    def __init__(self):
        self._lock = threading.Lock()
        self.requests = 0
        self.http_seconds = 0.0
        self.json_seconds = 0.0

    def record(self, http_seconds=0.0, json_seconds=0.0):
        with self._lock:
            if http_seconds:
                self.requests += 1
            self.http_seconds += http_seconds
            self.json_seconds += json_seconds


def request_form_parents(session, url, label, timing=None):
    """GET one FormHeader page, retrying transient failures with backoff."""
    for attempt in range(FORM_PAGE_MAX_ATTEMPTS):
        if attempt:
            time.sleep(2**attempt)
        try:
            request_started = time.perf_counter()
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as error:
            if timing is not None:
                timing.record(http_seconds=time.perf_counter() - request_started)
            log.warning(f"{label}: network error ({type(error).__name__}), attempt {attempt + 1}")
            continue
        if timing is not None:
            timing.record(http_seconds=time.perf_counter() - request_started)
        if is_form_access_denied(response):
            raise FormAccessDeniedError(f"{label}: SAP denied access to form data ([COE0018])")
        if is_form_expansion_unsupported(response):
            raise FormExpansionUnsupportedError(
                f"{label}: SAP failed to expand its own navigation ({FORM_SERVER_BUG_MARKER})"
            )
        if response.status_code >= 500 or response.status_code == 429:
            log.warning(
                f"{label}: SAP returned HTTP {response.status_code}, attempt {attempt + 1}"
            )
            continue
        raise_for_error(response, label)
        try:
            parse_started = time.perf_counter()
            payload = response.json()["d"]
            parents = payload["results"]
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError(f"Invalid OData response for {label}") from error
        finally:
            if timing is not None:
                timing.record(json_seconds=time.perf_counter() - parse_started)
        if not isinstance(parents, list) or any(
            not isinstance(parent, dict) for parent in parents
        ):
            raise RuntimeError(f"Invalid OData results for {label}")
        return parents, payload
    raise TransientFormPageError(f"{label} failed after {FORM_PAGE_MAX_ATTEMPTS} attempts")


def format_form_since(value):
    """Render a UTC datetime as the SAP filter literal used by form change probes."""
    return f"datetimeoffset'{value.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}'"


def build_form_header_probe_url(
    form_type, parent_keys, top, after_id=None, max_id=None, since=None
):
    """List form IDs in stable ascending key order without offset paging."""
    predicates = [f"formTemplateType eq '{form_type}'"]
    if since is not None:
        predicates.append(f"formLastModifiedDate gt {format_form_since(since)}")
    if after_id is not None:
        predicates.append(f"formDataId gt {int(after_id)}")
    if max_id is not None:
        predicates.append(f"formDataId le {int(max_id)}")
    query = {
        "$format": "json",
        "$filter": " and ".join(predicates),
        "$orderby": ",".join(parent_keys),
        "$select": "formDataId,formSubjectId",
        "$top": top,
    }
    return f"{BASE_URL}/FormHeader?{urllib.parse.urlencode(query)}"


def build_form_content_change_url(since, after_id=None, top=FORM_CHANGE_PROBE_PAGE_SIZE):
    """List form IDs whose FormContent changed, keyset-paged by formDataId."""
    predicates = [f"lastModifiedDate gt {format_form_since(since)}"]
    if after_id is not None:
        predicates.append(f"formDataId gt {int(after_id)}")
    query = {
        "$format": "json",
        "$filter": " and ".join(predicates),
        "$orderby": "formDataId",
        "$select": "formDataId",
        "$top": top,
    }
    return f"{BASE_URL}/FormContent?{urllib.parse.urlencode(query)}"


def build_form_identity_lookup_url(form_type, form_ids):
    """Resolve specific form IDs to identities of one form type."""
    query = {
        "$format": "json",
        "$filter": (
            f"formTemplateType eq '{form_type}' and ("
            + " or ".join(f"formDataId eq {int(form_id)}" for form_id in form_ids)
            + ")"
        ),
        "$orderby": "formDataId",
        "$select": "formDataId,formSubjectId",
        "$top": len(form_ids),
    }
    return f"{BASE_URL}/FormHeader?{urllib.parse.urlencode(query)}"


def collect_content_changed_form_ids(session, since, timing=None):
    """Return IDs of forms whose FormContent changed (any form type).

    FormContent has several rows per form, so a page can end mid-form; keyset paging
    with `gt` then skips that form's remaining rows, whose ID is already collected.
    """
    form_ids = set()
    last_id = None
    while True:
        rows = request_form_parents(
            session,
            build_form_content_change_url(since, last_id),
            f"FormContent changes after {last_id}",
            timing,
        )[0]
        if not rows:
            return form_ids
        ids = [int(row["formDataId"]) for row in rows]
        if ids != sorted(ids) or (last_id is not None and ids[0] <= last_id):
            raise RuntimeError("FormContent change probe returned non-increasing form IDs")
        form_ids.update(ids)
        # A short page is not proof of the end (SAP can cap pages); stop only on empty.
        last_id = ids[-1]


def collect_changed_form_identities(
    session, form_type, parent_keys, since, extra_ids, timing=None
):
    """Return the sorted identities of one form type to re-fetch incrementally.

    Combines forms whose FormHeader date moved with `extra_ids` (FormContent changes
    and forms not retrieved last sync), which are resolved to this form type here.
    """
    identities = {}
    last_id = None
    while True:
        rows = request_form_parents(
            session,
            build_form_header_probe_url(
                form_type, parent_keys, FORM_CHANGE_PROBE_PAGE_SIZE, last_id, since=since
            ),
            f"FormHeader[{form_type}] changes after {last_id}",
            timing,
        )[0]
        if not rows:
            break
        ids = [int(row["formDataId"]) for row in rows]
        if ids != sorted(set(ids)) or (last_id is not None and ids[0] <= last_id):
            raise RuntimeError(
                f"FormHeader[{form_type}] change probe returned non-increasing form IDs"
            )
        for row in rows:
            identities[int(row["formDataId"])] = row
        last_id = ids[-1]
    remaining = sorted({int(form_id) for form_id in extra_ids} - identities.keys())
    for start in range(0, len(remaining), FORM_ID_LOOKUP_CHUNK):
        chunk = remaining[start : start + FORM_ID_LOOKUP_CHUNK]
        rows = request_form_parents(
            session,
            build_form_identity_lookup_url(form_type, chunk),
            f"FormHeader[{form_type}] lookup {chunk[0]}..{chunk[-1]}",
            timing,
        )[0]
        for row in rows:
            identities[int(row["formDataId"])] = row
    return [identities[form_id] for form_id in sorted(identities)]


def parse_config_bool(value):
    """Fivetran passes configuration values as strings."""
    return str(value).strip().lower() in ("true", "1", "yes", "y")


def plan_form_sync(configuration, state, now):
    """Choose a full or incremental form phase; state changes only after success."""
    interval_days = FORM_FULL_PASS_INTERVAL_DAYS
    raw_interval = (configuration or {}).get("form_full_pass_interval_days")
    if raw_interval not in (None, ""):
        try:
            interval_days = float(raw_interval)
        except (TypeError, ValueError):
            log.warning(
                f"Ignoring invalid form_full_pass_interval_days={raw_interval!r}; "
                f"using {FORM_FULL_PASS_INTERVAL_DAYS}"
            )
    plan = {"started_at": now, "since": None, "retry_ids": [], "interval_days": interval_days}
    cursor = state.get(FORM_STATE_CURSOR_KEY)
    last_full = state.get(FORM_STATE_LAST_FULL_KEY)
    if parse_config_bool((configuration or {}).get("force_full_form_pass", "")):
        return {**plan, "mode": "full", "reason": "force_full_form_pass is set"}
    if not cursor or not last_full:
        return {**plan, "mode": "full", "reason": "no completed form phase in state"}
    last_full_at = parse_successfactors_datetime(last_full)
    if now - last_full_at >= timedelta(days=interval_days):
        return {
            **plan,
            "mode": "full",
            "reason": f"last full pass {last_full} is older than {interval_days:g} days",
        }
    since = parse_successfactors_datetime(cursor) - timedelta(
        seconds=FORM_INCREMENTAL_OVERLAP_SECONDS
    )
    return {
        **plan,
        "mode": "incremental",
        "since": since,
        "retry_ids": [int(form_id) for form_id in state.get(FORM_STATE_RETRY_KEY, [])],
        "reason": f"changes since {since.isoformat()} (last full pass {last_full})",
    }


def commit_form_sync_state(state, plan, form_issues):
    """Advance form state after a form phase that completed without aborting."""
    started = plan["started_at"].isoformat().replace("+00:00", "Z")
    state[FORM_STATE_CURSOR_KEY] = started
    if plan["mode"] == "full":
        state[FORM_STATE_LAST_FULL_KEY] = started
    retry_ids = sorted(
        {
            int(issue["form_data_id"])
            for issue in form_issues
            if issue["error_type"] == "FormNotRetrieved" and issue.get("form_data_id") is not None
        }
    )
    if len(retry_ids) > FORM_RETRY_ID_LIMIT:
        log.warning(
            f"{len(retry_ids):,} forms were not retrieved; only {FORM_RETRY_ID_LIMIT:,} are "
            "retried next incremental sync, the rest wait for the next full pass"
        )
        retry_ids = retry_ids[:FORM_RETRY_ID_LIMIT]
    state[FORM_STATE_RETRY_KEY] = retry_ids


def fetch_form_page(session, form_type, parent_keys, expansion_paths, identities, timing=None):
    """Fetch one expanded page, degrading rather than failing. Returns (parents, issues).

    SAP fails a whole request when any single form in it is off limits (HTTP 400
    [COE0018]) or when it cannot expand its own `metricLookupTables` navigation
    (HTTP 500). Retry each form with all paths first; only the form that triggers
    the SAP error loses the fragile paths.
    """
    form_ids = [identity["formDataId"] for identity in identities]
    label = f"FormHeader[{form_type}] formDataId={form_ids[0]}..{form_ids[-1]}"
    safe_paths, fragile_paths = drop_fragile_expansion_paths(expansion_paths)

    def attempt(paths, request_label, ids):
        url = build_form_hierarchy_url(form_type, parent_keys, paths, ids)
        parents = request_form_parents(session, url, request_label, timing)[0]
        returned = [parent.get("formDataId") for parent in parents]
        if len(returned) != len(ids) or set(map(str, returned)) != set(map(str, ids)):
            raise FormBatchMismatchError(
                f"{request_label}: SAP returned a different set of form IDs"
            )
        return parents

    try:
        return attempt(expansion_paths, label, form_ids), []
    except FormExpansionUnsupportedError:
        log.warning(f"{label}: isolating forms before omitting {FORM_FRAGILE_EXPANSION_SEGMENT}")
    except FormAccessDeniedError:
        log.info(f"{label}: SAP denied the page; retrying its forms individually")
    except (TransientFormPageError, FormBatchMismatchError) as error:
        log.warning(f"{error}; retrying its forms individually")

    parents = []
    issues = []
    for identity in identities:
        form_id = identity["formDataId"]
        single_label = f"FormHeader[{form_type}] formDataId={form_id}"
        paths, omitted = expansion_paths, []
        try:
            single = attempt(paths, single_label, [form_id])
        except FormExpansionUnsupportedError:
            paths, omitted = safe_paths, fragile_paths
            try:
                single = attempt(paths, single_label, [form_id])
            except RuntimeError as error:
                single, omitted = None, []
                failure = error
        except (TransientFormPageError, FormAccessDeniedError, FormBatchMismatchError) as error:
            single, failure = None, error
        if single is None:
            identity = {
                "form_type": form_type,
                "form_data_id": form_id,
                "form_subject_id": identity.get("formSubjectId"),
                "error": str(failure),
                "error_type": (
                    "PermissionBlocked"
                    if isinstance(failure, FormAccessDeniedError)
                    else "FormNotRetrieved"
                ),
            }
            log.error(f"Could not retrieve form content for {identity}")
            issues.append(identity)
            continue
        if omitted:
            issue = {
                "form_type": form_type,
                "form_data_id": form_id,
                "form_subject_id": identity.get("formSubjectId"),
                "error_type": "ExpansionOmitted",
                "error": f"{single_label}: SAP cannot expand {FORM_FRAGILE_EXPANSION_SEGMENT}",
                "omitted_paths": omitted,
                "forms": 1,
            }
            issues.append(issue)
        parents.extend(single)
    return parents, issues


def omitted_paths_by_form(issues):
    """Track omitted paths on the specific forms for which SAP refused them."""
    omitted = {}
    for issue in issues:
        if issue["error_type"] == "ExpansionOmitted":
            omitted.setdefault(str(issue["form_data_id"]), set()).update(issue["omitted_paths"])
    return omitted


def iter_all_form_identity_pages(session, form_type, parent_keys, timing=None):
    """Keyset-page every form ID of one type up to a fixed upper bound."""
    upper_query = {
        "$format": "json",
        "$filter": f"formTemplateType eq '{form_type}'",
        "$orderby": "formDataId desc",
        "$select": "formDataId",
        "$top": 1,
    }
    upper_url = f"{BASE_URL}/FormHeader?{urllib.parse.urlencode(upper_query)}"
    upper = request_form_parents(
        session, upper_url, f"FormHeader[{form_type}] upper bound", timing
    )[0]
    if not upper:
        return
    max_id = int(upper[0]["formDataId"])
    last_id = None
    while True:
        identities = request_form_parents(
            session,
            build_form_header_probe_url(
                form_type, parent_keys, FORM_EXPANSION_PAGE_SIZE, last_id, max_id
            ),
            f"FormHeader[{form_type}] IDs after {last_id}",
            timing,
        )[0]
        if not identities:
            return
        ids = [int(identity["formDataId"]) for identity in identities]
        if ids != sorted(set(ids)) or (last_id is not None and ids[0] <= last_id):
            raise RuntimeError(f"FormHeader[{form_type}] returned non-increasing form IDs")
        last_id = ids[-1]
        yield identities


def chunk_identities(identities, size=FORM_EXPANSION_PAGE_SIZE):
    """Split a pre-collected identity list into expansion-sized pages."""
    for start in range(0, len(identities), size):
        yield identities[start : start + size]


def iter_form_hierarchy_pages(
    session, form_type, parent_keys, expansion_paths, form_issues, timing=None, identity_pages=None
):
    """Yield expanded Review/360 pages in order, fetching a few pages concurrently.

    `identity_pages` defaults to every form of the type (a full pass); an incremental
    sync passes only the changed forms. Yields (parents, omitted_paths_by_form): only
    the forms SAP could not expand omit fragile paths; other forms on the same page
    retain their child records. Only HTTP runs on worker threads; callers upsert on
    the calling thread. Gaps are appended to `form_issues`.
    """
    if parent_keys != ["formDataId"]:
        raise RuntimeError(f"FormHeader keyset paging requires formDataId key, got {parent_keys}")
    if identity_pages is None:
        identity_pages = iter_all_form_identity_pages(session, form_type, parent_keys, timing)
    pending = deque()
    with ThreadPoolExecutor(max_workers=FORM_FETCH_WORKERS) as executor:
        for identities in identity_pages:
            if not identities:
                continue
            pending.append(
                executor.submit(
                    fetch_form_page,
                    session,
                    form_type,
                    parent_keys,
                    expansion_paths,
                    identities,
                    timing,
                )
            )
            if len(pending) >= FORM_FETCH_WORKERS * 2:
                parents, issues = pending.popleft().result()
                form_issues.extend(issues)
                yield parents, omitted_paths_by_form(issues)
        while pending:
            parents, issues = pending.popleft().result()
            form_issues.extend(issues)
            yield parents, omitted_paths_by_form(issues)


def iter_form_path_records(parent, navigation_path, source_entity):
    """Yield expanded entity records at a metadata-derived nested navigation path."""
    records = [parent]
    for navigation in navigation_path:
        children = []
        for record in records:
            value = record.get(navigation)
            if value is None:
                continue
            if isinstance(value, dict):
                if "__deferred" in value:
                    raise RuntimeError(
                        f"SAP did not expand '{navigation}' while retrieving {source_entity}"
                    )
                values = value.get("results", [value])
            elif isinstance(value, list):
                values = value
            else:
                raise RuntimeError(
                    f"Invalid expanded navigation '{navigation}' for {source_entity}"
                )
            if not isinstance(values, list) or any(
                not isinstance(child, dict) for child in values
            ):
                raise RuntimeError(f"Invalid expanded records for {source_entity}")
            children.extend(values)
        records = children
        if not records:
            break
    for record in records:
        if "__deferred" in record:
            raise RuntimeError(f"Invalid deferred record for {source_entity}")
        yield record


def compact_nested_payload(value):
    """Strip SAP plumbing from an expanded payload kept inline on a parent row.

    Removes `__metadata` blocks and unresolved `__deferred` link stubs so the stored
    JSON holds only reportable values.
    """
    if isinstance(value, list):
        return [compact_nested_payload(item) for item in value]
    if not isinstance(value, dict):
        return normalize_date_value(value)
    if "__deferred" in value:
        return None
    compacted = {}
    for key, item in value.items():
        if key == "__metadata":
            continue
        item = compact_nested_payload(item)
        if item is None and isinstance(value.get(key), dict):
            continue
        compacted[key] = item
    return compacted


def build_expanded_row(record, entity):
    """Keep expanded child payloads out of navigation-link columns on the parent row.

    Ratings are the exception: their expanded object is kept as JSON so the label and
    score sit on the section row, rather than leaving behind an unusable link URI.
    """
    navigation_fields = set(entity.get("navigation_fields", ()))
    flattened = {}
    for key, value in record.items():
        if key in navigation_fields and not (isinstance(value, dict) and "__deferred" in value):
            if key not in FORM_INLINE_EXPANSIONS:
                continue
            value = compact_nested_payload(value)
        flattened[key] = value
    return build_row(flattened, entity)


def sync_form_hierarchies(session, entities, checkpointer=None, plan=None):
    """Load Review and 360 forms through their mandatory FormHeader context.

    `plan` (from plan_form_sync) selects a full pass over every visible form or an
    incremental pass over changed forms; omitted, a full pass runs. Nothing is
    truncated: each child row carries the run's FORM_FETCHED_AT_COLUMN stamp, and
    FORM_FETCH_LOG_TABLE records the stamp of each form's latest fetch, so rows a
    form no longer has are those whose stamp is older than the form's latest fetch.
    """
    if plan is None:
        plan = {
            "mode": "full",
            "since": None,
            "retry_ids": [],
            "started_at": datetime.now(timezone.utc),
        }
    fetched_at = plan["started_at"].isoformat().replace("+00:00", "Z")
    entity_by_source = {entity["source"]: entity for entity in entities}
    if "FormHeader" not in entity_by_source:
        raise RuntimeError("Configured entities do not include FormHeader")
    paths_by_type = {
        form_type: build_form_expansion_paths(entities, form_type) for form_type in FORM_TYPES
    }
    skipped_sources = () if FORM_REQUEST_FRAGILE_EXPANSIONS else FORM_FRAGILE_ENTITIES
    header = entity_by_source["FormHeader"]
    log.info(f"Form phase mode={plan['mode']}: {plan.get('reason', 'full pass')}")
    extra_ids = set(plan.get("retry_ids") or [])
    if plan["mode"] == "incremental":
        content_ids = collect_content_changed_form_ids(session, plan["since"])
        log.info(
            f"Form changes: {len(content_ids):,} forms with FormContent changes; "
            f"{len(extra_ids):,} forms retried from the previous sync"
        )
        extra_ids |= content_ids
    counts = {source: 0 for source in entity_by_source}
    excluded_sources = sorted(FORM_EXPANSION_EXCLUSIONS)
    form_issues = []
    for source, reason in FORM_EXPANSION_EXCLUSIONS.items():
        if source in entity_by_source:
            log.warning(f"Skipping {source}: {reason}")
    for form_type, paths in paths_by_type.items():
        form_started = time.perf_counter()
        timing = FormRequestTiming()
        processing_seconds = 0.0
        expansion_paths = sorted(
            {
                "/".join(path)
                for source, found in paths.items()
                for path in found
                if source != "FormHeader" and path
            },
            key=lambda path: (path.count("/"), path),
        )
        skipped_paths = set()
        if not FORM_REQUEST_FRAGILE_EXPANSIONS:
            expansion_paths, skipped = drop_fragile_expansion_paths(expansion_paths)
            skipped_paths = set(skipped)
            if skipped_paths:
                log.info(
                    f"{form_type}: not requesting {len(skipped_paths)} "
                    f"{FORM_FRAGILE_EXPANSION_SEGMENT} paths; "
                    f"{', '.join(FORM_FRAGILE_ENTITIES)} are not synced"
                )
        forms_seen = 0
        identity_pages = None
        if plan["mode"] == "incremental":
            changed = collect_changed_form_identities(
                session, form_type, header["metadata_keys"], plan["since"], extra_ids, timing
            )
            log.info(f"{form_type}: {len(changed):,} changed forms to re-fetch")
            identity_pages = chunk_identities(changed)
        # FormHeader rows themselves come from the unfiltered top-level sync, which
        # also covers Compensation forms whose content SAP does not expose here.
        for parents, omitted_paths_by_form_id in iter_form_hierarchy_pages(
            session,
            form_type,
            header["metadata_keys"],
            expansion_paths,
            form_issues,
            timing,
            identity_pages,
        ):
            processing_started = time.perf_counter()
            for parent in parents:
                forms_seen += 1
                form_id = str(parent["formDataId"])
                omitted_paths = skipped_paths.union(omitted_paths_by_form_id.get(form_id, ()))
                for source, found in paths.items():
                    if source == "FormHeader" or source in skipped_sources:
                        continue
                    entity = entity_by_source[source]
                    # An entity can be reachable by more than one navigation; dedupe by
                    # key so a record seen twice is upserted once.
                    seen = set()
                    for path in found:
                        if "/".join(path) in omitted_paths:
                            continue
                        for record in iter_form_path_records(parent, path, source):
                            row = build_expanded_row(record, entity)
                            identity = tuple(row[key] for key in entity["primary_key"])
                            if identity in seen:
                                continue
                            seen.add(identity)
                            # Only rows owned by this form are stamped. Shared rows
                            # (FormTemplate, FormFolder) carry no formDataId; stamping
                            # them would tie them to whichever form was fetched last.
                            if str(row.get("form_data_id")) == form_id:
                                row[FORM_FETCHED_AT_COLUMN] = fetched_at
                                row[FORM_FETCHED_ID_COLUMN] = form_id
                            # The 'upsert' operation is used to insert or update data in the destination table.
                            # The first argument is the name of the destination table.
                            # The second argument is a dictionary containing the record to be upserted.
                            op.upsert(table=entity["table"], data=row)
                            counts[source] += 1
                # Written after the form's rows, so a form only counts as fetched at
                # this stamp once all of its child rows carry it.
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table=FORM_FETCH_LOG_TABLE,
                    data={
                        "form_data_id": form_id,
                        "form_type": form_type,
                        "form_subject_id": parent.get("formSubjectId"),
                        "last_fetched_at": fetched_at,
                        "fetch_mode": plan["mode"],
                    },
                )
                if checkpointer is not None:
                    checkpointer.maybe_checkpoint()
            processing_seconds += time.perf_counter() - processing_started
        plan["forms_fetched"] = plan.get("forms_fetched", 0) + forms_seen
        log.info(
            f"Completed {form_type} form extraction ({plan['mode']}): {forms_seen:,} forms; "
            f"elapsed={time.perf_counter() - form_started:.1f}s; "
            f"main-thread processing={processing_seconds:.1f}s; "
            f"HTTP requests={timing.requests}; worker HTTP={timing.http_seconds:.1f}s; "
            f"worker JSON decode={timing.json_seconds:.1f}s "
            "(concurrent worker times overlap)"
        )
    for source, count in counts.items():
        if count:
            log.info(
                f"Upserted {count:,} {source} records into {entity_by_source[source]['table']}"
            )
    if form_issues:
        log.warning(f"{len(form_issues)} form issues recorded; see {SYNC_ISSUE_TABLE}")
    return [source for source in excluded_sources if source in entity_by_source], form_issues


def flatten_private_key(private_key):
    """Accept a PEM private key or its base64 body for the SAP assertion endpoint."""
    if not isinstance(private_key, str):
        raise ValueError("sf_private_key must be a PEM private key or its base64 body")
    lines = [line.strip() for line in private_key.splitlines() if line.strip()]
    if lines and lines[0].startswith("-----BEGIN ") and lines[-1].startswith("-----END "):
        lines = lines[1:-1]
    flattened = "".join(lines)
    try:
        if not flattened or not base64.b64decode(flattened, validate=True):
            raise ValueError("Invalid sf_private_key")
    except binascii.Error as error:
        raise ValueError("Invalid sf_private_key") from error
    return flattened


class RateLimiter:
    """Token-bucket rate limiter capped at SAP_RATE_LIMIT_RPS requests per second.

    The native SuccessFactors connector proactively limits outbound requests to 40/sec.
    Without this, parallel workers can silently trigger SAP's server-side throttling,
    which drops records from pages without returning an HTTP error.
    """

    def __init__(self, rps):
        self._min_interval = 1.0 / rps
        self._lock = threading.Lock()
        self._last = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


_rate_limiter = RateLimiter(SAP_RATE_LIMIT_RPS)


class SuccessFactorsSession(requests.Session):
    """Refresh the SAP bearer token before expiry, including during long syncs."""

    def __init__(self, configuration):
        super().__init__()
        self._token_lock = threading.Lock()
        self.base_url = configuration["sf_api_url"].rstrip("/")
        self.headers.update({"Accept": "application/json"})
        configured = [field for field in OAUTH_FIELDS if configuration.get(field)]
        if configured:
            missing = [field for field in OAUTH_FIELDS if not configuration.get(field)]
            if missing:
                raise ValueError(
                    f"Incomplete SAP OAuth configuration; missing: {', '.join(missing)}"
                )
            self.oauth_configuration = configuration
            self.token_expires_at = 0
            flatten_private_key(configuration["sf_private_key"])
        elif configuration.get("sf_access_token"):
            self.oauth_configuration = None
            self.headers["Authorization"] = f"Bearer {configuration['sf_access_token']}"
        else:
            raise ValueError("Configure SAP OAuth credentials or sf_access_token")

    def refresh_access_token(self):
        """Obtain a token that will outlive the next stretch of requests.

        SAP reissues the *same* token until it expires, with `expires_in` counting
        down. A sync starting just before the 24h token lapses therefore gets a token
        with seconds (or zero) left; wait it out and request a fresh one.
        """
        for attempt in range(OAUTH_REFRESH_ATTEMPTS):
            try:
                token, lifetime = self._request_access_token()
            except RuntimeError as error:
                if attempt == OAUTH_REFRESH_ATTEMPTS - 1:
                    raise
                log.warning(f"{error}; retrying SAP OAuth (attempt {attempt + 2})")
                time.sleep(5 * 2**attempt)
                continue
            if lifetime >= OAUTH_MIN_TOKEN_LIFETIME_SECONDS:
                self.headers["Authorization"] = f"Bearer {token}"
                self.token_expires_at = time.monotonic() + lifetime - min(60, lifetime / 10)
                return
            log.warning(f"SAP OAuth token expires in {lifetime}s; waiting for a fresh token")
            time.sleep(max(lifetime, 0) + 5)
        raise RuntimeError("SAP OAuth kept returning an expiring access token")

    def _request_access_token(self):
        config = self.oauth_configuration
        oauth_base = self.base_url.removesuffix("/odata/v2")
        token_url = f"{oauth_base}/oauth/token"
        try:
            assertion_response = requests.post(
                f"{oauth_base}/oauth/idp",
                data={
                    "client_id": config["sf_client_id"],
                    "user_id": config["sf_user_id"],
                    "token_url": self.base_url,
                    "private_key": flatten_private_key(config["sf_private_key"]),
                },
                timeout=60,
                allow_redirects=False,
            )
            if assertion_response.status_code != 200:
                raise RuntimeError(
                    f"SAP OAuth assertion request failed (HTTP {assertion_response.status_code})"
                )
            assertion = assertion_response.text.strip()
            if not assertion:
                raise RuntimeError("SAP OAuth assertion response was empty")
            token_response = requests.post(
                token_url,
                data={
                    "client_id": config["sf_client_id"],
                    "company_id": config["sf_company_id"],
                    "grant_type": OAUTH_GRANT_TYPE,
                    "assertion": assertion,
                },
                timeout=60,
                allow_redirects=False,
            )
            if token_response.status_code != 200:
                raise RuntimeError(
                    f"SAP OAuth token request failed (HTTP {token_response.status_code})"
                )
        except requests.RequestException:
            raise RuntimeError("SAP OAuth request failed; check SAP connectivity") from None
        try:
            token_data = token_response.json()
            token = token_data["access_token"]
            lifetime = int(token_data["expires_in"])
            if not isinstance(token, str) or not token:
                raise ValueError("Invalid SAP OAuth token response")
        except (ValueError, KeyError, TypeError):
            raise RuntimeError(
                "SAP OAuth token response lacks a valid access_token or expires_in"
            ) from None
        return token, lifetime

    def request(self, method, url, **kwargs):
        _rate_limiter.acquire()
        if self.oauth_configuration and time.monotonic() >= self.token_expires_at:
            # Form pages are fetched concurrently; only one thread renews the token.
            with self._token_lock:
                if time.monotonic() >= self.token_expires_at:
                    self.refresh_access_token()
        return super().request(method, url, **kwargs)


def build_session(configuration):
    """Build a session that can renew SAP OAuth tokens, or use a supplied bearer token."""
    global BASE_URL
    BASE_URL = configuration["sf_api_url"].rstrip("/")
    return SuccessFactorsSession(configuration)


def fetch_metadata(session):
    """Fetch and parse authenticated OData v2 metadata (entity keys, properties, and nav links)."""
    response = session.get(
        f"{BASE_URL}/$metadata", headers={"Accept": "application/xml"}, timeout=60
    )
    raise_for_error(response, "$metadata")
    root = ET.fromstring(response.text)
    associations = {}
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "Association" or not element.attrib.get("Name"):
            continue
        associations[element.attrib["Name"]] = {
            child.attrib.get("Role"): child.attrib.get("Type", "").rsplit(".", 1)[-1]
            for child in element
            if child.tag.rsplit("}", 1)[-1] == "End"
        }
    entities = {}
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1] != "EntityType" or not element.attrib.get("Name"):
            continue
        keys = []
        fields = set()
        field_types = {}
        nav_properties = set()
        navigation_targets = {}
        for child in element:
            tag = child.tag.rsplit("}", 1)[-1]
            if tag == "Property":
                fields.add(child.attrib["Name"])
                field_types[child.attrib["Name"]] = child.attrib.get("Type")
            elif tag == "Key":
                keys.extend(ref.attrib["Name"] for ref in child if ref.attrib.get("Name"))
            elif tag == "NavigationProperty" and child.attrib.get("Name"):
                navigation_name = child.attrib["Name"]
                nav_properties.add(navigation_name)
                association_name = child.attrib.get("Relationship", "").rsplit(".", 1)[-1]
                target = associations.get(association_name, {}).get(child.attrib.get("ToRole"))
                if target:
                    navigation_targets[navigation_name] = target
        entities[element.attrib["Name"]] = {
            "keys": keys,
            "fields": fields,
            "field_types": field_types,
            "nav_properties": nav_properties,
            "navigation_targets": navigation_targets,
        }
    return entities


def discover_entities(session):
    """Resolve every configured entity's live metadata keys, primary key, and cursor support."""
    metadata = fetch_metadata(session)
    resolved = []
    for entity in ENTITIES:
        if ACTIVE_SOURCE_PREFIXES and not entity["source"].startswith(ACTIVE_SOURCE_PREFIXES):
            continue
        if entity["source"] in SKIPPED_ENTITIES:
            continue
        details = metadata.get(entity["source"])
        if not details:
            raise RuntimeError(f"Metadata does not contain entity '{entity['source']}'")
        if not details["keys"]:
            raise RuntimeError(f"Metadata declares no key fields for '{entity['source']}'")
        key_override = entity.get("key_override")
        if key_override:
            # A tiny, explicitly-documented exception list (see ENTITIES) for
            # the rare case where SAP's declared metadata key is incomplete.
            # Still validated against live metadata every run so a future SAP
            # fix or field rename is caught immediately instead of silently drifting.
            missing = [key for key in key_override if key not in details["fields"]]
            if missing:
                raise RuntimeError(
                    f"key_override for '{entity['source']}' references unknown field(s): {', '.join(missing)}"
                )
            keys = key_override
        else:
            keys = details["keys"]
        expand_parent = entity.get("expand_parent")
        parent_details = metadata.get(expand_parent) if expand_parent else None
        if expand_parent:
            if not parent_details:
                raise RuntimeError(f"Metadata does not contain expansion parent '{expand_parent}'")
            if entity["expand_navigation"] not in parent_details["nav_properties"]:
                raise RuntimeError(
                    f"Metadata does not contain navigation '{entity['expand_navigation']}' on '{expand_parent}'"
                )
            if not parent_details["keys"]:
                raise RuntimeError(
                    f"Metadata declares no key fields for expansion parent '{expand_parent}'"
                )
        cursor_field = resolve_cursor_field(entity["source"], details["fields"])
        has_cursor = cursor_field is not None
        # Checked against the union of SAP's declared key and any key_override so an
        # override can neither hide nor invent effective dating.
        effective_dated = is_effective_dated(
            set(details["keys"]) | set(keys), details["field_types"]
        )
        # Every metadata-declared property AND navigation-link property becomes a
        # destination column (see build_row()), so it always exists even on records/
        # pages where SAP omits it. Guard against two source names normalizing to the
        # same destination column, which would silently merge unrelated data.
        all_source_fields = sorted(details["fields"] | details["nav_properties"])
        destination_by_source = {}
        source_by_destination = {}
        for source_field in all_source_fields:
            destination_field = normalize_field_name(source_field)
            if destination_field in source_by_destination:
                raise RuntimeError(
                    f"Metadata field name collision for '{entity['source']}': '{source_field}' and "
                    f"'{source_by_destination[destination_field]}' both normalize to '{destination_field}'"
                )
            source_by_destination[destination_field] = source_field
            destination_by_source[source_field] = destination_field
        resolved_entity = {
            "source": entity["source"],
            "table": entity["table"],
            "metadata_keys": keys,
            "primary_key": [normalize_field_name(key) for key in keys],
            "has_cursor": has_cursor,
            "cursor_field": cursor_field,
            "cursor_column": normalize_field_name(cursor_field) if cursor_field else None,
            "effective_dated": effective_dated,
            "destination_by_source": destination_by_source,
            "all_destination_fields": sorted(destination_by_source.values()),
            "navigation_fields": sorted(details["nav_properties"]),
            "navigation_targets": details["navigation_targets"],
            "page_size": entity.get("page_size"),
            "select_fields": build_select_fields(entity["source"], details["fields"]),
        }
        if expand_parent:
            resolved_entity.update(
                {
                    "expand_parent": expand_parent,
                    "expand_navigation": entity["expand_navigation"],
                    "parent_metadata_keys": parent_details["keys"],
                }
            )
        resolved.append(resolved_entity)
        log.info(
            f"Discovered {entity['source']}: keys={','.join(keys)}; "
            f"cursor={cursor_field or 'unavailable'}; "
            f"effective_dated={'yes' if effective_dated else 'no'}; "
            f"fields={len(destination_by_source)}"
        )
    return resolved


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    session = build_session(configuration)
    resolved = discover_entities(session)
    tables = [
        {"table": entity["table"], "primary_key": entity["primary_key"]} for entity in resolved
    ]
    # Per-entity failures are tolerated so one bad entity cannot abort the other 675,
    # but that makes them easy to miss: the detail otherwise lives only in the
    # Fivetran dashboard log. Landing it in the warehouse makes failures queryable
    # alongside the data they affect.
    tables.append({"table": SYNC_ISSUE_TABLE, "primary_key": ["sync_started_at", "source_entity"]})
    tables.append({"table": FORM_FETCH_LOG_TABLE, "primary_key": ["form_data_id"]})
    tables.append({"table": FORM_SYNC_RUNS_TABLE, "primary_key": ["form_phase_started_at"]})
    return tables


def records_for_entity(session, entity, cursor):
    """Return the correct record iterator for a top-level or expanded entity."""
    if entity.get("expand_parent"):
        return iter_expanded_records(session, entity)
    if entity.get("page_size") and ENTITY_FETCH_WORKERS > 1:
        return iter_records_parallel(
            session,
            entity["source"],
            cursor,
            entity["metadata_keys"],
            entity["effective_dated"],
            entity.get("page_size"),
            entity.get("select_fields"),
            entity.get("cursor_field") or LAST_MODIFIED_FIELD,
        )
    return iter_records(
        session,
        entity["source"],
        cursor,
        entity["metadata_keys"],
        entity["effective_dated"],
        entity.get("page_size"),
        entity.get("select_fields"),
        entity.get("cursor_field") or LAST_MODIFIED_FIELD,
    )


class CheckpointScheduler:
    """Flush buffered rows on a time cadence, without advancing any in-flight cursor.

    Checkpointing only between entities is not enough: a single entity can run for
    hours (EmployeeTime can page millions of rows), and
    for that whole time nothing reaches the destination and a failure discards it all.

    Checkpointing *inside* an entity is only safe because this never writes that
    entity's new cursor. iter_records() sorts by primary key, not by the cursor field,
    so the maximum timestamp seen part-way through is not a watermark: records still
    unread can carry older timestamps. Persisting it mid-entity would make the resumed
    query filter them out permanently. The state passed here therefore still holds the
    entity's *previous* cursor, so an interrupted entity simply re-reads from where it
    started and re-upserts rows already written -- idempotent, since every table has a
    primary key. Only sync_entity(), on completion, advances the cursor.
    """

    def __init__(self, state, interval=CHECKPOINT_INTERVAL_SECONDS):
        self._state = state
        self._interval = interval
        self._last = time.monotonic()

    def maybe_checkpoint(self):
        """Checkpoint if the interval has elapsed; returns whether it did."""
        if time.monotonic() - self._last < self._interval:
            return False
        self.checkpoint()
        return True

    def checkpoint(self):
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=self._state)
        self._last = time.monotonic()


def sync_entity(session, entity, state, checkpointer=None):
    """Sync one entity's full field set and update its cursor when reliable."""
    started = time.perf_counter()
    cursor = state.get(state_key(entity)) if entity["has_cursor"] else None
    if not entity["has_cursor"]:
        op.truncate(table=entity["table"])
    count = 0
    undated = 0
    maximum = None
    cursor_column = entity.get("cursor_column") or LAST_MODIFIED_COLUMN
    for record in records_for_entity(session, entity, cursor):
        row = build_row(record, entity)
        if entity["has_cursor"]:
            # SAP omits null optional properties from the payload, so a record can
            # arrive with no cursor timestamp at all. Such a row still belongs in the
            # destination; it just cannot advance the cursor. Parsing it unguarded
            # would raise ValueError and abort every remaining entity's sync.
            raw_cursor_value = row.get(cursor_column)
            last_modified = None
            if raw_cursor_value is not None:
                try:
                    last_modified = parse_successfactors_datetime(raw_cursor_value)
                except (ValueError, OverflowError):
                    last_modified = None
            if last_modified is None:
                undated += 1
            else:
                maximum = max(maximum, last_modified) if maximum else last_modified
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=entity["table"], data=row)
        count += 1
        if checkpointer is not None:
            checkpointer.maybe_checkpoint()
    if maximum:
        state[state_key(entity)] = maximum.isoformat().replace("+00:00", "Z")
    log.info(
        f"Upserted {count:,} {entity['source']} records into {entity['table']}; "
        f"elapsed={time.perf_counter() - started:.1f}s; "
        f"mode={'incremental' if cursor else 'full'}"
    )
    if undated:
        # Worth surfacing: these rows are synced but invisible to the cursor, so a
        # later edit that leaves the timestamp empty would not be picked up.
        log.warning(
            f"{undated:,} of {count:,} {entity['source']} records had no usable "
            f"'{cursor_column}' value and did not contribute to its cursor"
        )
        if maximum is None:
            log.warning(
                f"{entity['source']} advanced no cursor this sync and will re-read "
                "from its previous position next run"
            )


def sync_entity_windowed(session, entity, state, checkpointer=None):
    """Sync a large time entity by slicing lastModifiedDateTime into 30-day windows.

    On first load, splits [WINDOW_INITIAL_VALUE, now] into WINDOW_STEP_DAYS-wide slices.
    Each window is fetched, upserted, then checkpointed before advancing, so a failure
    costs at most one window rather than hours of data. Matches the native connector's
    subset/by-time pattern.

    When a cursor already exists from a prior completed sync, the incremental dataset is
    small so delegates directly to sync_entity() (no windowing needed).

    Resume: if a sync was interrupted mid-window-scan, state holds `window_next_start`.
    On restart, the scan picks up from that window rather than 2001-01-01.
    """
    cursor = state.get(state_key(entity)) if entity["has_cursor"] else None
    if cursor:
        sync_entity(session, entity, state, checkpointer)
        return

    window_key = f"{entity['table']}_{STATE_VERSION}_window_next_start"
    window_start_str = state.get(window_key) or WINDOW_INITIAL_VALUE
    now = datetime.now(timezone.utc)
    window_start = datetime.fromisoformat(window_start_str.replace("Z", "+00:00"))
    step = timedelta(days=WINDOW_STEP_DAYS)

    started = time.perf_counter()
    total_count = 0
    window_count = 0
    maximum = None
    cursor_column = entity.get("cursor_column") or LAST_MODIFIED_COLUMN
    order_by = entity.get("metadata_keys", [])
    page_size = entity.get("page_size") or LARGE_ENTITY_PAGE_SIZE
    select_fields = entity.get("select_fields")

    while window_start < now:
        window_end = min(window_start + step, now)
        for record in iter_records_in_window(
            session,
            entity["source"],
            window_start,
            window_end,
            order_by,
            page_size,
            select_fields,
        ):
            row = build_row(record, entity)
            raw_cursor_value = row.get(cursor_column)
            if raw_cursor_value is not None:
                try:
                    last_modified = parse_successfactors_datetime(raw_cursor_value)
                    if last_modified:
                        maximum = max(maximum, last_modified) if maximum else last_modified
                except (ValueError, OverflowError):
                    pass
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=entity["table"], data=row)
            total_count += 1

        window_start = window_end
        window_count += 1
        state[window_key] = window_start.isoformat().replace("+00:00", "Z")
        if maximum:
            state[state_key(entity)] = maximum.isoformat().replace("+00:00", "Z")
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state=state)

    state.pop(window_key, None)
    if maximum:
        state[state_key(entity)] = maximum.isoformat().replace("+00:00", "Z")
    log.info(
        f"Windowed sync of {entity['source']} complete: {total_count:,} records "
        f"across {window_count} windows; elapsed={time.perf_counter() - started:.1f}s"
    )


def raise_dashboard_warning(message):
    """Raise a dashboard-visible warning on the connection when the SDK supports it.

    `op.warning()` surfaces a warning on the connection in the Fivetran dashboard,
    which `log.warning()` does not do. It is absent from older SDK releases, and the
    deployed runtime supplies its own SDK version, so its absence must degrade to a
    log line instead of raising AttributeError mid-sync.
    """
    warning = getattr(op, "warning", None)
    if warning is None:
        log.warning(f"(dashboard warning unavailable on this SDK version) {message}")
        return
    warning(message=message)


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    if not configuration.get("sf_api_url"):
        raise ValueError(
            "Configuration requires sf_api_url (e.g. https://api4.sapsf.com/odata/v2)"
        )
    oauth_fields_present = [field for field in OAUTH_FIELDS if configuration.get(field)]
    has_bearer_token = bool(configuration.get("sf_access_token"))
    if not oauth_fields_present and not has_bearer_token:
        raise ValueError(
            f"Configuration requires either SAP OAuth credentials ({', '.join(OAUTH_FIELDS)}) or sf_access_token."
        )
    if oauth_fields_present:
        missing = [field for field in OAUTH_FIELDS if not configuration.get(field)]
        if missing:
            raise ValueError(f"Incomplete SAP OAuth configuration; missing: {', '.join(missing)}")


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: SAP SuccessFactors : sap_successfactors")
    validate_configuration(configuration)
    session = build_session(configuration)
    resolved = discover_entities(session)
    sync_started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    failed_entities = []
    checkpointer = CheckpointScheduler(state)
    form_entities = [entity for entity in resolved if entity["source"].startswith("Form")]
    permission_blocked_forms = {
        entity["source"]
        for entity in form_entities
        if entity["source"] in FORM_EXPANSION_EXCLUSIONS
    }
    form_issues = []
    form_sync_attempted = False
    for entity in resolved:
        # FormHeader is synced top-level like any other entity (all form types);
        # every other Form* table is only reachable through FormHeader expansion.
        if entity["source"].startswith("Form") and entity["source"] != "FormHeader":
            if form_sync_attempted:
                continue
            form_sync_attempted = True
            try:
                form_plan = plan_form_sync(configuration, state, datetime.now(timezone.utc))
                _, form_issues = sync_form_hierarchies(
                    session, form_entities, checkpointer, form_plan
                )
                commit_form_sync_state(state, form_plan, form_issues)
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table=FORM_SYNC_RUNS_TABLE,
                    data={
                        "form_phase_started_at": form_plan["started_at"]
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "completed_at": datetime.now(timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "mode": form_plan["mode"],
                        "reason": form_plan.get("reason"),
                        "changes_since": (
                            form_plan["since"].isoformat().replace("+00:00", "Z")
                            if form_plan["since"]
                            else None
                        ),
                        "forms_fetched": form_plan.get("forms_fetched", 0),
                        "forms_not_retrieved": sum(
                            1 for issue in form_issues if issue["error_type"] == "FormNotRetrieved"
                        ),
                        "forms_permission_blocked": sum(
                            1
                            for issue in form_issues
                            if issue["error_type"] == "PermissionBlocked"
                        ),
                    },
                )
            except Exception as error:  # noqa: BLE001
                hierarchy_forms = [
                    form
                    for form in form_entities
                    if form["source"] != "FormHeader"
                    and form["source"] not in permission_blocked_forms
                ]
                failed_entities.extend(form["source"] for form in hierarchy_forms)
                log.warning(
                    f"Skipping the Review/360 form hierarchy for this sync: "
                    f"{type(error).__name__}: {error}"
                )
                for form in hierarchy_forms:
                    # The 'upsert' operation is used to insert or update data in the destination table.
                    # The first argument is the name of the destination table.
                    # The second argument is a dictionary containing the record to be upserted.
                    op.upsert(
                        table=SYNC_ISSUE_TABLE,
                        data={
                            "sync_started_at": sync_started_at,
                            "source_entity": form["source"],
                            "destination_table": form["table"],
                            "error_type": type(error).__name__,
                            "error_message": str(error)[:2000],
                        },
                    )
            for issue in form_issues:
                if issue["error_type"] == "ExpansionOmitted":
                    continue
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table=SYNC_ISSUE_TABLE,
                    data={
                        "sync_started_at": sync_started_at,
                        "source_entity": (
                            f"FormHeader[{issue['form_type']}] "
                            f"formDataId={issue.get('form_data_id', 'unknown')}"
                        ),
                        "destination_table": "form_* (form content)",
                        "error_type": issue["error_type"],
                        "error_message": json.dumps(issue)[:2000],
                    },
                )
            # Degraded pages are summarised per form type: there can be thousands of
            # them, and they all describe the same SAP defect on the same entities.
            for form_type, omitted in summarise_omitted_expansions(form_issues).items():
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table=SYNC_ISSUE_TABLE,
                    data={
                        "sync_started_at": sync_started_at,
                        "source_entity": f"FormHeader[{form_type}] {FORM_FRAGILE_EXPANSION_SEGMENT}",
                        "destination_table": ", ".join(
                            entity["table"]
                            for entity in form_entities
                            if entity["source"] in FORM_FRAGILE_ENTITIES
                        ),
                        "error_type": "ExpansionOmitted",
                        "error_message": json.dumps(omitted)[:2000],
                    },
                )
            for source in sorted(permission_blocked_forms):
                failed_entities.append(source)
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(
                    table=SYNC_ISSUE_TABLE,
                    data={
                        "sync_started_at": sync_started_at,
                        "source_entity": source,
                        "destination_table": next(
                            form["table"] for form in form_entities if form["source"] == source
                        ),
                        "error_type": "PermissionBlocked",
                        "error_message": FORM_EXPANSION_EXCLUSIONS[source][:2000],
                    },
                )
            checkpointer.maybe_checkpoint()
            continue
        try:
            if entity["source"] in WINDOWED_FETCH_ENTITIES:
                sync_entity_windowed(session, entity, state, checkpointer)
            else:
                sync_entity(session, entity, state, checkpointer)
        except Exception as error:  # noqa: BLE001
            # At hundreds of entities, one failure (a disabled module/feature, a
            # transient error, an unexpected payload shape) must not abort every
            # other entity's sync. RuntimeErrors are already logged with detail by
            # raise_for_error()/build_row(); anything else is logged with its type
            # so the offending entity is identifiable from the sync log. The entity
            # is simply retried (from its last saved cursor) on the next sync.
            failed_entities.append(entity["source"])
            log.warning(
                f"Skipping {entity['source']} for this sync: " f"{type(error).__name__}: {error}"
            )
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(
                table=SYNC_ISSUE_TABLE,
                data={
                    "sync_started_at": sync_started_at,
                    "source_entity": entity["source"],
                    "destination_table": entity["table"],
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:2000],
                },
            )
        # sync_entity() already checkpoints on the same cadence while paging, so this
        # only covers the gap between entities -- notably a failed entity, which
        # returns without having flushed the sync-issue row recorded above.
        checkpointer.maybe_checkpoint()
    checkpointer.checkpoint()
    missing_forms = [issue for issue in form_issues if issue["error_type"] != "ExpansionOmitted"]
    if missing_forms:
        denied = sum(1 for issue in missing_forms if issue["error_type"] == "PermissionBlocked")
        raise_dashboard_warning(
            f"{len(missing_forms)} Review/360 forms could not be retrieved from SAP this sync "
            f"({denied} denied by SAP permissions). "
            f"See error_type='PermissionBlocked'/'FormNotRetrieved' in the {SYNC_ISSUE_TABLE} table."
        )
    degraded = sum(
        summary["forms"] for summary in summarise_omitted_expansions(form_issues).values()
    )
    if degraded:
        raise_dashboard_warning(
            f"SAP could not expand {FORM_FRAGILE_EXPANSION_SEGMENT} for {degraded} forms, so "
            f"{', '.join(sorted(FORM_FRAGILE_ENTITIES))} are incomplete. "
            f"See error_type='ExpansionOmitted' in the {SYNC_ISSUE_TABLE} table."
        )
    if failed_entities:
        log.warning(
            f"{len(failed_entities)} of {len(resolved)} entities failed this sync and will be "
            f"retried next run: {', '.join(failed_entities)}"
        )
        # log.warning() only reaches the sync log. op.warning() raises a warning on
        # the connection in the dashboard, which is what actually gets noticed.
        # It was added after SDK 2.7.x, and the deployed runtime's SDK version is
        # Fivetran's rather than this project's, so it is called only when present:
        # an unconditional call would abort the sync with AttributeError on an older
        # runtime, after every entity had already been synced. The SDK drops more
        # than 10 warnings per sync, so send one summary rather than one per entity.
        raise_dashboard_warning(
            f"{len(failed_entities)} of {len(resolved)} SuccessFactors entities failed "
            f"this sync and were skipped. See the {SYNC_ISSUE_TABLE} table for the "
            "entity names and error detail."
        )
        # Per-entity tolerance is meant to absorb isolated source-side failures. A
        # broad failure means something systemic (expired credentials, SAP outage,
        # a connector bug), and reporting that sync as successful would quietly
        # leave the destination stale.
        if len(failed_entities) > len(resolved) * MAX_FAILED_ENTITY_RATIO:
            raise RuntimeError(
                f"{len(failed_entities)} of {len(resolved)} entities failed this sync, "
                "which indicates a systemic failure rather than isolated entity errors"
            )


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug()
