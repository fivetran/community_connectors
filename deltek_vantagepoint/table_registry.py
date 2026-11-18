"""Static table/endpoint registry for the Deltek Vantagepoint connector.

This registry documents every table this connector syncs and the Vantagepoint REST endpoint
each one maps to. Keeping it as a static, version-controlled registry (rather than discovering
tables dynamically) documents the connector's scope at build time and is the single place to
look when adding, removing, or re-scoping a table.
"""

# --------------------------------------------------------------------------------------
# Hubs master data
# --------------------------------------------------------------------------------------

# Hubs entities synced by this connector. Each entry maps a destination table to its Vantagepoint
# Hubs endpoint, primary key, and the audit column used for incremental filtering (all Hub records
# expose CreateDate/ModDate/CreateUser/ModUser audit columns).
# NOTE: "User-Defined Hub Records" (GET /UDIC/:InfocenterArea) is intentionally NOT included here -
# it requires a customer-specific custom hub name that varies per Vantagepoint instance and cannot
# be hardcoded. Add it as a configuration-driven table in a follow-up if needed.
__TABLES = [
    {"table": "employee", "endpoint": "employee", "primary_key": ["Employee"]},
    {"table": "firm", "endpoint": "firm", "primary_key": ["ClientID"]},
    {"table": "contact", "endpoint": "contact", "primary_key": ["ContactID"]},
    {"table": "project", "endpoint": "project", "primary_key": ["WBS1", "WBS2", "WBS3"]},
    {"table": "activity", "endpoint": "activity", "primary_key": ["ActivityID"]},
    {"table": "marketing_campaign", "endpoint": "campaign", "primary_key": ["CampaignID"]},
    {"table": "boilerplate", "endpoint": "boilerplate", "primary_key": ["Name"]},
]

# --------------------------------------------------------------------------------------
# General/Configuration, Workflow, and Third-Party Integration reference data
# --------------------------------------------------------------------------------------

# None of these endpoints document filterHash/pagination support (unlike Hubs), and their sample
# payloads carry no ModDate/CreateDate audit columns - they're small, admin-managed lookup tables
# meant to be fetched in a single call and fully replaced every sync.
# NOTE: "Workflow Action Webhook" (GET /Workflow/dlgWorkflowActionWebhook) is intentionally EXCLUDED -
# its response includes AuthUsername, AuthPassword, and ClientSecret in plaintext (credentials for
# the customer's own outbound webhook integrations). Syncing that into a destination warehouse would
# leak secrets; skip it unless the customer explicitly asks for it after being told what it contains.
# "Retrieve Third-Party Integrations" (GET /thirdpartyintegration) is also excluded - the Postman
# collection has no sample response for it, so its shape is unverified; confirm against a live
# instance before adding it.
__CONFIG_TABLES = [
    {
        "table": "accounting_company_config",
        "endpoint": "AccountingCompanyConfig/CFGAutoPosting",
        "primary_key": ["Company"],
    },
    {"table": "key_format", "endpoint": "KeyCvt/CFGFormat", "primary_key": ["PKey"]},
    {"table": "organization_level", "endpoint": "KeyCvt/CFGFormatOrg", "primary_key": ["cLevel"]},
    {
        "table": "labor_code",
        "endpoint": "accountConfiguration/laborCode",
        "primary_key": ["LCLevel", "Code"],
    },
    {"table": "organization", "endpoint": "organization", "primary_key": ["Org"]},
    {
        "table": "company_general_settings",
        "endpoint": "Settings/General",
        "primary_key": ["Company"],
    },
    {"table": "company", "endpoint": "Settings/Company", "primary_key": ["Company"]},
    {"table": "accounting_period", "endpoint": "Settings/Period", "primary_key": ["Period"]},
    {"table": "role", "endpoint": "roles", "primary_key": ["Role"]},
    {"table": "user", "endpoint": "user", "primary_key": ["Username"]},
    {
        "table": "workflow_event",
        "endpoint": "Workflow/WorkflowEvents",
        "primary_key": ["EventID"],
    },
    {
        "table": "workflow_action",
        "endpoint": "Workflow/WorkflowActions",
        "primary_key": ["EventID", "ActionID"],
    },
    {
        "table": "workflow_condition",
        "endpoint": "Workflow/dlgWorkflowConditions",
        "primary_key": ["ConditionID"],
    },
    {
        "table": "workflow_action_webhook_argument",
        "endpoint": "Workflow/dlgWorkflowActionWebhookArgs",
        "primary_key": ["ActionID", "ArgName"],
    },
    {
        "table": "thirdparty_integration_account",
        "endpoint": "thirdpartyintegration/accounts",
        "primary_key": ["Account"],
    },
    {
        "table": "thirdparty_integration_employee",
        "endpoint": "thirdpartyintegration/employees",
        "primary_key": ["Employee"],
    },
    {
        "table": "thirdparty_integration_tax_code",
        "endpoint": "thirdpartyintegration/taxes",
        "primary_key": ["Code"],
    },
]

# --------------------------------------------------------------------------------------
# Employee/Firm/Contact/Project detail (child) collections
# --------------------------------------------------------------------------------------

# Per-parent-record child collections for Activities, Contacts, Employees, Firms, and Projects.
# None of these child endpoints support filterHash/pagination or carry their own ModDate - they
# are fetched one parent at a time (fan-out), so this connector only fans out to a parent's
# children when that parent record itself was returned by this sync (i.e. its ModDate advanced
# since the last cursor, or it's the very first sync and every parent is "new"). This trades
# completeness for API call volume: ASSUMPTION (unverified without a live instance) - a child-only
# edit (e.g. adding a Skill without touching any other Employee field) is assumed to also bump the
# parent's ModDate. If Vantagepoint's UI doesn't do that consistently, child changes could be
# missed between syncs. Confirm this against real data before relying on it for anything
# audit-sensitive.
# "id_template" builds the parent path segment from the parent record; "url_template" builds the
# full child path from that segment plus the child's own path suffix.
__HUB_CHILDREN = {
    "activity": {
        "id_template": lambda record: record.get("ActivityID"),
        "url_template": "activity/{parent_id}/{path}",
        "children": [
            {
                "table": "activity_client",
                "path": "client",
                "primary_key": ["ActivityID", "ClientID"],
            },
            {
                "table": "activity_contact",
                "path": "contact",
                "primary_key": ["ActivityID", "ContactID"],
            },
            {
                "table": "activity_employee",
                "path": "employee",
                "primary_key": ["ActivityID", "Employee"],
            },
            {
                "table": "activity_employee_contact",
                "path": "employeeContacts",
                "primary_key": ["ActivityID", "RecordID"],
            },
            {"table": "activity_file", "path": "files", "primary_key": ["PKey", "FileID"]},
            {"table": "activity_link", "path": "links", "primary_key": ["LinkID", "ActivityID"]},
        ],
    },
    "contact": {
        "id_template": lambda record: record.get("ContactID"),
        "url_template": "contact/{parent_id}/{path}",
        "children": [
            {
                "table": "contact_associated_contact",
                "path": "contact",
                "primary_key": ["FromContactID", "ToContactID"],
            },
            {
                "table": "contact_category",
                "path": "category",
                "primary_key": ["ContactID", "Category"],
            },
            {
                "table": "contact_employee",
                "path": "employee",
                "primary_key": ["Employee", "ContactID"],
            },
            {"table": "contact_file", "path": "files", "primary_key": ["PKey", "FileID"]},
            {"table": "contact_link", "path": "links", "primary_key": ["LinkID", "ContactID"]},
            {"table": "contact_project", "path": "project", "primary_key": ["ContactID", "WBS1"]},
            {
                "table": "contact_marketing_campaign",
                "path": "campaign",
                "primary_key": ["CampaignID", "ContactID"],
            },
            {
                "table": "contact_qualification_status",
                "path": "qualifiedContact",
                "primary_key": ["ContactID"],
            },
        ],
    },
    "employee": {
        "id_template": lambda record: record.get("Employee"),
        "url_template": "employee/{parent_id}/{path}",
        "children": [
            {"table": "employee_skill", "path": "skills", "primary_key": ["Employee", "Skill"]},
            {
                "table": "employee_credential",
                "path": "credentials",
                "primary_key": ["Employee", "RecordID"],
            },
            {
                "table": "employee_education",
                "path": "education",
                "primary_key": ["Employee", "RecordID"],
            },
            {
                "table": "employee_citizenship",
                "path": "citizenship",
                "primary_key": ["Employee", "RecordID"],
            },
        ],
    },
    "firm": {
        "id_template": lambda record: record.get("ClientID"),
        "url_template": "firm/{parent_id}/{path}",
        "children": [
            {"table": "firm_alias", "path": "alias", "primary_key": ["ClientID", "PKey"]},
            {
                "table": "firm_address",
                "path": "address",
                "primary_key": ["ClientID", "CLAddressID"],
            },
            {
                "table": "firm_associated_firm",
                "path": "client",
                "primary_key": ["FromClientID", "ToClientID"],
            },
            {
                "table": "firm_employee",
                "path": "employee",
                "primary_key": ["ClientID", "Employee"],
            },
            {
                "table": "firm_vendor_accounting_info",
                "path": "vendorAccountingInfo",
                "primary_key": ["Vendor", "Company"],
            },
        ],
    },
    "project": {
        # Straight concatenation of WBS1/WBS2/WBS3 (still no documented formula for :wbskey, but
        # confirmed live to work for most project child endpoints - see README).
        # CONFIRMED LIVE: a "main" (top-level) project's WBS2/WBS3 come back as a single space
        # character " ", not an empty string - Vantagepoint fixed-width-pads blank WBS levels
        # rather than leaving them blank. A bare `if part` treats " " as truthy, so the naive
        # version of this concatenation appended stray trailing spaces onto every main project's
        # wbskey. Stripping each part before checking/joining avoids that.
        "id_template": lambda record: "".join(
            part.strip()
            for part in (record.get("WBS1"), record.get("WBS2"), record.get("WBS3"))
            if part and part.strip()
        ),
        "url_template": "project/{parent_id}/{path}",
        "children": [
            # Includes Seq: the sample payload shows a Seq field alongside ProjectCode, suggesting
            # a project can have multiple sequenced entries for the same code (e.g. a history of
            # assignments) rather than one row per (WBS, ProjectCode).
            {
                "table": "project_code",
                "path": "projectcode",
                "primary_key": ["WBS1", "WBS2", "WBS3", "ProjectCode", "Seq"],
            },
            {"table": "project_contact", "path": "contact", "primary_key": ["WBS1", "ContactID"]},
            {
                "table": "project_contract",
                "path": "contract",
                "primary_key": ["WBS1", "ContractNumber"],
            },
            {
                "table": "project_contract_detail",
                "path": "contractdetail",
                "primary_key": ["ContractDetail_UID"],
            },
            {
                "table": "project_description",
                "path": "description",
                "primary_key": ["WBS1", "WBS2", "WBS3", "DescCategory"],
            },
            {
                "table": "project_firm",
                "path": "firmClient",
                "primary_key": ["WBS1", "WBS2", "WBS3", "ClientID"],
            },
            {
                "table": "project_link",
                "path": "links",
                "primary_key": ["WBS1", "WBS2", "WBS3", "LinkID"],
            },
            {
                "table": "project_marketing_campaign",
                "path": "marketingCampaign",
                "primary_key": ["WBS1", "WBS2", "WBS3", "CampaignID"],
            },
            # RecordID is included alongside WBS1/WBS2/WBS3 in these endpoints' own sample
            # payloads, and it's a generic field name reused across unrelated child resources - both
            # signs it's a per-project sequence number, not a globally unique ID. Prefixing the full
            # WBS composite avoids a real risk of two different projects colliding on the same
            # RecordID and silently overwriting each other's rows.
            {"table": "project_competition", "path": "competition", "primary_key": ["RecordID"]},
            {
                "table": "project_employee",
                "path": "employee",
                "primary_key": ["WBS1", "WBS2", "WBS3", "RecordID"],
            },
            {
                "table": "project_team_member",
                "path": "teammember",
                "primary_key": ["WBS1", "WBS2", "WBS3", "RecordID"],
            },
            {
                "table": "project_award",
                "path": "award",
                "primary_key": ["WBS1", "WBS2", "WBS3", "RecordID"],
            },
            {
                "table": "project_milestone",
                "path": "milestone",
                "primary_key": ["WBS1", "WBS2", "WBS3", "RecordID"],
            },
            {"table": "project_proposal", "path": "proposal", "primary_key": ["CustomPropID"]},
            {"table": "project_revenue", "path": "revenue", "primary_key": ["RevAllocID"]},
        ],
    },
}

# --------------------------------------------------------------------------------------
# Billing
# --------------------------------------------------------------------------------------

# Billing invoice header data (GET /InteractiveDetail/:WBS1/InvoiceMaster). Recommended over the
# 7+ fragmented, un-timestamped line-item detail endpoints (AddOns, Consultant, Expenses, FixedFee,
# Interest, Unit) - one row per invoice is far more usable for reporting, and none of the detail
# endpoints carry any audit column to gate on anyway. Scoped per top-level project only (:WBS1, not
# the full WBS1/WBS2/WBS3 key), so this fans out only from "main" project records - i.e. rows in the
# project table where WBS2 and WBS3 are blank - not every phase/task row.
__INVOICE_HEADER_TABLE = {
    "table": "invoice_header",
    "path": "InvoiceMaster",
    "primary_key": ["Invoice", "MainWBS1"],
}
