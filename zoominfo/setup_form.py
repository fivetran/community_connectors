"""Setup form for the ZoomInfo connector. One field per configuration.json key."""

# For declaring the setup form, its fields, and the setup test result
from fivetran_connector_sdk import ConfigurationForm, form_field


def build_form(tests: list) -> ConfigurationForm:
    """
    Build the dashboard setup form for the ZoomInfo connector.

    Args:
        tests: (label, function) pairs; each function takes the configuration dict and
            returns Test().success() or Test().failure(reason)

    Returns:
        ConfigurationForm with one field per configuration key and the given setup tests
    """
    form = ConfigurationForm()

    # Required credentials. The refresh token comes from the one-time PKCE login
    # (pkce_login.py); the connector never sees a ZoomInfo username or password.
    form.add_field(
        form_field.TextField(
            "client_id",
            label="ZoomInfo client ID",
            required=True,
            description="Client ID of your app in the ZoomInfo Developer Portal.",
        )
    )
    form.add_field(
        form_field.TextField(
            "client_secret",
            label="ZoomInfo client secret",
            required=True,
            field_type=form_field.TextField.password,
            description="Client secret of the same app.",
        )
    )
    form.add_field(
        form_field.TextField(
            "refresh_token",
            label="ZoomInfo refresh token",
            required=True,
            field_type=form_field.TextField.password,
            description=(
                "Refresh token printed by pkce_login.py after you sign in to ZoomInfo once. "
                "Set a new value only to re-authorize."
            ),
        )
    )

    # Search filters and sync mode (free: no ZoomInfo credits).
    form.add_field(
        form_field.TextField(
            "countries",
            label="Country",
            description=(
                "Country to sync. The ZoomInfo Search API accepts one country per connection. "
                "Defaults to United States."
            ),
            placeholder="United States",
        )
    )
    form.add_field(
        form_field.ToggleField(
            "full_refresh",
            label="Full refresh",
            description="Re-fetch the whole record universe instead of only recent changes.",
        )
    )
    form.add_field(
        form_field.TextField(
            "intent_topics",
            label="Intent topics",
            description=(
                "Optional. Comma-separated ZoomInfo intent topic names, up to 50. "
                "Leave blank to skip the intent table."
            ),
        )
    )
    form.add_field(
        form_field.ToggleField(
            "sync_news",
            label="Sync news",
            description="Sync ZoomInfo news articles into the news table.",
        )
    )

    # Contact enrichment (1 ZoomInfo credit per enriched contact).
    form.add_field(
        form_field.ToggleField(
            "enrich_contacts",
            label="Enrich contacts (uses credits)",
            description="Enrich contacts with full profile data. Costs one credit per contact.",
        )
    )
    form.add_field(
        form_field.DropdownField(
            "enrich_filter",
            label="Contacts to enrich",
            fields=[
                form_field.DropdownFieldParam(
                    value="has_email_or_phone", label="Has email or phone"
                ),
                form_field.DropdownFieldParam(value="has_email", label="Has email"),
                form_field.DropdownFieldParam(value="has_phone", label="Has phone"),
                form_field.DropdownFieldParam(
                    value="all", label="All contacts", description="Highest credit usage."
                ),
            ],
            description="Which contacts are eligible when contact enrichment is on.",
        )
    )
    form.add_field(
        form_field.TextField(
            "enrich_management_levels",
            label="Management levels to enrich",
            description=(
                "Optional. Comma-separated list, for example C Level Exec,VP Level Exec. "
                "Leave blank for all levels."
            ),
        )
    )
    form.add_field(
        form_field.TextField(
            "enrich_output_fields",
            label="Contact enrich output fields",
            description="Optional. Comma-separated field names. Leave blank for the default set.",
        )
    )

    # Company enrichments (1 ZoomInfo credit per company and enrichment type).
    form.add_field(
        form_field.ToggleField(
            "enrich_companies",
            label="Enrich companies (uses credits)",
            description="Enrich companies with firmographic data. Costs one credit per company.",
        )
    )
    form.add_field(
        form_field.TextField(
            "enrich_companies_output_fields",
            label="Company enrich output fields",
            description="Optional. Comma-separated field names. Leave blank for the default set.",
        )
    )
    form.add_field(
        form_field.ToggleField(
            "enrich_scoops",
            label="Enrich scoops per company (uses credits)",
            description="Fetch the latest scoops for each company. One credit per company.",
        )
    )
    form.add_field(
        form_field.ToggleField(
            "enrich_technologies",
            label="Enrich technologies per company (uses credits)",
            description=(
                "Fetch each company's technology stack. One credit per company, and it can "
                "produce millions of rows for large syncs."
            ),
        )
    )
    form.add_field(
        form_field.ToggleField(
            "enrich_corporate_hierarchy",
            label="Enrich corporate hierarchy (uses credits)",
            description="Fetch each company's corporate hierarchy. One credit per company.",
        )
    )
    form.add_field(
        form_field.TextField(
            "enrich_corp_hier_output_fields",
            label="Corporate hierarchy output fields",
            description="Optional. Comma-separated field names. Leave blank for the default set.",
        )
    )

    for label, func in tests:
        form.add_test(label=label, func=func)
    return form
