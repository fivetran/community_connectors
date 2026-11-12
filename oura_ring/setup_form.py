"""Setup form for the Oura Ring connector. One field per configuration.json key."""

# For declaring the setup form and its fields
from fivetran_connector_sdk import ConfigurationForm, form_field


def build_form(defaults: dict, tests: list) -> ConfigurationForm:
    """
    Build the setup form from the connector's defaults and setup tests.

    Args:
        defaults: default value per optional key, shown in the field descriptions
        tests: (label, function) pairs; each function takes the configuration dict and
            returns Test().success() or Test().failure(reason)

    Returns:
        ConfigurationForm with one field per configuration key and the given setup tests
    """
    form = ConfigurationForm()
    form.add_field(
        form_field.TextField(
            "client_id",
            label="Oura OAuth application client ID",
            required=True,
            description="Client ID of your application at developer.ouraring.com.",
        )
    )
    form.add_field(
        form_field.TextField(
            "client_secret",
            label="Oura OAuth application client secret",
            required=True,
            field_type=form_field.TextField.password,
            description="Client secret of the same application.",
        )
    )
    form.add_field(
        form_field.TextField(
            "refresh_token",
            label="Oura refresh token",
            required=True,
            field_type=form_field.TextField.password,
            description=(
                "Refresh token from the authorization-code consent. It seeds the first sync; "
                "the connector keeps each rotated token encrypted in state."
            ),
        )
    )
    form.add_field(
        form_field.TextField(
            "fernet_key",
            label="Fernet key for the refresh token in state",
            required=True,
            field_type=form_field.TextField.password,
            description="32 url-safe base64-encoded bytes, generated once for this connection.",
        )
    )
    form.add_field(
        form_field.TextField(
            "lookback_days",
            label="Initial sync lookback in days",
            description=f"Optional. Defaults to {defaults['lookback_days']}.",
            placeholder=str(defaults["lookback_days"]),
        )
    )
    for label, func in tests:
        form.add_test(label=label, func=func)
    return form
