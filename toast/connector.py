"""
2025-04-28
To add new config endpoint and sync it back to configuration["initialSyncStart"]:
1. Add endpoint and table name to list of endpoints in sync_items() function
2. Add table name and primary_key to schema definition in schema() function
3. If synced_endpoints is not already in state, then make sure configuration.json has
    synced_endpoints (comma-delimited list of endpoints already synced, e.g.
    /config/v2/alternatePaymentTypes,/config/v2/diningOptions...)
"""

# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation
# (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

import traceback
import datetime
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import (
    Connector,
)  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import (
    Operations as op,
)  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log  # For enabling Logs in your connector code

from auth import get_session_token, get_api_response, push_token_to_fivetran
from transform import get_endpoint_timerange, set_endpoint_timerange, get_target_business_dates
from endpoints import (
    process_config,
    process_labor,
    process_cash,
    process_orders,
    process_prep_stations,
    process_item_fulfillments,
)


def validate_configuration(configuration: dict):
    """
    Checks for the configuration values needed to refresh and push the Toast token via
    the Fivetran REST API (see get_session_token/update_configuration). A system key's
    permissions can't be checked via the API (GET /v1/system-keys/{id} rejects system-key
    auth outright: "Access denied. Method is not allowed for system key.") and can't be
    changed after creation, so there's no scope pre-check here - a wrong/under-scoped key
    fails the first time get_session_token()/update_configuration() actually PATCHes the
    connection, and every sync after that behaves the same way once it's set correctly.
    The other required values (domain, clientId, clientSecret, userAccessType,
    initialSyncStart) already fail with a clear KeyError from direct dict access elsewhere
    in update().
    :param configuration: a dictionary that holds the configuration settings for the connector.
    :raises ValueError: if fivetran_system_key is missing.
    """
    if "fivetran_system_key" not in configuration:
        raise ValueError(
            "Could not find 'fivetran_system_key' in configuration - required to push "
            "refreshed Toast tokens back to this connection via the Fivetran REST API"
        )


def update(configuration: dict, state: dict):
    """
    # Define the update function, which is a required function, and is called by Fivetran during each sync.
    # See the technical reference documentation for more details on the update function
    # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    # The state dictionary is empty for the first sync or for any full re-sync
    :param configuration: a dictionary that holds the configuration settings for the connector.
    :param state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
    """

    try:
        validate_configuration(configuration)

        # Leftover from the old Fernet-encrypted-token-in-state approach this replaced -
        # unused by any code from here on, drop them so they don't linger indefinitely
        # on connections upgrading from that version. configuration["key"] was the
        # actual Fernet secret, so drop it from configuration too - it'll disappear
        # from Fivetran's stored config on the next push_token_to_fivetran() PATCH,
        # rather than lingering there unused.
        state.pop("encrypted_token", None)
        state.pop("token_ttl", None)
        configuration.pop("key", None)

        domain = configuration["domain"]
        base_url = f"https://{domain}"
        token = get_session_token(base_url, configuration, state)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        start_timestamp = (
            datetime.datetime.now(datetime.timezone.utc)
            .isoformat("T", "milliseconds")
            .replace("+00:00", "Z")
        )
        initial_sync_dt = datetime.datetime.fromisoformat(
            configuration["initialSyncStart"].replace("Z", "+00:00")
        )
        initial_sync_ts = initial_sync_dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        """
        Get list of synced tables from state.
        If state doesn't contain a list of tables, getting it from configuration allows
        implementing this with connectors that don't yet track which tables are synced.
        """
        if state.get("synced_endpoints"):
            endpoints_previously_synced = state["synced_endpoints"]
            log.debug(f"tables synced to date: {endpoints_previously_synced}")
        elif configuration.get("synced_endpoints"):
            endpoints_previously_synced = configuration["synced_endpoints"].split(",")
            log.debug(f"tables synced to date: {endpoints_previously_synced}")
        else:
            endpoints_previously_synced = []

        log.debug(state)

        sync_succeeded = False
        try:
            # start the sync
            sync_items(
                base_url,
                headers,
                initial_sync_ts,
                start_timestamp,
                configuration,
                state,
                endpoints_previously_synced,
            )
            sync_succeeded = True
        finally:
            # Push any refreshed Toast token to this connection's own Fivetran configuration
            # only now, after all of this sync's Toast data has already been upserted/
            # checkpointed - runs whether sync_items() succeeded or raised, so a broken
            # fivetran_system_key never costs a sync its Toast data. push_token_to_fivetran()
            # never raises itself, so it can't mask an exception from sync_items() above.
            # op.checkpoint() flushes every buffered op since the last checkpoint to the
            # destination (not just state) - only call it here if sync_items() actually
            # completed, so a mid-pass failure can never get flushed as if it were done.
            if state.pop("_needs_token_push", False):
                if push_token_to_fivetran(configuration, state) and sync_succeeded:
                    op.checkpoint(state)

        # If this line is missing from a sync's logs, the sync did not reach the end of
        # update() - it was killed/crashed somewhere above, not merely slow. This is the
        # clearest marker for distinguishing "ran to completion" from "cut off mid-run
        # with no exception".
        log.info("sync completed successfully")

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def sync_items(
    base_url,
    headers,
    initial_sync_ts,
    start_timestamp,
    configuration,
    state,
    endpoints_previously_synced,
):
    """
    This is the main generator function for the connector.
    It calls other functions that are specific to the endpoint type.
    Each endpoint tracks and advances its own timestamp cursor in state["endpoint_ts"],
    so endpoints sync independently of each other instead of sharing one global cursor.
    :param base_url: Toast API URL
    :param headers: authentication headers
    :param initial_sync_ts
    :param start_timestamp: timestamp that the sync was started
    :return:
    """
    more_data = True
    first_pass = True  # indicates whether to call endpoints that don't have an end timestamp

    # config_endpoints is a list of tuples ("endpoint", "destination_table_name")
    config_endpoints = [
        ("/config/v2/alternatePaymentTypes", "alternate_payment_types"),
        ("/config/v2/breakTypes", "break_types"),
        ("/config/v2/diningOptions", "dining_option"),
        ("/config/v2/discounts", "discounts"),
        ("/config/v2/menus", "menu"),
        ("/config/v2/menuGroups", "menu_group"),
        ("/config/v2/menuItems", "menu_item"),
        ("/config/v2/menuOptionGroups", "menu_option_group"),
        ("/config/v2/printers", "printers"),
        ("/config/v2/restaurantServices", "restaurant_service"),
        ("/config/v2/revenueCenters", "revenue_center"),
        ("/config/v2/salesCategories", "sale_category"),
        ("/config/v2/serviceAreas", "service_area"),
        ("/config/v2/tables", "tables"),
    ]

    # timerange_endpoints is a list of tuples
    # ("endpoint", "destination_table_name", process_fn, param_style)
    # each one syncs on its own independent, 30-day-chunked cursor in state["endpoint_ts"][endpoint]
    timerange_endpoints = [
        ("/cashmgmt/v1/entries", "cash_entry", process_cash, "range"),
        ("/cashmgmt/v1/deposits", "cash_deposit", process_cash, "range"),
        ("/orders/v2/ordersBulk", "orders", process_orders, "range"),
        ("/labor/v1/shifts", "shift", process_labor, "range"),
        ("/labor/v1/timeEntries", "time_entry", process_labor, "modified"),
    ]

    while more_data:
        # Get response from API call.
        response_page, next_token = get_api_response(
            base_url + "/partners/v1/restaurants",
            headers,
            configuration=configuration,
            base_url=base_url,
            state=state,
        )

        # Process the items.
        if not response_page:
            break  # End pagination if there are no records in response.

        # prep_station is small reference-style data - always a full pull, no state
        # needed. truncate() soft-deletes every existing row; the per-restaurant
        # upserts below then "revive" every prep station that still exists. A prep
        # station actually removed/deactivated in Toast since the last sync is never
        # re-upserted, so it correctly stays deleted - without this, a full-pull-
        # every-time table would only ever grow, never reflect removals. Must run
        # exactly once per sync, before any upserts to these tables.
        if first_pass:
            op.truncate(table="prep_station")
            op.truncate(table="prep_station_connected_prep_station")

        # Compute every endpoint's timerange for THIS pass once, before the restaurant
        # loop - not per restaurant. state["endpoint_ts"] is keyed only by endpoint, not
        # by (endpoint, restaurant), so reading/advancing it inside the restaurant loop
        # would hand each restaurant a different slice of the cursor instead of every
        # restaurant getting the same window in a given pass.
        config_endpoint_lastmodified = {}
        if first_pass:
            for endpoint, _table_name in config_endpoints:
                not_in_cursor = endpoint not in state.get("endpoint_ts", {})
                not_previously_synced = endpoint not in endpoints_previously_synced
                if not_in_cursor and not_previously_synced:
                    config_endpoint_lastmodified[endpoint] = initial_sync_ts
                    log.info(
                        f"endpoint {endpoint} not synced previously, syncing back to {initial_sync_ts}"
                    )
                else:
                    ep_from_ts, _ = get_endpoint_timerange(
                        endpoint, state, configuration, initial_sync_ts, start_timestamp
                    )
                    config_endpoint_lastmodified[endpoint] = ep_from_ts

        # active_timerange_endpoints holds (endpoint, table_name, process_fn, param_style,
        # ep_from_ts, ep_to_ts) only for endpoints that still need to sync this pass -
        # one already caught up to start_timestamp is skipped entirely, once, here.
        active_timerange_endpoints = []
        for endpoint, table_name, process_fn, param_style in timerange_endpoints:
            ep_from_ts, ep_to_ts = get_endpoint_timerange(
                endpoint, state, configuration, initial_sync_ts, start_timestamp
            )
            if ep_from_ts >= start_timestamp:
                continue  # this endpoint has already caught up, skip it this pass
            log.info(f"***** {endpoint} timerange is from {ep_from_ts} to {ep_to_ts} ***** ")
            active_timerange_endpoints.append(
                (endpoint, table_name, process_fn, param_style, ep_from_ts, ep_to_ts)
            )

        # item_fulfillment advances one business day at a time via its own state cursor
        # (see get_target_business_dates()) rather than the timestamp-range cursors above,
        # but the dates needed are still the same for every restaurant in this pass, so
        # they're computed once here rather than per restaurant.
        target_dates = []
        if first_pass:
            target_dates = get_target_business_dates(state, configuration, initial_sync_ts)
            if not target_dates:
                log.info(
                    "item_fulfillment: already caught up through yesterday, nothing new to sync"
                )

        # Iterate over each user in the 'items' list and perform an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        restaurant_count = len(response_page)
        for index, r in enumerate(response_page):
            id = r["restaurantGuid"]
            # rename some fields in response
            rename_fields = [("restaurantGuid", "id"), ("restaurantName", "name")]
            for old_name, new_name in rename_fields:
                r[new_name] = r.pop(old_name)
            log.info(f"***** starting restaurant {id}, {index + 1} of {restaurant_count} ***** ")
            op.upsert(table="restaurant", data=r)

            if r.get("deleted") and "id" in r:
                op.delete(table="restaurant", keys={"id": r["id"]})

            # config endpoints
            # only process these on the first pass since they don't have an end timestamp
            if first_pass:
                for endpoint, table_name in config_endpoints:
                    config_params = {"lastModified": config_endpoint_lastmodified[endpoint]}
                    process_config(
                        base_url,
                        headers,
                        endpoint,
                        table_name,
                        id,
                        config_params,
                        configuration,
                        state,
                    )

                # no timerange_params, only sync during first pass
                for endpoint, table_name in [
                    ("/labor/v1/jobs", "job"),
                    ("/labor/v1/employees", "employee"),
                ]:
                    process_labor(
                        base_url, headers, endpoint, table_name, id, {}, configuration, state
                    )

                process_prep_stations(base_url, headers, id, configuration, state)

                # item_fulfillment: same target_dates (computed above) for every restaurant
                for business_date in target_dates:
                    process_item_fulfillments(
                        base_url, headers, id, business_date, configuration, state
                    )

            # cash management, orders, and labor endpoints: each syncs on its own cursor,
            # same window (computed above) for every restaurant in this pass
            for (
                endpoint,
                table_name,
                process_fn,
                param_style,
                ep_from_ts,
                ep_to_ts,
            ) in active_timerange_endpoints:
                if param_style == "range":
                    ep_params = {"startDate": ep_from_ts, "endDate": ep_to_ts}
                else:
                    ep_params = {"modifiedStartDate": ep_from_ts, "modifiedEndDate": ep_to_ts}
                process_fn(
                    base_url, headers, endpoint, table_name, id, ep_params, configuration, state
                )

        # Advance every endpoint's cursor once per pass, after all restaurants in this
        # pass are done - not per restaurant, for the same reason the computation above
        # happens once per pass rather than once per restaurant.
        if first_pass:
            for endpoint, _table_name in config_endpoints:
                set_endpoint_timerange(endpoint, state, start_timestamp)
        for (
            endpoint,
            _table_name,
            _process_fn,
            _param_style,
            _ep_from_ts,
            ep_to_ts,
        ) in active_timerange_endpoints:
            set_endpoint_timerange(endpoint, state, ep_to_ts)

        # Advance item_fulfillment's date cursor once per pass too, to the last date in
        # this pass's target_dates - same reasoning as the cursors just above.
        if target_dates:
            state["item_fulfillment_last_synced_date"] = target_dates[-1]

        # Save the progress by checkpointing the state. This is important for ensuring that
        # the sync process can resume from the correct position in case of interruptions.
        # checkpointing every 30 days for convenience,
        # since we can only ask for 30 days of shifts and time entries at a time
        first_pass = False

        # Every endpoint has now been through get_endpoint_timerange/set_endpoint_timerange
        # at least once, so the old global cursor and synced-endpoints tracking are fully
        # redundant from here on - drop them.
        state.pop("to_ts", None)
        state.pop("synced_endpoints", None)

        log.debug(f"state updated, new state: {repr(state)}")
        op.checkpoint(state)

        # Keep looping while any timerange-based endpoint still hasn't caught up to start_timestamp.
        endpoint_ts = state.get("endpoint_ts", {})
        more_data = any(
            endpoint_ts.get(endpoint, initial_sync_ts) < start_timestamp
            for endpoint, _, _, _ in timerange_endpoints
        )


def schema(configuration: dict):
    """
    # Define the schema function which lets you configure the schema your connector delivers.
    # See the technical reference documentation for more details on the schema function:
    # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    :param configuration: a dictionary that holds the configuration settings for the connector.
    :return: a list of tables with primary keys and any datatypes that we want to specify
    """
    return [
        {"table": "restaurant", "primary_key": ["id"]},
        # labor tables
        {
            "table": "job",
            "primary_key": ["id"],
            "columns": {
                "createdDate": "UTC_DATETIME",
                "deletedDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "deleted": "BOOLEAN",
                "excludeFromReporting": "BOOLEAN",
                "tipped": "BOOLEAN",
            },
        },
        {
            "table": "shift",
            "primary_key": ["id"],
            "columns": {
                "createdDate": "UTC_DATETIME",
                "inDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "outDate": "UTC_DATETIME",
                "deleted": "BOOLEAN",
            },
        },
        {
            "table": "employee",
            "primary_key": ["id"],
            "columns": {
                "createdDate": "UTC_DATETIME",
                "deletedDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "deleted": "BOOLEAN",
            },
        },
        {"table": "employee_job_reference", "primary_key": ["id", "employee_id"]},
        {"table": "employee_wage_override", "primary_key": ["id", "employee_id"]},
        {
            "table": "time_entry",
            "primary_key": ["id"],
            "columns": {
                "createdDate": "UTC_DATETIME",
                "deletedDate": "UTC_DATETIME",
                "inDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "outDate": "UTC_DATETIME",
                "autoClockedOut": "BOOLEAN",
                "deleted": "BOOLEAN",
            },
        },
        {
            "table": "break",
            "primary_key": ["id"],
            "columns": {
                "inDate": "UTC_DATETIME",
                "outDate": "UTC_DATETIME",
                "auditResponse": "BOOLEAN",
                "missed": "BOOLEAN",
            },
        },
        # cash tables
        {"table": "cash_deposit", "primary_key": ["id"], "columns": {"date": "UTC_DATETIME"}},
        {"table": "cash_entry", "primary_key": ["id"], "columns": {"date": "UTC_DATETIME"}},
        # config tables
        {"table": "alternate_payment_types", "primary_key": ["id"]},
        {"table": "break_types", "primary_key": ["id"]},
        {"table": "dining_option", "primary_key": ["id"], "columns": {"curbside": "BOOLEAN"}},
        {
            "table": "discounts",
            "primary_key": ["id"],
            "columns": {"active": "BOOLEAN", "nonExclusive": "BOOLEAN"},
        },
        {"table": "menu", "primary_key": ["id"]},
        {"table": "menu_group", "primary_key": ["id"]},
        {
            "table": "menu_item",
            "primary_key": ["id"],
            "columns": {"inheritOptionGroups": "BOOLEAN", "inheritUnitOfMeasure": "BOOLEAN"},
        },
        {"table": "menu_option_group", "primary_key": ["id"]},
        {"table": "printers", "primary_key": ["id"]},
        {"table": "restaurant_service", "primary_key": ["id"]},
        {"table": "revenue_center", "primary_key": ["id"]},
        {"table": "sale_category", "primary_key": ["id"]},
        {"table": "service_area", "primary_key": ["id"]},
        {"table": "tables", "primary_key": ["id"]},
        # kitchen tables
        # Root cause confirmed via direct Snowflake query against real loaded data
        # (716,858 item_fulfillment rows, 2,044 prep_station rows): Toast reuses the
        # *same* prep_station guid across every one of Topgolf's ~109 locations (every
        # restaurant has an identically-configured "Grill", "Bev-Lower", etc., defined
        # once at the chain level) -- some guids repeat up to 110 times. So `id` alone
        # was never a valid primary key for prep_station; that's what caused Fivetran's
        # writer to fail trying to merge ~109 different restaurants' rows onto what it
        # believed was a single unique key within one sync batch (matches the repeated
        # "Close failed: has data to be flushed in the partition" error). The correct
        # key is the composite ["restaurant_id", "id"] -- same pattern this connector
        # already uses for orders_check_selection (["id", "orders_check_id"]) for the
        # identical reason: an entity that legitimately repeats per parent.
        #
        # This isn't just "the guid repeats, so restaurant_id must be needed" -- that
        # reasoning alone turned out to be wrong for dining_option/menu_item (checked
        # separately: every attribute is identical across every restaurant sharing a
        # guid there, so those are genuinely one canonical chain-wide entity each, and
        # do NOT need restaurant_id). prep_station is different because it's been
        # verified to have real per-restaurant variation: for one shared prep_station
        # guid, EXPO_ROUTING is 'SEND_TO_EXPO' at 62 restaurants and 'SKIP_EXPO' at 47
        # -- collapsing to one row would silently keep only whichever restaurant
        # happened to sync last, discarding the other's real routing config.
        {"table": "prep_station", "primary_key": ["restaurant_id", "id"]},
        # connectedPrepStations is empty in 100% of real Topgolf data (confirmed:
        # 0 rows loaded across every restaurant checked), so this table never
        # receives a row today and Fivetran can't infer a type for its primary key
        # columns from data alone -- force STRING explicitly, per Toast's own
        # ToastReference schema (guid and entityType are both declared `type:
        # string` in the Kitchen API spec).
        #
        # NOT including restaurant_id in the key here, even though prep_station_id
        # (like prep_station.id) isn't globally unique on its own -- adding
        # restaurant_id to both the key and the upsert data at once triggered a
        # real Fivetran-side migration-ordering bug on this table: a "check for
        # duplicate records" query ran against the new restaurant_id column before
        # that column had ever been created (since the table has literally never
        # received a row to infer it from), and failed with
        # "SQL compilation error: invalid identifier 'RESTAURANT_ID'". Given this
        # table is empirically always empty in production, restaurant_id isn't
        # worth the risk here -- revisit only if real connectedPrepStations data
        # ever actually shows up.
        {
            "table": "prep_station_connected_prep_station",
            "primary_key": ["prep_station_id", "guid"],
            "columns": {"prep_station_id": "STRING", "guid": "STRING", "restaurant_id": "STRING"},
        },
        # ItemFulfillment has no guid of its own from the API. Verified against
        # 716,858 real records: (order_id, selection_id, item_fulfillment_level,
        # prep_station_id) has zero collisions. prep_station_id is set to the
        # NO_PREP_STATION_ID sentinel (never a real null) for every level-1/2
        # (expediter) record, since a NULL in a primary key breaks Fivetran's
        # upsert/merge matching.
        {
            "table": "item_fulfillment",
            "primary_key": [
                "order_id",
                "selection_id",
                "item_fulfillment_level",
                "prep_station_id",
            ],
            "columns": {
                "prep_station_id": "STRING",
                "ticket_fired_at": "UTC_DATETIME",
                "item_fulfilled_at": "UTC_DATETIME",
                "item_started_at": "UTC_DATETIME",
            },
        },
        # orders tables
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "closedDate": "UTC_DATETIME",
                "createdDate": "UTC_DATETIME",
                "deletedDate": "UTC_DATETIME",
                "estimatedFulfillmentDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "openedDate": "UTC_DATETIME",
                "paidDate": "UTC_DATETIME",
                "promisedDate": "UTC_DATETIME",
                "voidDate": "UTC_DATETIME",
                "createdInTestMode": "BOOLEAN",
                "deleted": "BOOLEAN",
                "excessFood": "BOOLEAN",
                "voided": "BOOLEAN",
            },
        },
        {
            "table": "orders_check",
            "primary_key": ["id"],
            "columns": {
                "closedDate": "UTC_DATETIME",
                "createdDate": "UTC_DATETIME",
                "deletedDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "openedDate": "UTC_DATETIME",
                "paidDate": "UTC_DATETIME",
                "voidDate": "UTC_DATETIME",
                "deleted": "BOOLEAN",
                "taxExempt": "BOOLEAN",
                "voided": "BOOLEAN",
            },
        },
        {"table": "orders_check_applied_discount", "primary_key": ["id"]},
        {"table": "orders_check_applied_discount_combo_item", "primary_key": ["id"]},
        {
            "table": "orders_check_applied_discount_trigger",
            "primary_key": ["orders_check_applied_discount_id"],
        },
        {
            "table": "orders_check_applied_service_charge",
            "primary_key": ["id", "orders_check_id"],
            "columns": {
                "delivery": "BOOLEAN",
                "dineIn": "BOOLEAN",
                "gratuity": "BOOLEAN",
                "takeout": "BOOLEAN",
                "taxable": "BOOLEAN",
            },
        },
        {
            "table": "orders_check_payment",
            "primary_key": ["orders_check_id", "payment_id", "orders_guid"],
        },
        {
            "table": "orders_check_selection",
            "primary_key": ["id", "orders_check_id"],
            "columns": {
                "createdDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "voidDate": "UTC_DATETIME",
                "deferred": "BOOLEAN",
                "voided": "BOOLEAN",
            },
        },
        {"table": "orders_check_selection_applied_discount", "primary_key": ["id"]},
        {
            "table": "orders_check_selection_applied_discount_trigger",
            "primary_key": ["orders_check_selection_applied_discount_id"],
        },
        {
            "table": "orders_check_selection_applied_tax",
            "primary_key": ["id", "orders_check_selection_id"],
        },
        {
            "table": "orders_check_selection_modifier",
            "primary_key": ["id", "orders_check_selection_id"],
            "columns": {
                "createdDate": "UTC_DATETIME",
                "modifiedDate": "UTC_DATETIME",
                "voidDate": "UTC_DATETIME",
                "deferred": "BOOLEAN",
            },
        },
        {"table": "orders_pricing_feature", "primary_key": ["orders_id"]},
        {
            "table": "payment",
            "primary_key": ["id"],
            "columns": {
                "paidDate": "UTC_DATETIME",
                "refundDate": "UTC_DATETIME",
                "void_info_date": "UTC_DATETIME",
            },
        },
    ]


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the
# command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by
# Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running
    # your file directly from your IDE.
    connector.debug(configuration=configuration)
