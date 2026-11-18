import traceback

from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

from auth import get_api_response
from transform import (
    stringify_lists,
    replace_guid_with_id,
    flatten_fields,
    extract_fields,
    process_child,
    rename_guid_fields,
    generate_business_dates,
)


def process_config(
    base_url, headers, endpoint, table_name, rst_id, timerange, configuration, state
):
    """
    This is the generating function for configuration endpoints for a restaurant and timerange
    :param base_url: Toast API URL
    :param headers: authentication headers
    :param endpoint: Toast API endpoint
    :param table_name: table name to store data in destination
    :param rst_id: id for restaurant to query
    :param timerange: time range to query
    :param configuration: connector configuration, needed to re-authenticate on a 401
    :param state: connector state, needed to re-authenticate on a 401
    :return:
    """
    headers["Toast-Restaurant-External-ID"] = rst_id
    more_data = True
    pagination = {}
    # fields_to_extract is a mapping of fields to extract from source data.
    # Keys represent table names, and values are lists of tuples.
    # Each tuple defines a mapping for one or more fields in the table:
    # (field containing a dictionary, key to extract from dictionary, new field name).
    fields_to_extract = {
        "menu_group": [("menu", "guid", "menu_id")],
        "service_area": [("revenueCenter", "guid", "revenue_center_guid")],
        "tables": [
            ("revenueCenter", "guid", "revenue_center_guid"),
            ("serviceArea", "guid", "service_area_guid"),
        ],
    }

    while more_data:
        try:
            next_token = None
            # Pass timerange through requests' params= instead of concatenating it into
            # the URL - an unencoded "+" in timestamps like "...+0000" gets read as a
            # space by Toast's server, producing "Bad request: Invalid date format".
            params = {**timerange, **pagination}
            response_page, next_token = get_api_response(
                base_url + endpoint,
                headers,
                params=params,
                configuration=configuration,
                base_url=base_url,
                state=state,
            )
            log.debug(
                f"restaurant {rst_id}: response_page has {len(response_page or [])} items for {endpoint}"
            )
            for o in response_page or []:
                if fields_to_extract.get(table_name):
                    o = extract_fields(fields_to_extract[table_name], o)
                o = stringify_lists(o)
                o["restaurant_id"] = rst_id
                o = replace_guid_with_id(o)
                op.upsert(table=table_name, data=o)

            if next_token:
                pagination["pageToken"] = next_token
            else:
                more_data = False

        except Exception as e:
            # Return error response
            exception_message = str(e)
            stack_trace = traceback.format_exc()
            detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
            raise RuntimeError(detailed_message)


def process_labor(base_url, headers, endpoint, table_name, rst_id, params, configuration, state):
    """
    This is the generating function for labor endpoints, for a restaurant and a timerange
    Labor endpoints do not use pagination
    Time range parameters are optional for breaks, shifts, and time entries.
    Time range parameters are not accepted for jobs and employees
    :param base_url: Toast API URL
    :param headers: authentication headers
    :param endpoint: Toast API endpoint
    :param table_name: table name to store data in destination
    :param rst_id: id for restaurant to query
    :param params: This is a dictionary of timerange parameters which can vary by endpoint;
        pass {} when the endpoint takes none (jobs, employees)
    :param configuration: connector configuration, needed to re-authenticate on a 401
    :param state: connector state, needed to re-authenticate on a 401
    :return:
    """
    params = params or {}
    headers["Toast-Restaurant-External-ID"] = rst_id

    # fields_to_extract is a mapping of fields to extract from source data.
    # Keys represent table names, and values are lists of tuples.
    # Each tuple defines a mapping for one or more fields in the table:
    # (field containing a dictionary, key to extract from dictionary, new field name).
    fields_to_extract = {
        "shift": [
            ("employeeReference", "guid", "employee_reference_id"),
            ("jobReference", "guid", "job_reference_id"),
        ],
        "time_entry": [
            ("employeeReference", "guid", "employee_reference_id"),
            ("jobReference", "guid", "job_reference_id"),
            ("shiftReference", "guid", "shift_reference_id"),
        ],
    }

    try:
        response_page, next_token = get_api_response(
            base_url + endpoint,
            headers,
            params=params,
            configuration=configuration,
            base_url=base_url,
            state=state,
        )
        log.debug(
            f"restaurant {rst_id}: response_page has {len(response_page or [])} items for {endpoint}"
        )

        for o in response_page or []:
            if endpoint == "/labor/v1/timeEntries" and o.get("breaks"):
                process_child(o["breaks"], "break", "time_entry_id", o["guid"])
            elif endpoint == "/labor/v1/employees":
                process_child(
                    o.get("jobReferences", []), "employee_job_reference", "employee_id", o["guid"]
                )
                process_child(
                    o.get("wageOverrides", []), "employee_wage_override", "employee_id", o["guid"]
                )
            elif endpoint == "/labor/v1/shifts":
                o = flatten_fields(["scheduleConfig"], o)

            if table_name in fields_to_extract:
                o = extract_fields(fields_to_extract[table_name], o)

            o = stringify_lists(o)
            o["restaurant_id"] = rst_id
            o = replace_guid_with_id(o)
            op.upsert(table=table_name, data=o)

            if o.get("deleted") and "id" in o:
                op.delete(table=table_name, keys={"id": o["id"]})

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def process_cash(base_url, headers, endpoint, table_name, rst_id, params, configuration, state):
    """
    This is the generating function for cash management endpoints, for a restaurant and a timerange
    Cash management endpoints take a single business date as a parameter,
    so this function calls the generate_business_dates function to get a list of dates
    within the provided timerange
    :param base_url: Toast API URL
    :param headers: authentication headers
    :param endpoint: Toast API endpoint
    :param table_name: table name to store data in destination
    :param rst_id: id for restaurant to query
    :param params: This is a dictionary of timerange parameters
    :param configuration: connector configuration, needed to re-authenticate on a 401
    :param state: connector state, needed to re-authenticate on a 401
    :return:
    """
    headers["Toast-Restaurant-External-ID"] = rst_id
    # fields_to_flatten is a mapping of fields to flatten from source data.
    # Keys represent table names, and values are lists of field names.
    # The dictionary in each field should be used to create new fields, prefixed by the original field name.
    # e.g. "info": {"id": 1, "type": "foo"}
    # would become {"info_id": 1, "info_type": "foo} and the "info" key will be popped
    fields_to_flatten = {
        "cash_deposit": ["employee", "creator"],
        "cash_entry": [
            "approverOrShiftReviewSubject",
            "creatorOrShiftReviewSubject",
            "cashDrawer",
            "employee1",
            "employee2",
            "payoutReason",
            "noSaleReason",
        ],
    }
    try:
        date_range = generate_business_dates(params["startDate"], params["endDate"])

        for d in date_range:
            response_page, next_token = get_api_response(
                base_url + endpoint + "?businessDate=" + d,
                headers,
                configuration=configuration,
                base_url=base_url,
                state=state,
            )
            # log.debug(f"restaurant {rst_id}: response_page has "
            #           f"{len(response_page or [])} items for {endpoint}")
            for o in response_page or []:
                o = flatten_fields(fields_to_flatten[table_name], o)
                o["restaurant_id"] = rst_id
                o = replace_guid_with_id(o)
                op.upsert(table=table_name, data=o)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def process_orders(base_url, headers, endpoint, table_name, rst_id, params, configuration, state):
    """
    This is the main generating function for the bulkOrders endpoint.
    This function upserts/deletes the orders table only.
    Other tables that are children come from process_payments or process_pricing_features.
    :param base_url: Toast API URL
    :param headers: authentication headers
    :param endpoint: Toast API endpoint
    :param table_name: table name to store data in destination
    :param rst_id: id for restaurant to query
    :param params: This is a dictionary of timerange parameters which can vary by endpoint
    :param configuration: connector configuration, needed to re-authenticate on a 401
    :param state: connector state, needed to re-authenticate on a 401
    """

    headers["Toast-Restaurant-External-ID"] = rst_id  # Move outside loop for efficiency
    params = params.copy()  # Avoid modifying original params
    params.update({"pageSize": 100, "page": 1})  # Set pagination defaults

    # flatten these fields, e.g. "info": {"id": 1, "type": "foo"}
    # would become {"info_id": 1, "info_type": "foo} and the "info" key will be popped
    fields_to_flatten = ["server", "createdDevice", "lastModifiedDevice"]
    # extract guids from these fields, make a new field ending in _guid, and pop the original
    fields_extract_ids = ["diningOption", "table", "serviceArea", "revenueCenter"]

    try:
        for page_num in range(1, 1_000_000):  # Prevent infinite loops; max reasonable pages
            params["page"] = page_num
            response_page, next_token = get_api_response(
                base_url + endpoint,
                headers,
                params=params,
                configuration=configuration,
                base_url=base_url,
                state=state,
            )
            log.debug(
                f"restaurant {rst_id}: response_page has {len(response_page or [])} items for {endpoint}"
            )

            if not response_page:
                break  # No more data

            for order in response_page:
                order["restaurant_id"] = rst_id
                process_payments(order)
                process_pricing_features(order)

                order = flatten_fields(fields_to_flatten, order)

                for field in fields_extract_ids:
                    if order.get(field) and "guid" in order[field]:
                        order[f"{field}_guid"] = order[field]["guid"]
                        order.pop(field, None)

                order.pop("checks", None)
                order = stringify_lists(order)
                order = replace_guid_with_id(order)
                op.upsert(table=table_name, data=order)

                if order.get("deleted") and "id" in order:
                    op.delete(table=table_name, keys={"id": order["id"]})

            if len(response_page) < params["pageSize"]:
                break  # No more pages available

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def process_payments(order):
    """
    This function processes payment information for an order.
    It upserts the orders_check_payment and payment tables.
    It passes the checks list to the process_child function to generate the orders_check table
    and its children.
    :param order: a single order dictionary
    """
    # flatten these fields, e.g. "info": {"id": 1, "type": "foo"}
    # would become {"info_id": 1, "info_type": "foo} and the "info" key will be popped
    fields_to_flatten = [
        "cashDrawer",
        "createdDevice",
        "lastModifiedDevice",
        "otherPayment",
        "refund",
        "server",
    ]
    if "checks" in order and order["checks"]:
        process_child(order["checks"], "orders_check", "orders_id", order["guid"])
        for check in order["checks"]:
            if "payments" in check:
                for payment in check["payments"]:
                    op.upsert(
                        table="orders_check_payment",
                        data={
                            "orders_check_id": check["guid"],
                            "payment_id": payment["guid"],
                            "orders_guid": order["guid"],
                        },
                    )
                    payment = flatten_fields(fields_to_flatten, payment)
                    payment["restaurant_id"] = order["restaurant_id"]
                    process_void_info(payment)
                    payment = replace_guid_with_id(payment)
                    op.upsert(table="payment", data=payment)


def process_pricing_features(order):
    """
    This function processes pricing features for an order.
    It upserts the orders_pricing_feature table.
    :param order: a single order dictionary
    """
    if "pricingFeatures" in order and order["pricingFeatures"]:
        for feature in order["pricingFeatures"]:
            op.upsert(
                table="orders_pricing_feature",
                data={"orders_id": order["guid"], "pricing_feature": feature},
            )
        order.pop("pricingFeatures", None)  # Remove processed field


def process_void_info(payment):
    """
    Processing payment["voidInfo"], heavily nested field that seemed easier to handle this way
    May revisit in the future
    :param payment: single payment record
    :return:
    """
    if payment.get("voidInfo"):
        payment["void_info_approver_guid"] = payment["voidInfo"]["voidApprover"]["guid"]
        payment["void_info_business_date"] = payment["voidInfo"]["voidBusinessDate"]
        payment["void_info_date"] = payment["voidInfo"]["voidDate"]
        if payment["voidInfo"].get("voidUser"):
            payment["void_info_user_guid"] = payment["voidInfo"]["voidUser"]["guid"]
        if payment["voidInfo"].get("voidReason"):
            payment["void_info_reason_entity_type"] = payment["voidInfo"]["voidReason"][
                "entityType"
            ]
            payment["void_info_reason_guid"] = payment["voidInfo"]["voidReason"]["guid"]
        payment.pop("voidInfo", None)


# item_fulfillment's primary key includes prep_station_id, which Toast leaves null
# for every level-1/2 (expediter) record -- a real prep_station_id is only ever
# needed to disambiguate multiple level-0 records for the same order+selection, so
# (order_id, selection_id, item_fulfillment_level) alone is already unique for
# expediter records. This sentinel keeps the PK column fully non-null (a NULL in a
# primary key breaks Fivetran's upsert/merge matching) without changing which rows
# are treated as distinct. A join against prep_station.id will simply never match
# this value, same as a null join would never have matched.
NO_PREP_STATION_ID = "NO_PREP_STATION"


def process_prep_stations(base_url, headers, rst_id, configuration, state):
    """
    Pulls all prep stations for a restaurant and upserts prep_station rows.

    connectedPrepStations is a list of ToastReference objects. It's empty in 100% of
    the real prep stations checked across Topgolf's restaurants, so it isn't the
    common case -- but it's still broken out into its own child table rather than
    left to be silently stringified, since a list of objects being flattened into
    text is exactly the kind of data loss the orders_check_selection_modifier bug
    (nested modifiers) caused elsewhere in this connector.
    :param base_url: Toast API URL.
    :param headers: authentication headers.
    :param rst_id: restaurant guid to query.
    :param configuration: connector configuration, needed to re-authenticate on a 401.
    :param state: connector state, needed to re-authenticate on a 401.
    :return:
    """
    headers["Toast-Restaurant-External-ID"] = rst_id
    more_data = True
    pagination = {}

    try:
        while more_data:
            response_page, next_token = get_api_response(
                base_url + "/kitchen/v1/published/prepStations",
                headers,
                params=pagination,
                configuration=configuration,
                base_url=base_url,
                state=state,
            )
            log.debug(
                f"restaurant {rst_id}: response_page has {len(response_page or [])} items "
                f"for /kitchen/v1/published/prepStations"
            )

            for ps in response_page or []:
                ps["restaurant_id"] = rst_id

                # kitchenPrinter is null on a large share of real prep stations -- always
                # pop the raw field and set kitchen_printer_guid explicitly either way, so
                # the destination never ends up with both a populated kitchen_printer_guid
                # column AND a leftover null kitchenPrinter column depending on which rows
                # happened to have a printer.
                kitchen_printer = ps.pop("kitchenPrinter", None)
                ps["kitchen_printer_guid"] = (
                    kitchen_printer.get("guid") if kitchen_printer else None
                )

                for connected in ps.pop("connectedPrepStations", None) or []:
                    op.upsert(
                        table="prep_station_connected_prep_station",
                        data={
                            "restaurant_id": rst_id,
                            "prep_station_id": ps["guid"],
                            "guid": connected.get("guid"),
                            "entityType": connected.get("entityType"),
                        },
                    )

                ps = replace_guid_with_id(ps)
                op.upsert(table="prep_station", data=ps)

            if next_token:
                pagination["pageToken"] = next_token
            else:
                more_data = False

    except Exception as e:
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


def process_item_fulfillments(base_url, headers, rst_id, business_date, configuration, state):
    """
    Pulls itemFulfillments for a single business date for a restaurant, and
    upserts flat item_fulfillment rows.

    A restaurant without an active Toast RMS Pro subscription returns 204 (No
    Content) for this endpoint - get_api_response treats that as an empty page,
    not an error.
    :param base_url: Toast API URL.
    :param headers: authentication headers.
    :param rst_id: restaurant guid to query.
    :param business_date: single business date to pull, in YYYYMMDD format.
    :param configuration: connector configuration, needed to re-authenticate on a 401.
    :param state: connector state, needed to re-authenticate on a 401.
    :return:
    """
    headers["Toast-Restaurant-External-ID"] = rst_id

    try:
        response_page, _ = get_api_response(
            base_url + "/kitchen/v1/export/itemFulfillments",
            headers,
            params={"businessDate": business_date},
            configuration=configuration,
            base_url=base_url,
            state=state,
        )

        for item in response_page or []:
            item = rename_guid_fields(item)
            item["prep_station_id"] = item.get("prep_station_id") or NO_PREP_STATION_ID
            # item_fulfillment_level is part of the primary key, same as order_id/selection_id/
            # prep_station_id above -- rename it explicitly too rather than relying on
            # fivetran-core's automatic camelCase->snake_case conversion, so it's actually
            # present under its declared name in the dict handed to op.upsert().
            item["item_fulfillment_level"] = item.pop("itemFulfillmentLevel")
            # ticketFiredAt/itemFulfilledAt/itemStartedAt -> their schema()-declared
            # snake_case names (needed there to force UTC_DATETIME typing). Two candidate
            # source paths normalizing to the same destination column would make Fivetran
            # silently drop one of them - rename explicitly so there's only one literal
            # key, same fix as item_fulfillment_level above. .pop(key, None) rather than
            # .pop(key) since itemStartedAt in particular is documented as sometimes
            # mirroring ticketFiredAt rather than being independently guaranteed present.
            item["ticket_fired_at"] = item.pop("ticketFiredAt", None)
            item["item_fulfilled_at"] = item.pop("itemFulfilledAt", None)
            item["item_started_at"] = item.pop("itemStartedAt", None)
            op.upsert(table="item_fulfillment", data=item)

    except Exception as e:
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)
