import datetime
import uuid

from fivetran_connector_sdk import Operations as op

def stringify_lists(d):
    """
    The stringify_lists function changes lists to strings
    :param d: any dictionary
    :return: the dictionary with lists represented as strings
    """
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, list):
            new_dict[key] = str(value)
        else:
            new_dict[key] = value
    return new_dict

def flatten_dict (parent_row: dict, dict_field: dict, prefix: str):
    """
    Flattens a field containing a dictionary into a series of fields prefixed with the original field name
    Optionally leaves off prefix for specified fields
    :param parent_row: a dictionary containing a key whose value is a dictionary
    :param dict_field: the key whose value is a dictionary
    :param prefix: the prefix to add to the name of keys in dict_field to make new keys in parent_row
    :return: parent_row with dict_field flattened into multiple fields
    """
    # dicts in these fields have unique enough names that they do not need the field prefix
    fields_to_not_prefix = ["refundDetails", "jobReference"]

    if not dict_field:  # Quick exit for empty dictionaries
        return parent_row

    dict_field = replace_guid_with_id(dict_field)
    for key, value in dict_field.items():
        if key.startswith(prefix):
            new_key = key  # Keep it unchanged
        elif key == "tipRefundAmount" and prefix == "refund":
            new_key = "refund_tip_amount"
        elif prefix in fields_to_not_prefix:
            new_key = key  # Keep it unchanged for exempted fields
        else:
            new_key = f"{prefix}_{key}"  # Build the new flattened key

        if isinstance(value, dict):  # If the value is another dictionary, recurse
            flatten_dict(parent_row, value, new_key)
        else:
            parent_row[new_key] = value  # Store the value directly if not a dictionary

    return parent_row

def replace_guid_with_id(d: dict):
    if "guid" in d:
        d["id"] = d.pop("guid")
    return d

def flatten_fields(fields: list, row: dict):
    """
    Takes in a list of fields to flatten within a row, calls flatten_dict() if any of those fields are present
    :param fields: a list of strings which could be keys in "row"
    :param row: a dictionary "row" that could have values that are dictionaries
    :return: dictionary with dictionary values flattened, if their keys are in "fields". The original keys are removed.
    """
    row = {**row}  # Ensures row modifications don't affect the original dictionary
    row = replace_guid_with_id(row)
    for field in fields:
        value = row.get(field)  # Avoids multiple dictionary lookups
        if value is not None:
            row = flatten_dict(row, value, field)
        row.pop(field, None)  # Remove the field in a single step

    return row

def extract_fields(fields: list, row: dict):
    """
    Takes in a list of fields and sub-fields within a row.
    Returns the row with sub-field extracted and field popped.
    Used instead of flatten_fields() in cases where a value has a dict with more than one key,
    but we only want one of the keys.
    :param fields: a list of tuples indicating the keys to extract and the name of the new key.
    :param row: a dictionary "row" with keys that have keys that we need to extract
    :return: dictionary with new keys, if their keys have keys that are in "fields". The original keys are removed.
    """
    row = {**row}

    for (field, sub_field, new_name) in fields:
        if row.get(field) and sub_field in row[field]:
            row[new_name] = row[field][sub_field]
            row.pop(field, None)

    return row

def process_child (parent, table_name, id_field_name, id_field):
    """
    Iterates through records in parent list to generate child tables.
    If child tables also contain child records
    :param parent: parent record (list) which contains children
    :param table_name: connector table name for parent record
    :param id_field_name: id field name in parent record to tie child to parent
    :param id_field: id field value in parent record
    :return:
    """

    # dictionary of connector tables and the child fields (lists) that get their own tables
    # e.g. {"table_name": [("childField01", "child_table_name_01"), ("childField02", "child_table_name_02")] }
    relationships = {"orders_check": [
            ("selections", "orders_check_selection"),
            ("appliedDiscounts", "orders_check_applied_discount"),
            ("appliedServiceCharges", "orders_check_applied_service_charge")],
        "orders_check_applied_discount": [
            ("comboItems", "orders_check_applied_discount_combo_item"),
            ("triggers", "orders_check_applied_discount_trigger")],
        "orders_check_applied_service_charge": [
            ("appliedTax", "orders_check_applied_service_charge_applied_tax")],
        "orders_check_selection": [
            ("appliedTaxes", "orders_check_selection_applied_tax"),
            ("modifiers", "orders_check_selection_modifier"),
            ("appliedDiscounts", "orders_check_selection_applied_discount")],
        "orders_check_selection_applied_discount":
            [("comboItems", "orders_check_selection_applied_discount_combo_item"),
             ("triggers", "orders_check_selection_applied_discount_trigger")],
        # Toast modifiers can themselves have modifiers (e.g. a cocktail's liquor-upgrade
        # modifier nested inside its pour-size modifier), nested to arbitrary depth. Recurse
        # into the same table rather than a new one, and use the 3rd tuple element to say
        # "keep tying rows back to the same orders_check_selection_id at every depth"
        # instead of the default behavior of renaming the FK to table_name + '_id' each level.
        "orders_check_selection_modifier": [
            ("modifiers", "orders_check_selection_modifier", "orders_check_selection_id")]
                     }

    # fields_to_flatten is a mapping of fields to flatten from source data.
    # Keys represent table names, and values are lists of field names.
    # The dictionary in each field should be used to create new fields, prefixed by the original field name.
    # e.g. "info": {"id": 1, "type": "foo"}
    # would become {"info_id": 1, "info_type": "foo} and the "info" key will be popped
    fields_to_flatten = {
        "break": ["breakType"],
        "employee_wage_override": ["jobReference"],
        "orders_check": ["customer", "createdDevice", "lastModifiedDevice"],
        "orders_check_applied_discount": ["approver", "appliedDiscountReason", "discount"],
        "orders_check_applied_discount_trigger": ["selection"],
        "orders_check_applied_service_charge": ["serviceCharge"],
        "orders_check_selection": ["salesCategory", "itemGroup", "item", "diningOption", "refundDetails", "voidReason"],
        "orders_check_selection_applied_discount": ["approver", "appliedDiscountReason", "discount"],
        "orders_check_selection_applied_tax": ["taxRate"],
        "orders_check_selection_modifier": ["diningOption", "item", "itemGroup", "optionGroup", "salesCategory"
            , "preModifier", "voidReason" ],
        "orders_check_selection_applied_discount_trigger": ["selection"]}

    for p in parent:
        #log.debug(f"processing {table_name}")
        p[id_field_name] = id_field
        if table_name in relationships:
            for relationship in relationships[table_name]:
                child_key, child_table_name = relationship[0], relationship[1]
                # A 3rd tuple element marks a self-referential relationship (currently only
                # nested modifiers-of-modifiers): keep the child tied to the same ancestor
                # FK (id_field_name/id_field) instead of renaming the FK to table_name + '_id',
                # and stamp each child with the immediate parent's guid for hierarchy.
                self_referential = len(relationship) > 2
                if len(p.get(child_key, [])) > 0:  # Use .get() to handle missing keys gracefully
                    if self_referential:
                        for child in p[child_key]:
                            child["parent_modifier_id"] = p["guid"]
                        process_child(
                            p[child_key],
                            child_table_name,
                            relationship[2],
                            id_field
                        )
                    else:
                        process_child(
                            p[child_key],
                            child_table_name,
                            table_name + "_id",
                            p["guid"]
                        )
                p.pop(child_key, None)
        if table_name in fields_to_flatten:
            #log.debug(f"flattening fields in {table_name}")
            p = flatten_fields(fields_to_flatten[table_name], p)
        # Toast sometimes omits a guid for appliedTaxes[] entries. flatten_fields() above has
        # already renamed any real guid to "id", so check "id" (not "guid", which no longer
        # exists at this point) and generate a deterministic id from stable fields -- not a
        # random uuid4 -- so the same tax line gets the same id on every sync instead of a new
        # duplicate row each time.
        if table_name == "orders_check_selection_applied_tax" and p.get("id") is None:
            basis = f"{id_field}-{p.get('taxRate_id', '')}"
            p["id"] = "gen-" + str(uuid.uuid5(uuid.NAMESPACE_OID, basis))
        if table_name == "orders_check":
            p.pop("payments", None)
        p = stringify_lists(p)
        p = replace_guid_with_id(p)
        op.upsert(table=table_name, data=p)
        # need to also handle deletes here, for child tables and their children (e.g. orders_check and orders_check_selection)

        if p.get("deleted") and "id" in p:
            op.delete(table=table_name, keys={"id": p["id"]})

def rename_guid_fields(item):
    """
    Renames Toast's *Guid fields to the _id convention used elsewhere in this
    connector (e.g. orderGuid -> order_id). camelCase -> snake_case for other fields
    is handled automatically by fivetran-core, so only these semantic Guid -> id
    renames need to happen explicitly here.
    :param item: a single ItemFulfillment record.
    :return: the record with *Guid fields renamed to *_id.
    """
    rename_map = {
        "restaurantGuid": "restaurant_id",
        "orderGuid": "order_id",
        "selectionGuid": "selection_id",
        "selectionMenuItemGuid": "selection_menu_item_id",
        "ticketGuid": "ticket_id",
        "prepStationGuid": "prep_station_id",
        "diningOptionGuid": "dining_option_id",
        "courseGuid": "course_id",
    }
    for old_name, new_name in rename_map.items():
        if old_name in item:
            item[new_name] = item.pop(old_name)
    return item

def is_older_than_30_days(date_to_check):
    """
    Checks whether date_to_check is older than 30 days.
    Is time-zone aware and handles date_to_check being a string and not a datetime
    :param date_to_check:
    :return: boolean based on whether date is older than 30 days
    """
    now = datetime.datetime.now(datetime.UTC)  # Timezone-aware UTC datetime

    # Convert to datetime if input is a string
    if isinstance(date_to_check, str):
        date_to_check = datetime.datetime.fromisoformat(date_to_check.replace("Z", "+00:00"))

    return date_to_check < now - datetime.timedelta(days=30)

def generate_business_dates (start_ts, end_ts):
    """
    Takes in start_date and end_date, and generates a list of dates in YYYYMMDD format that include those dates
    :param start_ts: ISO format datetime
    :param end_ts: ISO format datetime later than start_ts
    :return: list of YYYYMMDD dates between start_ts and end_ts, inclusive
    """
    start_date = datetime.datetime.fromisoformat(start_ts)
    end_date = datetime.datetime.fromisoformat(end_ts)
    delta = end_date - start_date

    date_list = []
    for i in range(delta.days + 1):
        date_list.append((start_date + datetime.timedelta(days=i)).strftime("%Y%m%d"))

    return date_list

def get_target_business_dates(state, configuration, initial_sync_ts):
    """
    Determines which business dates item_fulfillment needs to sync this run, using
    a state cursor - this is what makes item_fulfillment behave like a real
    incremental connector rather than a rolling re-pull.

    "Yesterday" (UTC) is always the newest allowed target: today's business date is
    still open and accumulating fulfillments, so syncing it would capture a partial
    day that never gets revisited once the cursor moves past it.

    - First sync (no state yet): backfills all the way back to configuration["initialSyncStart"]
      by default, matching how far every other endpoint backfills - configuration["itemFulfillmentsInitialDaysBack"]
      overrides this with a specific day count instead, when set (e.g. for a bounded local test run).
    - Every sync after: resumes the day after the last synced date, through
      yesterday. Normally that's exactly one new day; if a sync was skipped or the
      connector was paused, this naturally catches up on every date in between,
      oldest first.
    - Already caught up (e.g. sync ran twice in the same UTC day): returns [].
    :param state: connector state dict.
    :param configuration: connector configuration - may hold itemFulfillmentsInitialDaysBack override.
    :param initial_sync_ts: configuration["initialSyncStart"], normalized to an ISO timestamp string.
    :return: list of YYYYMMDD strings to sync, oldest first.
    """
    yesterday = datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=1)

    last_synced = state.get("item_fulfillment_last_synced_date")
    if last_synced:
        start = datetime.datetime.strptime(last_synced, "%Y%m%d").date() + datetime.timedelta(days=1)
    else:
        configured_days_back = configuration.get("itemFulfillmentsInitialDaysBack")
        if configured_days_back:
            start = yesterday - datetime.timedelta(days=int(configured_days_back) - 1)
        else:
            start = datetime.datetime.fromisoformat(initial_sync_ts.replace("Z", "+00:00")).date()

    dates = []
    d = start
    while d <= yesterday:
        dates.append(d.strftime("%Y%m%d"))
        d += datetime.timedelta(days=1)
    return dates

def get_endpoint_timerange(endpoint, state, configuration, initial_sync_ts, start_timestamp):
    """
    Takes in current state and start timestamp of current sync.
    Returns (from_ts, to_ts) for a single endpoint, tracked independently in
    state["endpoint_ts"][endpoint], so each endpoint advances/catches up on its own
    instead of sharing one global cursor.
    from_ts is always either the end of that endpoint's last sync or initialSyncStart.
    If from_ts is more than 30 days ago, then set a to_ts that is 30 days later than from_ts.
    Otherwise, to_ts is the time that this sync was triggered.
    :param endpoint: Toast API endpoint being synced
    :param state:
    :param configuration:
    :param initial_sync_ts:
    :param start_timestamp:
    :return: from_ts, to_ts
    """
    endpoint_ts = state.setdefault("endpoint_ts", {})
    if endpoint not in endpoint_ts:
        # Migrate off the old single global cursor (state["to_ts"]) if present, so deploying
        # this doesn't look like every endpoint has never been synced and trigger a full re-pull.
        endpoint_ts[endpoint] = state.get("to_ts", initial_sync_ts)
    from_ts = endpoint_ts[endpoint]

    if is_older_than_30_days(from_ts):  # Pass the string, since function handles conversion
        from_ts_dt = datetime.datetime.fromisoformat(from_ts.replace("Z", "+00:00"))
        to_ts = from_ts_dt + datetime.timedelta(days=30)
        to_ts = to_ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    else:
        to_ts = start_timestamp

    return from_ts, to_ts

def set_endpoint_timerange(endpoint, state, to_ts):
    """
    Advances a single endpoint's cursor in state["endpoint_ts"] to to_ts.
    :param endpoint: Toast API endpoint being synced
    :param state:
    :param to_ts:
    """
    state.setdefault("endpoint_ts", {})[endpoint] = to_ts
