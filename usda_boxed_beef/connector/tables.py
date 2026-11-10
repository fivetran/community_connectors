"""Tables and report sections for the USDA boxed beef connector.

TABLE_CONFIG declares every destination table. REPORTS maps each supported USDA Livestock Mandatory Reporting
report to its sections, and each section to the table its rows land in and the function in transforms.py that
builds them.
"""

# Destination table names, used by schema() and by every upsert
REPORTS_TABLE = "reports"
CUTOUT_VALUES_TABLE = "cutout_values"
PRIMAL_VALUES_TABLE = "primal_values"
CUT_PRICES_TABLE = "cut_prices"
GROUND_BEEF_PRICES_TABLE = "ground_beef_prices"
TRIMMINGS_PRICES_TABLE = "trimmings_prices"
VOLUME_TABLE = "volume"

# Columns shared by every negotiated-sales price table. Prices are U.S. dollars per 100 pounds.
_PRICE_COLUMNS = {
    "number_trades": "DOUBLE",
    "total_pounds": "DOUBLE",
    "price_range_low": "DOUBLE",
    "price_range_high": "DOUBLE",
    "weighted_average": "DOUBLE",
}

TABLE_CONFIG = {
    REPORTS_TABLE: {
        "primary_key": ["slug_id", "report_date"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "report_title": "STRING",
            "published_at": "UTC_DATETIME",
            "is_correction": "BOOLEAN",
            "narrative": "STRING",
        },
    },
    CUTOUT_VALUES_TABLE: {
        "primary_key": ["slug_id", "report_date"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "choice_cutout": "DOUBLE",
            "select_cutout": "DOUBLE",
            "choice_change": "DOUBLE",
            "select_change": "DOUBLE",
            "published_at": "UTC_DATETIME",
        },
    },
    PRIMAL_VALUES_TABLE: {
        "primary_key": ["slug_id", "report_date", "primal"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "primal": "STRING",
            "choice_value": "DOUBLE",
            "select_value": "DOUBLE",
            "published_at": "UTC_DATETIME",
        },
    },
    CUT_PRICES_TABLE: {
        "primary_key": ["slug_id", "report_date", "grade", "item_description"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "grade": "STRING",
            "item_description": "STRING",
            "imps_code": "STRING",
            "fat_limitation": "STRING",
            "cut_name": "STRING",
            **_PRICE_COLUMNS,
            "published_at": "UTC_DATETIME",
        },
    },
    GROUND_BEEF_PRICES_TABLE: {
        "primary_key": ["slug_id", "report_date", "item_description"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "item_description": "STRING",
            "section": "STRING",
            **_PRICE_COLUMNS,
            "published_at": "UTC_DATETIME",
        },
    },
    TRIMMINGS_PRICES_TABLE: {
        "primary_key": ["slug_id", "report_date", "item_description"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "item_description": "STRING",
            **_PRICE_COLUMNS,
            "published_at": "UTC_DATETIME",
        },
    },
    VOLUME_TABLE: {
        "primary_key": ["slug_id", "report_date", "volume_class"],
        "columns": {
            "slug_id": "STRING",
            "report_date": "NAIVE_DATE",
            "volume_class": "STRING",
            "loads": "DOUBLE",
            "pounds": "DOUBLE",
            "published_at": "UTC_DATETIME",
        },
    },
}

# Each report's sections, in the order they are requested. "builder" names a row builder in transforms.py;
# "grade" is the value written to cut_prices.grade for that section. The two cutout sections are requested
# separately and merged into one cutout_values row per report date.
REPORTS = {
    "2453": {
        "title": "National Daily Boxed Beef Cutout and Boxed Beef Cuts, Negotiated Sales, Afternoon (LM_XB403)",
        # The first report date the DataMart holds
        "first_report_date": "2001-04-03",
        "sections": [
            {"name": "Summary", "table": REPORTS_TABLE, "builder": "report"},
            {"name": "Current Cutout Values", "table": CUTOUT_VALUES_TABLE, "builder": "cutout"},
            {"name": "Change From Prior Day", "table": CUTOUT_VALUES_TABLE, "builder": "cutout"},
            {"name": "Composite Primal Values", "table": PRIMAL_VALUES_TABLE, "builder": "primal"},
            {
                "name": "Current Volume",
                "table": VOLUME_TABLE,
                "builder": "volume",
                "volume_classes": ["choice", "select", "trimmings", "coarse"],
            },
            {
                "name": "Choice Cuts",
                "table": CUT_PRICES_TABLE,
                "builder": "cut",
                "grade": "Choice",
            },
            {
                "name": "Select Cuts",
                "table": CUT_PRICES_TABLE,
                "builder": "cut",
                "grade": "Select",
            },
            {
                "name": "Choice and Select Cuts",
                "table": CUT_PRICES_TABLE,
                "builder": "cut",
                "grade": "Choice, Select and Ungraded",
            },
            {"name": "Ground Beef", "table": GROUND_BEEF_PRICES_TABLE, "builder": "ground_beef"},
            {
                "name": "Blended Ground Beef",
                "table": GROUND_BEEF_PRICES_TABLE,
                "builder": "ground_beef",
            },
            {"name": "Beef Trimmings", "table": TRIMMINGS_PRICES_TABLE, "builder": "trimmings"},
        ],
    },
    "2460": {
        "title": "National Weekly Boxed Beef Cuts for Prime Product, Negotiated Sales (LM_XB456)",
        "first_report_date": "2002-06-17",
        "sections": [
            {"name": "Summary", "table": REPORTS_TABLE, "builder": "report"},
            {
                "name": "Current Volume",
                "table": VOLUME_TABLE,
                "builder": "volume",
                "volume_classes": ["total"],
            },
            {"name": "Prime Cuts", "table": CUT_PRICES_TABLE, "builder": "cut", "grade": "Prime"},
        ],
    },
}

# Volume fields in report 2453's Current Volume section, by volume class. Report 2460 reports one total.
VOLUME_CLASSES = {
    "choice": ("choice_volume_loads", "choice_volume_pounds"),
    "select": ("select_volume_loads", "select_volume_pounds"),
    "trimmings": ("trimmings_volume_loads", "trimmings_volume_pounds"),
    "coarse": ("coarse_volume_loads", "coarse_volume_pounds"),
    "total": ("loads", "pounds"),
}

# Cutout fields by section and destination column. Each section carries half of a cutout_values row. The 600 to 900
# pound carcass weight range is the only one reported.
CUTOUT_FIELDS = {
    "Current Cutout Values": {
        "choice_cutout": "choice_600_900_current",
        "select_cutout": "select_600_900_current",
    },
    "Change From Prior Day": {
        "choice_change": "choice_600_900_change",
        "select_change": "select_600_900_change",
    },
}
