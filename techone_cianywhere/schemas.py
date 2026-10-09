"""Column definitions for the TechOne CiAnywhere connector's destination tables.

Kept separate from connector.py so the `schema()` function stays readable.
"""

# TechOne chart-of-accounts and transaction records expose up to 40 selection-type codes
# (seln_type_1_code .. seln_type_40_code).
NUM_SELECTION_TYPES = 40

# TechOne chart-of-accounts records expose up to 12 user-defined fields of each type
# (user_fld_1 .. user_fld_12, user_num_1 .. user_num_12, user_datei_1 .. user_datei_12).
NUM_USER_DEFINED_FIELDS = 12

LEDGER_COLUMNS = {
    "ldg_name": "STRING",
    "descr": "STRING",
    "chart_name": "STRING",
    "stat_ind": "STRING",
    "system_code": "STRING",
    "chart_type": "STRING",
}

ACCOUNT_COLUMNS = {
    "chart_name": "STRING",
    "accnbri": "STRING",
    "accnbr": "STRING",
    "vers": "LONG",
    "descr_1": "STRING",
    "descr_2": "STRING",
    "sdescr": "STRING",
    "stat_ind": "STRING",
    "soundex_name": "STRING",
    "profile_type": "STRING",
    "profile_code": "STRING",
    "ldg_code_def": "STRING",
    "accnbri_def": "STRING",
    "vat_type_def": "STRING",
    "vat_rate_code_def": "STRING",
    "frgn_ccy_ind": "STRING",
    "ccy_code": "STRING",
    "exch_rate_table_name": "STRING",
    **{f"seln_type_{i}_code": "STRING" for i in range(1, NUM_SELECTION_TYPES + 1)},
}

USER_FIELD_COLUMNS = {
    "chart_name": "STRING",
    "accnbri": "STRING",
    "vers": "LONG",
    **{f"user_fld_{i}": "STRING" for i in range(1, NUM_USER_DEFINED_FIELDS + 1)},
    **{f"user_num_{i}": "DOUBLE" for i in range(1, NUM_USER_DEFINED_FIELDS + 1)},
    **{f"user_datei_{i}": "NAIVE_DATETIME" for i in range(1, NUM_USER_DEFINED_FIELDS + 1)},
}

LEDGER_ACCOUNT_COLUMNS = {
    "ldg_acct_rid": "STRING",
    "ldg_name": "STRING",
    "chart_name": "STRING",
    "accnbri": "STRING",
    "accnbr": "STRING",
    "stat_ind": "STRING",
    "bal_units_1": "DOUBLE",
    "bal_amt_1": "DOUBLE",
    "commitment_total": "DOUBLE",
    "total_balance": "DOUBLE",
    "descr": "STRING",
    "sdescr": "STRING",
}

TRANSACTION_COLUMNS = {
    "ldg_trans_rid": "STRING",
    "ldg_name": "STRING",
    "accnbri": "STRING",
    "accnbr": "STRING",
    "trans_nbr": "DOUBLE",
    "seqnbr": "LONG",
    "period": "INT",
    "bat_name": "STRING",
    "doc_type": "STRING",
    "doc_datei_1": "NAIVE_DATETIME",
    "doc_datei_2": "NAIVE_DATETIME",
    "doc_datei_3": "NAIVE_DATETIME",
    "doc_datei_4": "NAIVE_DATETIME",
    "doc_ref_1": "STRING",
    "doc_ref_2": "STRING",
    "doc_ref_3": "STRING",
    "source": "STRING",
    "source_id": "STRING",
    "source_datei": "NAIVE_DATETIME",
    "source_timei": "STRING",
    "narr_1": "STRING",
    "narr_2": "STRING",
    "narr_3": "STRING",
    "status": "STRING",
    "doc_unique_id": "STRING",
    "attach_ind": "STRING",
    "item_type": "STRING",
    "item_code": "STRING",
    "rate_amt": "DOUBLE",
    "units_1": "DOUBLE",
    "units_2": "DOUBLE",
    "units_3": "DOUBLE",
    "units_4": "DOUBLE",
    "amt_1": "DOUBLE",
    "amt_2": "DOUBLE",
    "amt_3": "DOUBLE",
    "amt_4": "DOUBLE",
    "ccy_code": "STRING",
    "ccy_amt": "DOUBLE",
    "exch_rate_amt": "DOUBLE",
    "vat_type": "STRING",
    "vat_rate_code": "STRING",
    "vat_rate_amt": "DOUBLE",
    "vat_amt": "DOUBLE",
    "vat_exc_amt": "DOUBLE",
    "vat_inc_amt": "DOUBLE",
    **{f"seln_type_{i}_code": "STRING" for i in range(1, NUM_SELECTION_TYPES + 1)},
}

TRANSACTION_DETAIL_COLUMNS = {
    "ldg_transd_rid": "STRING",
    "ldg_name": "STRING",
    "accnbri": "STRING",
    "trans_nbr": "DOUBLE",
    "seqnbr": "LONG",
    "period": "INT",
    "doc_type": "STRING",
    "doc_datei_1": "NAIVE_DATETIME",
    "doc_ref_1": "STRING",
    "source": "STRING",
    "narr_1": "STRING",
    "item_type": "STRING",
    "item_code": "STRING",
    "amt_1": "DOUBLE",
    "vat_amt": "DOUBLE",
    "vat_exc_amt": "DOUBLE",
    "linked_ldg_name": "STRING",
    "linked_accnbri": "STRING",
    "linked_doc_type": "STRING",
    "linked_doc_datei_1": "NAIVE_DATETIME",
    "linked_doc_ref_1": "STRING",
    "linked_source": "STRING",
    "linked_amt_1": "DOUBLE",
    "linked_vat_amt": "DOUBLE",
    "linked_vat_exc_amt": "DOUBLE",
}
