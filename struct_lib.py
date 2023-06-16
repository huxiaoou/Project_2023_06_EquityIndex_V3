import itertools as ittl
from skyrim.falkreath import CLib1Tab1, CTable
from project_config import factors
from project_config import universe_options

database_structure: dict[str, CLib1Tab1] = {}

# --- test returns
test_return_lbls = ["test_return_{}".format(w) for w in ["o", "c"]]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in test_return_lbls})

# --- factor exposures
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in factors
})

# # --- ic tests by factors
# database_structure.update({
#     "ic-{}-TW{:03d}".format(z, tw): CLib1Tab1(
#         t_lib_name="ic-{}-TW{:03d}.db".format(z, tw),
#         t_tab=CTable({
#             "table_name": z,
#             "primary_keys": {"trade_date": "TEXT"},
#             "value_columns": {"pearson": "REAL", "spearman": "REAL",
#                               "CH": "REAL", "CF": "REAL", "FH": "REAL"},
#         })
#     ) for z, tw in ittl.product(factors, test_windows)
# })
#
# # --- gp tests by factors
# database_structure.update({
#     "gp-{}-TW{:03d}-{}".format(z, tw, u): CLib1Tab1(
#         t_lib_name="gp-{}-TW{:03d}-{}.db".format(z, tw, u),
#         t_tab=CTable({
#             "table_name": z,
#             "primary_keys": {"trade_date": "TEXT"},
#             "value_columns": {"RL": "REAL", "RS": "REAL", "RH": "REAL"},
#         })
#     ) for z, tw, u in ittl.product(factors, test_windows, universe_options)
# })

# --- signals
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in ["S{:03d}".format(_) for _ in range(20)]
})

# --- em01 by major contract
database_structure.update({
    "em01_major": CLib1Tab1(
        t_lib_name="em01_major.db",
        t_tab=CTable({
            "table_name": "em01_major",
            "primary_keys": {"trade_date": "TEXT", "timestamp": "INT4", "loc_id": "TEXT"},
            "value_columns": {"instrument": "TEXT",
                              "exchange": "TEXT",
                              "wind_code": "TEXT",
                              "open": "REAL",
                              "high": "REAL",
                              "low": "REAL",
                              "close": "REAL",
                              "volume": "REAL",
                              "amount": "REAL",
                              "oi": "REAL",
                              "daily_open": "REAL",
                              "daily_high": "REAL",
                              "daily_low": "REAL",
                              "preclose": "REAL",
                              "preoi": "REAL"}
        })  # the same as em01 in E:\Deploy\Data\Futures\md\md_structure.json
    )
})

# --- hold position and delta position from public information
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": z,
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT", "institute": "TEXT"},
            "value_columns": {"lng": "REAL", "srt": "REAL"},
        })
    ) for z in ["hld_pos", "dlt_pos"]
})
