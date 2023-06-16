import itertools as ittl
from skyrim.falkreath import CLib1Tab1, CTable
from project_config import factors, factor_mov_ave_wins
from project_config import universe_options

database_structure: dict[str, CLib1Tab1] = {}

# --- test returns
test_return_lbls = ["test_return_{}".format(w) for w in ["o", "c"]]
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "TR",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in test_return_lbls})

# --- factor exposures
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "FE",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in factors
})

# --- factor exposures moving average
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "FEMA",
            "primary_keys": {"trade_date": "TEXT", "instrument": "TEXT"},
            "value_columns": {"value": "REAL"},
        })
    ) for z in ["{}-M{:03d}".format(f, w) for f, w in ittl.product(factors, factor_mov_ave_wins)]
})

# --- ic tests by factors
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "IC",
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"pearson": "REAL", "spearman": "REAL"},
        })
    ) for z in ["ic-{}-M{:03d}".format(f, w) for f, w in ittl.product(factors, factor_mov_ave_wins)]
})

# --- gp tests by factors
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "GP",
            "primary_keys": {"trade_date": "TEXT"},
            "value_columns": {"RL": "REAL", "RS": "REAL", "RH": "REAL"},
        })
    ) for z in ["gp-{}-M{:03d}".format(f, w) for f, w in ittl.product(factors, factor_mov_ave_wins)]
})

# --- signals
database_structure.update({
    z: CLib1Tab1(
        t_lib_name=z + ".db",
        t_tab=CTable({
            "table_name": "SIG",
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
