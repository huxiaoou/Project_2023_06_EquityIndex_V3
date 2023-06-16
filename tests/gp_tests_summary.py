import os
import datetime as dt
import pandas as pd
import multiprocessing as mp
import itertools as ittl
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.riften import CNAV


def cal_gp_tests_summary(
        test_window: int, uid: str, factors: list[str],
        bgn_date: str, stp_date: str,
        database_structure: dict[str, CLib1Tab1],
        gp_tests_dir: str,
        gp_tests_summary_dir: str,
):
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_rows", 1000)

    statistics_data = []
    for factor_lbl in factors:
        gp_test_lib_id = "gp-{}-TW{:03d}-{}".format(factor_lbl, test_window, uid)
        gp_test_lib_structure = database_structure[gp_test_lib_id]
        gp_test_lib = CManagerLibReader(t_db_save_dir=os.path.join(gp_tests_dir, factor_lbl), t_db_name=gp_test_lib_structure.m_lib_name)
        gp_test_lib.set_default(gp_test_lib_structure.m_tab.m_table_name)

        gp_df = gp_test_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "RL", "RS", "RH"]).set_index("trade_date")
        ret_df = gp_df / test_window
        gp_test_lib.close()

        nav = CNAV(t_raw_nav_srs=ret_df["RH"], t_annual_rf_rate=0, t_freq="D", t_type="RET", t_turnover_period=test_window)
        nav.cal_all_indicators()
        res = nav.to_dict(t_type="eng")
        res.update({"factor": factor_lbl})
        statistics_data.append(res)

    statistics_df = pd.DataFrame(statistics_data).set_index("factor")
    statistics_df.sort_values(by="sharpe_ratio", ascending=False, inplace=True)
    statistics_file = "gp_tests_summary-TW{:03d}-{}.csv.gz".format(test_window, uid)
    statistics_path = os.path.join(gp_tests_summary_dir, statistics_file)
    statistics_df.to_csv(statistics_path, float_format="%.6f")

    print("=" * 120)
    print("test window = {}, uid = {}".format(test_window, uid))
    print("-" * 120)
    print(statistics_df.head(5))
    print("-" * 120)

    return 0


def cal_gp_tests_summary_mp(
        proc_num: int,
        test_windows: list[int], universe_options: dict[str, list[str]], factors: list[str],
        bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for test_window, uid in ittl.product(test_windows, universe_options):
        pool.apply_async(
            cal_gp_tests_summary,
            args=(test_window, uid, factors, bgn_date, stp_date),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
