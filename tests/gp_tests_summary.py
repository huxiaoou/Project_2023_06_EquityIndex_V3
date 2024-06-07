import os
import datetime as dt
import pandas as pd
import multiprocessing as mp
from rich.progress import track
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.riften import CNAV
from skyrim.whiterun import error_handler


def cal_gp_tests_summary(
        factors_ma: list[str], sharpe_ratio_threshold: float,
        bgn_date: str, stp_date: str,
        database_structure: dict[str, CLib1Tab1],
        tests_result_dir: str,
        tests_result_summary_dir: str,
):
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_rows", 1000)

    statistics_data = []
    for factor_ma in track(factors_ma, description=f"[INF] Loading gp-tests results ..."):
        test_lib_id = "gp-{}".format(factor_ma)
        test_lib_structure = database_structure[test_lib_id]
        test_lib = CManagerLibReader(t_db_save_dir=tests_result_dir, t_db_name=test_lib_structure.m_lib_name)
        test_lib.set_default(test_lib_structure.m_tab.m_table_name)

        gp_df = test_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "RL", "RS", "RH"]).set_index("trade_date")
        test_lib.close()

        nav = CNAV(t_raw_nav_srs=gp_df["RH"], t_annual_rf_rate=0, t_type="RET")
        nav.cal_all_indicators()
        res = nav.to_dict(t_type="eng")
        res.update({"factor": factor_ma})
        statistics_data.append(res)

    statistics_df = pd.DataFrame(statistics_data).set_index("factor")
    statistics_df.sort_values(by="sharpe_ratio", ascending=False, inplace=True)
    statistics_file = "gp_tests_summary.csv.gz"
    statistics_path = os.path.join(tests_result_summary_dir, statistics_file)
    statistics_df.to_csv(statistics_path, float_format="%.6f")

    print("=" * 120)
    filter_selected_factors = statistics_df["sharpe_ratio"].astype(float).abs() > sharpe_ratio_threshold
    print(statistics_df.loc[filter_selected_factors])
    print("-" * 120)

    return 0


def cal_gp_tests_summary_mp(
        proc_num: int,
        factors_ma: list[str], sharpe_ratio_threshold: float,
        bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    pool.apply_async(
        cal_gp_tests_summary,
        args=(factors_ma, sharpe_ratio_threshold, bgn_date, stp_date),
        kwds=kwargs,
        error_callback=error_handler,
    )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
