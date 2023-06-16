import os
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.winterhold import plot_corr, plot_lines
from skyrim.markarth import minimize_utility


def cal_gp_tests_corr(
        test_window: int, uid: str, factors: list[str],
        bgn_date: str, stp_date: str,
        database_structure: dict[str, CLib1Tab1],
        gp_tests_dir: str,
        gp_tests_summary_dir: str,
):
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_rows", 1000)

    ret_data = {}
    for factor_lbl in factors:
        gp_test_lib_id = "gp-{}-TW{:03d}-{}".format(factor_lbl, test_window, uid)
        gp_test_lib_structure = database_structure[gp_test_lib_id]
        gp_test_lib = CManagerLibReader(t_db_save_dir=os.path.join(gp_tests_dir, factor_lbl), t_db_name=gp_test_lib_structure.m_lib_name)
        gp_test_lib.set_default(gp_test_lib_structure.m_tab.m_table_name)

        gp_df = gp_test_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "RL", "RS", "RH"]).set_index("trade_date")
        ret_data[factor_lbl] = gp_df["RH"]
    ret_df = pd.DataFrame(ret_data)
    ret_cum_df = ret_df.cumsum()
    print(ret_df)
    print(corr_df := ret_df.corr())
    plot_corr(t_corr_df=corr_df, t_save_dir=gp_tests_summary_dir, t_fig_name="corr-TW{:03d}-{}".format(test_window, uid))
    plot_lines(t_plot_df=ret_cum_df, t_save_dir=gp_tests_summary_dir, t_fig_name="cum-TW{:03d}-{}".format(test_window, uid))

    mu = ret_df.mean()
    sgm = ret_df.cov()
    opt_res = {}
    for lbd in [0.1, 1, 10, 100, 1000, 50000]:
        w, _ = minimize_utility(t_mu=mu.values, t_sigma=sgm.values, t_lbd=lbd)
        opt_res[lbd] = pd.Series(data=w, index=mu.index)
    print(df := pd.DataFrame.from_dict(opt_res))
    return 0
