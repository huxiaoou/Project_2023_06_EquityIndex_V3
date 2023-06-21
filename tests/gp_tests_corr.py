import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.winterhold import plot_corr, plot_lines
from skyrim.markarth import minimize_utility


def cal_gp_tests_corr(
        factors_ma: list[str],
        bgn_date: str, stp_date: str,
        database_structure: dict[str, CLib1Tab1],
        tests_result_dir: str,
        tests_result_summary_dir: str,
):
    pd.set_option("display.float_format", "{:.4f}".format)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_rows", 1000)

    ret_data = {}
    for factor_ma in factors_ma:
        test_lib_id = "gp-{}".format(factor_ma)
        test_lib_structure = database_structure[test_lib_id]
        test_lib = CManagerLibReader(t_db_save_dir=tests_result_dir, t_db_name=test_lib_structure.m_lib_name)
        test_lib.set_default(test_lib_structure.m_tab.m_table_name)
        gp_df = test_lib.read_by_conditions(t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "RL", "RS", "RH"]).set_index("trade_date")
        ret_data[factor_ma] = gp_df["RH"]
    ret_df = pd.DataFrame(ret_data)
    ret_cum_df = ret_df.cumsum()
    print(ret_df)
    print(corr_df := ret_df.corr())
    plot_corr(t_corr_df=corr_df, t_save_dir=tests_result_summary_dir, t_fig_name="corr")
    plot_lines(t_plot_df=ret_cum_df, t_save_dir=tests_result_summary_dir, t_fig_name="cum")

    mu = ret_df.mean()
    sgm = ret_df.cov()
    opt_res = {}
    for lbd in [0.1, 1, 10, 100, 1000, 50000]:
        w, _ = minimize_utility(t_mu=mu.values, t_sigma=sgm.values, t_lbd=lbd)
        opt_res[lbd] = pd.Series(data=w, index=mu.index)
    print(df := pd.DataFrame.from_dict(opt_res))
    return 0
