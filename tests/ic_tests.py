import os
import datetime as dt
import pandas as pd
import multiprocessing as mp
import itertools as ittl
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader, CManagerLibWriter
from skyrim.winterhold import check_and_mkdir
from skyrim.whiterun import CCalendar


def cal_corr_by_date(df: pd.DataFrame, x: str, y: str):
    _rp = df[[x, y]].corr(method="pearson").at[x, y]
    _rs = df[[x, y]].corr(method="spearman").at[x, y]
    _rch = 1 if (df.loc["IC.CFE", [x, y]] - df.loc["IH.CFE", [x, y]]).product() > 0 else 0
    _rcf = 1 if (df.loc["IC.CFE", [x, y]] - df.loc["IF.CFE", [x, y]]).product() > 0 else 0
    _rfh = 1 if (df.loc["IF.CFE", [x, y]] - df.loc["IH.CFE", [x, y]]).product() > 0 else 0
    return _rp, _rs, _rch, _rcf, _rfh


def shift_fac_exp(df: pd.DataFrame, row: str, col: str, val: str, shift_win: int):
    _pivot_df = pd.pivot_table(data=df, index=row, columns=col, values=val)
    _res_df = _pivot_df.shift(shift_win).stack().sort_index().reset_index()
    _res_df.rename(mapper={0: val}, axis=1, inplace=True)
    return _res_df


def ic_test_single_factor(factor_lbl: str, test_window: int,
                          run_mode: str, bgn_date: str, stp_date: str,
                          ic_tests_dir: str,
                          factors_exposure_dir: str,
                          test_returns_dir: str,
                          database_structure: dict[str, CLib1Tab1],
                          calendar_path: str):
    # --- directory check
    check_and_mkdir(os.path.join(ic_tests_dir, factor_lbl))

    # --- load calendar
    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -test_window - 1)

    # --- ic-tests lib
    ic_test_lib_id = "ic-{}-TW{:03d}".format(factor_lbl, test_window)
    ic_test_lib_structure = database_structure[ic_test_lib_id]
    ic_test_lib = CManagerLibWriter(t_db_save_dir=os.path.join(ic_tests_dir, factor_lbl), t_db_name=ic_test_lib_structure.m_lib_name)
    ic_test_lib.initialize_table(t_table=ic_test_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- factor library
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibReader(t_db_save_dir=factors_exposure_dir, t_db_name=factor_lib_structure.m_lib_name)
    factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)

    # --- test return library
    test_return_lib_id = "test_return_{:03d}".format(test_window)
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_returns_dir)
    test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)

    fac_exp_df = factor_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])
    fac_exp_df_shift = shift_fac_exp(fac_exp_df, row="trade_date", col="instrument", val="value", shift_win=test_window + 1)

    test_return_df = test_return_lib.read_by_conditions(
        t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"]
    )

    ic_test_input_df = pd.merge(
        left=fac_exp_df_shift, right=test_return_df,
        on=["trade_date", "instrument"], suffixes=("_e", "_r"),
        how="right"
    ).set_index("instrument")
    res_srs = ic_test_input_df.groupby(by="trade_date").apply(cal_corr_by_date, x="value_e", y="value_r")
    pr_srs, sr_srs, ch_srs, cf_srs, fh_srs = zip(*res_srs)
    ic_test_df = pd.DataFrame({
        "pearson": pr_srs,
        "spearman": sr_srs,
        "ch": ch_srs,
        "cf": cf_srs,
        "fh": fh_srs,
    }, index=res_srs.index)
    ic_test_lib.update(t_update_df=ic_test_df, t_using_index=True)
    ic_test_lib.close()
    factor_lib.close()
    test_return_lib.close()
    return 0


def cal_ic_tests_mp(
        proc_num: int,
        factor_lbls: list[str], test_windows: list[int],
        run_mode: str, bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor_lbl, test_window in ittl.product(factor_lbls, test_windows):
        pool.apply_async(
            ic_test_single_factor,
            args=(factor_lbl, test_window, run_mode, bgn_date, stp_date),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
