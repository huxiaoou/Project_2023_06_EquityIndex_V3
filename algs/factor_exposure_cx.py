import os
import datetime as dt
import itertools as ittl
import multiprocessing as mp
import sys
import pandas as pd
from skyrim.whiterun import error_handler
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibWriter


def cal_corr(t_sub_df: pd.DataFrame, t_x: str, t_y: str, t_sort_var: str, t_top_size: int):
    _sorted_df = t_sub_df.sort_values(by=t_sort_var, ascending=False)
    _top_df = _sorted_df.head(t_top_size)
    _r = _top_df[[t_x, t_y]].corr(method="spearman").at[t_x, t_y]
    return -_r


def fac_exp_alg_cx(
        run_mode: str, bgn_date: str, stp_date: str | None,
        cx: str, cx_window: int, top_prop: float,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        major_return_dir: str,
        factors_exposure_dir: str,
):
    """

    :param run_mode:
    :param bgn_date:
    :param stp_date:
    :param cx: must be one of ["CSP", "CSR", "CTP", "CTR", "CVP", "CVR"]
    :param cx_window:
    :param top_prop:
    :param instruments_universe:
    :param database_structure:
    :param major_return_dir:
    :param factors_exposure_dir:
    :return:
    """
    factor_lbl = "{}{:03d}T{:02d}".format(cx, cx_window, int(top_prop * 10))
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    top_size = int(cx_window * top_prop) + 1
    x, y = {
        "CSP": ("sigma", "instru_idx"),
        "CSR": ("sigma", "major_return"),
        "CTP": ("turnover", "instru_idx"),
        "CTR": ("turnover", "major_return"),
        "CVP": ("volume", "instru_idx"),
        "CVR": ("volume", "major_return"),
    }.get(cx.upper(), (None, None))
    if (x, y) == (None, None):
        print("... Error! when calculating CX")
        print("... cx = ", cx, "is not a legal input, please check again")
        print("... this function will terminate at once")
        sys.exit()

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        major_return_file = "major_return.{}.close.csv.gz".format(instrument)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).set_index("trade_date")
        if cx.upper() in ["CSP", "CSR"]:
            major_return_df[x] = major_return_df["high"] / major_return_df["low"] - 1
        elif cx.upper() in ["CTP", "CTR"]:
            major_return_df[x] = major_return_df["volume"] / major_return_df["oi"]
        r_data = {}
        for i in range(len(major_return_df)):
            trade_date = major_return_df.index[i]
            if (trade_date < bgn_date) or (trade_date >= stp_date):
                continue
            sub_df = major_return_df.iloc[i - cx_window + 1:i + 1]
            r_data[trade_date] = cal_corr(t_sub_df=sub_df, t_x=x, t_y=y, t_sort_var="volume", t_top_size=top_size)
        factor_df = pd.DataFrame({"instrument": instrument, factor_lbl: pd.Series(r_data)})
        all_factor_dfs.append(factor_df[["instrument", factor_lbl]])

    # --- reorganize
    all_factor_df = pd.concat(all_factor_dfs, axis=0, ignore_index=False)
    all_factor_df.sort_index(inplace=True)

    # --- save
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibWriter(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
    factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
    factor_lib.close()

    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl))
    return 0


def cal_fac_exp_cx_mp(proc_num: int,
                      run_mode: str, bgn_date: str, stp_date: str | None,
                      mgr_cx_windows: dict[str, list[int]], top_props: list[float],
                      instruments_universe: list[str],
                      database_structure: dict,
                      major_return_dir: str,
                      factors_exposure_dir: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for cx, cx_windows in mgr_cx_windows.items():
        for cx_window, top_prop in ittl.product(cx_windows, top_props):
            pool.apply_async(fac_exp_alg_cx,
                             args=(run_mode, bgn_date, stp_date,
                                   cx, cx_window, top_prop,
                                   instruments_universe,
                                   database_structure,
                                   major_return_dir,
                                   factors_exposure_dir),
                             error_callback=error_handler,
                             )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
