import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar, error_handler
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def find_extreme_return(t_df: pd.DataFrame, t_ret: str, t_drifts: list[int]):
    ret_min, ret_max, ret_median = t_df[t_ret].min(), t_df[t_ret].max(), t_df[t_ret].median()
    if (ret_max + ret_min) > (2 * ret_median):
        idx_exr, exr = t_df[t_ret].argmax(), -ret_max
    else:
        idx_exr, exr = t_df[t_ret].argmin(), -ret_min
    dxrs = []
    for drift in t_drifts:
        idx_dxr = idx_exr - drift
        dxr = -t_df[t_ret].iloc[idx_dxr] if idx_exr >= 0 else exr
        dxrs.append(dxr)
    return exr, dxrs


def fac_exp_alg_exr(
        run_mode: str, bgn_date: str, stp_date: str | None,
        exr_window: int, drifts: list[int],
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        intermediary_dir: str,
        calendar_path: str,
):
    factor_exr_lbl = "EXR{:03d}".format(exr_window)
    factor_dxr_d_lbls = ["DXR{:03d}D{:d}".format(exr_window, d) for d in drifts]
    factor_exr_d_lbls = ["EXR{:03d}D{:d}".format(exr_window, d) for d in drifts]
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- init em01_major reader
    em01_major_lib_structure = database_structure["em01_major"]  # make sure it has the columns as em01_lib
    em01_major_lib = CManagerLibReader(
        t_db_name=em01_major_lib_structure.m_lib_name,
        t_db_save_dir=intermediary_dir
    )
    em01_major_lib.set_default(t_default_table_name=em01_major_lib_structure.m_tab.m_table_name)

    # ---
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -exr_window + 1)

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        em01_df = em01_major_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument.split(".")[0]),
            ], t_value_columns=["trade_date", "timestamp", "loc_id", "volume", "amount", "close", "preclose"]
        )
        em01_df[["close", "preclose"]] = em01_df[["close", "preclose"]].round(2)
        em01_df["m01_return_cls"] = (em01_df["close"] / em01_df["preclose"] - 1).replace(np.inf, 0)
        res_srs = em01_df.groupby(by="trade_date").apply(find_extreme_return, t_ret="m01_return_cls", t_drifts=drifts)
        exr_srs, dxrs_srs = zip(*res_srs)
        exr_dxr_df = pd.DataFrame({
            "exr": exr_srs,
            "dxrs": dxrs_srs,
        }, index=res_srs.index)
        for di, drift in enumerate(drifts):
            exr_dxr_df["d" + str(drift)] = [_[di] for _ in exr_dxr_df["dxrs"]]
        exr_dxr_df = exr_dxr_df.drop(axis=1, labels="dxrs").rolling(window=exr_window).mean()
        exr_dxr_df["instrument"] = instrument
        all_factor_dfs.append(exr_dxr_df)
    all_exr_dxr_df = pd.concat(all_factor_dfs, axis=0, ignore_index=False)
    exr_df = pd.pivot_table(data=all_exr_dxr_df, index="trade_date", columns="instrument", values="exr")
    dxr_d_dfs = {
        drift: pd.pivot_table(data=all_exr_dxr_df, index="trade_date", columns="instrument", values="d" + str(drift))
        for drift in drifts
    }
    exr_d_dfs = {drift: (exr_df + dxr_d_dfs[drift] * np.sqrt(2)) * 0.5 for drift in drifts}

    for factor_lbl, factor_df in zip(
            [factor_exr_lbl] + factor_dxr_d_lbls + factor_exr_d_lbls,
            [exr_df] + list(dxr_d_dfs.values()) + list(exr_d_dfs.values())
    ):
        df: pd.DataFrame = factor_df[factor_df.index >= bgn_date]
        all_factor_df = df.stack().reset_index(level=1).sort_index(ascending=True)
        # --- save
        factor_lib_structure = database_structure[factor_lbl]
        factor_lib = CManagerLibWriter(
            t_db_name=factor_lib_structure.m_lib_name,
            t_db_save_dir=factors_exposure_dir
        )
        factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
        factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
        factor_lib.close()

    em01_major_lib.close()
    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_exr_lbl))
    return 0


def cal_fac_exp_exr_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       exr_windows: list[int], drifts: list[float],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       intermediary_dir: str,
                       calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in exr_windows:
        pool.apply_async(fac_exp_alg_exr,
                         args=(run_mode, bgn_date, stp_date,
                               p_window, drifts,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               intermediary_dir,
                               calendar_path),
                         error_callback=error_handler,
                         )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
