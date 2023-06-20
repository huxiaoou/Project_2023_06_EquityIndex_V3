import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def find_time_weighted_center(t_df: pd.DataFrame, t_ret: str):
    index_reset_df = t_df.reset_index()
    pos_idx = index_reset_df[t_ret] > 0
    neg_idx = index_reset_df[t_ret] < 0
    pos_grp = index_reset_df.loc[pos_idx, t_ret]
    neg_grp = index_reset_df.loc[neg_idx, t_ret]
    pos_wgt = pos_grp.abs() / pos_grp.abs().sum()
    neg_wgt = neg_grp.abs() / neg_grp.abs().sum()
    twcu = pos_grp.index @ pos_wgt
    twcd = neg_grp.index @ neg_wgt
    twct = twcu - twcd
    twcv = np.abs(twct)
    return twcu, twcd, -twct, -twcv


def fac_exp_alg_twc(
        run_mode: str, bgn_date: str, stp_date: str | None,
        twc_window: int,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        intermediary_dir: str,
        calendar_path: str,
):
    factor_u_lbl, factor_d_lbl, factor_t_lbl, factor_v_lbl = [
        "TWC{}{:03d}".format(_, twc_window) for _ in ["U", "D", "T", "V"]]
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
    base_date = calendar.get_next_date(iter_dates[0], -twc_window + 1)

    # --- init major contracts
    all_factor_u_dfs, all_factor_d_dfs, all_factor_t_dfs, all_factor_v_dfs = [], [], [], []
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
        res_srs = em01_df.groupby(by="trade_date").apply(find_time_weighted_center, t_ret="m01_return_cls")
        twcu_srs, twcd_srs, twct_srs, twcv_srs = zip(*res_srs)

        for _iter_data, _iter_dfs, _iter_factor_lbl in zip([twcu_srs, twcd_srs, twct_srs, twcv_srs],
                                                           [all_factor_u_dfs, all_factor_d_dfs, all_factor_t_dfs, all_factor_v_dfs],
                                                           [factor_u_lbl, factor_d_lbl, factor_t_lbl, factor_v_lbl]):
            _iter_srs = pd.Series(data=_iter_data, index=res_srs.index).rolling(window=twc_window).mean()
            factor_df = pd.DataFrame({"instrument": instrument, _iter_factor_lbl: _iter_srs[_iter_srs.index >= bgn_date]})
            _iter_dfs.append(factor_df[["instrument", _iter_factor_lbl]])

    for _iter_dfs, _iter_factor_lbl in zip([all_factor_u_dfs, all_factor_d_dfs, all_factor_t_dfs, all_factor_v_dfs],
                                           [factor_u_lbl, factor_d_lbl, factor_t_lbl, factor_v_lbl]):
        # --- reorganize
        all_factor_df = pd.concat(_iter_dfs, axis=0, ignore_index=False)
        all_factor_df.sort_index(inplace=True)

        # --- save
        factor_lib_structure = database_structure[_iter_factor_lbl]
        factor_lib = CManagerLibWriter(
            t_db_name=factor_lib_structure.m_lib_name,
            t_db_save_dir=factors_exposure_dir
        )
        factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
        factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
        factor_lib.close()

        print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), _iter_factor_lbl))
    return 0


def cal_fac_exp_twc_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       twc_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       intermediary_dir: str,
                       calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in twc_windows:
        pool.apply_async(fac_exp_alg_twc,
                         args=(run_mode, bgn_date, stp_date,
                               p_window,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               intermediary_dir,
                               calendar_path))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
