import os
import datetime as dt
import itertools as ittl
import multiprocessing as mp
import pandas as pd
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibWriter


def cal_amp(t_sub_df: pd.DataFrame, t_x: str, t_sort_var: str, t_top_size: int):
    _sorted_df = t_sub_df.sort_values(by=t_sort_var, ascending=False)
    _amp_h = _sorted_df.head(t_top_size)[t_x].mean()
    _amp_l = _sorted_df.tail(t_top_size)[t_x].mean()
    _amp_d = _amp_h - _amp_l
    return -_amp_h, _amp_l, -_amp_d


def fac_exp_alg_amp(
        run_mode: str, bgn_date: str, stp_date: str | None,
        amp_window: int, lbd: float,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        major_return_dir: str,
        factors_exposure_dir: str,
        mapper_fut_to_idx: dict[str, str],
        equity_index_by_instrument_dir: str,
):
    """

    :param run_mode:
    :param bgn_date:
    :param stp_date:
    :param amp_window:
    :param lbd:
    :param instruments_universe:
    :param database_structure:
    :param major_return_dir:
    :param factors_exposure_dir:
    :param mapper_fut_to_idx: {'IH.CFE': '000016.SH', 'IF.CFE': '000300.SH', 'IC.CFE': '000905.SH', 'IM.CFE': '000852.SH', None: '881001.WI'}
    :param equity_index_by_instrument_dir:
    :return:
    """
    factor_h_lbl, factor_l_lbl, factor_d_lbl = ["AMP{}{:03d}T{:02d}".format(_, amp_window, int(lbd * 10))
                                                for _ in ["H", "L", "D"]]
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    top_size = int(amp_window * lbd) + 1

    # --- init major contracts
    all_factor_h_dfs, all_factor_l_dfs, all_factor_d_dfs = [], [], []
    for instrument in instruments_universe:
        equity_index_code = mapper_fut_to_idx[instrument]
        spot_data_file = "{}.csv".format(equity_index_code)
        spot_data_path = os.path.join(equity_index_by_instrument_dir, spot_data_file)
        spot_df = pd.read_csv(spot_data_path, dtype={"trade_date": str}).set_index("trade_date")

        major_return_file = "major_return.{}.close.csv.gz".format(instrument)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).set_index("trade_date")
        major_return_df["amp"] = major_return_df["high"] / major_return_df["low"] - 1
        major_return_df["spot"] = spot_df["close"]

        r_h_data, r_l_data, r_d_data = {}, {}, {}
        for i in range(len(major_return_df)):
            trade_date = major_return_df.index[i]
            if (trade_date < bgn_date) or (trade_date >= stp_date):
                continue
            sub_df = major_return_df.iloc[i - amp_window + 1:i + 1]
            r_h_data[trade_date], r_l_data[trade_date], r_d_data[trade_date] = cal_amp(
                t_sub_df=sub_df, t_x="amp", t_sort_var="spot", t_top_size=top_size)
        for _iter_data, _iter_dfs, _iter_factor_lbl in zip([r_h_data, r_l_data, r_d_data],
                                                           [all_factor_h_dfs, all_factor_l_dfs, all_factor_d_dfs],
                                                           [factor_h_lbl, factor_l_lbl, factor_d_lbl]):
            factor_df = pd.DataFrame({"instrument": instrument, _iter_factor_lbl: pd.Series(_iter_data)})
            _iter_dfs.append(factor_df[["instrument", _iter_factor_lbl]])

    for _iter_dfs, _iter_factor_lbl in zip([all_factor_h_dfs, all_factor_l_dfs, all_factor_d_dfs],
                                           [factor_h_lbl, factor_l_lbl, factor_d_lbl]):
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


def cal_fac_exp_amp_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       amp_windows: list[int], lbds: list[float],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       mapper_fut_to_idx: dict[str, str],
                       equity_index_by_instrument_dir: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window, lbd in ittl.product(amp_windows, lbds):
        pool.apply_async(fac_exp_alg_amp,
                         args=(run_mode, bgn_date, stp_date,
                               p_window, lbd,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir,
                               mapper_fut_to_idx,
                               equity_index_by_instrument_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
