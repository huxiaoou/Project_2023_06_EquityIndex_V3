import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def fac_exp_alg_basis(
        run_mode: str, bgn_date: str, stp_date: str | None, max_win: int,
        instruments_universe: list[str],
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        major_return_dir: str,
        equity_index_by_instrument_dir: str,
        factors_exposure_dir: str,
):
    factor_lbl = "BASIS"
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -max_win + 1)

    equity_index_code_mapper = {
        "IH.CFE": "000016.SH",
        "IF.CFE": "000300.SH",
        "IC.CFE": "000905.SH",
        "IM.CFE": "000852.SH",
    }

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        major_return_file = "major_return.{}.close.csv.gz".format(instrument)
        major_return_path = os.path.join(major_return_dir, major_return_file)
        major_return_df = pd.read_csv(major_return_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (major_return_df.index >= base_date) & (major_return_df.index < stp_date)
        selected_major_return_df = major_return_df.loc[filter_dates, ["n_contract", "close"]].round(2)

        equity_index_code = equity_index_code_mapper[instrument]
        spot_data_file = "{}.csv".format(equity_index_code)
        spot_data_path = os.path.join(equity_index_by_instrument_dir, spot_data_file)
        spot_df = pd.read_csv(spot_data_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (spot_df.index >= base_date) & (spot_df.index < stp_date)
        selected_spot_df = spot_df.loc[filter_dates, ["close"]]

        factor_df = pd.merge(
            left=selected_major_return_df, right=selected_spot_df,
            left_index=True, right_index=True, how="outer",
            suffixes=("_f", "_s")
        )
        factor_df["instrument"] = instrument
        factor_df[factor_lbl] = factor_df["close_s"] / factor_df["close_f"] - 1
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


def fac_exp_alg_basis_ma_and_diff(
        run_mode: str, bgn_date: str, stp_date: str | None, basis_window: int,
        instruments_universe: list[str],
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
):
    src_lbl = "BASIS"
    factor_lbl_ma, factor_lbl_diff = "BASIS_M{:03d}".format(basis_window), "BASIS_D{:03d}".format(basis_window)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    calendar = CCalendar(calendar_path)

    # --- init reader
    src_lib_structure = database_structure[src_lbl]
    src_lib = CManagerLibReader(
        t_db_name=src_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    src_lib.set_default(t_default_table_name=src_lib_structure.m_tab.m_table_name)

    # --- init ma writer
    ma_lib_structure = database_structure[factor_lbl_ma]
    ma_lib = CManagerLibWriter(
        t_db_name=ma_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir,
    )
    ma_lib.initialize_table(t_table=ma_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- init diff writer
    diff_lib_structure = database_structure[factor_lbl_diff]
    diff_lib = CManagerLibWriter(
        t_db_name=diff_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir,
    )
    diff_lib.initialize_table(t_table=diff_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- load hist and calculate
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -basis_window + 1)
    src_df = src_lib.read_by_conditions(
        t_conditions=[
            ("trade_date", ">=", base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"],
    ).set_index("trade_date")

    ma_dfs, diff_dfs = [], []
    for instrument, instrument_df in src_df.groupby(by="instrument"):
        if instrument not in instruments_universe:
            continue
        res_df = instrument_df.sort_index(ascending=True)
        res_df["ma"] = res_df["value"].rolling(window=basis_window).mean()
        res_df["diff"] = res_df["value"] - res_df["ma"]
        ma_dfs.append(res_df[["instrument", "ma"]])
        diff_dfs.append(res_df[["instrument", "diff"]])
    ma_df = pd.concat(ma_dfs, axis=0, ignore_index=False).sort_index(ascending=True)
    diff_df = pd.concat(diff_dfs, axis=0, ignore_index=False).sort_index(ascending=True)
    ma_df = ma_df.loc[ma_df.index >= bgn_date]
    diff_df = diff_df.loc[diff_df.index >= bgn_date]
    ma_lib.update(t_update_df=ma_df, t_using_index=True)
    diff_lib.update(t_update_df=diff_df, t_using_index=True)

    src_lib.close()
    ma_lib.close()
    diff_lib.close()

    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl_ma))
    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl_diff))
    return 0


def cal_fac_exp_basis_mp(proc_num: int,
                         run_mode: str, bgn_date: str, stp_date: str | None,
                         basis_windows: list[int],
                         instruments_universe: list[str],
                         database_structure: dict,
                         major_return_dir: str,
                         equity_index_by_instrument_dir: str,
                         factors_exposure_dir: str,
                         calendar_path: str):
    t0 = dt.datetime.now()
    fac_exp_alg_basis(run_mode, bgn_date, stp_date, max(basis_windows),
                      instruments_universe,
                      calendar_path,
                      database_structure,
                      major_return_dir,
                      equity_index_by_instrument_dir,
                      factors_exposure_dir)
    pool = mp.Pool(processes=proc_num)
    for p_window in basis_windows:
        pool.apply_async(fac_exp_alg_basis_ma_and_diff,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
