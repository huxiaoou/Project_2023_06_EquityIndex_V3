import os
import datetime as dt
import multiprocessing as mp
import pandas as pd
from rich.progress import track
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter
from skyrim.whiterun import CCalendar, error_handler


def fac_exp_alg_beta(
        run_mode: str, bgn_date: str, stp_date: str | None, beta_window: int,
        instruments_universe: list[str],
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        by_instrument_dir: str,
        equity_index_by_instrument_dir: str,
        factors_exposure_dir: str,
):
    factor_lbl = "BETA{:03d}".format(beta_window)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -beta_window + 1)

    # --- load market return
    market_index_file = "881001.WI.csv"
    market_index_path = os.path.join(equity_index_by_instrument_dir, market_index_file)
    market_index_df = pd.read_csv(market_index_path, dtype={"trade_date": str}).set_index("trade_date")

    # --- init major contracts
    all_factor_dfs = []
    for instrument in track(instruments_universe):
        major_return_lib_reader = CManagerLibReader(by_instrument_dir, "major_return.db")
        major_return_df = major_return_lib_reader.read(
            t_value_columns=["trade_date", "major_return"],
            t_using_default_table=False,
            t_table_name=instrument.replace(".", "_"),
        ).set_index("trade_date")
        filter_dates = (major_return_df.index >= base_date) & (major_return_df.index < stp_date)
        factor_df = major_return_df.loc[filter_dates, ["major_return"]].copy()
        factor_df["market"] = (market_index_df["close"] / market_index_df["pre_close"] - 1) * 100
        factor_df["xy"] = (factor_df["major_return"] * factor_df["market"]).rolling(window=beta_window).mean()
        factor_df["xx"] = (factor_df["market"] * factor_df["market"]).rolling(window=beta_window).mean()
        factor_df["y"] = factor_df["major_return"].rolling(window=beta_window).mean()
        factor_df["x"] = factor_df["market"].rolling(window=beta_window).mean()
        factor_df["cov_xy"] = factor_df["xy"] - factor_df["x"] * factor_df["y"]
        factor_df["cov_xx"] = factor_df["xx"] - factor_df["x"] * factor_df["x"]
        factor_df[factor_lbl] = factor_df["cov_xy"] / factor_df["cov_xx"]
        filter_dates = (factor_df.index >= bgn_date) & (factor_df.index < stp_date)
        factor_df = factor_df.loc[filter_dates, [factor_lbl]].copy()
        factor_df["instrument"] = instrument
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


def fac_exp_alg_beta_diff(
        run_mode: str, bgn_date: str, stp_date: str | None, beta_window: int, beta_base_window: int,
        instruments_universe: list[str],
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
):
    src_lbl = "BETA{:03d}".format(beta_base_window)
    factor_lbl_diff = "BETA_D{:03d}".format(beta_window)

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

    # --- init diff writer
    diff_lib_structure = database_structure[factor_lbl_diff]
    diff_lib = CManagerLibWriter(
        t_db_name=diff_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir,
    )
    diff_lib.initialize_table(t_table=diff_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- load hist and calculate
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -beta_window)
    src_df = src_lib.read_by_conditions(
        t_conditions=[
            ("trade_date", ">=", base_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"],
    ).set_index("trade_date")

    diff_dfs = []
    for instrument, instrument_df in src_df.groupby(by="instrument"):
        if instrument not in instruments_universe:
            continue
        res_df = instrument_df.sort_index(ascending=True)
        res_df["diff"] = res_df["value"] - res_df["value"].shift(beta_window)
        diff_dfs.append(res_df[["instrument", "diff"]])
    diff_df = pd.concat(diff_dfs, axis=0, ignore_index=False).sort_index(ascending=True)
    diff_df = diff_df.loc[diff_df.index >= bgn_date]
    diff_lib.update(t_update_df=diff_df, t_using_index=True)

    src_lib.close()
    diff_lib.close()

    print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), factor_lbl_diff))
    return 0


def cal_fac_exp_beta_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str | None,
                        beta_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        by_instrument_dir: str,
                        equity_index_by_instrument_dir: str,
                        factors_exposure_dir: str,
                        calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in beta_windows:
        pool.apply_async(fac_exp_alg_beta,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               by_instrument_dir,
                               equity_index_by_instrument_dir,
                               factors_exposure_dir),
                         error_callback=error_handler,
                         )
    pool.close()
    pool.join()
    pool = mp.Pool(processes=proc_num)
    for p_window in beta_windows[1:]:
        pool.apply_async(fac_exp_alg_beta_diff,
                         args=(run_mode, bgn_date, stp_date, p_window, beta_windows[0],
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir),
                         error_callback=error_handler,
                         )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
