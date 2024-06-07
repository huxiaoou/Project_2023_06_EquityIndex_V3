import datetime as dt
import multiprocessing as mp
import pandas as pd
from rich.progress import track
from skyrim.whiterun import error_handler
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibWriter, CManagerLibReader


def fac_exp_alg_size(
        run_mode: str, bgn_date: str, stp_date: str | None, size_window: int,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        by_instrument_dir: str,
        factors_exposure_dir: str,
):
    factor_lbl = "SIZE{:03d}".format(size_window)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- init major contracts
    all_factor_dfs = []
    for instrument in track(instruments_universe):
        major_return_lib_reader = CManagerLibReader(by_instrument_dir, "major_return.db")
        major_return_df = major_return_lib_reader.read(
            t_value_columns=["trade_date", "major_return", "amount", "oi", "volume"],
            t_using_default_table=False,
            t_table_name=instrument.replace(".", "_"),
        ).set_index("trade_date")
        major_return_df[factor_lbl] = major_return_df["oi"] * major_return_df["amount"] / major_return_df["volume"]
        major_return_df[factor_lbl] = major_return_df[factor_lbl].rolling(window=size_window).mean()
        filter_dates = (major_return_df.index >= bgn_date) & (major_return_df.index < stp_date)
        factor_df = major_return_df.loc[filter_dates, [factor_lbl]].copy()
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


def cal_fac_exp_size_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str | None,
                        size_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        by_instrument_dir: str,
                        factors_exposure_dir: str,
                        ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in size_windows:
        pool.apply_async(fac_exp_alg_size,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               by_instrument_dir,
                               factors_exposure_dir),
                         error_callback=error_handler,
                         )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
