import os
import json
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable, CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriterByDate


def lookup_av_ratio(db: CManagerLibReader, date: str, contract: str):
    m01_df = db.read_by_date(
        t_trade_date=date,
        t_value_columns=["loc_id", "amount", "volume"]
    ).set_index("loc_id")
    try:
        return m01_df.at[contract, "amount"].iloc[0] / m01_df.at[contract, "volume"].iloc[0]
    except KeyError:
        print("... Warning! KeyError when lookup av ratio at {} for {}".format(date, contract))
        return np.nan


def cal_test_returns_for_test_window(
        run_mode: str, bgn_date: str, stp_date: str | None, test_window: int,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        test_returns_dir: str,
        major_minor_dir: str,
        futures_md_dir: str,
        calendar_path: str,
        futures_md_structure_path: str,
        futures_em01_db_name: str,

):
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- init major contracts
    major_minor_manager = {}
    for instrument in instruments_universe:
        major_minor_file = "major_minor.{}.csv.gz".format(instrument)
        major_minor_path = os.path.join(major_minor_dir, major_minor_file)
        major_minor_df = pd.read_csv(major_minor_path, dtype=str).set_index("trade_date")
        major_minor_manager[instrument] = major_minor_df

    # --- init lib reader
    with open(futures_md_structure_path, "r") as j:
        m01_table_struct = json.load(j)[futures_em01_db_name]["CTable"]
    m01_table = CTable(t_table_struct=m01_table_struct)
    m01_db = CManagerLibReader(t_db_save_dir=futures_md_dir, t_db_name=futures_em01_db_name + ".db")
    m01_db.set_default(m01_table.m_table_name)

    # --- init lib writer
    test_return_lib_id = "test_return_{:03d}".format(test_window)
    test_return_lib_struct = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibWriterByDate(t_db_save_dir=test_returns_dir, t_db_name=test_return_lib_struct.m_lib_name)
    test_return_lib.initialize_table(t_table=test_return_lib_struct.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    test_return_data = {instrument: [] for instrument in instruments_universe}
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_iter_dates = [calendar.get_next_date(iter_dates[0], i - test_window) for i in range(test_window)]
    for test_end_date in base_iter_dates + iter_dates:
        for instrument in instruments_universe:
            instru_major_contract = major_minor_manager[instrument].at[test_end_date, "n_contract"]  # format like = "IC2305.CFE"
            test_end_av_ratio = lookup_av_ratio(m01_db, test_end_date, instru_major_contract)
            test_return_data[instrument].append({
                "trade_date": test_end_date,
                "av_ratio": test_end_av_ratio,
            })
        if run_mode in ["A", "APPEND"]:
            test_return_lib.delete_by_date(t_date=test_end_date)

    selected_dfs = []
    for instrument, instrument_data in test_return_data.items():
        instru_update_df = pd.DataFrame(instrument_data).sort_values(by="trade_date", ascending=True)
        instru_update_df["instrument"] = instrument
        instru_update_df["test_return"] = instru_update_df["av_ratio"] / instru_update_df["av_ratio"].shift(test_window) - 1
        filter_dates_since_bgn_date = instru_update_df["trade_date"] >= bgn_date
        instru_update_df_selected = instru_update_df.loc[filter_dates_since_bgn_date, ["trade_date", "instrument", "test_return"]]
        selected_dfs.append(instru_update_df_selected)

    update_df = pd.concat(selected_dfs, axis=0, ignore_index=True)
    update_df.sort_values(by=["trade_date", "instrument"], ascending=True, inplace=True)
    test_return_lib.update(t_update_df=update_df)
    test_return_lib.close()
    m01_db.close()

    print("... @", dt.datetime.now(), run_mode, bgn_date, stp_date, "TW={:02d}".format(test_window), "calculated")
    return 0


def cal_test_returns_mp(
        proc_num: int,
        run_mode: str, bgn_date: str, stp_date: str | None, test_windows: list[int],
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for test_window in test_windows:
        pool.apply_async(
            cal_test_returns_for_test_window,
            args=(run_mode, bgn_date, stp_date, test_window),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
