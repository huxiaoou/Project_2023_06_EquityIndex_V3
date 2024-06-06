import json
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from rich.progress import track
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CTable, CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriterByDate


def error_handler(error):
    print(f"'Error': {error}", flush=True)
    return -1


def lookup_av_ratio(db: CManagerLibReader, date: str, contract: str, ret_type: str):
    _s, _d = (0, 1) if ret_type == "o" else (-1, -1)
    m01_df = db.read_by_date(
        t_trade_date=date,
        t_value_columns=["loc_id", "open", "high", "low", "close", "amount", "volume"]
    ).set_index("loc_id")
    m01_df = m01_df.dropna(axis=0, how="all", subset=["open", "high", "low", "close"])
    try:
        i = 0
        while (v := m01_df.at[contract, "volume"].iloc[_s + _d * i]) == 0:
            i += 1
        if i > 0:
            print("... 0 volume found for {} at {} with ret_type = {}, i = {:>3d}".format(contract, date, ret_type, i))
        a = m01_df.at[contract, "amount"].iloc[_s + _d * i]
        return a / v
    except KeyError:
        print("... Warning! KeyError when lookup av ratio at {} for {}".format(date, contract))
        return np.nan


def cal_test_returns_for_test_window(
        test_return_type: str,
        run_mode: str, bgn_date: str, stp_date: str | None,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        test_returns_dir: str,
        by_instrument_dir: str,
        futures_dir: str,
        calendar_path: str,
        futures_md_structure_path: str,
        futures_em01_db_name: str,

):
    """

    :param test_return_type: must be in {"o", "c"}
    :param run_mode:
    :param bgn_date:
    :param stp_date:
    :param instruments_universe:
    :param database_structure:
    :param test_returns_dir:
    :param by_instrument_dir:
    :param futures_dir:
    :param calendar_path:
    :param futures_md_structure_path:
    :param futures_em01_db_name:
    :return:
    """
    calendar = CCalendar(calendar_path)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    _test_window = 1

    # --- init major contracts
    major_minor_manager = {}
    major_minor_lib_reader = CManagerLibReader(by_instrument_dir, "major_minor.db")
    for instrument in instruments_universe:
        major_minor_df = major_minor_lib_reader.read(
            t_value_columns=["trade_date", "n_contract", "d_contract"],
            t_using_default_table=False,
            t_table_name=instrument.replace(".", "_"),
        ).set_index("trade_date")
        major_minor_manager[instrument] = major_minor_df

    # --- init lib reader
    with open(futures_md_structure_path, "r") as j:
        m01_table_struct = json.load(j)[futures_em01_db_name]["CTable"]
    m01_table = CTable(t_table_struct=m01_table_struct)
    m01_db = CManagerLibReader(t_db_save_dir=futures_dir, t_db_name=futures_em01_db_name)
    m01_db.set_default(m01_table.m_table_name)

    # --- init lib writer
    test_return_lib_id = f"test_return_{test_return_type}"
    test_return_lib_struct = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibWriterByDate(
        t_db_save_dir=test_returns_dir,
        t_db_name=test_return_lib_struct.m_lib_name,
    )
    test_return_lib.initialize_table(
        t_table=test_return_lib_struct.m_tab,
        t_remove_existence=run_mode in ["O", "OVERWRITE"],
    )

    # --- main loop
    test_return_data = []
    iter_end_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    iter_bgn_dates = calendar.get_iter_list(
        calendar.get_next_date(iter_end_dates[0], -_test_window),
        calendar.get_next_date(iter_end_dates[-1], -_test_window + 1),
        True
    )
    iter_dates_pair = list(zip(iter_bgn_dates, iter_end_dates))
    for test_bgn_date, test_end_date in track(iter_dates_pair, description=f"[INF] Test return {test_return_type}"):
        for instrument in instruments_universe:
            instru_major_contract = major_minor_manager[instrument].at[
                test_end_date, "n_contract"]  # format like = "IC2305.CFE"
            test_bgn_av_ratio = lookup_av_ratio(m01_db, test_bgn_date, instru_major_contract, test_return_type)
            test_end_av_ratio = lookup_av_ratio(m01_db, test_end_date, instru_major_contract, test_return_type)
            test_return_data.append({
                "trade_date": test_end_date,
                "instrument": instrument,
                "test_return": test_end_av_ratio / test_bgn_av_ratio - 1,
            })
        if run_mode in ["A", "APPEND"]:
            test_return_lib.delete_by_date(t_date=test_end_date)
    test_return_df = pd.DataFrame(test_return_data).sort_values(by=["trade_date", "instrument"])
    test_return_lib.update(t_update_df=test_return_df)
    test_return_lib.close()
    m01_db.close()
    print("... @", dt.datetime.now(), run_mode, bgn_date, stp_date, test_return_type, "calculated")
    return 0


def cal_test_returns_mp(
        proc_num: int,
        test_return_types: tuple[str],
        run_mode: str, bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for test_return_type in test_return_types:
        pool.apply_async(
            cal_test_returns_for_test_window,
            args=(test_return_type, run_mode, bgn_date, stp_date),
            kwds=kwargs,
            error_callback=error_handler,
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
