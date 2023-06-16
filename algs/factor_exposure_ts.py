import os
import datetime as dt
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def find_price(t_x: pd.Series, t_md_df: pd.DataFrame):
    _trade_date = t_x.name
    try:
        n_prc = t_md_df.at[_trade_date, t_x["n_contract"]]
    except KeyError:
        print("... Warning! price for {} at {} is not found".format(
            t_x["n_contract"], _trade_date))
        n_prc = np.nan

    try:
        d_prc = t_md_df.at[_trade_date, t_x["d_contract"]]
    except KeyError:
        print("... Warning! price for {} at {} is not found".format(
            t_x["d_contract"], _trade_date))
        d_prc = np.nan
    return n_prc, d_prc


def cal_roll_return(t_x: pd.Series, t_n_prc_lbl: str, t_d_prc_lbl: str):
    _dlt_month = int(t_x["d_contract"].split(".")[0][-2:]) - int(t_x["n_contract"].split(".")[0][-2:])
    _dlt_month = _dlt_month + (12 if _dlt_month <= 0 else 0)
    return -(t_x[t_n_prc_lbl] / t_x[t_d_prc_lbl] - 1) / _dlt_month * 12


def fac_exp_alg_ts(
        run_mode: str, bgn_date: str, stp_date: str | None, max_win: int,
        instruments_universe: list[str],
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        major_minor_dir: str,
        md_dir: str,
        factors_exposure_dir: str,
        price_type: str,
):
    factor_lbl = "TS"
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -max_win + 1)

    # --- init major contracts
    all_factor_dfs = []
    for instrument in instruments_universe:
        major_minor_file = "major_minor.{}.csv.gz".format(instrument)
        major_minor_path = os.path.join(major_minor_dir, major_minor_file)
        major_minor_df = pd.read_csv(major_minor_path, dtype={"trade_date": str}).set_index("trade_date")
        md_file = "{}.md.{}.csv.gz".format(instrument, price_type)
        md_path = os.path.join(md_dir, md_file)
        md_df = pd.read_csv(md_path, dtype={"trade_date": str}).set_index("trade_date")
        filter_dates = (major_minor_df.index >= base_date) & (major_minor_df.index < stp_date)
        factor_df = major_minor_df.loc[filter_dates].copy()
        factor_df["instrument"] = instrument
        factor_df["n_" + price_type], factor_df["d_" + price_type] = zip(*factor_df.apply(find_price, args=(md_df,), axis=1))
        factor_df[factor_lbl] = factor_df.apply(cal_roll_return, args=("n_" + price_type, "d_" + price_type), axis=1)
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


def fac_exp_alg_ts_ma_and_diff(
        run_mode: str, bgn_date: str, stp_date: str | None, ts_window: int,
        instruments_universe: list[str],
        calendar_path: str,
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
):
    src_lbl = "TS"
    factor_lbl_ma, factor_lbl_diff = "TS_M{:03d}".format(ts_window), "TS_D{:03d}".format(ts_window)

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
    base_date = calendar.get_next_date(iter_dates[0], -ts_window + 1)
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
        res_df["ma"] = res_df["value"].rolling(window=ts_window).mean()
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
