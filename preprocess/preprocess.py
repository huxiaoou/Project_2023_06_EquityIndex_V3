import os
import sys
import json
import datetime as dt
import pandas as pd
from rich.progress import track
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CManagerLibReader, CTable
from skyrim.falkreath import CManagerLibWriter


def split_spot_daily_k(equity_index_by_instrument_dir: str, equity_indexes: tuple[tuple]):
    daily_k_file = "daily_k.xlsx"
    daily_k_path = os.path.join(equity_index_by_instrument_dir, daily_k_file)
    for equity_index_code, _ in equity_indexes:
        daily_k_df = pd.read_excel(daily_k_path, sheet_name=equity_index_code)
        daily_k_df["trade_date"] = daily_k_df["trade_date"].map(lambda z: z.strftime("%Y%m%d"))
        equity_index_file = "{}.csv".format(equity_index_code)
        equity_index_path = os.path.join(equity_index_by_instrument_dir, equity_index_file)
        daily_k_df.to_csv(equity_index_path, index=False, float_format="%.4f")
        print("...", equity_index_code, "saved as csv")
    return 0


def update_major_minute(
        run_mode: str, bgn_date: str, stp_date: str,
        instruments: list[str],
        calendar_path: str,
        futures_md_structure_path: str,
        futures_em01_db_name: str,
        futures_dir: str,
        by_instrument_dir: str,
        intermediary_dir: str,
        database_structure: dict,
):
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- majors manager
    major_minor_manager = {}
    major_minor_lib_reader = CManagerLibReader(by_instrument_dir, "major_minor.db")
    for instrument in instruments:
        major_minor_df = major_minor_lib_reader.read(
            t_value_columns=["trade_date", "n_contract", "d_contract"],
            t_using_default_table=False,
            t_table_name=instrument.replace(".", "_"),
        ).set_index("trade_date")
        major_minor_manager[instrument] = major_minor_df

    # --- init lib reader
    with open(futures_md_structure_path, "r") as j:
        em01_table_struct = json.load(j)[futures_em01_db_name]["CTable"]
    em01_table = CTable(t_table_struct=em01_table_struct)
    em01_lib = CManagerLibReader(t_db_save_dir=futures_dir, t_db_name=futures_em01_db_name)
    em01_lib.set_default(em01_table.m_table_name)
    em01_cols = list(em01_table.m_primary_keys) + list(em01_table.m_value_columns)

    # --- init lib writer
    em01_major_lib_structure = database_structure["em01_major"]  # make sure it has the columns as em01_lib
    em01_major_lib = CManagerLibWriter(
        t_db_name=em01_major_lib_structure.m_lib_name,
        t_db_save_dir=intermediary_dir
    )
    em01_major_lib.initialize_table(t_table=em01_major_lib_structure.m_tab,
                                    t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- main loop
    dfs_list = []
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    for trade_date in track(iter_dates, description="[INF] Getting major m01 ..."):
        theory_number_of_bars = 270 if trade_date < "20160101" else 240
        for instrument in instruments:
            try:
                major_contract = major_minor_manager[instrument].at[trade_date, "n_contract"]
            except KeyError:
                print("... Error!", instrument, "does not have major contract @ ", trade_date)
                print("... called by misc.update_major_minute")
                sys.exit()
            major_contract_m01_df = em01_lib.read_by_conditions(
                t_conditions=[
                    ("trade_date", "=", trade_date),
                    ("loc_id", "=", major_contract),
                ], t_value_columns=em01_cols)
            major_contract_m01_df = major_contract_m01_df.dropna(
                axis=0, how="all", subset=["open", "high", "low", "close"]
            )
            if (num_of_bars := len(major_contract_m01_df)) != theory_number_of_bars:
                print(
                    f"Error! Number of bars = {num_of_bars} @ {trade_date} for {instrument} - {major_contract}, theory = {theory_number_of_bars}"
                )
                sys.exit()
            dfs_list.append(major_contract_m01_df)

    update_df = pd.concat(dfs_list, axis=0, ignore_index=True)
    round_df = update_df.round(2)
    em01_major_lib.update(t_update_df=round_df)

    em01_lib.close()
    em01_major_lib.close()
    return 0


def update_public_info(
        value_type: str,
        run_mode: str, bgn_date: str, stp_date: str,
        instruments: list[str],
        calendar_path: str,
        futures_md_structure_path,
        futures_md_db_name,
        futures_dir: str,
        futures_by_date_dir,
        intermediary_dir: str,
        database_structure: dict,
):
    """

    :param value_type:  {"delta", "pos"}
    :param run_mode:
    :param bgn_date:
    :param stp_date:
    :param instruments:
    :param calendar_path:
    :param futures_md_structure_path:
    :param futures_md_db_name:
    :param futures_dir:
    :param futures_by_date_dir:
    :param intermediary_dir:
    :param database_structure:
    :return:
    """
    factor_lbl = {"pos": "hld_pos", "delta": "dlt_pos"}[value_type]
    instru_sub_ids = [_.split(".")[0] for _ in instruments]

    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- init lib reader
    with open(futures_md_structure_path, "r") as j:
        md_table_struct = json.load(j)[futures_md_db_name]["CTable"]
    md_table = CTable(t_table_struct=md_table_struct)
    md_db = CManagerLibReader(t_db_save_dir=futures_dir, t_db_name=futures_md_db_name)
    md_db.set_default(t_default_table_name=md_table.m_table_name)

    # w = [0.4, 0.3, 0.2, 0.2]
    w = [0.25, 0.25, 0.25, 0.25]
    dlt_dfs = []
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    for trade_date in track(iter_dates, description=f"[INF] Working Pub {value_type} ... "):
        raw_pos_file = "positions.E.{}.csv.gz".format(trade_date)
        raw_pos_path = os.path.join(futures_by_date_dir, trade_date[0:4], trade_date, raw_pos_file)
        raw_pos_df = pd.read_csv(raw_pos_path, dtype={"type": str})  # type:ignore
        pivot_pos_df = pd.pivot_table(
            data=raw_pos_df, index=["type", "member_chs"],
            columns=["instrument", "loc_id"], values=value_type
        )

        for instrument, instru_sub_id in zip(instruments, instru_sub_ids):
            instru_pub_info_df = md_db.read_by_conditions(t_conditions=[
                ("trade_date", "=", trade_date),
                ("instrument", "=", instru_sub_id),
            ], t_value_columns=["loc_id", "volume"]).set_index("loc_id")
            instru_pub_info_df.sort_values(by="volume", ascending=False, inplace=True)
            w_srs = pd.Series(data=w, index=instru_pub_info_df.index)

            lng_df_by_contract = pivot_pos_df.loc["2", instru_sub_id].dropna(axis=0, how="all").fillna(0)
            srt_df_by_contract = pivot_pos_df.loc["3", instru_sub_id].dropna(axis=0, how="all").fillna(0)
            raw_lng_wgt_srs = w_srs[lng_df_by_contract.columns]
            raw_srt_wgt_srs = w_srs[srt_df_by_contract.columns]
            lng_wgt_srs = raw_lng_wgt_srs / raw_lng_wgt_srs.sum()
            srt_wgt_srs = raw_srt_wgt_srs / raw_srt_wgt_srs.sum()

            td_instru_pos_df = pd.DataFrame({
                "lng": lng_df_by_contract @ lng_wgt_srs,
                "srt": srt_df_by_contract @ srt_wgt_srs,
            }).fillna(0).round(2)
            td_instru_pos_df["trade_date"] = trade_date
            td_instru_pos_df["instrument"] = instrument
            td_instru_pos_df.reset_index(inplace=True)
            td_instru_pos_df = td_instru_pos_df[["trade_date", "instrument", "member_chs", "lng", "srt"]]
            dlt_dfs.append(td_instru_pos_df)

    # --- reorganize
    all_factor_df = pd.concat(dlt_dfs, axis=0, ignore_index=True)

    # --- save
    factor_lib_structure = database_structure[factor_lbl]
    factor_lib = CManagerLibWriter(
        t_db_name=factor_lib_structure.m_lib_name,
        t_db_save_dir=intermediary_dir
    )
    factor_lib.initialize_table(t_table=factor_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])
    factor_lib.update(t_update_df=all_factor_df, t_using_index=False)
    factor_lib.close()

    md_db.close()
    print("... @ {} {} positions are calculated".format(dt.datetime.now(), value_type))
    return 0
