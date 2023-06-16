import datetime as dt
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def fac_exp_alg_pos(
        run_mode: str, bgn_date: str, stp_date: str | None,
        pos_window: int, top_player_qty: int,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        test_returns_dir: str,
        intermediary_dir: str,
        calendar_path: str,
):
    factor_hl_lbl, factor_hs_lbl = ["POSH{}{:03d}Q{:02d}".format(d, pos_window, top_player_qty) for d in list("LS")]
    factor_dl_lbl, factor_ds_lbl = ["POSD{}{:03d}Q{:02d}".format(d, pos_window, top_player_qty) for d in list("LS")]
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- load test return
    test_window = pos_window
    test_return_lib_id = "test_return_{:03d}".format(test_window)
    test_return_lib_struct = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_struct.m_lib_name, t_db_save_dir=test_returns_dir)
    test_return_lib.set_default(t_default_table_name=test_return_lib_struct.m_tab.m_table_name)

    # --- load hold pos
    hld_pos_lib_structure = database_structure["hld_pos"]
    hld_pos_lib = CManagerLibReader(t_db_name=hld_pos_lib_structure.m_lib_name, t_db_save_dir=intermediary_dir)
    hld_pos_lib.set_default(t_default_table_name=hld_pos_lib_structure.m_tab.m_table_name)

    # --- load hold pos
    dlt_pos_lib_structure = database_structure["dlt_pos"]
    dlt_pos_lib = CManagerLibReader(t_db_name=dlt_pos_lib_structure.m_lib_name, t_db_save_dir=intermediary_dir)
    dlt_pos_lib.set_default(t_default_table_name=dlt_pos_lib_structure.m_tab.m_table_name)

    # ---
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    model_iter_dates = calendar.get_iter_list(
        calendar.get_next_date(iter_dates[0], -pos_window - 1),
        calendar.get_next_date(iter_dates[-1], -pos_window),
        True
    )
    base_date = calendar.get_next_date(iter_dates[0], -pos_window * 2)

    # --- load test return
    test_return_df = test_return_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"]).set_index(["trade_date", "instrument"])

    # --- init major contracts
    all_factor_hl_dfs, all_factor_hs_dfs = [], []
    all_factor_dl_dfs, all_factor_ds_dfs = [], []
    for instrument in instruments_universe:
        hld_pos_df = hld_pos_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument),
            ], t_value_columns=["trade_date", "instrument", "institute", "lng", "srt"]
        )
        dlt_pos_df = dlt_pos_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument),
            ], t_value_columns=["trade_date", "instrument", "institute", "lng", "srt"]
        )

        hl_df = pd.pivot_table(data=hld_pos_df, index="trade_date", columns="institute", values="lng")
        hs_df = pd.pivot_table(data=hld_pos_df, index="trade_date", columns="institute", values="srt")
        dl_df = pd.pivot_table(data=dlt_pos_df, index="trade_date", columns="institute", values="lng")
        ds_df = pd.pivot_table(data=dlt_pos_df, index="trade_date", columns="institute", values="srt")

        hl_rolling_df = hl_df.fillna(method="ffill", limit=3).rolling(window=pos_window).mean()
        hs_rolling_df = hs_df.fillna(method="ffill", limit=3).rolling(window=pos_window).mean()
        dl_rolling_df = dl_df.fillna(0).rolling(window=pos_window).mean()
        ds_rolling_df = ds_df.fillna(0).rolling(window=pos_window).mean()

        hl_nan_indicator = hl_df.isnull().rolling(window=pos_window).sum() < pos_window  # at least one day in pos window is available
        hs_nan_indicator = hs_df.isnull().rolling(window=pos_window).sum() < pos_window  # at least one day in pos window is available
        dl_nan_indicator = dl_df.isnull().rolling(window=pos_window).sum() < pos_window  # at least one day in pos window is available
        ds_nan_indicator = ds_df.isnull().rolling(window=pos_window).sum() < pos_window  # at least one day in pos window is available

        r_hl_data, r_hs_data, r_dl_data, r_ds_data = {}, {}, {}, {}
        for model_date, trade_date in zip(model_iter_dates, iter_dates):
            test_return = test_return_df.at[(trade_date, instrument), "value"]
            if test_return > 0:
                hl_smart_players = hl_rolling_df.loc[model_date, hl_nan_indicator.loc[model_date]].sort_values(ascending=False).head(top_player_qty).index
                hs_smart_players = hs_rolling_df.loc[model_date, hs_nan_indicator.loc[model_date]].sort_values(ascending=False).tail(top_player_qty).index
                dl_smart_players = dl_rolling_df.loc[model_date, dl_nan_indicator.loc[model_date]].sort_values(ascending=False).head(top_player_qty).index
                ds_smart_players = ds_rolling_df.loc[model_date, ds_nan_indicator.loc[model_date]].sort_values(ascending=False).tail(top_player_qty).index
            else:
                hl_smart_players = hl_rolling_df.loc[model_date, hl_nan_indicator.loc[model_date]].sort_values(ascending=False).tail(top_player_qty).index
                hs_smart_players = hs_rolling_df.loc[model_date, hs_nan_indicator.loc[model_date]].sort_values(ascending=False).head(top_player_qty).index
                dl_smart_players = dl_rolling_df.loc[model_date, dl_nan_indicator.loc[model_date]].sort_values(ascending=False).tail(top_player_qty).index
                ds_smart_players = ds_rolling_df.loc[model_date, ds_nan_indicator.loc[model_date]].sort_values(ascending=False).head(top_player_qty).index
            hl_prediction = hl_rolling_df.loc[trade_date, hl_smart_players].mean()
            hs_prediction = hs_rolling_df.loc[trade_date, hs_smart_players].mean()
            dl_prediction = -dl_rolling_df.loc[trade_date, dl_smart_players].mean()
            ds_prediction = -ds_rolling_df.loc[trade_date, ds_smart_players].mean()
            r_hl_data[trade_date], r_hs_data[trade_date] = hl_prediction, hs_prediction
            r_dl_data[trade_date], r_ds_data[trade_date] = dl_prediction, ds_prediction

        for _iter_data, _iter_dfs, _iter_factor_lbl in zip([r_hl_data, r_hs_data, r_dl_data, r_ds_data],
                                                           [all_factor_hl_dfs, all_factor_hs_dfs, all_factor_dl_dfs, all_factor_ds_dfs],
                                                           [factor_hl_lbl, factor_hs_lbl, factor_dl_lbl, factor_ds_lbl]):
            factor_df = pd.DataFrame({"instrument": instrument, _iter_factor_lbl: pd.Series(_iter_data)})
            _iter_dfs.append(factor_df[["instrument", _iter_factor_lbl]])

    for _iter_dfs, _iter_factor_lbl in zip([all_factor_hl_dfs, all_factor_hs_dfs, all_factor_dl_dfs, all_factor_ds_dfs],
                                           [factor_hl_lbl, factor_hs_lbl, factor_dl_lbl, factor_ds_lbl]):
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

    test_return_lib.close()
    hld_pos_lib.close()
    return 0
