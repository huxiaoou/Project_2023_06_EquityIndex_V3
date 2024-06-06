import datetime as dt
import multiprocessing as mp
import pandas as pd
from skyrim.whiterun import CCalendar, error_handler
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def drop_values_from_series(s: pd.Series, v: float | str = 0):
    return s[s != v]


def fac_exp_alg_pos(
        run_mode: str, bgn_date: str, stp_date: str | None,
        top_player_qty: int,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        test_returns_dir: str,
        intermediary_dir: str,
        calendar_path: str,
):
    factor_hl_lbl, factor_hs_lbl = ["POSH{}Q{:02d}".format(d, top_player_qty) for d in list("LS")]
    factor_dl_lbl, factor_ds_lbl = ["POSD{}Q{:02d}".format(d, top_player_qty) for d in list("LS")]
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -1)
    model_iter_dates = calendar.get_iter_list(base_date, iter_dates[-1], True)

    # --- load test return
    test_return_lib_id = "test_return_c"
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

        hl_nan_indicator = ~hl_df.isnull()  # at least one day in pos window is available
        hs_nan_indicator = ~hs_df.isnull()  # at least one day in pos window is available
        dl_nan_indicator = ~dl_df.isnull()  # at least one day in pos window is available
        ds_nan_indicator = ~ds_df.isnull()  # at least one day in pos window is available

        r_hl_data, r_hs_data, r_dl_data, r_ds_data = {}, {}, {}, {}
        for model_date, trade_date in zip(model_iter_dates, iter_dates):
            test_return = test_return_df.at[(trade_date, instrument), "value"]
            if test_return > 0:
                try:
                    hl_smart_players = drop_values_from_series(
                        hl_df.loc[model_date, hl_nan_indicator.loc[model_date]]).sort_values(ascending=False).head(
                        top_player_qty).index
                    hs_smart_players = drop_values_from_series(
                        hs_df.loc[model_date, hs_nan_indicator.loc[model_date]]).sort_values(ascending=False).tail(
                        top_player_qty).index
                    dl_smart_players = dl_df.loc[model_date, dl_nan_indicator.loc[model_date]].sort_values(
                        ascending=False).head(top_player_qty).index
                    ds_smart_players = ds_df.loc[model_date, ds_nan_indicator.loc[model_date]].sort_values(
                        ascending=False).tail(top_player_qty).index
                except KeyError:
                    hl_smart_players, hs_smart_players, dl_smart_players, ds_smart_players = [], [], [], []
                    print("... {} @ {} top = {} position data are not found".format(instrument, model_date,
                                                                                    top_player_qty))
            elif test_return < 0:
                try:
                    hl_smart_players = hl_df.loc[model_date, hl_nan_indicator.loc[model_date]].sort_values(
                        ascending=False).tail(top_player_qty).index
                    hs_smart_players = hs_df.loc[model_date, hs_nan_indicator.loc[model_date]].sort_values(
                        ascending=False).head(top_player_qty).index
                    dl_smart_players = dl_df.loc[model_date, dl_nan_indicator.loc[model_date]].sort_values(
                        ascending=False).tail(top_player_qty).index
                    ds_smart_players = ds_df.loc[model_date, ds_nan_indicator.loc[model_date]].sort_values(
                        ascending=False).head(top_player_qty).index
                except KeyError:
                    hl_smart_players, hs_smart_players, dl_smart_players, ds_smart_players = [], [], [], []
                    print("... {} @ {} top = {} position data are not found".format(instrument, model_date,
                                                                                    top_player_qty))
            else:
                hl_smart_players, hs_smart_players, dl_smart_players, ds_smart_players = [], [], [], []
            hl_prediction = hl_df.loc[trade_date, hl_smart_players].mean()
            hs_prediction = hs_df.loc[trade_date, hs_smart_players].mean()
            dl_prediction = dl_df.loc[trade_date, dl_smart_players].mean()
            ds_prediction = ds_df.loc[trade_date, ds_smart_players].mean()
            r_hl_data[trade_date], r_hs_data[trade_date] = hl_prediction, hs_prediction
            r_dl_data[trade_date], r_ds_data[trade_date] = -dl_prediction, -ds_prediction

        for _iter_data, _iter_dfs, _iter_factor_lbl in zip([r_hl_data, r_hs_data, r_dl_data, r_ds_data],
                                                           [all_factor_hl_dfs, all_factor_hs_dfs, all_factor_dl_dfs,
                                                            all_factor_ds_dfs],
                                                           [factor_hl_lbl, factor_hs_lbl, factor_dl_lbl,
                                                            factor_ds_lbl]):
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
        factor_lib.initialize_table(t_table=factor_lib_structure.m_tab,
                                    t_remove_existence=run_mode in ["O", "OVERWRITE"])
        factor_lib.update(t_update_df=all_factor_df, t_using_index=True)
        factor_lib.close()

        print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), _iter_factor_lbl))

    test_return_lib.close()
    hld_pos_lib.close()
    return 0


def cal_fac_exp_pos_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       top_players_qty: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       test_returns_dir: str,
                       intermediary_dir: str,
                       calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for top_player_qty in top_players_qty:
        pool.apply_async(fac_exp_alg_pos,
                         args=(run_mode, bgn_date, stp_date,
                               top_player_qty,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               test_returns_dir,
                               intermediary_dir,
                               calendar_path),
                         error_callback=error_handler,
                         )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
