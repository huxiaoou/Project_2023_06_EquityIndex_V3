import datetime as dt
import numpy as np
import pandas as pd
from skyrim.whiterun import CCalendar, CInstrumentInfoTable
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def cal_smart(t_sub_df: pd.DataFrame, t_sort_var: str, t_lbd: float):
    tot_vwap = t_sub_df["vwap"] @ t_sub_df["amount"] / t_sub_df["amount"].sum()
    tot_ret = t_sub_df["m01_return_cls"] @ t_sub_df["amount"] / t_sub_df["amount"].sum()

    _sorted_df = t_sub_df.sort_values(by=t_sort_var, ascending=False)
    volume_threshold = _sorted_df["volume"].sum() * t_lbd
    n = sum(_sorted_df["volume"].cumsum() < volume_threshold) + 1
    smart_df = _sorted_df.head(n)
    if (amt_sum := smart_df["amount"].sum()) > 0:
        w = smart_df["amount"] / amt_sum
        smart_p = -smart_df["vwap"] @ w / tot_vwap + 1
        smart_r = -smart_df["m01_return_cls"] @ w + tot_ret
        return smart_p, smart_r
    else:
        print("... Warning! Sum of volume of smart df is ZERO")
        return 0, 0


def fac_exp_alg_smt(
        run_mode: str, bgn_date: str, stp_date: str | None,
        smt_window: int, lbd: float,
        instruments_universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        intermediary_dir: str,
        calendar_path: str,
        futures_instru_info_path: str,
        amount_scale: float,
):
    factor_p_lbl, factor_r_lbl = ["SMT{}{:03d}T{:02d}".format(_, smt_window, int(lbd * 10)) for _ in ["P", "R"]]
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)

    # --- load instru info table
    instru_info_table = CInstrumentInfoTable(t_path=futures_instru_info_path, t_index_label="windCode", t_type="CSV")

    # --- init em01_major reader
    em01_major_lib_structure = database_structure["em01_major"]  # make sure it has the columns as em01_lib
    em01_major_lib = CManagerLibReader(
        t_db_name=em01_major_lib_structure.m_lib_name,
        t_db_save_dir=intermediary_dir
    )
    em01_major_lib.set_default(t_default_table_name=em01_major_lib_structure.m_tab.m_table_name)

    # ---
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -smt_window + 1)

    # --- init major contracts
    all_factor_p_dfs, all_factor_r_dfs = [], []
    for instrument in instruments_universe:
        contract_multiplier = instru_info_table.get_multiplier(instrument)
        em01_df = em01_major_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", stp_date),
                ("instrument", "=", instrument.split(".")[0]),
            ], t_value_columns=["trade_date", "timestamp", "loc_id", "volume", "amount", "close", "preclose"]
        )
        em01_df[["close", "preclose"]] = em01_df[["close", "preclose"]].round(2)
        em01_df["vwap"] = (em01_df["amount"] / em01_df["volume"] / contract_multiplier * amount_scale).fillna(method="ffill")
        em01_df["m01_return_cls"] = (em01_df["close"] / em01_df["preclose"] - 1).replace(np.inf, 0)
        em01_df["smart_idx"] = em01_df[["m01_return_cls", "volume"]].apply(
            lambda z: np.abs(z["m01_return_cls"]) / np.log(z["volume"]) * 1e4 if z["volume"] > 1 else 0, axis=1)

        r_p_data, r_r_data = {}, {}
        for iter_end_date in iter_dates:
            iter_bgn_date = calendar.get_next_date(iter_end_date, -smt_window + 1)
            filter_dates = (em01_df["trade_date"] >= iter_bgn_date) & (em01_df["trade_date"] <= iter_end_date)
            sub_df = em01_df.loc[filter_dates]
            r_p_data[iter_end_date], r_r_data[iter_end_date] = cal_smart(
                t_sub_df=sub_df, t_sort_var="smart_idx", t_lbd=lbd)

        for _iter_data, _iter_dfs, _iter_factor_lbl in zip([r_p_data, r_r_data],
                                                           [all_factor_p_dfs, all_factor_r_dfs],
                                                           [factor_p_lbl, factor_r_lbl]):
            factor_df = pd.DataFrame({"instrument": instrument, _iter_factor_lbl: pd.Series(_iter_data)})
            _iter_dfs.append(factor_df[["instrument", _iter_factor_lbl]])

    for _iter_dfs, _iter_factor_lbl in zip([all_factor_p_dfs, all_factor_r_dfs],
                                           [factor_p_lbl, factor_r_lbl]):
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

        em01_major_lib.close()
        print("... @ {} factor = {:>12s} calculated".format(dt.datetime.now(), _iter_factor_lbl))
    return 0
