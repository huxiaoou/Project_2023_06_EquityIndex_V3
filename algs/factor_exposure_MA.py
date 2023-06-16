import itertools as ittl
import datetime as dt
import pandas as pd
import multiprocessing as mp
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


def moving_average(df: pd.DataFrame, row: str, col: str, val: str, mov_ave_win: int):
    _pivot_df = pd.pivot_table(data=df, index=row, columns=col, values=val)
    _res_df = _pivot_df.rolling(mov_ave_win).mean().stack().sort_index().reset_index()
    _res_df.rename(mapper={0: val}, axis=1, inplace=True)
    return _res_df


def fac_exp_MA(
        factor: str, mov_ave_win: int,
        run_mode: str, bgn_date: str, stp_date: str | None,
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        calendar_path: str,
):
    factor_ma = "{}-M{:03d}".format(factor, mov_ave_win)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    # --- load calendar
    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -mov_ave_win + 1)

    # --- init src lib
    factor_src_lib_structure = database_structure[factor]
    factor_src_lib = CManagerLibReader(
        t_db_name=factor_src_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_src_lib.set_default(t_default_table_name=factor_src_lib_structure.m_tab.m_table_name)

    # --- init dst lib
    factor_dst_lib_structure = database_structure[factor_ma]
    factor_dst_lib = CManagerLibWriter(
        t_db_name=factor_dst_lib_structure.m_lib_name,
        t_db_save_dir=factors_exposure_dir
    )
    factor_dst_lib.initialize_table(t_table=factor_dst_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    src_df = factor_src_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])

    mov_ave_df = moving_average(src_df, "trade_date", "instrument", "value", mov_ave_win)
    update_df = mov_ave_df.loc[mov_ave_df["trade_date"] >= bgn_date]
    factor_dst_lib.update(t_update_df=update_df, t_using_index=True)

    # --- close libs
    factor_src_lib.close()
    factor_dst_lib.close()
    return 0


def cal_gp_tests_mp(
        proc_num: int,
        factor_lbls: list[str], mov_ave_wins: list[int],
        run_mode: str, bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for factor_lbl, mov_ave_win in ittl.product(factor_lbls, mov_ave_wins):
        pool.apply_async(
            fac_exp_MA,
            args=(factor_lbl, mov_ave_win, run_mode, bgn_date, stp_date),
            kwds=kwargs
        )
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
