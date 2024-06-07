import itertools as ittl
import datetime as dt
import numpy as np
import pandas as pd
import multiprocessing as mp
from rich.progress import Progress
from skyrim.whiterun import CCalendar, error_handler
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader
from skyrim.falkreath import CManagerLibWriter


class CSigFromFactor(object):
    def __init__(self, t_universe: list[str]):
        self.m_universe = t_universe
        self.m_u_size = len(self.m_universe)
        k = int(self.m_u_size / 2)
        wh = np.array([1] * k + [0] * (self.m_u_size - 2 * k) + [-1] * k)
        self.m_wh = wh / np.abs(wh).sum()

    def convert(self, xs: pd.Series, d: dict):
        d[xs.name] = pd.Series(data=self.m_wh, index=xs.sort_values(ascending=False).index)
        return 0


def moving_average(df: pd.DataFrame, row: str, col: str, val: str, mov_ave_win: int):
    _pivot_df = pd.pivot_table(data=df, index=row, columns=col, values=val)
    _res_df = _pivot_df.rolling(mov_ave_win).mean().stack().sort_index().reset_index()
    _res_df.rename(mapper={0: val}, axis=1, inplace=True)
    return _res_df


def fac_exp_MA(
        factor: str, mov_ave_win: int,
        run_mode: str, bgn_date: str, stp_date: str | None,
        universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        calendar_path: str,
):
    factor_ma = "{}-M{:03d}".format(factor, mov_ave_win)
    if stp_date is None:
        stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")

    signal = CSigFromFactor(universe)

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
    factor_dst_lib.initialize_table(t_table=factor_dst_lib_structure.m_tab,
                                    t_remove_existence=run_mode in ["O", "OVERWRITE"])

    src_df = factor_src_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])

    exp_df_by_date = pd.pivot_table(data=src_df, values="value", index="trade_date", columns="instrument")
    res = {}
    exp_df_by_date[universe].apply(signal.convert, axis=1, d=res)
    fac_sig_raw_df = pd.DataFrame.from_dict(res, orient="index")
    mov_ave_df = fac_sig_raw_df.rolling(window=mov_ave_win).mean()
    mov_ave_norm_df = mov_ave_df.div(mov_ave_df.abs().sum(axis=1), axis=0).fillna(0)
    update_df = mov_ave_norm_df.loc[mov_ave_norm_df.index >= bgn_date].stack().reset_index()
    factor_dst_lib.update(t_update_df=update_df, t_using_index=False)

    # --- close libs
    factor_src_lib.close()
    factor_dst_lib.close()
    return 0


def cal_fac_exp_MA_mp(
        proc_num: int,
        factor_lbls: list[str], mov_ave_wins: list[int],
        run_mode: str, bgn_date: str, stp_date: str | None,
        universe: list[str],
        database_structure: dict[str, CLib1Tab1],
        factors_exposure_dir: str,
        calendar_path: str,
):
    t0 = dt.datetime.now()
    with Progress() as pb:
        iter_args = list(ittl.product(factor_lbls, mov_ave_wins))
        main_task = pb.add_task(description=f"[INF] Calculating moving average of factors ...", total=len(iter_args))
        with mp.Pool(processes=proc_num) as pool:
            for factor_lbl, mov_ave_win in iter_args:
                pool.apply_async(
                    fac_exp_MA,
                    args=(factor_lbl, mov_ave_win,
                          run_mode, bgn_date, stp_date,
                          universe,
                          database_structure,
                          factors_exposure_dir,
                          calendar_path),
                    callback=lambda _: pb.update(main_task, advance=1),
                    error_callback=error_handler,
                )
            pool.close()
            pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
