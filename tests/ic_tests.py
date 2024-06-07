import datetime as dt
import pandas as pd
import multiprocessing as mp
from rich.progress import Progress
from skyrim.falkreath import CLib1Tab1
from skyrim.falkreath import CManagerLibReader, CManagerLibWriter
from skyrim.whiterun import CCalendar, error_handler


def cal_corr_by_date(df: pd.DataFrame, fe: str, tr: str):
    _rp = df[[fe, tr]].corr(method="pearson").at[fe, tr]
    _rs = df[[fe, tr]].corr(method="spearman").at[fe, tr]
    return _rp, _rs


def shift_fac_exp(df: pd.DataFrame, row: str, col: str, val: str, shift_win: int):
    _pivot_df = pd.pivot_table(data=df, index=row, columns=col, values=val)
    _res_df = _pivot_df.shift(shift_win).stack().sort_index().reset_index()
    _res_df.rename(mapper={0: val}, axis=1, inplace=True)
    return _res_df


def ic_test_single_factor(
        factor_ma: str,
        run_mode: str, bgn_date: str, stp_date: str,
        tests_result_dir: str,
        factors_exposure_dir: str,
        test_returns_dir: str,
        database_structure: dict[str, CLib1Tab1],
        calendar_path: str):
    _test_window = 1

    # --- load calendar
    calendar = CCalendar(calendar_path)
    iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
    base_date = calendar.get_next_date(iter_dates[0], -_test_window - 1)

    # --- tests lib
    test_lib_id = "ic-{}".format(factor_ma)
    test_lib_structure = database_structure[test_lib_id]
    test_lib = CManagerLibWriter(t_db_save_dir=tests_result_dir, t_db_name=test_lib_structure.m_lib_name)
    test_lib.initialize_table(t_table=test_lib_structure.m_tab, t_remove_existence=run_mode in ["O", "OVERWRITE"])

    # --- factor library
    factor_lib_structure = database_structure[factor_ma]
    factor_lib = CManagerLibReader(t_db_save_dir=factors_exposure_dir, t_db_name=factor_lib_structure.m_lib_name)
    factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)

    # --- test return library
    test_return_lib_id = "test_return_o"
    test_return_lib_structure = database_structure[test_return_lib_id]
    test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_returns_dir)
    test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)

    fac_exp_df = factor_lib.read_by_conditions(t_conditions=[
        ("trade_date", ">=", base_date),
        ("trade_date", "<", stp_date),
    ], t_value_columns=["trade_date", "instrument", "value"])
    fac_exp_df_shift = shift_fac_exp(
        fac_exp_df, row="trade_date", col="instrument", val="value", shift_win=_test_window + 1)

    test_return_df = test_return_lib.read_by_conditions(
        t_conditions=[
            ("trade_date", ">=", bgn_date),
            ("trade_date", "<", stp_date),
        ], t_value_columns=["trade_date", "instrument", "value"]
    )

    test_input_df = pd.merge(
        left=fac_exp_df_shift, right=test_return_df,
        on=["trade_date", "instrument"], suffixes=("_e", "_r"),
        how="right"
    ).set_index("instrument")
    res_srs = test_input_df.groupby(by="trade_date").apply(
        cal_corr_by_date, fe="value_e", tr="value_r")
    pr_srs, sr_srs = zip(*res_srs)
    test_res_df = pd.DataFrame({
        "pearson": pr_srs,
        "spearman": sr_srs,
    }, index=res_srs.index)
    test_lib.update(t_update_df=test_res_df, t_using_index=True)
    test_lib.close()
    factor_lib.close()
    test_return_lib.close()
    return 0


def cal_ic_tests_mp(
        proc_num: int,
        factors_ma: list[str],
        run_mode: str, bgn_date: str, stp_date: str | None,
        **kwargs
):
    t0 = dt.datetime.now()
    with Progress() as pb:
        main_task = pb.add_task(description=f"[INF] Calculating ic ...", total=len(factors_ma))
        pool = mp.Pool(processes=proc_num)
        for factor_ma in factors_ma:
            pool.apply_async(
                ic_test_single_factor,
                args=(factor_ma, run_mode, bgn_date, stp_date),
                kwds=kwargs,
                callback=lambda _: pb.update(main_task, advance=1),
                error_callback=error_handler,
            )
        pool.close()
        pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
