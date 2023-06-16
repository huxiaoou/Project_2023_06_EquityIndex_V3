import datetime as dt
import itertools as ittl
import multiprocessing as mp
from algs.factor_exposure_amp import fac_exp_alg_amp
from algs.factor_exposure_amt import fac_exp_alg_amt
from algs.factor_exposure_basis import fac_exp_alg_basis, fac_exp_alg_basis_ma_and_diff
from algs.factor_exposure_beta import fac_exp_alg_beta, fac_exp_alg_beta_diff
from algs.factor_exposure_cx import fac_exp_alg_cx
from algs.factor_exposure_exr import fac_exp_alg_exr
from algs.factor_exposure_mtm import fac_exp_alg_mtm
from algs.factor_exposure_pos import fac_exp_alg_pos
from algs.factor_exposure_sgm import fac_exp_alg_sgm
from algs.factor_exposure_size import fac_exp_alg_size
from algs.factor_exposure_skew import fac_exp_alg_skew
from algs.factor_exposure_smt import fac_exp_alg_smt
from algs.factor_exposure_to import fac_exp_alg_to
from algs.factor_exposure_ts import fac_exp_alg_ts, fac_exp_alg_ts_ma_and_diff
from algs.factor_exposure_twc import fac_exp_alg_twc


def cal_fac_exp_amp_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       amp_windows: list[int], lbds: list[float],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       mapper_fut_to_idx: dict[str, str],
                       equity_index_by_instrument_dir: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window, lbd in ittl.product(amp_windows, lbds):
        pool.apply_async(fac_exp_alg_amp,
                         args=(run_mode, bgn_date, stp_date,
                               p_window, lbd,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir,
                               mapper_fut_to_idx,
                               equity_index_by_instrument_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_amt_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       amt_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       money_scale: int):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in amt_windows:
        pool.apply_async(fac_exp_alg_amt,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir,
                               money_scale))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_basis_mp(proc_num: int,
                         run_mode: str, bgn_date: str, stp_date: str | None,
                         basis_windows: list[int],
                         instruments_universe: list[str],
                         database_structure: dict,
                         major_return_dir: str,
                         equity_index_by_instrument_dir: str,
                         factors_exposure_dir: str,
                         calendar_path: str):
    t0 = dt.datetime.now()
    fac_exp_alg_basis(run_mode, bgn_date, stp_date, max(basis_windows),
                      instruments_universe,
                      calendar_path,
                      database_structure,
                      major_return_dir,
                      equity_index_by_instrument_dir,
                      factors_exposure_dir)
    pool = mp.Pool(processes=proc_num)
    for p_window in basis_windows:
        pool.apply_async(fac_exp_alg_basis_ma_and_diff,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_beta_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str | None,
                        beta_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        major_return_dir: str,
                        equity_index_by_instrument_dir: str,
                        factors_exposure_dir: str,
                        calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in beta_windows:
        pool.apply_async(fac_exp_alg_beta,
                         args=(run_mode, bgn_date, stp_date, p_window, max(beta_windows),
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               major_return_dir,
                               equity_index_by_instrument_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    pool = mp.Pool(processes=proc_num)
    for p_window in beta_windows[1:]:
        pool.apply_async(fac_exp_alg_beta_diff,
                         args=(run_mode, bgn_date, stp_date, p_window, beta_windows[0],
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_cx_mp(proc_num: int,
                      run_mode: str, bgn_date: str, stp_date: str | None,
                      mgr_cx_windows: dict[str, list[int]], top_props: list[float],
                      instruments_universe: list[str],
                      database_structure: dict,
                      major_return_dir: str,
                      factors_exposure_dir: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for cx, cx_windows in mgr_cx_windows.items():
        for cx_window, top_prop in ittl.product(cx_windows, top_props):
            pool.apply_async(fac_exp_alg_cx,
                             args=(run_mode, bgn_date, stp_date,
                                   cx, cx_window, top_prop,
                                   instruments_universe,
                                   database_structure,
                                   major_return_dir,
                                   factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_exr_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       exr_windows: list[int], drifts: list[float],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       intermediary_dir: str,
                       calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in exr_windows:
        pool.apply_async(fac_exp_alg_exr,
                         args=(run_mode, bgn_date, stp_date,
                               p_window, drifts,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               intermediary_dir,
                               calendar_path))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_mtm_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       mtm_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window, tag_adj in ittl.product(mtm_windows, (False, True)):
        pool.apply_async(fac_exp_alg_mtm,
                         args=(run_mode, bgn_date, stp_date, p_window, tag_adj,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_pos_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       pos_windows: list[int], top_players_qty: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       test_returns_dir: str,
                       intermediary_dir: str,
                       calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window, top_player_qty in ittl.product(pos_windows, top_players_qty):
        pool.apply_async(fac_exp_alg_pos,
                         args=(run_mode, bgn_date, stp_date,
                               p_window, top_player_qty,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               test_returns_dir,
                               intermediary_dir,
                               calendar_path))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_sgm_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       sgm_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       major_return_dir: str,
                       factors_exposure_dir: str,
                       ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in sgm_windows:
        pool.apply_async(fac_exp_alg_sgm,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_size_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str | None,
                        size_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        major_return_dir: str,
                        factors_exposure_dir: str,
                        ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in size_windows:
        pool.apply_async(fac_exp_alg_size,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_skew_mp(proc_num: int,
                        run_mode: str, bgn_date: str, stp_date: str | None,
                        skew_windows: list[int],
                        instruments_universe: list[str],
                        database_structure: dict,
                        major_return_dir: str,
                        factors_exposure_dir: str,
                        ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in skew_windows:
        pool.apply_async(fac_exp_alg_skew,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_smt_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       smt_windows: list[int], lbds: list[float],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       intermediary_dir: str,
                       calendar_path: str,
                       futures_instru_info_path: str,
                       amount_scale: float):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window, lbd in ittl.product(smt_windows, lbds):
        pool.apply_async(fac_exp_alg_smt,
                         args=(run_mode, bgn_date, stp_date,
                               p_window, lbd,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               intermediary_dir,
                               calendar_path,
                               futures_instru_info_path,
                               amount_scale))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_to_mp(proc_num: int,
                      run_mode: str, bgn_date: str, stp_date: str | None,
                      to_windows: list[int],
                      instruments_universe: list[str],
                      database_structure: dict,
                      major_return_dir: str,
                      factors_exposure_dir: str,
                      ):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in to_windows:
        pool.apply_async(fac_exp_alg_to,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               database_structure,
                               major_return_dir,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_ts_mp(proc_num: int,
                      run_mode: str, bgn_date: str, stp_date: str | None,
                      ts_windows: list[int],
                      instruments_universe: list[str],
                      database_structure: dict,
                      major_minor_dir: str,
                      md_dir: str,
                      factors_exposure_dir: str,
                      calendar_path: str,
                      price_type: str):
    t0 = dt.datetime.now()
    fac_exp_alg_ts(run_mode, bgn_date, stp_date, max(ts_windows),
                   instruments_universe,
                   calendar_path,
                   database_structure,
                   major_minor_dir,
                   md_dir,
                   factors_exposure_dir,
                   price_type)
    pool = mp.Pool(processes=proc_num)
    for p_window in ts_windows:
        pool.apply_async(fac_exp_alg_ts_ma_and_diff,
                         args=(run_mode, bgn_date, stp_date, p_window,
                               instruments_universe,
                               calendar_path,
                               database_structure,
                               factors_exposure_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_fac_exp_twc_mp(proc_num: int,
                       run_mode: str, bgn_date: str, stp_date: str | None,
                       twc_windows: list[int],
                       instruments_universe: list[str],
                       database_structure: dict,
                       factors_exposure_dir: str,
                       intermediary_dir: str,
                       calendar_path: str):
    t0 = dt.datetime.now()
    pool = mp.Pool(processes=proc_num)
    for p_window in twc_windows:
        pool.apply_async(fac_exp_alg_twc,
                         args=(run_mode, bgn_date, stp_date,
                               p_window,
                               instruments_universe,
                               database_structure,
                               factors_exposure_dir,
                               intermediary_dir,
                               calendar_path))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
