import argparse
from struct_lib import database_structure

# from project_config import equity_indexes, mapper_futures_to_index
# from project_config import instruments_universe
# from project_config import factors, factors_ma, factors_args, test_return_types, factor_mov_ave_wins
# from project_config import manager_cx_windows
# from project_config import cost_rate

# from project_setup import calendar_path, futures_instru_info_path
# from project_setup import major_minor_dir, major_return_dir, md_by_instru_dir
# from project_setup import futures_md_dir, futures_md_structure_path
# from project_setup import futures_md_db_name, futures_em01_db_name
# from project_setup import futures_fundamental_intermediary_dir
#
# from project_setup import research_test_returns_dir, research_factors_exposure_dir
# from project_setup import research_intermediary_dir, research_signals_dir, research_simulations_dir, \
#     research_simulations_summary_dir
# from project_setup import research_ic_tests_dir, research_ic_tests_summary_dir
# from project_setup import research_gp_tests_dir, research_gp_tests_summary_dir
#
# from preprocess.preprocess import split_spot_daily_k, update_major_minute, update_public_info
# from test_returns.test_returns import cal_test_returns_mp
# from algs.factor_exposure_amp import cal_fac_exp_amp_mp
# from algs.factor_exposure_amt import cal_fac_exp_amt_mp
# from algs.factor_exposure_basis import cal_fac_exp_basis_mp
# from algs.factor_exposure_beta import cal_fac_exp_beta_mp
# from algs.factor_exposure_cx import cal_fac_exp_cx_mp
# from algs.factor_exposure_exr import cal_fac_exp_exr_mp
# from algs.factor_exposure_mtm import cal_fac_exp_mtm_mp
# from algs.factor_exposure_pos import cal_fac_exp_pos_mp
# from algs.factor_exposure_rng import cal_fac_exp_rng_mp
# from algs.factor_exposure_sgm import cal_fac_exp_sgm_mp
# from algs.factor_exposure_size import cal_fac_exp_size_mp
# from algs.factor_exposure_skew import cal_fac_exp_skew_mp
# from algs.factor_exposure_smt import cal_fac_exp_smt_mp
# from algs.factor_exposure_to import cal_fac_exp_to_mp
# from algs.factor_exposure_ts import cal_fac_exp_ts_mp
# from algs.factor_exposure_twc import cal_fac_exp_twc_mp
# from algs.factor_exposure_MA import cal_fac_exp_MA_mp
#
# from tests.ic_tests import cal_ic_tests_mp
# from tests.ic_tests_summary import cal_ic_tests_summary_mp
# from tests.gp_tests import cal_gp_tests_mp
# from tests.gp_tests_summary import cal_gp_tests_summary_mp
# from tests.gp_tests_corr import cal_gp_tests_corr
from signals.signals import cal_signals_mp, cal_simulations_mp, cal_simulations_summary

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser(description="Entry point of this project")
    args_parser.add_argument("--switch", type=str, help="""
        use this to decide which parts to run, available options = {'preprocess', 'test_returns', 'factors_exposure'}
        """)
    args_parser.add_argument("--factor", type=str, default="", help="""
        optional, must be provided if switch = {'preprocess', 'factors_exposure'},
        use this to decide which factor, available options = {
        'amp', 'amt', 'basis', 'beta', 'cx', 'exr', 'mtm', 'pos', 'sgm', 'size', 'skew', 'smt', 'to', 'ts', 'twc'}
        """)
    args_parser.add_argument("--mode", type=str, choices=("o", "a"), help="""
        run mode, available options = {'o', 'overwrite', 'a', 'append'}
        """)
    args_parser.add_argument("--bgn", type=str, help="""
        begin date, may be different according to different switches, suggestion of different switch:
        {   
            "preprocess/m01": "20150416",
            "preprocess/pub": "20150416",
            "test_returns": "20150416",
            "factor_exposures": "20150416", 
            "factor_exposures_moving_average": "20160615", 
            "tests": "20160701", 
            "tests_summary": "20160701", 
            "signals": "20160627", 
            "simu": "20160701", 
            "simusum": "20160701",  # not necessary indeed
        }
        """)
    args_parser.add_argument("--stp", type=str, help="""
        stop date, not included, usually it would be the day after the last trade date, such as
        "20230619" if last trade date is "20230616"  
        """)
    args_parser.add_argument("-p", "--process", type=int, default=5, help="""
        number of process to be called when calculating, default = 5
        """)
    args = args_parser.parse_args()
    switch = args.switch.upper()
    factor = args.factor.lower()
    run_mode = args.mode.upper() if args.mode else args.mode
    bgn_date, stp_date = args.bgn, args.stp
    proc_num = args.process

    if switch in ["PREPROCESS"]:
        if factor == "split":
            from preprocess.preprocess import split_spot_daily_k
            from project_setup import equity_index_by_instrument_dir
            from project_config import equity_indexes

            split_spot_daily_k(equity_index_by_instrument_dir, equity_indexes)
        elif factor == "m01":
            from preprocess.preprocess import update_major_minute
            from project_setup import (calendar_path, futures_dir,
                                       futures_md_structure_path, futures_em01_db_name,
                                       futures_by_instru_dir)
            from project_setup import research_intermediary_dir
            from project_config import instruments_universe

            update_major_minute(run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                                instruments=instruments_universe, calendar_path=calendar_path,
                                futures_md_structure_path=futures_md_structure_path,
                                futures_em01_db_name=futures_em01_db_name,
                                futures_dir=futures_dir,
                                by_instrument_dir=futures_by_instru_dir,
                                intermediary_dir=research_intermediary_dir,
                                database_structure=database_structure)
        elif factor == "pub":
            from preprocess.preprocess import update_public_info
            from project_setup import (calendar_path, futures_dir,
                                       futures_md_structure_path, futures_md_db_name,
                                       futures_by_date_dir)
            from project_setup import research_intermediary_dir
            from project_config import instruments_universe

            for value_type in ["pos", "delta"]:
                update_public_info(value_type=value_type,
                                   run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                                   instruments=instruments_universe,
                                   calendar_path=calendar_path,
                                   futures_md_structure_path=futures_md_structure_path,
                                   futures_md_db_name=futures_md_db_name,
                                   futures_dir=futures_dir,
                                   futures_by_date_dir=futures_by_date_dir,
                                   intermediary_dir=research_intermediary_dir,
                                   database_structure=database_structure
                                   )
    elif switch in ["TEST_RETURNS"]:
        from project_setup import (research_test_returns_dir, calendar_path, futures_dir, futures_by_instru_dir,
                                   futures_md_structure_path, futures_em01_db_name)
        from project_config import instruments_universe, test_return_types
        from test_returns.test_returns import cal_test_returns_mp

        cal_test_returns_mp(
            proc_num=proc_num,
            test_return_types=test_return_types,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            instruments_universe=instruments_universe,
            database_structure=database_structure,
            test_returns_dir=research_test_returns_dir,
            by_instrument_dir=futures_by_instru_dir,
            futures_dir=futures_dir,
            calendar_path=calendar_path,
            futures_md_structure_path=futures_md_structure_path,
            futures_em01_db_name=futures_em01_db_name,
        )
    # elif switch in ["FE", "FACTORS_EXPOSURE"]:
    #     if factor == "amp":
    #         cal_fac_exp_amp_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             amp_windows=factors_args["amp_windows"], lbds=factors_args["lbds"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             mapper_fut_to_idx=mapper_futures_to_index,
    #             equity_index_by_instrument_dir=equity_index_by_instrument_dir
    #         )
    #     elif factor == "amt":
    #         cal_fac_exp_amt_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             amt_windows=factors_args["amt_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             money_scale=10000
    #         )
    #     elif factor == "basis":
    #         cal_fac_exp_basis_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             basis_windows=factors_args["basis_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             equity_index_by_instrument_dir=equity_index_by_instrument_dir,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             calendar_path=calendar_path,
    #         )
    #     elif factor == "beta":
    #         cal_fac_exp_beta_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             beta_windows=factors_args["beta_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             equity_index_by_instrument_dir=equity_index_by_instrument_dir,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             calendar_path=calendar_path,
    #         )
    #     elif factor == "cx":
    #         cal_fac_exp_cx_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             mgr_cx_windows=manager_cx_windows, top_props=factors_args["top_props"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir
    #         )
    #     elif factor == "exr":
    #         cal_fac_exp_exr_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             exr_windows=factors_args["exr_windows"], drifts=factors_args["drifts"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             intermediary_dir=research_intermediary_dir,
    #             calendar_path=calendar_path,
    #         )
    #     elif factor == "mtm":
    #         cal_fac_exp_mtm_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             mtm_windows=factors_args["mtm_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #         )
    #     elif factor == "pos":
    #         cal_fac_exp_pos_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             top_players_qty=factors_args["top_players_qty"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             test_returns_dir=research_test_returns_dir,
    #             intermediary_dir=research_intermediary_dir,
    #             calendar_path=calendar_path,
    #         )
    #     elif factor == "rng":
    #         cal_fac_exp_rng_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             rng_windows=factors_args["rng_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir
    #         )
    #     elif factor == "sgm":
    #         cal_fac_exp_sgm_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             sgm_windows=factors_args["sgm_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir
    #         )
    #     elif factor == "size":
    #         cal_fac_exp_size_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             size_windows=factors_args["size_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir
    #         )
    #     elif factor == "skew":
    #         cal_fac_exp_skew_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             skew_windows=factors_args["skew_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir
    #         )
    #     elif factor == "smt":
    #         cal_fac_exp_smt_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             smt_windows=factors_args["smt_windows"], lbds=factors_args["lbds"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             intermediary_dir=research_intermediary_dir,
    #             calendar_path=calendar_path,
    #             futures_instru_info_path=futures_instru_info_path,
    #             amount_scale=1e4
    #         )
    #     elif factor == "to":
    #         cal_fac_exp_to_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             to_windows=factors_args["to_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_return_dir=major_return_dir,
    #             factors_exposure_dir=research_factors_exposure_dir
    #         )
    #     elif factor == "ts":
    #         cal_fac_exp_ts_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             ts_windows=factors_args["ts_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             major_minor_dir=major_minor_dir,
    #             md_dir=md_by_instru_dir,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             calendar_path=calendar_path,
    #             price_type="close",
    #         )
    #     elif factor == "twc":
    #         cal_fac_exp_twc_mp(
    #             proc_num=proc_num,
    #             run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #             twc_windows=factors_args["twc_windows"],
    #             instruments_universe=instruments_universe,
    #             database_structure=database_structure,
    #             factors_exposure_dir=research_factors_exposure_dir,
    #             intermediary_dir=research_intermediary_dir,
    #             calendar_path=calendar_path,
    #         )
    #     else:
    #         print("... factor = {} is not a legal option, please check again".format(factor))
    # elif switch in ["FEMA", "FACTORS_EXPOSURE_MOVING_AVERAGE"]:
    #     cal_fac_exp_MA_mp(
    #         proc_num=proc_num,
    #         factor_lbls=factors, mov_ave_wins=factor_mov_ave_wins,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         universe=instruments_universe,
    #         database_structure=database_structure,
    #         factors_exposure_dir=research_factors_exposure_dir,
    #         calendar_path=calendar_path)
    # elif switch in ["IC"]:
    #     cal_ic_tests_mp(
    #         proc_num=5,
    #         factors_ma=factors_ma,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         tests_result_dir=research_ic_tests_dir,
    #         factors_exposure_dir=research_factors_exposure_dir,
    #         test_returns_dir=research_test_returns_dir,
    #         database_structure=database_structure,
    #         calendar_path=calendar_path)
    # elif switch in ["ICSUM"]:
    #     cal_ic_tests_summary_mp(
    #         proc_num=proc_num,
    #         factors_ma=factors_ma,
    #         methods=["pearson", "spearman"], icir_threshold=1.2,
    #         bgn_date=bgn_date, stp_date=stp_date,
    #         database_structure=database_structure,
    #         tests_result_dir=research_ic_tests_dir,
    #         tests_result_summary_dir=research_ic_tests_summary_dir,
    #         days_per_year=252)
    # elif switch in ["GP"]:
    #     cal_gp_tests_mp(
    #         proc_num=5,
    #         factors_ma=factors_ma, universe=instruments_universe,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         tests_result_dir=research_gp_tests_dir,
    #         factors_exposure_dir=research_factors_exposure_dir,
    #         test_returns_dir=research_test_returns_dir,
    #         database_structure=database_structure,
    #         calendar_path=calendar_path)
    # elif switch in ["GPSUM"]:
    #     cal_gp_tests_summary_mp(
    #         proc_num=proc_num,
    #         factors_ma=factors_ma, sharpe_ratio_threshold=1.0,
    #         bgn_date=bgn_date, stp_date=stp_date,
    #         database_structure=database_structure,
    #         tests_result_dir=research_gp_tests_dir,
    #         tests_result_summary_dir=research_gp_tests_summary_dir)
    # elif switch in ["GPCOR"]:
    #     cal_gp_tests_corr(
    #         # factors_ma=[
    #         #     "EXR042D3-M005",
    #         #     "CTP063T10-M005",
    #         #     "AMPH063T02-M005",
    #         #     "SKEW126-M005",
    #         #     "BASIS_D021-M005",
    #         #     "BETA_D063-M005",
    #         #     "CTR126T01-M005",
    #         #     "TS_D252-M005",
    #         #     "TWCV021-M005",
    #         # ],
    #         # factors_ma=[
    #         #     "CTP063T10-M010",
    #         #     "EXR042D3-M010",
    #         #     "CSR252T05-M010",
    #         #     "SMTR005T02-M010",
    #         #     "AMPH063T02-M010",
    #         #     "BASIS_D021-M010",
    #         #     "MTM010ADJ-M010",
    #         #     "BETA_D063-M010",
    #         #     "BASIS-M010",
    #         #     "TS_D252-M010",
    #         #     "TWCV021-M010",
    #         # ],
    #         factors_ma=[
    #             "EXR042D3-M010",
    #             "CTP063T10-M010",
    #             "AMPH063T02-M010",
    #             "SKEW126-M010",
    #             "BASIS_D021-M010",
    #             "BETA_D063-M010",
    #             "CTR126T01-M010",
    #             "TS_D252-M010",
    #             "TWCV021-M010",
    #         ],
    #         bgn_date=bgn_date, stp_date=stp_date,
    #         database_structure=database_structure,
    #         tests_result_dir=research_gp_tests_dir,
    #         tests_result_summary_dir=research_gp_tests_summary_dir)
    # elif switch in ["SIG"]:
    #     cal_signals_mp(
    #         proc_num=5,
    #         sids_f_ma_syn_fix=sids_fix_f_ma_syn, sids_f_syn_ma_fix=sids_fix_f_syn_ma,
    #         sids_dyn=sids_dyn,
    #         signals_structure=signals_structure,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         trn_win=3, lbd=1000,
    #         signals_dir=research_signals_dir,
    #         factors_exposure_dir=research_factors_exposure_dir,
    #         gp_tests_dir=research_gp_tests_dir,
    #         database_structure=database_structure,
    #         calendar_path=calendar_path)
    # elif switch in ["SIMU"]:
    #     cal_simulations_mp(
    #         proc_num=5, sids=sids_fix_f_ma_syn + sids_fix_f_syn_ma + sids_dyn,
    #         run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
    #         cost_rate=cost_rate,
    #         signals_dir=research_signals_dir,
    #         test_returns_dir=research_test_returns_dir,
    #         simulations_dir=research_simulations_dir,
    #         database_structure=database_structure,
    #         calendar_path=calendar_path)
    elif switch in ["SIMUSUM"]:
        from struct_sig import signals_structure, sids_fix_f_ma_syn, sids_fix_f_syn_ma, sids_dyn
        from project_setup import research_simulations_dir, research_simulations_summary_dir

        cal_simulations_summary(
            sids=sids_fix_f_ma_syn + sids_fix_f_syn_ma + sids_dyn,
            simulations_dir=research_simulations_dir,
            simulations_summary_dir=research_simulations_summary_dir)
    else:
        raise ValueError(f"... switch = {switch} is not a legal option, please check again")
