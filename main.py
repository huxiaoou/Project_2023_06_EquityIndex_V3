import argparse


def parse_args():
    args_parser = argparse.ArgumentParser(description="Entry point of this project")
    args_parser.add_argument(
        "--switch", type=str,
        choices=("preprocess", "test_returns", "factors_exposure", "fema", "ic", "icsum"),
        help="use this to decide which parts to run, available options",
    )
    args_parser.add_argument(
        "--factor", type=str, default="",
        help="""
            optional, must be provided if switch = {'preprocess', 'factors_exposure'},
            use this to decide which factor, available options = {
            'amp', 'amt', 'basis', 'beta', 'cx', 'exr', 'mtm', 'pos', 'sgm', 'size', 'skew', 'smt', 'to', 'ts', 'twc'}
            """)
    args_parser.add_argument("--mode", type=str, choices=("o", "a"), help="run mode")
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
    args_parser.add_argument("-p", "--process", type=int, default=4, help="""
            number of process to be called when calculating, default = 4
            """)
    args = args_parser.parse_args()
    __switch = args.switch.upper()
    __factor = args.factor.lower()
    __run_mode = args.mode.upper() if args.mode else args.mode
    __bgn_date, __stp_date = args.bgn, args.stp
    __proc_num = args.process
    return __switch, __factor, __run_mode, __bgn_date, __stp_date, __proc_num


if __name__ == "__main__":
    from struct_lib import database_structure
    from project_setup import futures_by_instru_dir, equity_index_by_instrument_dir, calendar_path
    from project_setup import research_factors_exposure_dir
    from project_config import instruments_universe

    switch, factor, run_mode, bgn_date, stp_date, proc_num = parse_args()

    if switch in ["PREPROCESS"]:
        if factor == "split":
            from preprocess.preprocess import split_spot_daily_k
            from project_config import equity_indexes

            split_spot_daily_k(equity_index_by_instrument_dir, equity_indexes)
        elif factor == "m01":
            from preprocess.preprocess import update_major_minute
            from project_setup import futures_dir, futures_md_structure_path, futures_em01_db_name
            from project_setup import research_intermediary_dir

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
            from project_setup import (futures_dir, futures_md_structure_path,
                                       futures_md_db_name, futures_by_date_dir)
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
        from project_setup import (research_test_returns_dir, futures_dir,
                                   futures_md_structure_path, futures_em01_db_name)
        from project_config import test_return_types
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
    elif switch in ["FACTORS_EXPOSURE"]:
        from project_config import factors_args

        if factor == "amp":
            from project_config import mapper_futures_to_index
            from algs.factor_exposure_amp import cal_fac_exp_amp_mp

            cal_fac_exp_amp_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                amp_windows=factors_args["amp_windows"], lbds=factors_args["lbds"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                mapper_fut_to_idx=mapper_futures_to_index,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir
            )
        elif factor == "amt":
            from algs.factor_exposure_amt import cal_fac_exp_amt_mp

            cal_fac_exp_amt_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                amt_windows=factors_args["amt_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                money_scale=10000
            )
        elif factor == "basis":
            from algs.factor_exposure_basis import cal_fac_exp_basis_mp

            cal_fac_exp_basis_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                basis_windows=factors_args["basis_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
            )
        elif factor == "beta":
            from algs.factor_exposure_beta import cal_fac_exp_beta_mp

            cal_fac_exp_beta_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                beta_windows=factors_args["beta_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                equity_index_by_instrument_dir=equity_index_by_instrument_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
            )
        elif factor == "cx":
            from project_config import manager_cx_windows
            from algs.factor_exposure_cx import cal_fac_exp_cx_mp

            cal_fac_exp_cx_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                mgr_cx_windows=manager_cx_windows, top_props=factors_args["top_props"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "exr":
            from project_setup import research_intermediary_dir
            from algs.factor_exposure_exr import cal_fac_exp_exr_mp

            cal_fac_exp_exr_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                exr_windows=factors_args["exr_windows"], drifts=factors_args["drifts"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
            )
        elif factor == "mtm":
            from algs.factor_exposure_mtm import cal_fac_exp_mtm_mp

            cal_fac_exp_mtm_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                mtm_windows=factors_args["mtm_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir,
            )
        elif factor == "pos":
            from project_setup import research_test_returns_dir, research_intermediary_dir
            from algs.factor_exposure_pos import cal_fac_exp_pos_mp

            cal_fac_exp_pos_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                top_players_qty=factors_args["top_players_qty"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                test_returns_dir=research_test_returns_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
            )
        elif factor == "rng":
            from algs.factor_exposure_rng import cal_fac_exp_rng_mp

            cal_fac_exp_rng_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                rng_windows=factors_args["rng_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "sgm":
            from algs.factor_exposure_sgm import cal_fac_exp_sgm_mp

            cal_fac_exp_sgm_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                sgm_windows=factors_args["sgm_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "size":
            from algs.factor_exposure_size import cal_fac_exp_size_mp

            cal_fac_exp_size_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                size_windows=factors_args["size_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "skew":
            from algs.factor_exposure_skew import cal_fac_exp_skew_mp

            cal_fac_exp_skew_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                skew_windows=factors_args["skew_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "smt":
            from project_setup import research_intermediary_dir, futures_instru_info_path
            from algs.factor_exposure_smt import cal_fac_exp_smt_mp

            cal_fac_exp_smt_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                smt_windows=factors_args["smt_windows"], lbds=factors_args["lbds"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
                futures_instru_info_path=futures_instru_info_path,
                amount_scale=1e4
            )
        elif factor == "to":
            from algs.factor_exposure_to import cal_fac_exp_to_mp

            cal_fac_exp_to_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                to_windows=factors_args["to_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                factors_exposure_dir=research_factors_exposure_dir
            )
        elif factor == "ts":
            from algs.factor_exposure_ts import cal_fac_exp_ts_mp
            from project_setup import futures_by_instru_md_dir

            cal_fac_exp_ts_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                ts_windows=factors_args["ts_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                by_instrument_dir=futures_by_instru_dir,
                md_dir=futures_by_instru_md_dir,
                factors_exposure_dir=research_factors_exposure_dir,
                calendar_path=calendar_path,
                price_type="close",
            )
        elif factor == "twc":
            from project_setup import research_intermediary_dir
            from algs.factor_exposure_twc import cal_fac_exp_twc_mp

            cal_fac_exp_twc_mp(
                proc_num=proc_num,
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                twc_windows=factors_args["twc_windows"],
                instruments_universe=instruments_universe,
                database_structure=database_structure,
                factors_exposure_dir=research_factors_exposure_dir,
                intermediary_dir=research_intermediary_dir,
                calendar_path=calendar_path,
            )
        else:
            raise ValueError(f"factor = {factor} is illegal, please check again")
    elif switch in ["FEMA"]:  # "FACTORS_EXPOSURE_MOVING_AVERAGE"
        from algs.factor_exposure_MA import cal_fac_exp_MA_mp
        from project_config import factors, factor_mov_ave_wins

        cal_fac_exp_MA_mp(
            proc_num=proc_num,
            factor_lbls=factors, mov_ave_wins=factor_mov_ave_wins,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            universe=instruments_universe,
            database_structure=database_structure,
            factors_exposure_dir=research_factors_exposure_dir,
            calendar_path=calendar_path)
    elif switch in ["IC"]:
        from project_setup import research_ic_tests_dir, research_test_returns_dir
        from tests.ic_tests import cal_ic_tests_mp
        from project_config import factors_ma

        cal_ic_tests_mp(
            proc_num=proc_num,
            factors_ma=factors_ma,
            run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
            tests_result_dir=research_ic_tests_dir,
            factors_exposure_dir=research_factors_exposure_dir,
            test_returns_dir=research_test_returns_dir,
            database_structure=database_structure,
            calendar_path=calendar_path,
        )
    elif switch in ["ICSUM"]:
        from project_setup import research_ic_tests_dir, research_ic_tests_summary_dir
        from project_config import factors_ma
        from tests.ic_tests_summary import cal_ic_tests_summary_mp

        cal_ic_tests_summary_mp(
            proc_num=proc_num,
            factors_ma=factors_ma,
            methods=["pearson", "spearman"], icir_threshold=1.1,
            bgn_date=bgn_date, stp_date=stp_date,
            database_structure=database_structure,
            tests_result_dir=research_ic_tests_dir,
            tests_result_summary_dir=research_ic_tests_summary_dir,
            days_per_year=252,
        )
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
        from struct_sig import sids_fix_f_ma_syn, sids_fix_f_syn_ma, sids_dyn
        from project_setup import research_simulations_dir, research_simulations_summary_dir
        from signals.signals import cal_simulations_summary

        cal_simulations_summary(
            sids=sids_fix_f_ma_syn + sids_fix_f_syn_ma + sids_dyn,
            simulations_dir=research_simulations_dir,
            simulations_summary_dir=research_simulations_summary_dir)
    else:
        raise ValueError(f"... switch = {switch} is not a legal option, please check again")
