import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader, CManagerLibWriter
from skyrim.whiterun import CCalendar, CCalendarMonthly
from skyrim.winterhold import check_and_mkdir, plot_lines
from skyrim.markarth import minimize_utility
from skyrim.riften import CNAV


def shift_wgt(df: pd.DataFrame, row: str, col: str, val: str, shift_win: int):
    _pivot_df = pd.pivot_table(data=df, index=row, columns=col, values=val)
    _res_df = _pivot_df.shift(shift_win).stack().sort_index().reset_index()
    _res_df.rename(mapper={0: val}, axis=1, inplace=True)
    _dlt_wgt_srs = (_pivot_df - _pivot_df.shift(shift_win - 1)).shift(1).abs().sum(axis=1)
    return _res_df, _dlt_wgt_srs


class CSignalBase(object):
    def __init__(self, sid: str,
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 signals_dir: str,
                 calendar_path: str,
                 ):
        self.m_sid = sid
        self.m_run_mode = run_mode
        if stp_date is None:
            stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
        self.m_bgn_date, self.m_stp_date = bgn_date, stp_date
        calendar = CCalendar(calendar_path)
        self.m_FIX_TEST_WIN = 1
        self.m_base_date = calendar.get_next_date(self.m_bgn_date, -self.m_FIX_TEST_WIN - 1)
        self.m_iter_dates: list[str] = calendar.get_iter_list(bgn_date, stp_date, True)

        # --- save destination
        self.m_signals_dir = signals_dir
        self.m_signals_weights_dir = os.path.join(self.m_signals_dir, "weights")

    def main_cal_sim(self, cost_rate: float,
                     database_structure: dict[str, CLib1Tab1],
                     test_returns_dir: str,
                     simulations_dir: str):
        # --- signals
        sig_lib_id = self.m_sid
        sig_lib_structure = database_structure[sig_lib_id]
        sig_lib = CManagerLibReader(t_db_save_dir=self.m_signals_weights_dir, t_db_name=sig_lib_structure.m_lib_name)
        sig_lib.set_default(sig_lib_structure.m_tab.m_table_name)
        sig_df = sig_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", self.m_base_date),
                ("trade_date", "<", self.m_stp_date),
            ], t_value_columns=["trade_date", "instrument", "value"]
        )
        sig_lib.close()
        sig_df_shift, dlt_wgt_srs = shift_wgt(sig_df, row="trade_date", col="instrument", val="value", shift_win=self.m_FIX_TEST_WIN + 1)

        # --- test return library
        test_return_lib_id = "test_return_o"
        test_return_lib_structure = database_structure[test_return_lib_id]
        test_return_lib = CManagerLibReader(t_db_name=test_return_lib_structure.m_lib_name, t_db_save_dir=test_returns_dir)
        test_return_lib.set_default(test_return_lib_structure.m_tab.m_table_name)
        test_return_df = test_return_lib.read_by_conditions(
            t_conditions=[
                ("trade_date", ">=", self.m_bgn_date),
                ("trade_date", "<", self.m_stp_date),
            ], t_value_columns=["trade_date", "instrument", "value"]
        )
        test_return_lib.close()

        simu_input_df = pd.merge(
            left=sig_df_shift, right=test_return_df,
            on=["trade_date", "instrument"], suffixes=("_e", "_r"),
            how="right"
        ).set_index("instrument").fillna(0)

        res_srs = simu_input_df.groupby(by="trade_date").apply(lambda z: z["value_e"] @ z["value_r"])
        simu_df = pd.DataFrame({
            "rawRet": res_srs,
            "dltWgt": dlt_wgt_srs,
        })
        simu_df = simu_df[simu_df.index >= self.m_bgn_date]
        simu_df["netRet"] = simu_df["rawRet"] - simu_df["dltWgt"] * cost_rate
        simu_df["nav"] = (1 + simu_df["netRet"]).cumprod()
        simu_file = "{}-nav.csv.gz".format(self.m_sid)
        simu_path = os.path.join(simulations_dir, simu_file)
        simu_df[["netRet", "nav"]].to_csv(simu_path, index_label="trade_date", float_format="%.8f")

        return 0


class CSignal(CSignalBase):
    def __init__(self, sid: str, universe: list[str],
                 factors: list[str],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 signals_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar_path: str):
        super().__init__(sid, run_mode, bgn_date, stp_date, signals_dir, calendar_path)
        self.m_universe = universe
        self.m_factors = factors

        # --- factor weights
        self.m_factors_weight = {}
        self.m_raw_wgt_df = pd.DataFrame()

        # --- signal data and lib
        self.m_sig_save_df = pd.DataFrame()  # with index = "trade_date", values = ["instrument", "value"]
        self.m_sig_lib_structure = database_structure[self.m_sid]

    def _cal_weight(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        dfs_list = []
        for factor in self.m_factors:
            factor_lib_structure = database_structure[factor]
            factor_lib = CManagerLibReader(
                t_db_name=factor_lib_structure.m_lib_name,
                t_db_save_dir=factors_exposure_dir
            )
            factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
            df = factor_lib.read_by_conditions(t_conditions=[
                ("trade_date", ">=", self.m_bgn_date),
                ("trade_date", "<", self.m_stp_date),
            ], t_value_columns=["trade_date", "instrument", "value"])
            factor_lib.close()
            pivot_df = pd.pivot_table(data=df, index="trade_date", columns="instrument", values="value")[self.m_universe]
            self.m_factors_weight[factor] = pivot_df.sort_index()
            pivot_df["factor"] = factor
            dfs_list.append(pivot_df)
        raw_wgt_df = pd.concat(dfs_list, axis=0, ignore_index=False)
        self.m_raw_wgt_df = raw_wgt_df.sort_values(by=["trade_date", "factor"])
        return 0

    def _sumprod_weights(self, daily_fac_instru_df: pd.DataFrame):
        pass

    def _save(self):
        sig_lib = CManagerLibWriter(t_db_save_dir=self.m_signals_weights_dir, t_db_name=self.m_sig_lib_structure.m_lib_name)
        sig_lib.initialize_table(t_table=self.m_sig_lib_structure.m_tab, t_remove_existence=self.m_run_mode in ["O", "OVERWRITE"])
        sig_lib.update(self.m_sig_save_df, t_using_index=True)
        sig_lib.close()
        return 0

    def main_cal_sig(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        pass


class CSignalFixWeight(CSignal):
    def __init__(self, sid: str, universe: list[str],
                 factors_struct: tuple[tuple],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 signals_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar_path: str):
        factors, fix_weights = zip(*factors_struct)
        super().__init__(sid, universe, factors, run_mode, bgn_date, stp_date, signals_dir, database_structure, calendar_path)
        ws = pd.Series(data=fix_weights, index=factors)
        self.m_fix_weights = ws / ws.abs().sum()

    def _sumprod_weights(self, daily_fac_instru_df: pd.DataFrame):
        xt = daily_fac_instru_df.set_index("factor").T
        y = self.m_fix_weights[xt.columns]
        return xt @ y

    def main_cal_sig(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        self._cal_weight(database_structure, factors_exposure_dir)
        self._save()
        return 0


class CSignalFixWeightFMaSyn(CSignalFixWeight):
    def _cal_weight(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        super()._cal_weight(database_structure, factors_exposure_dir)
        sig_grp_df = self.m_raw_wgt_df.groupby(lambda z: z).apply(self._sumprod_weights)
        sig_nrm_df = sig_grp_df.div(sig_grp_df.abs().sum(axis=1), axis=0).fillna(0)
        self.m_sig_save_df = sig_nrm_df.stack().reset_index(level=1)
        return 0


class CSignalFixWeightFSynMa(CSignalFixWeight):
    def __init__(self, sid: str, universe: list[str], mov_ave_win: int,
                 factors_struct: tuple[tuple],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 signals_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar_path: str):
        self.m_mov_ave_win = mov_ave_win
        super().__init__(sid, universe, factors_struct, run_mode, bgn_date, stp_date, signals_dir, database_structure, calendar_path)

    def _cal_weight(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        super()._cal_weight(database_structure, factors_exposure_dir)
        sig_grp_df = self.m_raw_wgt_df.groupby(lambda z: z).apply(self._sumprod_weights)
        sig_nrm_df = sig_grp_df.div(sig_grp_df.abs().sum(axis=1), axis=0).fillna(0)
        sig_rol_df = sig_nrm_df.rolling(window=self.m_mov_ave_win).mean()
        sig_df = sig_rol_df.div(sig_rol_df.abs().sum(axis=1), axis=0).fillna(0)
        self.m_sig_save_df = sig_df.stack().reset_index(level=1)
        return 0


class CSignalDynamicWeight(CSignal):
    def __init__(self, sid: str, universe: list[str], mov_ave_win: int, min_model_days: int,
                 factors_struct: list[str],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 trn_win: int, lbd: float,
                 signals_dir: str,
                 gp_tests_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar_path: str):
        factors, fix_weights = zip(*factors_struct)
        ws = pd.Series(data=fix_weights, index=factors)
        self.m_default_weights = ws / ws.abs().sum()

        super().__init__(sid, universe, factors, run_mode, bgn_date, stp_date, signals_dir, database_structure, calendar_path)
        self.m_mov_ave_win = mov_ave_win
        self.m_min_model_days = min_model_days
        self.m_signals_models_dir = os.path.join(self.m_signals_dir, "models")
        self.m_gp_tests_dir = gp_tests_dir
        self.m_trn_win = trn_win
        self.m_lbd = lbd

        calendar = CCalendarMonthly(calendar_path)
        self.m_train_dates = []
        self.__load_train_dates(calendar)

        self.m_gp_ret_df = pd.DataFrame()
        self.__load_gp_test_return(database_structure)

        self.m_opt_wgt = {}
        self.m_opt_wgt_df = pd.DataFrame()

    def __load_train_dates(self, calendar: CCalendarMonthly):
        iter_months = calendar.map_iter_dates_to_iter_months(self.m_bgn_date, self.m_stp_date)
        for train_end_month in iter_months:
            train_bgn_date, train_end_date = calendar.get_bgn_and_end_dates_for_trailing_window(train_end_month, self.m_trn_win)
            self.m_train_dates.append((train_end_month, train_bgn_date, train_end_date))
            check_and_mkdir(year_dir := os.path.join(self.m_signals_models_dir, train_end_month[0:4]))
            check_and_mkdir(os.path.join(year_dir, train_end_month))
        return 0

    def __load_gp_test_return(self, database_structure: dict[str, CLib1Tab1]):
        _, train_bgn_dates, _ = zip(*self.m_train_dates)
        base_date = min(train_bgn_dates)
        ret_data = {}
        for factor in self.m_factors:
            _f = factor.replace("-M001", "-M{:03d}".format(self.m_mov_ave_win))
            test_lib_id = "gp-{}".format(_f)
            test_lib_structure = database_structure[test_lib_id]
            test_lib = CManagerLibReader(t_db_save_dir=self.m_gp_tests_dir, t_db_name=test_lib_structure.m_lib_name)
            test_lib.set_default(test_lib_structure.m_tab.m_table_name)
            gp_df = test_lib.read_by_conditions(t_conditions=[
                ("trade_date", ">=", base_date),
                ("trade_date", "<", self.m_stp_date),
            ], t_value_columns=["trade_date", "RH"]).set_index("trade_date")
            ret_data[factor] = gp_df["RH"]
            test_lib.close()
        self.m_gp_ret_df = pd.DataFrame(ret_data)
        return 0

    def _cal_models(self):
        for train_end_month, train_bgn_date, train_end_date in self.m_train_dates:
            filter_dates = (self.m_gp_ret_df.index >= train_bgn_date) & (self.m_gp_ret_df.index <= train_end_date)
            ret_df = self.m_gp_ret_df.loc[filter_dates, self.m_factors]
            if len(ret_df) < self.m_min_model_days:
                ws = self.m_default_weights
            else:
                mu, sgm = ret_df.mean(), ret_df.cov()
                if (r0 := np.linalg.matrix_rank(ret_df)) < (r1 := len(self.m_factors)):
                    print(self.m_sid, train_end_month, train_bgn_date, train_end_date, "{}/{}".format(r0, r1))
                    ws = None
                else:
                    w, _ = minimize_utility(t_mu=mu.values, t_sigma=sgm.values, t_lbd=self.m_lbd)
                    ws = pd.Series(data=w, index=mu.index)
            self.m_opt_wgt[train_end_date] = ws

            # save model
            model_save_df = pd.DataFrame({self.m_sid: ws})
            model_save_file = "{}-{}.csv.gz".format(self.m_sid, train_end_month)
            model_save_path = os.path.join(self.m_signals_models_dir, train_end_month[0:4], train_end_month, model_save_file)
            model_save_df.to_csv(model_save_path, index_label="factor", float_format="%.6f")
        return 0

    def _sumprod_weights(self, daily_fac_instru_df: pd.DataFrame):
        xt = daily_fac_instru_df.set_index("factor").T
        t = daily_fac_instru_df.index[0]
        y = self.m_opt_wgt_df.loc[t, xt.columns]
        return xt @ y

    def _cal_weight(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        super()._cal_weight(database_structure, factors_exposure_dir)
        header_df = pd.DataFrame({"trade_date": self.m_iter_dates})
        opt_wgt_df = pd.DataFrame.from_dict(self.m_opt_wgt, orient="index")
        self.m_opt_wgt_df = pd.merge(
            left=header_df, right=opt_wgt_df,
            left_on="trade_date", right_index=True,
            how="left"
        ).set_index("trade_date").fillna(method="ffill").shift(1).fillna(method="bfill")

        sig_grp_df = self.m_raw_wgt_df.groupby(lambda z: z).apply(self._sumprod_weights)
        sig_nrm_df = sig_grp_df.div(sig_grp_df.abs().sum(axis=1), axis=0).fillna(0)
        sig_rol_df = sig_nrm_df.rolling(window=self.m_mov_ave_win).mean()
        sig_df = sig_rol_df.div(sig_rol_df.abs().sum(axis=1), axis=0).fillna(0)
        self.m_sig_save_df = sig_df.stack().reset_index(level=1)
        return 0

    def main_cal_sig(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        self._cal_models()
        self._cal_weight(database_structure, factors_exposure_dir)
        self._save()
        return 0


def cal_signals_mp(
        proc_num: int, sids_f_ma_syn_fix: list[str], sids_f_syn_ma_fix: list[str], sids_dyn: list[str],
        signals_structure: dict[str, dict],
        run_mode: str, bgn_date: str, stp_date: str | None,
        trn_win: int, lbd: float,
        signals_dir: str,
        factors_exposure_dir: str,
        gp_tests_dir: str,
        database_structure: dict[str, CLib1Tab1],
        calendar_path: str):
    t0 = dt.datetime.now()

    # --- for fix
    pool = mp.Pool(processes=proc_num)
    for sid in sids_f_ma_syn_fix:
        sig_struct = signals_structure["sigFixFMaSyn"][sid]
        signal = CSignalFixWeightFMaSyn(sid, sig_struct["universe"],
                                        sig_struct["factors_struct"],
                                        run_mode, bgn_date, stp_date,
                                        signals_dir,
                                        database_structure,
                                        calendar_path)
        pool.apply_async(signal.main_cal_sig, args=(database_structure, factors_exposure_dir))

    for sid in sids_f_syn_ma_fix:
        sig_struct = signals_structure["sigFixFSynMa"][sid]
        signal = CSignalFixWeightFSynMa(sid, sig_struct["universe"], sig_struct["mov_ave_win"],
                                        sig_struct["factors_struct"],
                                        run_mode, bgn_date, stp_date,
                                        signals_dir,
                                        database_structure,
                                        calendar_path)
        pool.apply_async(signal.main_cal_sig, args=(database_structure, factors_exposure_dir))

    # --- for dynamics
    for sid in sids_dyn:
        sig_struct = signals_structure["sigDyn"][sid]
        signal = CSignalDynamicWeight(sid, sig_struct["universe"], sig_struct["mov_ave_win"], sig_struct["min_model_days"],
                                      sig_struct["factors_struct"],
                                      run_mode, bgn_date, stp_date,
                                      trn_win, lbd,
                                      signals_dir,
                                      gp_tests_dir,
                                      database_structure,
                                      calendar_path)
        pool.apply_async(signal.main_cal_sig, args=(database_structure, factors_exposure_dir))

    pool.close()
    pool.join()

    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_simulations_mp(
        proc_num: int, sids: list[str],
        run_mode: str, bgn_date: str, stp_date: str | None,
        cost_rate: float,
        signals_dir: str,
        test_returns_dir: str,
        simulations_dir: str,
        database_structure: dict[str, CLib1Tab1],
        calendar_path: str
):
    t0 = dt.datetime.now()

    # --- for fix
    pool = mp.Pool(processes=proc_num)
    for sid in sids:
        signal = CSignalBase(sid, run_mode, bgn_date, stp_date, signals_dir, calendar_path)
        pool.apply_async(signal.main_cal_sim, args=(cost_rate,
                                                    database_structure,
                                                    test_returns_dir,
                                                    simulations_dir))
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0


def cal_simulations_summary(sids: list[str],
                            simulations_dir: str,
                            simulations_summary_dir: str,
                            top_n: int = 5):
    summary_data = {}
    nav_data = {}
    for sid in sids:
        simu_file = "{}-nav.csv.gz".format(sid)
        simu_path = os.path.join(simulations_dir, simu_file)
        simu_df = pd.read_csv(simu_path, dtype={"trade_date": str}).set_index("trade_date")
        nav = CNAV(t_raw_nav_srs=simu_df["netRet"], t_annual_rf_rate=0, t_type="RET")
        nav.cal_all_indicators()
        summary_data[sid] = nav.to_dict(t_type="eng")
        nav_data[sid] = simu_df["nav"]
    summary_df = pd.DataFrame.from_dict(summary_data, orient="index")
    summary_df = summary_df[["return_mean", "return_std", "hold_period_return", "annual_return", "annual_volatility", "sharpe_ratio", "calmar_ratio", "max_drawdown_scale"]]
    print(summary_df.sort_values("sharpe_ratio", ascending=False))
    summary_file = "simulations-summary.csv"
    summary_path = os.path.join(simulations_summary_dir, summary_file)
    summary_df.to_csv(summary_path, index_label="sid", float_format="%.6f")

    top_sids = summary_df.sort_values("sharpe_ratio", ascending=False).head(top_n).index
    top_nav_df = pd.DataFrame(nav_data)[top_sids]
    plot_lines(t_plot_df=top_nav_df, t_fig_name="equity.top_sharpe", t_save_dir=simulations_summary_dir, t_colormap="jet", t_fig_size=(32, 9))
    return 0
