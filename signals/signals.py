import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from skyrim.falkreath import CLib1Tab1, CManagerLibReader
from skyrim.falkreath import CManagerLibWriter
from skyrim.whiterun import CCalendarMonthly
from skyrim.winterhold import check_and_mkdir
from skyrim.markarth import minimize_utility


class CSignal(object):
    def __init__(self, sid: str, universe: list[str],
                 factors: list[str],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 signals_dir: str,
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1]):
        self.m_sid = sid
        self.m_universe = universe
        self.m_factors = factors

        self.m_run_mode = run_mode
        if stp_date is None:
            stp_date = (dt.datetime.strptime(bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
        self.m_bgn_date, self.m_stp_date = bgn_date, stp_date

        # --- factor weights
        self.m_factors_weight = {}
        self.m_raw_wgt_df = pd.DataFrame()
        self.__load_factors_weight(database_structure, factors_exposure_dir)

        # --- signal data and lib
        self.m_sig_save_df = pd.DataFrame()  # with index = "trade_date", values = ["instrument", "value"]
        self.m_sig_lib_structure = database_structure[self.m_sid]

        # --- save destination
        self.m_signals_dir = signals_dir
        self.m_signals_weights_dir = os.path.join(self.m_signals_dir, "weights")

    def __load_factors_weight(self, database_structure: dict[str, CLib1Tab1], factors_exposure_dir: str):
        dfs_list = []
        for factor in self.m_factors:
            factor_dst_lib_structure = database_structure[factor]
            factor_lib = CManagerLibReader(
                t_db_name=factor_dst_lib_structure.m_lib_name,
                t_db_save_dir=factors_exposure_dir
            )
            factor_lib.set_default(factor_dst_lib_structure.m_tab.m_table_name)
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

    def _save(self):
        sig_lib = CManagerLibWriter(t_db_save_dir=self.m_signals_weights_dir, t_db_name=self.m_sig_lib_structure.m_lib_name)
        sig_lib.initialize_table(t_table=self.m_sig_lib_structure.m_tab, t_remove_existence=self.m_run_mode in ["O", "OVERWRITE"])
        sig_lib.update(self.m_sig_save_df, t_using_index=True)
        sig_lib.close()
        return 0

    def __cal_weight(self):
        pass

    def main(self):
        pass


class CSignalFixWeight(CSignal):
    def __init__(self, sid: str, universe: list[str],
                 factors_struct: tuple[tuple],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 signals_dir: str,
                 factors_exposure_dir: str,
                 database_structure: dict[str, CLib1Tab1]):
        factors, fix_weights = zip(*factors_struct)
        super().__init__(sid, universe, factors, run_mode, bgn_date, stp_date, signals_dir, factors_exposure_dir, database_structure)
        ws = pd.Series(data=fix_weights, index=factors)
        self.m_fix_weights = ws / ws.abs().sum()

    def __sumprod_weights(self, daily_fac_instru_df: pd.DataFrame):
        xt = daily_fac_instru_df.set_index("factor").T
        y = self.m_fix_weights[xt.columns]
        return xt @ y

    def __cal_weight(self):
        sig_grp_df = self.m_raw_wgt_df.groupby(lambda z: z).apply(self.__sumprod_weights)
        sig_nrm_df = sig_grp_df.div(sig_grp_df.abs().sum(axis=1), axis=0).fillna(0)
        self.m_sig_save_df = sig_nrm_df.stack().reset_index(level=1)
        return 0

    def main(self):
        self.__cal_weight()
        self._save()
        return 0


class CSignalDynamicWeight(CSignal):
    def __init__(self, sid: str, universe: list[str],
                 factors: list[str],
                 run_mode: str, bgn_date: str, stp_date: str | None,
                 trn_win: int, lbd: float,
                 signals_dir: str,
                 factors_exposure_dir: str,
                 gp_tests_dir: str,
                 database_structure: dict[str, CLib1Tab1],
                 calendar: CCalendarMonthly):
        super().__init__(sid, universe, factors, run_mode, bgn_date, stp_date, signals_dir, factors_exposure_dir, database_structure)
        self.m_signals_models_dir = os.path.join(self.m_signals_dir, "models")
        self.m_gp_tests_dir = gp_tests_dir
        self.m_trn_win = trn_win
        self.m_lbd = lbd

        self.m_train_dates = []
        self.__load_train_dates(calendar)

        self.m_gp_ret_df = pd.DataFrame()
        self.__load_gp_test_return(database_structure)

        self.m_opt_wgt = {}
        self.m_opt_wgt_df = pd.DataFrame()
        self.m_iter_dates: list[str] = calendar.get_iter_list(bgn_date, stp_date, True)

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
            test_lib_id = "gp-{}".format(factor)
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

    def __cal_models(self):
        for train_end_month, train_bgn_date, train_end_date in self.m_train_dates:
            filter_dates = (self.m_gp_ret_df.index >= train_bgn_date) & (self.m_gp_ret_df.index <= train_end_date)
            ret_df = self.m_gp_ret_df.loc[filter_dates, self.m_factors]
            if len(ret_df) < 60:
                k = len(self.m_factors)
                ws = pd.Series(data=1 / k, index=self.m_factors)
            else:
                mu, sgm = ret_df.mean(), ret_df.cov()
                if (r0 := np.linalg.matrix_rank(ret_df)) < (r1 := len(self.m_factors)):
                    print(self.m_sid, train_end_month, train_bgn_date, train_end_date, "{}/{}".format(r0, r1))
                    ws = None
                else:
                    w, _ = minimize_utility(t_mu=mu.values, t_sigma=sgm.values, t_lbd=self.m_lbd)
                    ws = pd.Series(data=w, index=mu.index)

            if ws is not None:
                self.m_opt_wgt[train_end_date] = ws
                model_save_df = pd.DataFrame({self.m_sid: ws})
                model_save_file = "{}-{}.csv.gz".format(self.m_sid, train_end_month)
                model_save_path = os.path.join(self.m_signals_models_dir, train_end_month[0:4], train_end_month, model_save_file)
                model_save_df.to_csv(model_save_path, index_label="factor", float_format="%.6f")
        return 0

    def __sumprod_weights(self, daily_fac_instru_df: pd.DataFrame):
        xt = daily_fac_instru_df.set_index("factor").T
        t = daily_fac_instru_df.index[0]
        y = self.m_opt_wgt_df.loc[t, xt.columns]
        return xt @ y

    def __cal_weight(self):
        header_df = pd.DataFrame({"trade_date": self.m_iter_dates})
        opt_wgt_df = pd.DataFrame.from_dict(self.m_opt_wgt, orient="index")
        self.m_opt_wgt_df = pd.merge(
            left=header_df, right=opt_wgt_df,
            left_on="trade_date", right_index=True,
            how="left"
        ).set_index("trade_date").fillna(method="ffill").shift(1).fillna(method="bfill")

        sig_grp_df = self.m_raw_wgt_df.groupby(lambda z: z).apply(self.__sumprod_weights)
        sig_nrm_df = sig_grp_df.div(sig_grp_df.abs().sum(axis=1), axis=0).fillna(0)
        self.m_sig_save_df = sig_nrm_df.stack().reset_index(level=1)
        return 0

    def main(self):
        self.__cal_models()
        self.__cal_weight()
        self._save()
        return 0


def cal_signals_mp(
        proc_num: int, sids_fix: list[str], sids_dyn: list[str],
        signals_structure: dict[str, dict],
        run_mode: str, bgn_date: str, stp_date: str | None,
        trn_win: int, lbd: float,
        signals_dir: str,
        factors_exposure_dir: str,
        gp_tests_dir: str,
        database_structure: dict[str, CLib1Tab1],
        calendar_path: str):
    t0 = dt.datetime.now()
    calendar = CCalendarMonthly(calendar_path)

    # --- for fix
    pool = mp.Pool(processes=proc_num)
    for sid in sids_fix:
        sig_struct = signals_structure["sigFix"][sid]
        signal = CSignalFixWeight(sid, sig_struct["universe"],
                                  sig_struct["factors_struct"],
                                  run_mode, bgn_date, stp_date,
                                  signals_dir,
                                  factors_exposure_dir,
                                  database_structure)
        pool.apply_async(signal.main())
    pool.close()
    pool.join()

    # --- for dynamics
    pool = mp.Pool(processes=proc_num)
    for sid in sids_dyn:
        sig_struct = signals_structure["sigDyn"][sid]
        signal = CSignalDynamicWeight(sid, sig_struct["universe"],
                                      sig_struct["factors"],
                                      run_mode, bgn_date, stp_date,
                                      trn_win, lbd,
                                      signals_dir,
                                      factors_exposure_dir,
                                      gp_tests_dir,
                                      database_structure,
                                      calendar)
        pool.apply_async(signal.main())
    pool.close()
    pool.join()
    t1 = dt.datetime.now()
    print("... total time consuming: {:.2f} seconds".format((t1 - t0).total_seconds()))
    return 0
