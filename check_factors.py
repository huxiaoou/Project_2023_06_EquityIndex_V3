import pandas as pd
from skyrim.falkreath import CManagerLibReader
from struct_lib import database_structure
from project_config import factors, factors_ma
from project_setup import research_factors_exposure_dir


def check_factors(fs: list[str], res_id: str, db_struct: dict, exp_dir: str):
    sum_data = []
    for factor in fs:
        factor_lib_structure = db_struct[factor]
        factor_lib = CManagerLibReader(
            t_db_name=factor_lib_structure.m_lib_name,
            t_db_save_dir=exp_dir
        )
        factor_lib.set_default(factor_lib_structure.m_tab.m_table_name)
        factor_df = factor_lib.read(t_value_columns=["trade_date", "instrument", "value"])
        factor_lib.close()

        df = pd.pivot_table(data=factor_df, values="value", index="trade_date", columns="instrument")
        df.dropna(axis=0, how="any", inplace=True)
        sum_data.append({
            "factor": factor,
            "obs": len(df),
            "bgn": df.index[0],
            "stp": df.index[-1],
        })

    sum_df = pd.DataFrame(sum_data).sort_values(["obs", "factor", "bgn"])
    sum_df.to_csv(".\\.tmp\\check_factor-{}.csv".format(res_id), index=False)
    print(sum_df)


if __name__ == "__main__":
    check_factors(fs=factors, res_id="va", db_struct=database_structure, exp_dir=research_factors_exposure_dir)
    check_factors(fs=factors_ma, res_id="ma", db_struct=database_structure, exp_dir=research_factors_exposure_dir)
