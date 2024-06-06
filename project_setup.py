"""
Created by huxo
Initialized @ 13:08, 2023/6/16
=========================================
This project is mainly about:
0.  CTA strategy on futures of equity index
1.  trading frequency is daily at least
2.  factors can be divided into three parts
    a. traditional price and volume factors
    b. fundamental factors, such as term structure and basis
    c. extra public information from exchanged
3.  use move average technology to cancel the test_window settings
    this is THE BIGGEST DIFFERENCE from
    E:\\Works\\2023\\Project_2023_06_EquityIndex_ML_V2
    or you can think, in this project, test window would always be 1
    two kinds of test returns are provided: open and close
"""
import os
import sys
import json
import platform

# platform confirmation
this_platform = platform.system().upper()
if this_platform == "WINDOWS":
    with open("/Deploy/config3.json", "r", encoding="utf-8") as j:
        global_config = json.load(j)
elif this_platform == "LINUX":
    with open("/home/huxo/Deploy/config3.json", "r", encoding="utf-8") as j:
        global_config = json.load(j)
else:
    print("... this platform is {}.".format(this_platform))
    print("... it is not a recognized platform, please check again.")
    sys.exit()

deploy_dir = global_config["deploy_dir"][this_platform]
project_data_root_dir = os.path.join(deploy_dir, "Data")

# --- calendar
calendar_dir = os.path.join(project_data_root_dir, global_config["calendar"]["dir"])
calendar_path = os.path.join(calendar_dir, global_config["calendar"]["file"])

# --- futures data
futures_dir = os.path.join(project_data_root_dir, global_config["futures"]["dir"])
futures_instru_info_path = os.path.join(futures_dir, global_config["futures"]["instrument_info_file"])

futures_by_instru_dir = os.path.join(futures_dir, global_config["futures"]["by_instrument"]["dir"])
futures_md_dir = os.path.join(futures_dir, global_config["futures"]["by_instrument"]["md"]["dir"])
futures_md_structure_path = os.path.join(futures_dir, global_config["futures"]["db_struct_file"])
futures_md_db_name = global_config["futures"]["md"]["wds_db"]
futures_em01_db_name = global_config["futures"]["md"]["em01_db"]
futures_by_date_dir = os.path.join(futures_dir, global_config["futures"]["by_date"]["dir"])

#
# futures_fundamental_dir = os.path.join(futures_dir, global_config["futures"]["fundamental_dir"])
# futures_fundamental_structure_path = os.path.join(futures_fundamental_dir, global_config["futures"]["fundamental_structure_file"])
# futures_fundamental_db_name = global_config["futures"]["fundamental_db_name"]
# futures_fundamental_intermediary_dir = os.path.join(futures_fundamental_dir, global_config["futures"]["fundamental_intermediary_dir"])
#
# futures_by_instrument_dir = os.path.join(futures_dir, global_config["futures"]["by_instrument"]["dir"])
# major_minor_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["major_minor_dir"])
# major_return_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["major_return_dir"])
# md_by_instru_dir = os.path.join(futures_by_instrument_dir, global_config["futures"]["md_by_instru_dir"])
#
# --- equity
equity_dir = os.path.join(project_data_root_dir, global_config["equity"]["dir"])
equity_by_instrument_dir = os.path.join(equity_dir, global_config["equity"]["dir_by_instrument"])
equity_index_by_instrument_dir = os.path.join(equity_by_instrument_dir, global_config["equity"]["index_dir"])

# --- projects
projects_dir = os.path.join(deploy_dir, global_config["projects"]["projects_save_dir"])

# --- projects data
research_data_root_dir = "/ProjectsData"
research_project_name = os.getcwd().split("\\")[-1]
research_project_data_dir = os.path.join(research_data_root_dir, research_project_name)
research_test_returns_dir = os.path.join(research_project_data_dir, "test_returns")
research_factors_exposure_dir = os.path.join(research_project_data_dir, "factors_exposure")
research_intermediary_dir = os.path.join(research_project_data_dir, "intermediary")
research_ic_tests_dir = os.path.join(research_project_data_dir, "ic_tests")
research_gp_tests_dir = os.path.join(research_project_data_dir, "gp_tests")
research_ic_tests_summary_dir = os.path.join(research_project_data_dir, "ic_tests_summary")
research_gp_tests_summary_dir = os.path.join(research_project_data_dir, "gp_tests_summary")
research_signals_dir = os.path.join(research_project_data_dir, "signals")
research_simulations_dir = os.path.join(research_project_data_dir, "simulations")
research_simulations_summary_dir = os.path.join(research_project_data_dir, "simulations_summary")

if __name__ == "__main__":
    from skyrim.winterhold import check_and_mkdir

    check_and_mkdir(research_data_root_dir)
    check_and_mkdir(research_project_data_dir)
    check_and_mkdir(research_test_returns_dir)
    check_and_mkdir(research_factors_exposure_dir)
    check_and_mkdir(research_intermediary_dir)
    check_and_mkdir(research_ic_tests_dir)
    check_and_mkdir(research_gp_tests_dir)
    check_and_mkdir(research_ic_tests_summary_dir)
    check_and_mkdir(research_gp_tests_summary_dir)
    check_and_mkdir(research_signals_dir)
    check_and_mkdir(os.path.join(research_signals_dir, "weights"))
    check_and_mkdir(os.path.join(research_signals_dir, "models"))
    check_and_mkdir(research_simulations_dir)
    check_and_mkdir(research_simulations_summary_dir)

    print("... directory system for this project has been established.")
