import itertools as ittl

instruments_universe = ["IH.CFE", "IF.CFE", "IC.CFE"]
equity_indexes = (
    ("000016.SH", "IH.CFE"),
    ("000300.SH", "IF.CFE"),
    ("000905.SH", "IC.CFE"),
    ("000852.SH", "IM.CFE"),
    ("881001.WI", None),
)
mapper_index_to_futures = {k: v for k, v in equity_indexes}
mapper_futures_to_index = {v: k for k, v in equity_indexes}

test_return_types = ("o", "c")
factor_mov_ave_wins = [1, 2, 3, 5, 10]

# --- factors
factors_args = {
    "amt_windows": [21, 63, 126, 252],
    "amp_windows": [21, 63, 126, 252],
    "basis_windows": [21, 63, 126, 252],
    "beta_windows": [21, 63, 126, 252],
    "csp_windows": [21, 63, 126, 252],
    "csr_windows": [21, 63, 126, 252],
    "ctp_windows": [21, 63, 126, 252],
    "ctr_windows": [21, 63, 126, 252],
    "cvp_windows": [21, 63, 126, 252],
    "cvr_windows": [21, 63, 126, 252],
    "exr_windows": [10, 21, 42, 63],
    "mtm_windows": [5, 10, 21, 63, 126, 252],
    "pos_windows": [1, 2, 3, 5],  # must be a subset of test windows
    "sgm_windows": [21, 63, 126, 252],
    "size_windows": [21, 63, 126, 252],
    "skew_windows": [21, 63, 126, 252],
    "smt_windows": [5, 10, 21, 42, 63],
    "to_windows": [21, 63, 126, 252],
    "ts_windows": [21, 63, 126, 252],
    "twc_windows": [3, 5, 10, 21, 42, 63],

    "top_props": [0.1, 0.2, 0.5, 1],
    "top_players_qty": [1, 2, 3, 5],
    "lbds": [0.2, 0.4, 0.6, 0.8],
    "drifts": [1, 2, 3],
}
amt_windows = factors_args["amt_windows"]
amp_windows = factors_args["amp_windows"]
basis_windows = factors_args["basis_windows"]
beta_windows = factors_args["beta_windows"]
csp_windows = factors_args["csp_windows"]
csr_windows = factors_args["csr_windows"]
ctp_windows = factors_args["ctp_windows"]
ctr_windows = factors_args["ctr_windows"]
cvp_windows = factors_args["cvp_windows"]
cvr_windows = factors_args["cvr_windows"]
exr_windows = factors_args["exr_windows"]
mtm_windows = factors_args["mtm_windows"]
pos_windows = factors_args["pos_windows"]
sgm_windows = factors_args["sgm_windows"]
size_windows = factors_args["size_windows"]
skew_windows = factors_args["skew_windows"]
smt_windows = factors_args["smt_windows"]
to_windows = factors_args["to_windows"]
ts_windows = factors_args["ts_windows"]
twc_windows = factors_args["twc_windows"]
top_props = factors_args["top_props"]
top_players_qty = factors_args["top_players_qty"]
lbds = factors_args["lbds"]
drifts = factors_args["drifts"]

manager_cx_windows = {
    "CSP": csp_windows,
    "CSR": csr_windows,
    "CTP": ctp_windows,
    "CTR": ctr_windows,
    "CVP": cvp_windows,
    "CVR": cvr_windows,
}

fac_sub_grp_amt = ["AMT{:03d}".format(_) for _ in amt_windows]
fac_sub_grp_amp = ["AMPH{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(amp_windows, lbds)] \
                  + ["AMPL{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(amp_windows, lbds)] \
                  + ["AMPD{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(amp_windows, lbds)]
fac_sub_grp_basis = ["BASIS"] + ["BASIS_M{:03d}".format(_) for _ in basis_windows] + ["BASIS_D{:03d}".format(_) for _ in basis_windows]
fac_sub_grp_beta = ["BETA{:03d}".format(_) for _ in beta_windows] + ["BETA_D{:03d}".format(_) for _ in beta_windows[1:]]
fac_sub_grp_csp = ["CSP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(csp_windows, top_props)]
fac_sub_grp_csr = ["CSR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(csr_windows, top_props)]
fac_sub_grp_ctp = ["CTP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(ctp_windows, top_props)]
fac_sub_grp_ctr = ["CTR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(ctr_windows, top_props)]
fac_sub_grp_cvp = ["CVP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(cvp_windows, top_props)]
fac_sub_grp_cvr = ["CVR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(cvr_windows, top_props)]
fac_sub_grp_exr = ["EXR{:03d}".format(_) for _ in exr_windows] \
                  + ["DXR{:03d}D{:d}".format(_, d) for _, d in ittl.product(exr_windows, drifts)] \
                  + ["EXR{:03d}D{:d}".format(_, d) for _, d in ittl.product(exr_windows, drifts)]
fac_sub_grp_mtm = ["MTM{:03d}".format(_) for _ in mtm_windows] + ["MTM{:03d}ADJ".format(_) for _ in mtm_windows]
fac_sub_grp_pos = ["POSH{}{:03d}Q{:02d}".format(d, _, t) for d, _, t in ittl.product(["L", "S"], pos_windows, top_players_qty)] + \
                  ["POSD{}{:03d}Q{:02d}".format(d, _, t) for d, _, t in ittl.product(["L", "S"], pos_windows, top_players_qty)]
fac_sub_grp_sgm = ["SGM{:03d}".format(_) for _ in sgm_windows]
fac_sub_grp_size = ["SIZE{:03d}".format(_) for _ in size_windows]
fac_sub_grp_skew = ["SKEW{:03d}".format(_) for _ in skew_windows]
fac_sub_grp_smt = ["SMTP{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(smt_windows, lbds)] + \
                  ["SMTR{:03d}T{:02d}".format(_, int(p * 10)) for _, p in ittl.product(smt_windows, lbds)]
fac_sub_grp_to = ["TO{:03d}".format(_) for _ in to_windows]
fac_sub_grp_ts = ["TS"] + ["TS_M{:03d}".format(_) for _ in ts_windows] + ["TS_D{:03d}".format(_) for _ in ts_windows]
fac_sub_grp_twc = ["TWCU{:03d}".format(_) for _ in twc_windows] \
                  + ["TWCD{:03d}".format(_) for _ in twc_windows] \
                  + ["TWCT{:03d}".format(_) for _ in twc_windows] \
                  + ["TWCV{:03d}".format(_) for _ in twc_windows]

factors = fac_sub_grp_amp + fac_sub_grp_amt \
          + fac_sub_grp_basis + fac_sub_grp_beta \
          + fac_sub_grp_csp + fac_sub_grp_csr + fac_sub_grp_ctp + fac_sub_grp_ctr + fac_sub_grp_cvp + fac_sub_grp_cvr \
          + fac_sub_grp_exr \
          + fac_sub_grp_mtm \
          + fac_sub_grp_sgm + fac_sub_grp_size + fac_sub_grp_skew + fac_sub_grp_smt \
          + fac_sub_grp_to + fac_sub_grp_ts + fac_sub_grp_twc \
          + fac_sub_grp_pos

universe_options = {
    "U3": ["IC.CFE", "IF.CFE", "IH.CFE"],
    "UCH": ["IC.CFE", "IH.CFE"],
    "UCF": ["IC.CFE", "IF.CFE"],
    "UFH": ["IF.CFE", "IH.CFE"],
}

# --- simulation
cost_rate = 5e-4

if __name__ == "__main__":
    print("Number of factors = {}".format(len(factors)))
