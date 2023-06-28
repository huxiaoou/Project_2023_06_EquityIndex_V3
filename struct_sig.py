from project_config import instruments_universe

signals_structure = {
    "sigFixFMaSyn": {
        "S000": {
            "universe": instruments_universe,
            "factors_struct": (
                ("EXR042D3-M005", 1),
                ("CTP063T10-M005", 1),
                ("AMPH063T02-M005", 1),
                ("SKEW126-M005", 1),
                ("BASIS_D021-M005", 1),
                ("BETA_D063-M005", 1),
                ("CTR126T01-M005", 1),
                ("TS_D252-M005", 1),
                ("TWCV021-M005", 1),
            )
        },
        "S001": {
            "universe": instruments_universe,
            "factors_struct": (
                ("EXR042D3-M005", 0.0699),
                ("CTP063T10-M005", 0.1910),
                ("AMPH063T02-M005", 0.0723),
                ("SKEW126-M005", 0.1829),
                ("BASIS_D021-M005", 0.1295),
                ("BETA_D063-M005", 0.1134),
                ("CTR126T01-M005", 0.0932),
                ("TS_D252-M005", 0.0751),
                ("TWCV021-M005", 0.0728),
            )
        },
        "S002": {
            "universe": instruments_universe,
            "factors_struct": (
                ("EXR042D3-M010", 0.0699),
                ("CTP063T10-M010", 0.1910),
                ("AMPH063T02-M010", 0.0723),
                ("SKEW126-M010", 0.1829),
                ("BASIS_D021-M010", 0.1295),
                ("BETA_D063-M010", 0.1134),
                ("CTR126T01-M010", 0.0932),
                ("TS_D252-M010", 0.0751),
                ("TWCV021-M010", 0.0728),
            ), },
        "S003": {
            "universe": instruments_universe,
            "factors_struct": (
                ("EXR042D3-M010", 0.0778),
                ("CTP063T10-M010", 0.1832),
                ("AMPH063T02-M010", 0.0752),
                ("SKEW126-M010", 0.2075),
                ("BASIS_D021-M010", 0.1041),
                ("BETA_D063-M010", 0.1006),
                ("CTR126T01-M010", 0.0848),
                ("TS_D252-M010", 0.0705),
                ("TWCV021-M010", 0.0963),
            ), },
        "S004": {
            "universe": instruments_universe,
            "factors_struct": (
                ("CTP063T10-M002", 1),
                ("EXR042D3-M002", 1),
                ("CSR252T05-M002", 1),
                ("SMTR005T02-M002", 1),
            ), },
        "S005": {
            "universe": instruments_universe,
            "factors_struct": (
                ("CTP063T10-M010", 1),
                ("EXR042D3-M010", 1),
                ("CSR252T05-M005", 1),
                ("SMTR005T02-M005", 1),
                ("AMPH063T02-M005", 1),
                ("BASIS_D021-M010", 1),
                ("MTM010ADJ-M005", 1),
                ("BETA_D063-M010", 1),
                ("BASIS-M005", 1),
                ("TS_D252-M005", 1),
                ("TWCV021-M005", 1),
            ), },
        "S006": {
            "universe": instruments_universe,
            "factors_struct": (
                ("CTP063T10-M010", 0.0956),
                ("EXR042D3-M010", 0.0471),
                ("CSR252T05-M010", 0.0964),
                ("SMTR005T02-M010", 0.1180),
                ("AMPH063T02-M010", 0.0815),
                ("BASIS_D021-M010", 0.1356),
                ("MTM010ADJ-M010", 0.0420),
                ("BETA_D063-M010", 0.1482),
                ("BASIS-M010", 0.0336),
                ("TS_D252-M010", 0.1505),
                ("TWCV021-M010", 0.0516),
            ), },
    },
    "sigFixFSynMa": {
        "S012": {
            "universe": instruments_universe,
            "mov_ave_win": 10,
            "factors_struct": (
                ("EXR042D3-M001", 0.0699),
                ("CTP063T10-M001", 0.1910),
                ("AMPH063T02-M001", 0.0723),
                ("SKEW126-M001", 0.1829),
                ("BASIS_D021-M001", 0.1295),
                ("BETA_D063-M001", 0.1134),
                ("CTR126T01-M001", 0.0932),
                ("TS_D252-M001", 0.0751),
                ("TWCV021-M001", 0.0728),
            ), },
        "S013": {
            "universe": instruments_universe,
            "mov_ave_win": 10,
            "factors_struct": (
                ("EXR042D3-M001", 0.0778),
                ("CTP063T10-M001", 0.1832),
                ("AMPH063T02-M001", 0.0752),
                ("SKEW126-M001", 0.2075),
                ("BASIS_D021-M001", 0.1041),
                ("BETA_D063-M001", 0.1006),
                ("CTR126T01-M001", 0.0848),
                ("TS_D252-M001", 0.0705),
                ("TWCV021-M001", 0.0963),
            ), },
    },
    "sigDyn": {
        "S100": {
            "universe": instruments_universe,
            "factors": (
                "EXR042D3-M010",
                "CTP063T10-M010",
                "AMPH063T02-M010",
                "SKEW126-M010",
                "BASIS_D021-M010",
                "BETA_D063-M010",
                "CTR126T01-M010",
                "TS_D252-M010",
                "TWCV021-M010",
            )
        },
    }
}

sids_fix_f_ma_syn = list(signals_structure["sigFixFMaSyn"].keys())
sids_fix_f_syn_ma = list(signals_structure["sigFixFSynMa"].keys())
sids_dyn = list(signals_structure["sigDyn"].keys())

if __name__ == "__main__":
    print(sids_fix_f_syn_ma)
    print(sids_fix_f_ma_syn)
    print(sids_dyn)

# CTP063T10-M010     112.8331     11.3592      1.2118      0.1971      0.0956      0.0846
# EXR042D3-M010      117.6684     11.7986      1.2116      0.1529      0.0471      0.0356
# CSR252T05-M010       4.2176      0.5081      0.1372      0.1001      0.0964      0.0960
# SMTR005T02-M010   -110.8344    -10.9672     -0.9805      0.0181      0.1180      0.1289
# AMPH063T02-M010    -10.2437     -0.9501     -0.0208      0.0722      0.0815      0.0825
# BASIS_D021-M010     30.8607      3.2054      0.4398      0.1633      0.1356      0.1326
# MTM010ADJ-M010     -70.6190     -7.0177     -0.6576     -0.0216      0.0420      0.0489
# BETA_D063-M010     -34.9954     -3.3630     -0.1998      0.1165      0.1482      0.1516
# BASIS-M010         -62.8475     -6.2489     -0.5890     -0.0230      0.0336      0.0397
# TS_D252-M010       -40.2922     -3.8901     -0.2499      0.1141      0.1505      0.1545
# TWCV021-M010        65.2524      6.5658      0.6971      0.1103      0.0516      0.0452
