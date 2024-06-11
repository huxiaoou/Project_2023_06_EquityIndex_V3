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
        "S014": {  # weights from grp_tests M001
            "universe": instruments_universe,
            "mov_ave_win": 10,
            "factors_struct": (
                ("EXR042D3-M001", 0.0771),
                ("CTP063T10-M001", 0.1895),
                ("AMPH063T02-M001", 0.0570),
                ("SKEW126-M001", 0.1947),
                ("BASIS_D021-M001", 0.1095),
                ("BETA_D063-M001", 0.1184),
                ("CTR126T01-M001", 0.1033),
                ("TS_D252-M001", 0.0839),
                ("TWCV021-M001", 0.0666),
            ), },
        "S015": {  # weights from grp_tests M010
            "universe": instruments_universe,
            "mov_ave_win": 10,
            "factors_struct": (
                ("EXR042D3-M001", 0.0487),
                ("CTP063T10-M001", 0.2059),
                ("AMPH063T02-M001", 0.0659),
                ("SKEW126-M001", 0.1971),
                ("BASIS_D021-M001", 0.1286),
                ("BETA_D063-M001", 0.1144),
                ("CTR126T01-M001", 0.0914),
                ("TS_D252-M001", 0.0567),
                ("TWCV021-M001", 0.0914),
            ), },
    },
    "sigDyn": {
        "S100": {
            "universe": instruments_universe,
            "mov_ave_win": 10,
            "min_model_days": 126,
            "factors_struct": (
                ("EXR042D3-M001", 0.0464),
                ("CTP063T10-M001", 0.2008),
                ("AMPH063T02-M001", 0.0719),
                ("SKEW126-M001", 0.1995),
                ("BASIS_D021-M001", 0.1217),
                ("BETA_D063-M001", 0.1157),
                ("CTR126T01-M001", 0.0952),
                ("TS_D252-M001", 0.0594),
                ("TWCV021-M001", 0.0895),
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
