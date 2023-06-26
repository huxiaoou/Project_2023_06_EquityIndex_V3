from project_config import instruments_universe

signals_structure = {
    "sigFix": {
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
    },
    "sigDyn": {
        "S100": {
            "universe": instruments_universe,
            "factors": (
                "EXR042D3-M005",
                "CTP063T10-M005",
                "AMPH063T02-M005",
                "SKEW126-M005",
                "BASIS_D021-M005",
                "BETA_D063-M005",
                "CTR126T01-M005",
                "TS_D252-M005",
                "TWCV021-M005",
            )
        },
    }
}

fix_sids = list(signals_structure["sigFix"].keys())
dyn_sids = list(signals_structure["sigDyn"].keys())

if __name__ == "__main__":
    print(fix_sids)
    print(dyn_sids)
