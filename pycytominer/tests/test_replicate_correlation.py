import pandas as pd

data_one_df = pd.DataFrame(
    {
        "Metadata_plate": ["a", "a", "a", "a", "b", "b", "b", "b"],
        "Metadata_treatment": ["drug", "drug", "control", "control", "drug", "drug", "control", "control"],
        "Metadata_batch": ["day1", "day1", "day1", "day1", "day1", "day1", "day1", "day1"],
        "x": [1, 2, 8, 2, 5, 5, 5, 1],
        "y": [3, 1, 7, 4, 5, 9, 6, 1],
        "z": [1, 8, 2, 5, 6, 22, 2, 2],
        "zz": [14, 46, 1, 6, 30, 100, 2, 2],

    }
)

data_two_df = pd.DataFrame(
    {
        "Metadata_plate": ["a", "a", "a", "a", "b", "b", "b", "b"],
        "Metadata_treatment": ["drug", "drug", "control", "control", "drug", "drug", "control", "control"],
        "Metadata_batch": ["day2", "day2", "day2", "day2", "day2", "day2", "day2", "day2"],
        "x": [x * 0.5 for x in [1, 2, 8, 2, 5, 5, 5, 1]],
        "y": [x  - (1 * 0.2) for x in [3, 1, 7, 4, 5, 9, 6, 1]],
        "z": [x * 0.1 for x in [1, 8, 2, 5, 6, 22, 2, 2]],
        "zz": [x * 1.1 for x in [14, 46, 1, 6, 30, 100, 2, 2]],

    }
)


data_df = pd.concat([data_one_df, data_two_df]).reset_index(drop=True)
