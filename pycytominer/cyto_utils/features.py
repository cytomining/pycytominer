"""
Utility function to manipulate cell profiler features
"""

import os
import pandas as pd

blacklist_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "blacklist_features.txt"
)


def get_blacklist_features(blacklist_file=blacklist_file, population_df=None):
    """
    Get a list of blacklist features

    Arguments:
    blacklist_file - file location of dataframe with features to exclude
    population_df - profile dataframe used to subset blacklist features [default: None]

    Return:
    list of features to exclude from downstream analysis
    """

    blacklist = pd.read_csv(blacklist_file)

    assert any(
        [x == "blacklist" for x in blacklist.columns]
    ), "one column must be named 'blacklist'"

    blacklist_features = blacklist.blacklist.to_list()
    if isinstance(population_df, pd.DataFrame):
        population_features = population_df.columns.tolist()
        blacklist_features = [x for x in blacklist_features if x in population_features]

    return blacklist_features
