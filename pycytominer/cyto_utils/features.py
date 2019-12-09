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


def label_compartment(cp_features, compartment, metadata_cols):
    """
    Assign compartment label to each features as a prefix

    Arguments:
    cp_features - list of all features being used
    compartment - a string indicating the measured compartment
    metadata_cols - a list indicating which columns should be considered metadata

    Return:
    Recoded column names with appopriate metadata and compartment labels
    """

    compartment = compartment.Title()
    avail_compartments = ["Cells", "Cytoplasm", "Nuceli", "Image", "Barcode"]

    assert (
        compartment in avail_compartments
    ), "provide valid compartment. One of: {}".format(avail_compartments)

    cp_features = [
        "Metadata_{}".format(x)
        if x in metadata_cols
        else "{}_{}".format(compartment, x)
        for x in cp_features
    ]

    return cp_features


def infer_cp_features(population_df, metadata=False):
    """
    Given a dataframe, output features that we expect to be cell painting features
    """
    features = [
        x
        for x in population_df.columns.tolist()
        if (
            x.startswith("Cells_")
            | x.startswith("Nuclei_")
            | x.startswith("Cytoplasm_")
        )
    ]

    if metadata:
        features = population_df.columns[
            population_df.columns.str.startswith("Metadata_")
        ].tolist()

    assert (
        len(features) > 0
    ), "No CP features found. Are you sure this dataframe is from CellProfiler?"

    return features


def drop_outlier_features(
    population_df, features="infer", samples="none", outlier_cutoff=15
):
    """
    Exclude a feature if its min or max absolute value is greater than the threshold

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features
    features - a list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that start with
               "Cells_", "Nuclei_", or "Cytoplasm_"
    samples - list samples to perform operation on
              [default: "none"] - if "none", use all samples to calculate
    outlier_cutoff - threshold to remove feature if absolute value is greater

    Return:
    list of features to exclude from the population_df
    """
    # Subset dataframe
    if samples != "none":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = infer_cp_features(population_df)
        population_df = population_df.loc[:, features]
    else:
        population_df = population_df.loc[:, features]

    max_feature_values = population_df.max().abs()
    min_feature_values = population_df.min().abs()

    outlier_features = max_feature_values[
        (max_feature_values > outlier_cutoff) | (min_feature_values > outlier_cutoff)
    ].index.tolist()

    return outlier_features
