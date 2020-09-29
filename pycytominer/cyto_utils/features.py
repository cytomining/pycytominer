"""
Utility function to manipulate cell profiler features
"""

import os
import pandas as pd

blocklist_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "blocklist_features.txt"
)


def get_blocklist_features(blocklist_file=blocklist_file, population_df=None):
    """
    Get a list of blocklist features

    Arguments:
    blocklist_file - file location of dataframe with features to exclude
    population_df - profile dataframe used to subset blocklist features [default: None]

    Return:
    list of features to exclude from downstream analysis
    """

    blocklist = pd.read_csv(blocklist_file)

    assert any(
        [x == "blocklist" for x in blocklist.columns]
    ), "one column must be named 'blocklist'"

    blocklist_features = blocklist.blocklist.to_list()
    if isinstance(population_df, pd.DataFrame):
        population_features = population_df.columns.tolist()
        blocklist_features = [x for x in blocklist_features if x in population_features]

    return blocklist_features


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


def infer_cp_features(population_df, compartments, metadata=False):
    """
    Given a dataframe, output features that we expect to be cell painting features
    """
    features = []
    for col in population_df.columns.tolist():
        for ind, val in enumerate(compartments):
            if col.startswith( compartments[ind].title() ):
                features.append(col)
                

    if metadata:
        features = population_df.columns[
            population_df.columns.str.startswith("Metadata_")
        ].tolist()

    assert (
        len(features) > 0
    ), "No CP features found. Are you sure this dataframe is from CellProfiler?"

    return features


def count_na_features(population_df, features):
    """
    Given a population dataframe and features, count how many nas per feature

    Arguments:
    population_df - pandas DataFrame storing profiles
    features - a list of features present in the population dataframe

    Return:
    Dataframe of NA counts per variable
    """

    return pd.DataFrame(population_df.loc[:, features].isna().sum(), columns=["num_na"])


def drop_outlier_features(
    population_df, features="infer", samples="all", outlier_cutoff=15
):
    """
    Exclude a feature if its min or max absolute value is greater than the threshold

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation features
    features - a list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that start with
               "Cells_", "Nuclei_", or "Cytoplasm_"
    samples - list samples to perform operation on
              [default: "all"] - if "all", use all samples to calculate
    outlier_cutoff - threshold to remove feature if absolute value is greater

    Return:
    list of features to exclude from the population_df
    """
    # Subset dataframe
    if samples != "all":
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
