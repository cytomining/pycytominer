"""
Utility function to manipulate cell profiler features
"""

import os
import pandas as pd
from typing import Union

blocklist_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "blocklist_features.txt"
)


def get_blocklist_features(blocklist_file=blocklist_file, population_df=None):
    """Get a list of blocklist features.

    Parameters
    ----------
    blocklist_file : path-like object
        Location of the dataframe with features to exclude.
    population_df : pandas.core.frame.DataFrame, optional
        Profile dataframe used to subset blocklist features.

    Returns
    -------
    blocklist_features : list of str
        Features to exclude from downstream analysis.
    """

    blocklist = pd.read_csv(blocklist_file)

    assert any(  # noqa: S101
        x == "blocklist" for x in blocklist.columns
    ), "one column must be named 'blocklist'"

    blocklist_features = blocklist.blocklist.to_list()
    if isinstance(population_df, pd.DataFrame):
        population_features = population_df.columns.tolist()
        blocklist_features = [x for x in blocklist_features if x in population_features]

    return blocklist_features


def label_compartment(cp_features, compartment, metadata_cols):
    """Assign compartment label to each features as a prefix.

    Parameters
    ----------
    cp_features : list of str
        All features being used.
    compartment : str
       Measured compartment.
    metadata_cols : list
        Columns that should be considered metadata.

    Returns
    -------
    cp_features: list of str
        Recoded column names with appropriate metadata and compartment labels.
    """

    compartment = compartment.Title()
    avail_compartments = ["Cells", "Cytoplasm", "Nuceli", "Image", "Barcode"]

    assert (  # noqa: S101
        compartment in avail_compartments
    ), f"provide valid compartment. One of: {avail_compartments}"

    cp_features = [
        f"Metadata_{x}" if x in metadata_cols else f"{compartment}_{x}"
        for x in cp_features
    ]

    return cp_features


def infer_cp_features(
    population_df,
    compartments=["Cells", "Nuclei", "Cytoplasm"],
    metadata=False,
    image_features=False,
):
    """Given CellProfiler output data read as a DataFrame, output feature column names as a list.

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame from which features are to be inferred.
    compartments : list of str, default ["Cells", "Nuclei", "Cytoplasm"]
        Compartments from which Cell Painting features were extracted.
    metadata : bool, default False
        Whether or not to infer metadata features.
        If metadata is set to True, find column names that begin with the `Metadata_` prefix.
        This convention is expected by CellProfiler defaults.
    image_features : bool, default False
        Whether or not the profiles contain image features.

    Returns
    -------
    features: list of str
        List of Cell Painting features.
    """

    compartments = convert_compartment_format_to_list(compartments)
    compartments = [x.title() for x in compartments]

    if image_features:
        compartments = list({"Image", *compartments})

    features = []
    for col in population_df.columns.tolist():
        if any(col.startswith(x.title()) for x in compartments):
            features.append(col)

    if metadata:
        features = population_df.columns[
            population_df.columns.str.startswith("Metadata_")
        ].tolist()

    if len(features) == 0:
        raise ValueError(
            "No features or metadata found. Pycytominer expects CellProfiler column names by default. "
            "If you're using non-CellProfiler data, please do not 'infer' features. "
            "Instead, check if the function has a `features` or `meta_features` parameter, and input column names manually."
        )

    return features


def count_na_features(population_df, features):
    """Given a population dataframe and features, count how many nas per feature.

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame of profiles.
    features : list of str
        Features present in the population dataframe.

    Returns
    -------
    Dataframe of NA counts per feature
    """

    return pd.DataFrame(population_df.loc[:, features].isna().sum(), columns=["num_na"])


def drop_outlier_features(
    population_df, features="infer", samples="all", outlier_cutoff=500
):
    """Exclude a feature if its min or max absolute value is greater than the threshold.

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    features : list of str or str, default "infer"
        Features present in the population dataframe. If "infer",
        then assume CellProfiler feature conventions
        (start with "Cells_", "Nuclei_", or "Cytoplasm_")
    samples : str, default "all"
        List of samples to perform operation on. The function uses a pd.DataFrame.query()
        function, so you should  structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes).
        If "all", use all samples to calculate.
    outlier_cutoff : int or float, default 500
    see https://github.com/cytomining/pycytominer/issues/237 for details.
        Threshold to remove features if absolute values is greater

    Returns
    -------
    outlier_features: list of str
        Features greater than the threshold.
    """

    # Subset dataframe
    if samples != "all":
        population_df.query(samples, inplace=True)

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


def convert_compartment_format_to_list(
    compartments: Union[list[str], str],
) -> list[str]:
    """Converts compartment to a list.

    Parameters
    ----------
    compartments : list of str or str
        Cell Painting compartment(s).

    Returns
    -------
    compartments : list of str
        List of Cell Painting compartments.
    """

    return compartments if isinstance(compartments, list) else [compartments]
