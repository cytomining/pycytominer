"""
Functions for counting the number of fields and aggregating other images features
"""

import numpy as np
import pandas as pd

from pycytominer import aggregate


def aggregate_fields_count(
    image_df: pd.DataFrame, strata: list[str], fields_of_view_feature: str
):
    """Compute the number of fields per well and create a new column called Metadata_Site_Count

    Parameters
    ----------
    image_df : pd.DataFrame
        Image table dataframe which includes the strata and fields of view feature as columns.
    strata :  list of str
        The columns to groupby and aggregate single cells.
    fields_of_view_feature: str
        Name of the fields of the view column.

    Returns
    -------
    fields_count_df: pd.DataFrame
        DataFrame with the Metadata_Site_Count column.

    """

    fields_count_df = image_df.loc[:, list(np.union1d(strata, fields_of_view_feature))]

    fields_count_df = (
        fields_count_df.groupby(strata)[fields_of_view_feature]
        .count()
        .reset_index()
        .rename(columns={f"{fields_of_view_feature}": "Metadata_Site_Count"})
    )

    return fields_count_df


def aggregate_image_count_features(
    df: pd.DataFrame,
    image_features_df: pd.DataFrame,
    image_cols: list[str],
    strata: list[str],
    count_prefix: str = "Count",
):
    """Aggregate the Count features in the Image table.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe of aggregated profiles.
    image_features_df : pd.DataFrame
        Image table dataframe with Count features
    image_cols : list of str
        Columns to select from the image table.
    strata :  list of str
        The columns to groupby and aggregate single cells.
    count_prefix : str, default "Count"
        Prefix of the count columns in the image table.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with aggregated Count features in the Image table.
    remove_cols : list of str
        Columns to remove from the image table before aggregating using aggregate_image_features()
    """

    count_features = list(
        image_features_df.columns[
            image_features_df.columns.str.startswith("Metadata_" + str(count_prefix))
        ]
    )

    remove_cols = list(np.union1d(image_cols, count_features))
    keep_cols = list(np.union1d(strata, count_features))
    count_df = image_features_df[keep_cols].copy()
    count_df = count_df.groupby(strata, dropna=False).sum().reset_index()
    df = df.merge(count_df, on=strata, how="left")

    return df, remove_cols


def aggregate_image_features(
    df: pd.DataFrame,
    image_features_df: pd.DataFrame,
    image_feature_categories: list[str],
    image_cols: list[str],
    strata: list[str],
    aggregation_operation: str,
    count_prefix: str = "Count",
):
    """Aggregate the non-Count image features.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe of aggregated profiles.
    image_features_df : pd.DataFrame
        Image table dataframe with all the image_feature_category features.
    image_feature_categories : list of str
        List of categories of features from the image table to add to the profiles.
    image_cols : list of str
        Columns to select from the image table.
    strata :  list of str
        The columns to groupby and aggregate single cells.
    aggregation_operation : str
        Operation to perform image table feature aggregation.
    count_prefix : str, default "Count"
        Prefix of the count columns in the image table.

    Returns
    -------
    df : pd.DataFrame
        DataFrame of aggregated image features.

    """

    # Aggregate image count features
    if count_prefix in image_feature_categories:
        df, remove_cols = aggregate_image_count_features(
            df, image_features_df, image_cols, strata
        )
    else:
        remove_cols = list(image_cols) + list(
            image_features_df.columns[
                image_features_df.columns.str.startswith(f"Metadata_{count_prefix}")
            ]
        )

    # Aggregate other image features
    if len(np.setdiff1d(image_feature_categories, [count_prefix])) != 0:
        image_features_df = image_features_df.drop(
            remove_cols, axis="columns", errors="ignore"
        )
        features = list(np.setdiff1d(list(image_features_df.columns), strata))
        result = aggregate.aggregate(
            population_df=image_features_df,
            strata=strata,
            features=features,
            operation=aggregation_operation,
        )

        # check that aggregate returned a dataframe
        if not isinstance(result, pd.DataFrame):
            raise RuntimeError("aggregate() did not return a DataFrame")

        df = df.merge(result, on=strata, how="left")

    return df
