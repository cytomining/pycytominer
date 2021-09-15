"""
Functions for aggregating "non-compartment" features
"""

import pandas as pd
import numpy as np


def aggregate_fields_count(image_df, strata, fields_of_view_feature):
    """Compute the number of fields per well and create a new column called Metadata_Site_Count

    Parameters
    ----------
    image_df : pandas.core.frame.DataFrame
        Image table dataframe which includes the strata and fields of view feature as columns.
    strata :  list of str
        The columns to groupby and aggregate single cells.
    fields_of_view_feature: str
        Name of the fields of the view column.

    Returns
    -------
    fields_count_df: pandas.core.frame.DataFrame
        DataFrame with the Metadata_Site_Count column.

    """

    fields_count_df = image_df.loc[:, list(np.union1d(strata, fields_of_view_feature))]

    fields_count_df = (
        fields_count_df.groupby(strata)[fields_of_view_feature]
        .count()
        .reset_index()
        .rename(columns={f"{fields_of_view_feature}": f"Metadata_Site_Count"})
    )

    return fields_count_df


def aggregate_image_count_features(df, image_features_df, image_cols, strata):
    """Aggregate the Count features in the Image table.

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        Dataframe of aggregated profiles.
    image_features_df : pandas.core.frame.DataFrame
        Image table dataframe with Count features
    image_cols : list of str
        Columns to select from the image table.
    strata :  list of str
        The columns to groupby and aggregate single cells.

    Returns
    -------
    df : pandas.core.frame.DataFrame
        DataFrame with aggregated Count features in the Image table.
    remove_cols : list of str
        Columns to remove from the image table before aggregating using aggregate_image_features()
    """

    count_features = list(
        image_features_df.columns[
            image_features_df.columns.str.startswith("Image_Count")
        ]
    )

    remove_cols = list(np.union1d(image_cols, count_features))
    keep_cols = list(np.union1d(strata, count_features))
    count_df = image_features_df[keep_cols].copy()
    count_df = count_df.groupby(strata, dropna=False).sum().reset_index()
    df = df.merge(count_df, on=strata, how="left")

    return df, remove_cols


def aggregate_image_features(
    df,
    image_features_df,
    image_feature_categories,
    image_cols,
    strata,
    aggregation_operation,
):
    """Aggregate the non-Count image features.

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        Dataframe of aggregated profiles.
    image_features_df : pandas.core.frame.DataFrame
        Image table dataframe with all the image_feature_category features.
    image_feature_categories : list of str
        List of categories of features from the image table to add to the profiles.
    image_cols : list of str
        Columns to select from the image table.
    strata :  list of str
        The columns to groupby and aggregate single cells.
    aggregation_operation : str
        Operation to perform image table feature aggregation.

    Returns
    -------
    df : pandas.core.frame.DataFrame
        DataFrame of aggregated image features.

    """

    if "Count" in image_feature_categories:
        df, remove_cols = aggregate_image_count_features(
            df, image_features_df, image_cols, strata
        )
    else:
        remove_cols = list(image_cols) + list(
            image_features_df.columns[
                image_features_df.columns.str.startswith("Image_Count")
            ]
        )

    if (
        not len(np.setdiff1d(image_feature_categories, ["Count"])) == 0
    ):  # The following block will not be run if the input image category is only "Count"
        for col in remove_cols:
            if col in image_features_df.columns:
                image_features_df = image_features_df.drop([col], axis="columns")

        image_features_df = image_features_df.groupby(strata, dropna=False)

        # Aggregate the other image features

        if aggregation_operation == "median":
            image_features_df = image_features_df.median().reset_index()
        else:
            image_features_df = image_features_df.mean().reset_index()

        df = df.merge(image_features_df, on=strata, how="left")

    return df
