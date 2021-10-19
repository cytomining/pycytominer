"""
Functions for counting the number of fields and aggregating other images features
"""

import pandas as pd
import numpy as np
from pycytominer import aggregate


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


def aggregate_image_count_features(
    df, image_features_df, image_cols, strata, count_prefix="Count"
):
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
    count_prefix : str, default "Count"
        Prefix of the count columns in the image table.

    Returns
    -------
    df : pandas.core.frame.DataFrame
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
    df,
    image_features_df,
    image_feature_categories,
    image_cols,
    strata,
    aggregation_operation,
    count_prefix="Count",
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
    count_prefix : str, default "Count"
        Prefix of the count columns in the image table.

    Returns
    -------
    df : pandas.core.frame.DataFrame
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
    if not len(np.setdiff1d(image_feature_categories, [count_prefix])) == 0:
        image_features_df = image_features_df.drop(
            remove_cols, axis="columns", errors="ignore"
        )
        features = list(np.setdiff1d(list(image_features_df.columns), strata))
        image_features_df = aggregate.aggregate(
            population_df=image_features_df,
            strata=strata,
            features=features,
            operation=aggregation_operation,
        )

        df = df.merge(image_features_df, on=strata, how="left")

    return df
