"""
Normalize observation variables based on specified normalization method
"""

import pandas as pd
from sklearn.preprocessing import StandardScaler, RobustScaler


def normalize(
    population_df, variables, meta_variables="none", samples="all", method="standardize"
):
    """
    Normalize variables

    Arguments:
    population_df - pandas DataFrame that includes metadata and observation variables
    variables - list of cell painting features
    meta_variables - if specified, then output these with specified variables
                     [default: "none"]
    samples - string indicating which metadata column and values to use to subset
              the control samples are often used here [default: 'all']
              the format of this variable will be used in a pd.query() function. An
              example is "Metadata_treatment == 'control'" (include all quotes)
    method - string indicating how the dataframe will be normalized
             [default: 'standardize']

    Return:
    A normalized DataFrame
    """

    # Define which scaler to use
    method = method.lower()

    if method == "standardize":
        scaler = StandardScaler()
    elif method == "robustize":
        scaler = RobustScaler()
    else:
        ValueError(
            "Undefined method {}. Use one of ['standardize', 'robustize']".format(
                method
            )
        )

    # Separate out the features and meta
    feature_df = population_df.loc[:, variables]
    if meta_variables == "none":
        meta_df = population_df.drop(variables, axis='columns')
    else:
        meta_df = population_df.loc[:, meta_variables]

    # Fit the sklearn scaler
    if samples == "all":
        fitted_scaler = scaler.fit(feature_df)
    else:
        # Subset to only the features measured in the sample query
        fitted_scaler = scaler.fit(population_df.query(samples).loc[:, variables])

    # Scale the feature dataframe
    feature_df = pd.DataFrame(
        fitted_scaler.transform(feature_df),
        columns=feature_df.columns,
        index=feature_df.index,
    )

    return meta_df.merge(feature_df, left_index=True, right_index=True)
