"""
Normalize observation features based on specified normalization method
"""

import pandas as pd
from sklearn.preprocessing import StandardScaler, RobustScaler


def normalize(
    profiles,
    features="none",
    meta_features="none",
    samples="all",
    method="standardize",
    output_file="none",
):
    """
    Normalize features

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    features - list of cell painting features [default: "infer"]
               if "infer", then assume cell painting features are those that do not
               start with "Metadata_"
    meta_features - if specified, then output these with specified features
                    [default: "none"]
    samples - string indicating which metadata column and values to use to subset
              the control samples are often used here [default: 'all']
              the format of this variable will be used in a pd.query() function. An
              example is "Metadata_treatment == 'control'" (include all quotes)
    method - string indicating how the dataframe will be normalized
             [default: 'standardize']
    output_file - [default: "none"] if provided, will write annotated profiles to file
                  if not specified, will return the annotated profiles. We recommend
                  that this output file be suffixed with "_normalized.csv".

    Return:
    A normalized DataFrame
    """
    # Load Data
    if not isinstance(profiles, pd.DataFrame):
        try:
            profiles = pd.read_csv(profiles)
        except FileNotFoundError:
            raise FileNotFoundError("{} profile file not found".format(profiles))

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

    if features == "infer":
        features = [
            x for x in profiles.columns.tolist() if not x.startswith("Metadata_")
        ]

    # Separate out the features and meta
    feature_df = profiles.loc[:, features]
    if meta_features == "none":
        meta_df = profiles.drop(features, axis="columns")
    else:
        meta_df = profiles.loc[:, meta_features]

    # Fit the sklearn scaler
    if samples == "all":
        fitted_scaler = scaler.fit(feature_df)
    else:
        # Subset to only the features measured in the sample query
        fitted_scaler = scaler.fit(profiles.query(samples).loc[:, features])

    # Scale the feature dataframe
    feature_df = pd.DataFrame(
        fitted_scaler.transform(feature_df),
        columns=feature_df.columns,
        index=feature_df.index,
    )

    normalized = meta_df.merge(feature_df, left_index=True, right_index=True)

    if output_file != "none":
        normalized.to_csv(output_file, index=False)
    else:
        return normalized
