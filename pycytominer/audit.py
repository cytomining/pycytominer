"""
Compare replicate correlation to random pairwise correlations.
"""

import numpy as np
import pandas as pd


def audit(
    profiles,
    operation="replicate_quality",
    groups=["Metadata_Well"],
    cor_method="pearson",
    quantile=0.95,
    output_file="none",
    samples="all",
    iterations=10,
):
    """
    Exclude features that have correlations above a certain threshold

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    operation - [default: "replicate_quality"] the operation to perform the audit.
    groups - [default: ["Metadata_Well"]] list of columns to identify replicates
    cor_method - [default: "pearson"] the method to obtain pairwise correlations.
    quantile - [default: 0.95] float indicating the quantile to calculate non-replicate
               correlation
    output_file - [default: "none"] if provided, will write profile audits to file
                  if not specified, will return the audits. We recommend
                  that this output file be suffixed with "_audit.csv".
    samples - [default: 'all'] string indicating which metadata column and values to
              use to subset the control samples are often used here.
              the format of this variable will be used in a pd.query() function. An
              example is "Metadata_treatment == 'control'" (include all quotes)
    iterations - [default: 10] Number of iterations of permutation test to estimate
                 null threshold estimation (higher = more robust)


    Return:
    Pandas DataFrame of audits or written to file
    """

    # Load Data
    if not isinstance(profiles, pd.DataFrame):
        try:
            profiles = pd.read_csv(profiles)
        except FileNotFoundError:
            raise FileNotFoundError("{} profile file not found".format(profiles))

    if samples != "all":
        profiles = profiles.query(samples)

    metadata_bool = profiles.columns.str.startswith("Metadata_")
    features = profiles.loc[:, ~metadata_bool].columns.tolist()
    metadata = profiles.loc[:, metadata_bool].columns.tolist()

    # Check input arguments
    assert all(
        [x in metadata for x in groups]
    ), "one of {} not found in metadata".format(groups)

    valid_operations = ["replicate_quality"]
    assert operation in valid_operations, "operation must be one of {}".format(
        valid_operations
    )

    assert 0 < quantile and 1 >= quantile, "quantile must be between 0 and 1"

    # Get pairwise correlation of replicates
    replicate_audit = (
        profiles.groupby(groups)
        .apply(
            lambda x: get_median_pairwise_correlation(
                x, features=features, method=cor_method
            )
        )
        .reset_index()
        .rename({0: "correlation"}, axis="columns")
        .assign(replicate_type="replicate")
    )

    # Now, shuffle the groupby columns and calculate pairwise correlations
    profile_shuff = profiles.copy()
    non_replicate_audit_iterations = []
    for i in range(0, iterations):
        profile_shuff.loc[:, groups] = (
            profile_shuff.loc[:, groups]
            .sample(frac=1, axis="rows")
            .reset_index(drop=True)
        )
        non_replicate_audit = profile_shuff.groupby(groups).apply(
            lambda x: get_median_pairwise_correlation(x, features=features)
        )
        non_replicate_audit_iterations.append(non_replicate_audit)

    non_replicate_audit = (
        pd.concat(non_replicate_audit_iterations)
        .reset_index()
        .rename({0: "correlation"}, axis="columns")
        .groupby(groups)
        .quantile(q=quantile)
        .reset_index()
        .assign(replicate_type="non_replicate")
    )

    audit_df = (
        pd.concat([replicate_audit, non_replicate_audit])
        .reset_index(drop=True)
        .assign(
            quantile=quantile,
            iterations=iterations,
            cor_method=cor_method,
            samples=samples,
            groups=",".join(groups),
        )
    )

    if output_file != "none":
        audit_df.to_csv(output_file, index=False)
    else:
        return audit_df


def get_median_pairwise_correlation(x, features, method="pearson"):
    """
    Obtain median pairwise correlation

    Usage: Applied to pandas groupby object
    """
    df = x.loc[:, features].transpose().corr(method=method)

    pairwise_cor = df.where(
        pd.np.tril(pd.np.ones(df.shape), k=-1).astype(bool)
    ).values.flatten()

    pairwise_cor = pairwise_cor[~np.isnan(pairwise_cor)]
    return np.median(pairwise_cor)
