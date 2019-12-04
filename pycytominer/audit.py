"""
Compare replicate correlation to random pairwise correlations.
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils.output import output
from pycytominer.cyto_utils.features import infer_cp_features


def audit(
    profiles,
    operation="replicate_quality",
    audit_groups=["Metadata_Well"],
    cor_method="pearson",
    quantile=0.95,
    output_file="none",
    samples="all",
    cp_features="infer",
    metadata_features="infer",
    iterations=10,
    compression=None,
    float_format=None,
    audit_resolution="median",
):
    """
    Exclude features that have correlations above a certain threshold

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    operation - [default: "replicate_quality"] the operation to perform the audit.
    audit_groups - [default: ["Metadata_Well"]] list of columns to identify replicates
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
    cp_features - list of cell painting features [default: "infer"]
                  if "infer", then assume cell painting features are those that start
                  with "Cells", "Nuclei", or "Cytoplasm"
    metadata_features - list of metadata cell paiting features [default: "infer"]
    iterations - [default: 10] Number of iterations of permutation test to estimate
                 null threshold estimation (higher = more robust)
    compression - the mechanism to compress [default: "gzip"]
    float_format - decimal precision to use in writing output file [default: None]
                   For example, use "%.3g" for 3 decimal precision.
    audit_resolution - a string indicating level of audit to return [default: "median"]

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

    if cp_features == "infer":
        cp_features = infer_cp_features(profiles)

    if metadata_features == "infer":
        metadata_features = infer_cp_features(profiles, metadata=True)

    # Check input arguments
    assert all(
        [x in metadata_features for x in audit_groups]
    ), "one of {} not found in metadata".format(audit_groups)

    valid_operations = ["replicate_quality"]
    assert operation in valid_operations, "operation must be one of {}".format(
        valid_operations
    )

    valid_audit_resolutions = ["median", "full"]
    assert (
        audit_resolution in valid_audit_resolutions
    ), "audit_resolution must be one of {}".format(valid_audit_resolutions)

    audit_package = {
        "profiles": profiles,
        "audit_groups": audit_groups,
        "samples": samples,
        "cp_features": cp_features,
        "metadata_features": metadata_features,
        "cor_method": cor_method,
        "iterations": iterations,
        "quantile": quantile,
    }

    if audit_resolution == "median":
        assert 0 < quantile and 1 >= quantile, "quantile must be between 0 and 1"
        audit_df = _audit_resolution_median(audit_package)
    elif audit_resolution == "full":
        audit_df = _audit_resolution_full(audit_package)

    if output_file != "none":
        output(
            df=audit_df,
            output_filename=output_file,
            compression=compression,
            float_format=float_format,
        )
    else:
        return audit_df


def _audit_resolution_median(audit_package):
    """
    Internal method of performing a median audit
    """
    profiles = audit_package["profiles"]
    audit_groups = audit_package["audit_groups"]
    samples = audit_package["samples"]
    cp_features = audit_package["cp_features"]
    metadata_features = audit_package["metadata_features"]
    cor_method = audit_package["cor_method"]
    iterations = audit_package["iterations"]
    samples = audit_package["samples"]
    quantile = audit_package["quantile"]

    # Get pairwise correlation of replicates
    replicate_audit = (
        profiles.groupby(audit_groups)
        .apply(
            lambda x: get_median_pairwise_correlation(
                x, features=cp_features, method=cor_method
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
        profile_shuff.loc[:, audit_groups] = (
            profile_shuff.loc[:, audit_groups]
            .sample(frac=1, axis="rows")
            .reset_index(drop=True)
        )
        non_replicate_audit = profile_shuff.groupby(audit_groups).apply(
            lambda x: get_median_pairwise_correlation(x, features=cp_features)
        )
        non_replicate_audit_iterations.append(non_replicate_audit)

    non_replicate_audit = (
        pd.concat(non_replicate_audit_iterations)
        .reset_index()
        .rename({0: "correlation"}, axis="columns")
        .groupby(audit_groups)
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
            groups=",".join(audit_groups),
        )
    )

    return audit_df


def _audit_resolution_full(audit_package):
    """
    Internal method of performing a full pairwise audit.
    This will calculate a full correlation matrix given input profiles
    """
    profiles = audit_package["profiles"]
    audit_groups = audit_package["audit_groups"]
    samples = audit_package["samples"]
    cp_features = audit_package["cp_features"]
    metadata_features = audit_package["metadata_features"]
    cor_method = audit_package["cor_method"]
    iterations = audit_package["iterations"]
    samples = audit_package["samples"]
    quantile = audit_package["quantile"]

    # Step 1: Get correlation matrix
    profile_cor_df = profiles.loc[:, cp_features].transpose().corr()

    # Step 2: Align with metadata and remove upper triangle
    column_audit_groups = ["column_match"] + audit_groups
    profile_cor_meta_df = (
        pd.concat([profiles.loc[:, audit_groups], profile_cor_df], axis="columns")
        .assign(column_match=range(0, profile_cor_df.shape[1]))
        .set_index(column_audit_groups)
        .where(pd.np.tril(pd.np.ones(profile_cor_df.shape), k=0).astype(bool))
    )

    # Step 3: Align metadata to columns of correlation matrix
    meta_profile_cor_df = (
        profile_cor_meta_df.reset_index()
        .melt(
            id_vars=column_audit_groups,
            value_vars=profile_cor_meta_df.columns.tolist(),
            value_name="pairwise_correlation",
            var_name="row_match",
        )
        .dropna()
    )

    # Get metadata information
    metadata_audit_info_df = meta_profile_cor_df.loc[
        :, column_audit_groups
    ].drop_duplicates()

    # Merge metadata with row match
    complete_audit_df = meta_profile_cor_df.merge(
        metadata_audit_info_df,
        left_on="row_match",
        right_on="column_match",
        how="left",
        suffixes=["_pair_a", "_pair_b"],
    )

    # Drop self correlations
    complete_audit_df = complete_audit_df.loc[
        ~(
            complete_audit_df.column_match_pair_a
            == complete_audit_df.column_match_pair_b
        ),
        :,
    ]

    return complete_audit_df


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
