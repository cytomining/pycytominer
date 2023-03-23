"""
Measure replicate correlation of samples across features
"""

import pandas as pd


def replicate_correlation(sample_df, variables, strata, n_replicates, batch_groups):
    """
    Measure the correlation of replicate samples across batches for each variable.
    Will be used to remove variables that are unstable

    Arguments:
    sample_df - pandas DataFrame storing, typically, a sampling of the full data
    variables - list specifying cell painting features
    strata - list specifying grouping variables
    n_replicates - int representing the number of replicates to expect. Specify "none"
                   to not subset by replicate number (this would happen if some samples
                   had different number of replicates)
    batch_groups - list specifying the column (or columns) that indicate batch

    Output:
    A pandas dataframe of variable quality metrics
    """

    # Process the input sample to identify replicate samples
    count_df = sample_df.groupby(strata).count().iloc[:, 0].reset_index()
    count_df.columns = strata + ["n_rep"]

    # Merge count back to sample data frame
    sample_df = sample_df.merge(count_df, how='left', left_on=strata, right_on=strata)

    # Filter the sample dataframe by expected number of replicates
    if n_replicates != "none":
        sample_df = sample_df.query("n_rep == @num_replicates")
