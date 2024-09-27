"""
Remove variables with specified threshold of NA values
Note: This was called `drop_na_columns` in cytominer for R
"""

from pycytominer.cyto_utils.features import infer_cp_features


def get_na_columns(population_df, features="infer", samples="all", cutoff=0.05):
    """Get features that have more NA values than cutoff defined

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    features : list, default "infer"
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume CellProfiler features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    samples : str, default "all"
        List of samples to perform operation on. The function uses a pd.DataFrame.query()
        function, so you should  structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes).
        If "all", use all samples to calculate.
    cutoff : float
        Exclude features that have a certain proportion of missingness

    Returns
    -------
    excluded_features : list of str
         List of features to exclude from the population_df.
    """

    if samples != "all":
        population_df.query(samples, inplace=True)

    if features == "infer":
        features = infer_cp_features(population_df)

    population_df = population_df.loc[:, features]

    num_rows = population_df.shape[0]
    na_prop_df = population_df.isna().sum() / num_rows

    na_prop_df = na_prop_df[na_prop_df > cutoff]
    return list(set(na_prop_df.index.tolist()))
