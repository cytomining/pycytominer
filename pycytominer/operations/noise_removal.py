"""
Remove noisy features, as defined by features with excessive standard deviation within the same perturbation group.
"""

from pycytominer.cyto_utils.features import infer_cp_features


def noise_removal(
    population_df,
    noise_removal_perturb_groups,
    features="infer",
    samples="all",
    noise_removal_stdev_cutoff=0.8,
):
    """

    Parameters
    ----------
    population_df : pandas.core.frame.DataFrame
        DataFrame that includes metadata and observation features.
    noise_removal_perturb_groups : list or array of str
        The list of unique perturbations corresponding to the rows in population_df. For example,
        perturb1_well1 and perturb1_well2 would both be "perturb1".
    features : list, default "infer"
        A list of strings corresponding to feature measurement column names in the
        `population_df` DataFrame. All features listed must be found in `population_df`.
        Defaults to "infer". If "infer", then assume CellProfiler features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    samples : str, default "all"
        List of samples to perform operation on. The function uses a pd.DataFrame.query()
        function, so you should  structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes).
        If "all", use all samples to calculate.
    noise_removal_stdev_cutoff : float
        Maximum mean stdev value for a feature to be kept, with features grouped according to the perturbations in
        noise_removal_perturbation_groups.

    Returns
    -------
    to_remove : list
        A list of features to be removed, due to having too high standard deviation within replicate groups.

    """

    # Subset the DataFrame if specific samples are specified
    # If "all", use the entire DataFrame without subsetting
    if samples != "all":
        # Using pandas query to filter rows based on the conditions provided in the
        # samples parameter
        population_df = population_df.query(expr=samples)

    # Infer  CellProfiler features if 'features' is set to 'infer'
    if features == "infer":
        # Infer CellProfiler features
        features = infer_cp_features(population_df)
        # Subset the DataFrame to only include inferred CellProfiler features

    # if a Metadata columns name is specified, use that as the perturb groups
    if isinstance(noise_removal_perturb_groups, str):
        # Check if the column exists
        if noise_removal_perturb_groups not in population_df.columns:
            raise ValueError(
                'f"{perturb} not found. Are you sure it is a metadata column?'
            )
        # Assign the group info to the specified column
        group_info = population_df[noise_removal_perturb_groups]

    # Otherwise, the user specifies a list of perturbs
    elif isinstance(noise_removal_perturb_groups, list):
        # Check if the length of the noise_removal_perturb_groups is the same as the
        # number of rows in the df
        if not len(noise_removal_perturb_groups) == len(population_df):
            raise ValueError(
                f"The length of input list: {len(noise_removal_perturb_groups)} is not equivalent to your "
                f"data: {population_df.shape[0]}"
            )
        # Assign the group info to the the noise_removal_perturb_groups
        group_info = noise_removal_perturb_groups
    else:
        # Raise an error if the input is not a list or a string
        raise TypeError(
            "noise_removal_perturb_groups must be a list corresponding to row perturbations or a str \
                        specifying the name of the metadata column."
        )

    # Subset and df and assign each row with the identity of its perturbation group
    population_df = population_df.loc[:, features]
    population_df = population_df.assign(group_id=group_info)

    # Get the standard deviations of features within each group then calculate the mean
    # of these standard deviations.
    # This tells us how much the standard deviation of each feature varies within each
    # perturbation group.
    stdev_means_df = population_df.groupby("group_id").std(ddof=0).mean()

    # With the stdev_means_df, we can identify features that have a mean stdev greater than
    # the cutoff
    # These features are considered to have too much variation within replicate groups
    # and are removed. This returns a list of features to remove.
    to_remove = stdev_means_df[
        stdev_means_df > noise_removal_stdev_cutoff
    ].index.tolist()

    return to_remove
