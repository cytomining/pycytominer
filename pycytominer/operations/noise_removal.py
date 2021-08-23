"""
Remove noisy features, as defined by features with excessive standard deviation within the same perturbation group.
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils import infer_cp_features


def noise_removal(
    population_df,
    noise_removal_perturb_groups,
    features,
    samples="all",
    noise_removal_stdev_cutoff=0.8,
):
    """

    Parameters
    ----------
    population_df: pandas.core.frame.DataFrame
        Dataframe which contains all measurement data and optionally metadata such as the identity of the perturbation
        group for each row.
    noise_removal_perturb_groups: list or array of str
        The list of unique perturbations corresponding to the rows in population_df. For example,
        perturb1_well1 and perturb1_well2 would both be "perturb1".
    features: list of str, default "infer"
        List of features. Can be inferred or manually supplied.
    samples: list of str, default "infer"
        Which rows to use from population_df. Use "all" if applicable.
    noise_removal_stdev_cutoff: float
        Maximum mean stdev value for a feature to be kept, with features grouped according to the perturbations in
        noise_removal_perturbation_groups.

    Returns
    ----------
    list
        A list of features to be removed, due to having too high standard deviation within replicate groups.

    """
    # Subset dataframe
    if samples != "all":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = infer_cp_features(population_df)

    # If a metadata column name is specified, use that as the perturb groups
    if isinstance(noise_removal_perturb_groups, str):
        assert noise_removal_perturb_groups in population_df.columns, (
            'f"{perturb} not found. Are you sure it is a ' "metadata column?"
        )
        group_info = population_df[noise_removal_perturb_groups]
    # Otherwise, the user specifies a list of perturbs
    elif isinstance(noise_removal_perturb_groups, list):
        assert len(noise_removal_perturb_groups) == len(population_df), (
            f"The length of input list: {len(noise_removal_perturb_groups)} is not equivalent to your "
            f"data: {population_df.shape[0]}"
        )
        group_info = noise_removal_perturb_groups
    else:
        raise TypeError(
            "noise_removal_perturb_groups must be a list corresponding to row perturbations or a str \
                        specifying the name of the metadata column."
        )
    # Subset and df and assign each row with the identity of its perturbation group
    population_df = population_df.loc[:, features]
    population_df = population_df.assign(group_id=group_info)

    # Get the standard deviations of features within each group
    stdev_means_df = population_df.groupby("group_id").apply(lambda x: np.std(x)).mean()

    # Identify noisy features with a greater mean stdev within perturbation group than the threshold
    to_remove = stdev_means_df[
        stdev_means_df > noise_removal_stdev_cutoff
    ].index.tolist()

    return to_remove
