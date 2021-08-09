"""
Remove noisy features, as defined by features with excessive standard deviation within the same perturbation group.
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils import infer_cp_features


def noise_removal(
        population_df, perturb_list, features, samples="all", stdev_cutoff=0.8,
):
    """

    Parameters
    ----------
    population_df: pandas.core.frame.DataFrame
        Dataframe which contains all measurement data.
    perturb_list: list or array of str
        The list of unique perturbations corresponding to the rows in population_df. For example,
        perturb1_well1 and perturb1_well2 would both be "perturb1".
    features: list of str, default "infer"
        List of features. Can be inferred or manually supplied.
    samples: list of str, default "infer"
        Which rows to use from population_df. Use "all" if applicable.
    stdev_cutoff: float
        Maximum value for stdev for a given feature to be kept.

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

    population_df = population_df.loc[:, features]

    # Label each row with the identity of its perturbation group
    population_df['group'] = perturb_list

    # Get the standard deviations of features within each group
    stdev_df = population_df.groupby('group').apply(lambda x: np.std(x))
    stdev_means_df = stdev_df.mean()
    to_remove = stdev_means_df[stdev_means_df > stdev_cutoff].index.tolist()

    return to_remove
