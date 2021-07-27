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

    Args:
        population_df: Panda Dataframe which contains all measurement data.
        perturb_list: The list of unique perturbations corresponding to the rows in population_df. For example,
            perturb1_well1 and perturb1_well2 would both be "perturb1".
        features: List of features. Can be inferred or manually supplied.
        samples: Which rows to use from population_df. Use "all" if applicable.
        stdev_cutoff: Maximum value for stdev for a given feature to be kept.

    Returns: A list of features to be removed, due to having too high standard deviation within replicate groups.

    """
    # Subset dataframe
    if samples != "all":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = infer_cp_features(population_df)

    population_df = population_df.loc[:, features]

    # Label each row with the identity of its perturbation group
    population_df['group'] = perturb_list

    # List of features to remove
    features_to_remove = []

    # Loop through every feature, and find the mean stdev for each feature within a perturbation group. Remove if
    # it's too high.
    for feature in features:
        feature_stdevs = []  # Feature stdev for each perturbation group

        # Populate feature_stdevs with the standard deviations of the feature for every perturbation group
        for perturb in perturb_list:
            temp = population_df[population_df['group'] == perturb][feature]
            temp_values = list(temp.values)
            # Check to make sure there are enough data points to calculate stdev
            assert (~np.isnan(
                temp_values)).sum() >= 2, "Feature {} for perturbation group {} does not have sufficient non-NaN " \
                                          "values. Require at least 2 to calculate standard deviation".\
                format(feature, perturb)

            feature_stdevs.append(np.std(temp_values))

        # Find mean feature standard deviation. If it's too high, add to the exclude list.
        feature_mean_stdev = np.mean(feature_stdevs)
        if feature_mean_stdev > stdev_cutoff:
            features_to_remove.append(feature)

    return features_to_remove
