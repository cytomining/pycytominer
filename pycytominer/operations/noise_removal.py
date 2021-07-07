"""
Remove noisy features, as defined by features with excessive standard deviation within the same perturbation group.
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils import infer_cp_features


def noise_removal(
        population_df, perturb_list, features="infer", samples="all", stdev_cutoff=0.8,
):
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
            feature_stdevs.append(np.std(temp_values))

        # Find mean feature standard deviation. If it's too high, add to the exclude list.
        feature_mean_stdev = np.mean(feature_stdevs)
        if feature_mean_stdev > stdev_cutoff:
            features_to_remove.append(feature)

    return features_to_remove


# df = pd.read_csv(
#     'C:/Users/Ruifan/neuronal-cell-painting/1.run-workflows/profiles/NCP_STEM_1/BR_NCP_STEM_1/BR_NCP_STEM_1_normalized.csv.gz',
#     index_col=0)
# df = df.dropna(axis=1, how='any')
# groups = pd.read_csv(
#     'C:/Users/Ruifan/neuronal-cell-painting/1.run-workflows/profiles/NCP_STEM_1/BR_NCP_STEM_1/BR_NCP_STEM_1_groups.csv',
#     index_col=0)['group'].tolist()
#
# print(groups)
# print(noise_removal(df, groups))
