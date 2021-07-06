"""
Remove noisy features, as defined by features with excessive standard deviation within the same perturbation group.
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils import infer_cp_features

def noise_removal(
    population_df, features="infer", samples="all", stdev_cutoff=0.8,
):
    """
    Removes features which are insufficiently reproducible within the same perturbation group.
    Args:
        population_df:
        features:
        samples:
        stdev_cutoff:

    Returns:

    """
    # Subset dataframe
    if samples != "all":
        population_df = population_df.loc[samples, :]

    if features == "infer":
        features = infer_cp_features(population_df)

    population_df = population_df.loc[:, features]

    return population_df

df = pd.read_csv('C:/Users/Ruifan/neuronal-cell-painting/1.run-workflows/profiles/NCP_STEM_1/BR_NCP_STEM_1/BR_NCP_STEM_1_normalized.csv.gz')
print(noise_removal(df))
