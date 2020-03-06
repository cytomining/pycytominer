import os
import random
import numpy as np
import pandas as pd
from scipy.stats import median_absolute_deviation
from pycytominer.cyto_utils.transform import Whiten, RobustMAD

random.seed(123)

a_feature = random.sample(range(1, 100), 10)
b_feature = random.sample(range(1, 100), 10)
c_feature = random.sample(range(1, 100), 10)
d_feature = random.sample(range(1, 100), 10)

data_df = pd.DataFrame(
    {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
).reset_index(drop=True)


def test_whiten():
    """
    Testing the base covariance pycytominer function
    """
    scaler = Whiten()
    scaler = scaler.fit(data_df)
    transform_df = scaler.transform(data_df)

    # The transfomed data is expected to have uncorrelated samples
    result = pd.DataFrame(np.cov(np.transpose(transform_df))).round().sum().sum()
    expected_result = data_df.shape[1]

    assert int(result) == expected_result


def test_whiten_no_center():
    """
    Testing the base covariance pycytominer function
    """
    data_precentered = data_df - data_df.mean()
    scaler = Whiten(center=False)
    scaler = scaler.fit(data_precentered)
    transform_df = scaler.transform(data_df)

    # The transfomed data is expected to have uncorrelated samples
    result = pd.DataFrame(np.cov(np.transpose(transform_df))).round().sum().sum()
    expected_result = data_df.shape[1]

    assert int(result) == expected_result


def test_robust_mad():
    """
    Testing the RobustMAD class
    """
    scaler = RobustMAD()
    scaler = scaler.fit(data_df)
    transform_df = scaler.transform(data_df)

    # The transfomed data is expected to have a median equal to zero
    result = transform_df.median().sum()
    expected_result = 0

    assert int(result) == expected_result

    # Check a median absolute deviation equal to the number of columns
    result = median_absolute_deviation(transform_df).sum()
    expected_result = data_df.shape[1]

    assert int(result) == expected_result
