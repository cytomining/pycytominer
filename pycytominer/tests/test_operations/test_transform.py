import os
import random
import pytest
import numpy as np
import pandas as pd
from scipy.stats import median_abs_deviation
from pycytominer.operations.transform import Spherize, RobustMAD

random.seed(123)

a_feature = random.sample(range(1, 100), 10)
b_feature = random.sample(range(1, 100), 10)
c_feature = random.sample(range(1, 100), 10)
d_feature = random.sample(range(1, 100), 10)

data_df = pd.DataFrame(
    {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
).reset_index(drop=True)


def test_spherize():
    spherize_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        for center in [True, False]:
            scaler = Spherize(method=method, center=center)
            scaler = scaler.fit(data_df)
            transform_df = scaler.transform(data_df)

            # The transfomed data is expected to have uncorrelated samples
            result = (
                pd.DataFrame(np.cov(np.transpose(transform_df)))
                .abs()
                .round()
                .sum()
                .clip(1)  # necessary for when center == False (numerically unstable)
                .sum()
            )
            expected_result = data_df.shape[1]

            assert int(result) == expected_result


def test_low_variance_spherize():
    err_str = "Divide by zero error, make sure low variance columns are removed"
    data_no_variance = data_df.assign(e=1)
    spherize_methods = ["PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        for center in [True, False]:
            scaler = Spherize(method=method, center=center)
            with pytest.raises(ValueError) as errorinfo:
                scaler = scaler.fit(data_no_variance)

            assert err_str in str(errorinfo.value.args[0])


def test_spherize_precenter():
    data_precentered = data_df - data_df.mean()
    spherize_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        scaler = Spherize(method=method, center=False)
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
    result = median_abs_deviation(transform_df, scale=1/1.4826).sum()
    expected_result = data_df.shape[1]

    assert int(result) == expected_result
