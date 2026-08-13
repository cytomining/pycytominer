import random

import numpy as np
import pandas as pd
import pytest
from scipy.stats import median_abs_deviation
from sklearn.preprocessing import QuantileTransformer

from pycytominer.normalize import normalize
from pycytominer.operations.transform import InverseNormalTransform, RobustMAD, Spherize

random.seed(123)

a_feature = random.sample(range(1, 100), 10)
b_feature = random.sample(range(1, 100), 10)
c_feature = random.sample(range(1, 100), 10)
d_feature = random.sample(range(1, 100), 10)

data_df = pd.DataFrame({
    "a": a_feature,
    "b": b_feature,
    "c": c_feature,
    "d": d_feature,
}).reset_index(drop=True)


def test_spherize():
    spherize_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        for center in [True, False]:
            if ["PCA-cor", "ZCA-cor"] and not center:
                continue
            scaler = Spherize(method=method, center=center)
            scaler = scaler.fit(data_df)
            transform_df = scaler.transform(data_df)

            # The transfomed data is expected to have uncorrelated samples
            result = (
                pd
                .DataFrame(np.cov(np.transpose(transform_df)))
                .abs()
                .round()
                .sum()
                .clip(1)  # necessary for when center == False (numerically unstable)
                .sum()
            )
            expected_result = data_df.shape[1]

            assert int(result) == expected_result


def test_spherize_whitens_data():
    """The transformed output of Spherize should be white: cov(XW) should
    be (near) the identity matrix for every method/center combination.

    test_spherize() above only checks this to the nearest integer (via
    `.round()`), which is far too coarse to catch a whitening bug: it was
    passing even when `fit()` computed the SVD of the raw, uncentered
    input instead of the mean-centered (and, for the -cor variants,
    standardized) data -- see
    https://github.com/cytomining/pycytominer/issues/747.
    """
    rng = np.random.default_rng(0)
    # An off-origin mean makes an uncentered-SVD bug visible: with
    # data centered at/near the origin already, `svd(X)` and
    # `svd(X_transformed)` coincide, masking the bug.
    data_off_center = pd.DataFrame(
        rng.normal(loc=100, scale=25, size=(200, 5)),
        columns=list("abcde"),
    )

    spherize_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        scaler = Spherize(method=method, center=True)
        scaler = scaler.fit(data_off_center)
        transform_df = scaler.transform(data_off_center)

        cov = np.cov(transform_df.to_numpy(), rowvar=False)
        max_abs_deviation_from_identity = np.abs(cov - np.eye(cov.shape[0])).max()

        assert max_abs_deviation_from_identity < 1e-6, (
            f"method={method}: transformed data is not white "
            f"(max|cov(Y) - I| = {max_abs_deviation_from_identity})"
        )


def test_low_variance_spherize():
    err_str = "Divide by zero error, make sure low variance columns are removed"
    data_no_variance = data_df.assign(e=1)
    spherize_methods = ["PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        for center in [True, False]:
            if method in ["PCA-cor", "ZCA-cor"] and not center:
                continue
            scaler = Spherize(method=method, center=center)
            with pytest.raises(ValueError) as errorinfo:
                scaler = scaler.fit(data_no_variance)

            assert err_str in str(errorinfo.value.args[0])


def test_spherize_precenter():
    data_precentered = data_df - data_df.mean()
    spherize_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]
    for method in spherize_methods:
        if method in ["PCA-cor", "ZCA-cor"]:
            continue
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
    result = median_abs_deviation(transform_df, scale=1 / 1.4826).sum()
    expected_result = data_df.shape[1]

    assert int(result) == expected_result


def test_inverse_normal_transform_matches_quantile_transformer():
    """Test that InverseNormalTransform wraps QuantileTransformer to produce normal quantile scores."""
    scaler = InverseNormalTransform(n_quantiles=5)
    scaler = scaler.fit(data_df)
    transform_df = scaler.transform(data_df)

    expected_transform_df = QuantileTransformer(
        n_quantiles=5,
        output_distribution="normal",
    ).fit_transform(data_df)

    assert scaler.n_quantiles_ == 5
    np.testing.assert_allclose(transform_df, expected_transform_df)


def test_inverse_normal_transform_matches_quantile_transformer_default_n_quantiles():
    """Test that the default number of quantiles matches QuantileTransformer."""
    scaler = InverseNormalTransform().fit(data_df)
    transform_df = scaler.transform(data_df)

    with pytest.warns(UserWarning, match="n_quantiles .* greater"):
        expected_transform_df = QuantileTransformer(
            output_distribution="normal",
        ).fit_transform(data_df)

    # The default of 1000 is capped at the number of samples during fitting.
    assert scaler.n_quantiles_ == data_df.shape[0]
    np.testing.assert_allclose(transform_df, expected_transform_df)


def test_inverse_normal_transform_fit_transform():
    """Test that InverseNormalTransform supports sklearn fit_transform usage and returns finite values."""
    scaler = InverseNormalTransform(n_quantiles=5)
    transform_df = scaler.fit_transform(data_df)

    assert transform_df.shape == data_df.shape
    assert np.isfinite(transform_df).all()


def test_inverse_normal_transform_normalize_usage():
    """Test that normalize uses InverseNormalTransform to inverse-normalize features while preserving metadata."""
    profiles = pd.concat(
        [
            pd.DataFrame({
                "Metadata_plate": ["plate_a"] * data_df.shape[0],
                "Metadata_well": [f"A{i + 1}" for i in range(data_df.shape[0])],
            }),
            data_df,
        ],
        axis="columns",
    )

    normalize_result = normalize(
        profiles=profiles,
        features=["a", "b", "c", "d"],
        meta_features=["Metadata_plate", "Metadata_well"],
        samples="all",
        method="inverse_normal",
        inverse_normal_n_quantiles=5,
    )

    expected_features = QuantileTransformer(
        n_quantiles=5,
        output_distribution="normal",
    ).fit_transform(data_df)
    expected_result = pd.concat(
        [
            profiles.loc[:, ["Metadata_plate", "Metadata_well"]],
            pd.DataFrame(expected_features, columns=data_df.columns),
        ],
        axis="columns",
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)
