import random
import warnings

import numpy as np
import pandas as pd
import pytest
import scipy.stats as ss
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


def jump_rank_int_array(array, c=3.0 / 8, stochastic=True, seed=0):
    """Reference implementation: rank_int_array from the JUMP profiling recipe.

    https://github.com/broadinstitute/jump-profiling-recipe/blob/d2512d978ca17aafead0e99de66386337e6312e4/src/jump_profiling_recipe/preprocessing/transform.py#L12-L55
    """
    rng = np.random.default_rng(seed=seed)
    if stochastic:
        ix = rng.permutation(len(array))
        rev_ix = np.argsort(ix)
        array = array[ix]
        rank = ss.rankdata(array, method="ordinal")
        rank = rank[rev_ix]
    else:
        rank = ss.rankdata(array, method="average")
    x = (rank - c) / (len(rank) - 2 * c + 1)
    return ss.norm.ppf(x)


# features with many tied values, to exercise the tie handling
tied_df = pd.DataFrame(
    np.random.default_rng(0).integers(0, 6, size=(200, 3)).astype(float),
    columns=["a", "b", "c"],
)


def test_inverse_normal_transform_blom_random_ties_scores_match_jump_recipe():
    """Test that random tie-breaking hands out the same set of scores as the JUMP recipe's rank_int_array.

    Only the order in which tied values receive their scores is random, so the sorted
    scores of every feature equal those of the reference.
    """
    scaler = InverseNormalTransform(method="blom", ties="random", random_state=0)
    transform_df = scaler.fit(tied_df).transform(tied_df)

    assert transform_df.shape == tied_df.shape
    for i, column in enumerate(tied_df.columns):
        expected = jump_rank_int_array(tied_df[column].to_numpy())
        np.testing.assert_allclose(np.sort(transform_df[:, i]), np.sort(expected))
        # random tie-breaking gives every value in a feature its own score
        assert len(np.unique(transform_df[:, i])) == len(tied_df)
        # ties are broken randomly, but every score of a lower value stays below every score of a higher value
        by_value = pd.Series(transform_df[:, i]).groupby(tied_df[column].to_numpy())
        assert (
            by_value.max().iloc[:-1].to_numpy() < by_value.min().iloc[1:].to_numpy()
        ).all()


def test_inverse_normal_transform_blom_average_ties_matches_jump_recipe():
    """Test that method='blom' with the default average ties matches rank_int_array(stochastic=False)."""
    transform_df = InverseNormalTransform(method="blom").fit_transform(tied_df)

    expected = np.column_stack([
        jump_rank_int_array(tied_df[column].to_numpy(), stochastic=False)
        for column in tied_df.columns
    ])

    np.testing.assert_allclose(transform_df, expected)
    # tied values share one score
    assert all(len(np.unique(transform_df[:, i])) == 6 for i in range(3))


def test_inverse_normal_transform_blom_scores():
    """Test that Blom scores preserve order, are symmetric and are bounded for untied data."""
    values = np.random.default_rng(1).normal(size=(1000, 1))
    scores = InverseNormalTransform(method="blom").fit_transform(values)

    np.testing.assert_array_equal(np.argsort(scores[:, 0]), np.argsort(values[:, 0]))
    assert np.isclose(scores.mean(), 0.0, atol=1e-12)
    # the extreme ranks map to +/- ppf((1 - 3/8) / (n + 1/4)) rather than being clipped near 5.2
    assert np.isclose(scores.max(), ss.norm.ppf((1000 - 3 / 8) / (1000 + 1 / 4)))
    assert np.isclose(scores.min(), -scores.max())


def test_inverse_normal_transform_blom_random_ties_reproducible():
    """Test that random tie-breaking depends on random_state."""
    first = InverseNormalTransform(method="blom", ties="random", random_state=3)
    second = InverseNormalTransform(method="blom", ties="random", random_state=3)
    other = InverseNormalTransform(method="blom", ties="random", random_state=4)

    np.testing.assert_array_equal(
        first.fit_transform(tied_df), second.fit_transform(tied_df)
    )
    assert not np.array_equal(
        first.fit_transform(tied_df), other.fit_transform(tied_df)
    )


def test_inverse_normal_transform_blom_random_ties_independent_across_features():
    """Test that identical features are shuffled independently when breaking ties."""
    identical_df = pd.concat([tied_df["a"], tied_df["a"]], axis="columns")
    transform_df = InverseNormalTransform(
        method="blom", ties="random", random_state=0
    ).fit_transform(identical_df)

    assert not np.array_equal(transform_df[:, 0], transform_df[:, 1])
    np.testing.assert_allclose(np.sort(transform_df[:, 0]), np.sort(transform_df[:, 1]))


def test_inverse_normal_transform_invalid_method_and_ties():
    """Test that unknown method and ties values raise a ValueError."""
    with pytest.raises(ValueError, match="method must be"):
        InverseNormalTransform(method="rank").fit(data_df)

    with pytest.raises(ValueError, match="ties must be"):
        InverseNormalTransform(method="blom", ties="first").fit(data_df)


def test_inverse_normal_transform_blom_normalize_usage():
    """Test that normalize forwards the Blom options to InverseNormalTransform."""
    profiles = pd.concat(
        [
            pd.DataFrame({
                "Metadata_plate": ["plate_a"] * tied_df.shape[0],
                "Metadata_well": [f"A{i + 1}" for i in range(tied_df.shape[0])],
            }),
            tied_df,
        ],
        axis="columns",
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        normalize_result = normalize(
            profiles=profiles,
            features=["a", "b", "c"],
            meta_features=["Metadata_plate", "Metadata_well"],
            samples="all",
            method="inverse_normal",
            inverse_normal_method="blom",
            inverse_normal_ties="random",
            inverse_normal_random_state=0,
        )

    expected_features = InverseNormalTransform(
        method="blom", ties="random", random_state=0
    ).fit_transform(tied_df)
    expected_result = pd.concat(
        [
            profiles.loc[:, ["Metadata_plate", "Metadata_well"]],
            pd.DataFrame(expected_features, columns=tied_df.columns),
        ],
        axis="columns",
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)


def test_normalize_blom_warns_when_samples_is_not_all():
    """Test that normalize warns that method='blom' ignores a samples subset."""
    profiles = pd.concat(
        [
            pd.DataFrame({"Metadata_plate": ["plate_a"] * 100 + ["plate_b"] * 100}),
            tied_df,
        ],
        axis="columns",
    )

    with pytest.warns(UserWarning, match='samples="all"'):
        result = normalize(
            profiles=profiles,
            features=["a", "b", "c"],
            meta_features=["Metadata_plate"],
            samples="Metadata_plate == 'plate_a'",
            method="inverse_normal",
            inverse_normal_method="blom",
        )

    # the scores come from the ranks over all rows, whatever the samples were
    expected = InverseNormalTransform(method="blom").fit_transform(tied_df)
    np.testing.assert_allclose(result.loc[:, ["a", "b", "c"]], expected)
