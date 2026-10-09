import random
import warnings

import numpy as np
import pandas as pd
import pytest
import scipy.stats as ss
from scipy.stats import median_abs_deviation
from sklearn.preprocessing import QuantileTransformer

from pycytominer.normalize import normalize
from pycytominer.operations.transform import (
    _RANKIT_CONSTANTS,
    InverseNormalTransform,
    RobustMAD,
    Spherize,
)

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


RANKIT_METHODS = list(_RANKIT_CONSTANTS)
# `fit`/`transform`/`_rankit_scores` take the same code path regardless of which
# rank-based method is chosen -- only the constant plugged into the formula
# differs. So only tests that check a method's constant against an independent
# reference (below) are parametrized over every method; tests of method-agnostic
# behavior (tie handling, missing values, normalize() forwarding, etc.) run
# against this one representative method, which already exercises every line.
DEFAULT_RANKIT_METHOD = "blom"


@pytest.mark.parametrize("ties", ["average", "random"])
@pytest.mark.parametrize("method", RANKIT_METHODS)
def test_inverse_normal_transform_rankit_matches_jump_recipe(method, ties):
    """Test that every rank-based method matches the JUMP recipe's rank_int_array, using that method's constant."""
    transform_df = InverseNormalTransform(
        method=method, ties=ties, random_state=0
    ).fit_transform(tied_df)

    assert transform_df.shape == tied_df.shape
    for i, column in enumerate(tied_df.columns):
        values = tied_df[column].to_numpy()
        scores = transform_df[:, i]
        expected = jump_rank_int_array(
            values, c=_RANKIT_CONSTANTS[method], stochastic=ties == "random"
        )

        if ties == "average":
            # deterministic: tied values share one score and match the reference exactly
            np.testing.assert_allclose(scores, expected)
            assert len(np.unique(scores)) == len(np.unique(values))
        else:
            # only the order in which tied values receive their scores is random
            np.testing.assert_allclose(np.sort(scores), np.sort(expected))
            # every value gets its own score, but a lower value still scores below a higher one
            assert len(np.unique(scores)) == len(values)
            by_value = pd.Series(scores).groupby(values)
            assert (
                by_value.max().iloc[:-1].to_numpy() < by_value.min().iloc[1:].to_numpy()
            ).all()


@pytest.mark.parametrize("ties", ["average", "random"])
def test_inverse_normal_transform_rankit_missing_values(ties):
    """Test that missing values are left out of each column's ranking instead of blanking the column.

    Without that, scipy's rankdata (and therefore every score in the column)
    would become NaN because of a single missing entry.
    """
    values = np.array([
        [1.0, 1.0, np.nan],
        [2.0, np.nan, np.nan],
        [3.0, 3.0, np.nan],
        [4.0, 4.0, np.nan],
        [5.0, 5.0, np.nan],
    ])

    def score(x):
        return InverseNormalTransform(
            method=DEFAULT_RANKIT_METHOD, ties=ties, random_state=0
        ).fit_transform(x)

    scores = score(values)

    assert scores.shape == values.shape
    # a column without missing values is scored as usual
    np.testing.assert_allclose(scores[:, [0]], score(values[:, [0]]))
    # a column with one NaN: that row stays NaN, the others are ranked only among themselves
    valid_rows = [0, 2, 3, 4]
    assert np.isnan(scores[1, 1])
    np.testing.assert_allclose(
        scores[valid_rows][:, [1]], score(values[valid_rows][:, [1]])
    )
    # an entirely missing column has nothing to rank, so it stays NaN
    assert np.isnan(scores[:, 2]).all()


def test_inverse_normal_transform_rankit_random_ties_depend_on_random_state():
    """Test that random tie-breaking is reproducible per random_state and independent per feature."""
    # identical features, so any difference between their scores comes from independent shuffles
    identical_df = pd.concat([tied_df["a"], tied_df["a"]], axis="columns")

    def score(random_state):
        return InverseNormalTransform(
            method=DEFAULT_RANKIT_METHOD, ties="random", random_state=random_state
        ).fit_transform(identical_df)

    first = score(3)
    np.testing.assert_array_equal(first, score(3))
    assert not np.array_equal(first, score(4))
    # each feature is shuffled independently, though both receive the same set of scores
    assert not np.array_equal(first[:, 0], first[:, 1])
    np.testing.assert_allclose(np.sort(first[:, 0]), np.sort(first[:, 1]))


def test_inverse_normal_transform_invalid_method_and_ties():
    """Test that unknown method and ties values raise a ValueError."""
    with pytest.raises(ValueError, match="method must be"):
        InverseNormalTransform(method="rank").fit(data_df)

    with pytest.raises(ValueError, match="ties must be"):
        InverseNormalTransform(method="blom", ties="first").fit(data_df)


def test_normalize_invalid_inverse_normal_method():
    """Test that normalize rejects an unknown inverse_normal_method before using `samples`."""
    profiles = pd.concat(
        [pd.DataFrame({"Metadata_plate": ["plate_a"] * len(data_df)}), data_df],
        axis="columns",
    )

    # the missing `samples` column would raise a different error if it were evaluated first
    with pytest.raises(ValueError, match="inverse_normal_method must be one of"):
        normalize(
            profiles=profiles,
            features=["a", "b", "c", "d"],
            meta_features=["Metadata_plate"],
            samples="Metadata_missing_column == 'plate_a'",
            method="inverse_normal",
            inverse_normal_method="rank",
        )


def test_inverse_normal_transform_rankit_normalize_usage():
    """Test that normalize forwards the rank-based inverse-normal options to InverseNormalTransform."""
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
            inverse_normal_method=DEFAULT_RANKIT_METHOD,
            inverse_normal_ties="random",
            inverse_normal_random_state=0,
        )

    expected_features = InverseNormalTransform(
        method=DEFAULT_RANKIT_METHOD, ties="random", random_state=0
    ).fit_transform(tied_df)
    expected_result = pd.concat(
        [
            profiles.loc[:, ["Metadata_plate", "Metadata_well"]],
            pd.DataFrame(expected_features, columns=tied_df.columns),
        ],
        axis="columns",
    )

    pd.testing.assert_frame_equal(normalize_result, expected_result)


@pytest.mark.parametrize(
    "samples",
    [
        pytest.param("Metadata_plate == 'plate_a'", id="existing_column"),
        pytest.param("Metadata_missing_column == 'plate_a'", id="missing_column"),
    ],
)
def test_normalize_rankit_raises_when_samples_is_not_all(samples):
    """Test that normalize raises when samples != "all" for a rank-based method.

    Rank-based methods rank every row, so `samples` can never affect the
    result; this must raise (rather than silently ignoring it) even when
    `samples` queries a metadata column that is not present, since the query
    is never evaluated either way.
    """
    profiles = pd.concat(
        [
            pd.DataFrame({"Metadata_plate": ["plate_a"] * 100 + ["plate_b"] * 100}),
            tied_df,
        ],
        axis="columns",
    )

    with pytest.raises(ValueError, match='samples="all"'):
        normalize(
            profiles=profiles,
            features=["a", "b", "c"],
            meta_features=["Metadata_plate"],
            samples=samples,
            method="inverse_normal",
            inverse_normal_method=DEFAULT_RANKIT_METHOD,
        )
