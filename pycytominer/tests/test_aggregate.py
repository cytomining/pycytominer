import pandas as pd
from pycytominer.aggregate import aggregate

# Build data to use in tests
data_df = pd.concat(
    [
        pd.DataFrame({"g": "a", "x": [1, 3, 8], "y": [5, 3, 1]}),
        pd.DataFrame({"g": "b", "x": [1, 3, 5], "y": [8, 3, 1]}),
    ]
).reset_index(drop=True)


def test_aggregate_no_input():
    """
    Testing aggregate pycytominer function
    """
    with pytest.raises(ValueError) as e_info:
        aggregate()


def test_aggregate_median_allvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features="all", operation="median"
    )

    expected_result = pd.concat(
        [
            pd.DataFrame({"g": "a", "x": [3], "y": [3]}),
            pd.DataFrame({"g": "b", "x": [3], "y": [3]}),
        ]
    ).reset_index(drop=True)

    assert aggregate_result.equals(expected_result)


def test_aggregate_mean_allvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features="all", operation="mean"
    )

    expected_result = pd.concat(
        [
            pd.DataFrame({"g": "a", "x": [4], "y": [3]}),
            pd.DataFrame({"g": "b", "x": [3], "y": [4]}),
        ]
    ).reset_index(drop=True)

    assert aggregate_result.equals(expected_result)


def test_aggregate_median_subsetvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features=["x"], operation="median"
    )

    expected_result = pd.DataFrame({"g": ["a", "b"], "x": [3, 3]})

    assert aggregate_result.equals(expected_result)


def test_aggregate_mean_subsetvar():
    """
    Testing aggregate pycytominer function
    """
    aggregate_result = aggregate(
        population_df=data_df, strata=["g"], features=["x"], operation="mean"
    )

    expected_result = pd.DataFrame({"g": ["a", "b"], "x": [4, 3]})

    assert aggregate_result.equals(expected_result)
