import pandas as pd
from pycytominer.correlation_threshold import correlation_threshold

# Build data to use in tests
data_df = pd.DataFrame(
    {
        "x": [1, 3, 8, 5, 2, 2],
        "y": [1, 2, 8, 5, 2, 1],
        "z": [9, 3, 8, 9, 2, 9],
        "zz": [0, -3, 8, 9, 6, 9],
    }
).reset_index(drop=True)


def test_correlation_threshold():
    """
    Testing correlation_threshold pycytominer function
    """
    correlation_threshold_result = correlation_threshold(
        population_df=data_df,
        samples="none",
        threshold=0.9,
        method="pearson",
    )

    expected_result = ['y']

    assert correlation_threshold_result == expected_result


def test_correlation_threshold_samples():
    """
    Testing correlation_threshold pycytominer function
    """
    correlation_threshold_result = correlation_threshold(
        population_df=data_df,
        samples=[0, 1, 3, 4, 5],
        threshold=0.9,
        method="pearson",
    )

    expected_result = ['y']

    assert correlation_threshold_result == expected_result
