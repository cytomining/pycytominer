"""
Miscellaneous utility functions
"""

import inspect
import os
import warnings
from functools import wraps
from typing import Any, Callable, Literal, Optional, Union, cast

import numpy as np
import pandas as pd

from pycytominer.cyto_utils.features import convert_compartment_format_to_list
from pycytominer.cyto_utils.output import output

default_metadata_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "metadata_feature_dictionary.txt"
)


def get_default_compartments() -> list[str]:
    """Returns default compartments.

    Returns
    -------
    list of str
        Default compartments.

    """

    return ["cells", "cytoplasm", "nuclei"]


def check_compartments(compartments: Union[str, list[str]]):
    """Checks if the input compartments are noncanonical compartments.

    Parameters
    ----------
    compartments : list of str
        Input compartments.

    Returns
    -------
    None
        Nothing is returned.

    """

    default_compartments = get_default_compartments()

    compartments = convert_compartment_format_to_list(compartments)

    non_canonical_compartments = []
    for compartment in compartments:
        if compartment not in default_compartments:
            non_canonical_compartments.append(compartment)

    if len(non_canonical_compartments) > 0:
        warn_str = "Non-canonical compartment detected: {x}".format(
            x=", ".join(non_canonical_compartments)
        )
        warnings.warn(warn_str)


def load_known_metadata_dictionary(
    metadata_file: str = default_metadata_file,
) -> dict[str, list[str]]:
    """From a tab separated text file (two columns: ["compartment", "feature"]), load
    previously known metadata columns per compartment.

    Parameters
    ----------
    metadata_file : str
        File location of the metadata text file. Uses a default dictionary if you do not specify.

    Returns
    -------
    dict
        Compartment (keys) mappings to previously known metadata (values).

    """

    metadata_dict: dict[str, list[str]] = {}
    with open(metadata_file) as meta_fh:
        next(meta_fh)
        for line in meta_fh:
            compartment, feature = line.strip().split("\t")
            compartment = compartment.lower()
            if compartment in metadata_dict:
                metadata_dict[compartment].append(feature)
            else:
                metadata_dict[compartment] = [feature]

    return metadata_dict


def check_correlation_method(method: str) -> Literal["pearson", "kendall", "spearman"]:
    """Confirm that the input method is currently supported.

    Parameters
    ----------
    method : str
        The correlation metric to use.

    Returns
    -------
    str
        Correctly formatted correlation method.

    """

    method = method.lower()
    avail_methods = ["pearson", "spearman", "kendall"]

    if method not in avail_methods:
        raise ValueError(
            f"method {method} not supported, select one of {avail_methods}"
        )

    return cast(Literal["pearson", "kendall", "spearman"], method)


def check_aggregate_operation(operation: str) -> str:
    """Confirm that the input operation for aggregation is currently supported.

    Parameters
    ----------
    operation : str
        Aggregation operation to provide.

    Returns
    -------
    str
        Correctly formatted operation method.

    """

    operation = operation.lower()
    avail_ops = ["mean", "median"]

    if operation not in avail_ops:
        raise ValueError(
            f"operation {operation} not supported, select one of {avail_ops}"
        )

    return operation


def check_consensus_operation(operation: str) -> str:
    """Confirm that the input operation for consensus is currently supported.

    Parameters
    ----------
    operation: str
        Consensus operation to provide.

    Returns
    -------
    str
        Correctly formatted operation method.

    """

    operation = operation.lower()
    avail_ops = ["modz"]  # All aggregation operations are also supported

    try:
        operation = check_aggregate_operation(operation)

    except ValueError:
        if operation not in avail_ops:
            raise ValueError(
                f"operation {operation} not supported, select one of {avail_ops} or see aggregate.py"
            )

    return operation


def maybe_write_to_file(
    func: Callable[..., pd.DataFrame]
) -> Callable[..., Union[pd.DataFrame, str]]:
    """Decorate a function to optionally write its output to disk.

    The decorator intercepts common output-related keyword arguments
    (``output_file``, ``output_type``, ``compression_options``, ``float_format``)
    from the decorated function call. The wrapped function should return a
    :class:`pandas.DataFrame`; when ``output_file`` is provided, the DataFrame is
    written using :func:`pycytominer.cyto_utils.output` and the resulting path is
    returned instead.
    """

    signature = inspect.signature(func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        bound_arguments = signature.bind_partial(*args, **kwargs)
        bound_arguments.apply_defaults()

        output_file = bound_arguments.arguments.pop("output_file", None)
        output_type = bound_arguments.arguments.pop("output_type", None)
        compression_options = bound_arguments.arguments.pop(
            "compression_options", None
        )
        float_format = bound_arguments.arguments.pop("float_format", None)

        result = func(**bound_arguments.arguments)

        if output_file is None:
            return result

        if not isinstance(result, pd.DataFrame):
            raise TypeError(
                "Decorated function must return a pandas DataFrame when output_file is provided"
            )

        return output(
            df=result,
            output_filename=output_file,
            output_type=output_type,
            compression_options=compression_options,
            float_format=float_format,
        )

    return wrapper


def check_fields_of_view_format(
    fields_of_view: Union[str, list[int]],
) -> Union[str, list[int]]:
    """Confirm that the input fields of view is valid.

    Parameters
    ----------
    fields_of_view : list of int
        List of integer fields of view.

    Returns
    -------
    str or list of int
        Correctly formatted fields_of_view variable.

    """

    if fields_of_view != "all":
        if isinstance(fields_of_view, list):
            if all(isinstance(x, int) for x in fields_of_view):
                return fields_of_view
            else:
                try:
                    return list(map(int, fields_of_view))
                except ValueError:
                    raise TypeError(
                        "Variables of type int expected, however some of the input fields of view are not integers."
                    )
        else:
            raise TypeError(
                f"Variable of type list expected, however type {type(fields_of_view)} was passed."
            )
    else:
        return fields_of_view


def check_fields_of_view(
    data_fields_of_view: list[int], input_fields_of_view: list[int]
):
    """Confirm that the input list of fields of view is a subset of the list of fields of view in the image table.

    Parameters
    ----------
    data_fields_of_view : list of int
        Fields of view in the image table.
    input_fields_of_view : list of int
        Input fields of view.

    Returns
    -------
    None
        Nothing is returned.

    """

    if not len(list(np.intersect1d(data_fields_of_view, input_fields_of_view))) == len(
        input_fields_of_view
    ):
        raise ValueError(
            "Some of the input fields of view are not present in the image table."
        )


def check_image_features(image_features: list[str], image_columns: list[str]):
    """Confirm that the input list of image features are present in the image table

    Parameters
    ----------
    image_features: list of str
        Input image features to extract from the image table.
    image_columns: list of str
        Columns in the image table

    Returns
    -------
    None
        Nothing is returned.
    """

    if "Image" in list({img_col.split("_")[0] for img_col in image_columns}):
        # Image has already been prepended to most, but not all, columns
        level = 1
        image_columns = [x for x in image_columns if "_" in x]
    else:
        level = 0

    if not all(
        feature in list({img_col.split("_")[level] for img_col in image_columns})
        for feature in image_features
    ):
        raise ValueError(
            "Some of the input image features are not present in the image table."
        )


def extract_image_features(
    image_feature_categories: list[str],
    image_df: pd.DataFrame,
    image_cols: list[str],
    strata: list[str],
) -> pd.DataFrame:
    """Confirm that the input list of image features categories are present in the image table and then extract those features.

    Parameters
    ----------
    image_feature_categories : list of str
        Input image feature groups to extract from the image table.
    image_df : pd.DataFrame
        Image dataframe.
    image_cols : list of str
        Columns to select from the image table.
    strata :  list of str
        The columns to groupby and aggregate single cells.

    Returns
    -------
    image_features_df : pd.DataFrame
        Dataframe with extracted image features.

    """

    # Check if the input image feature groups are valid.
    check_image_features(image_feature_categories, list(image_df.columns))

    # Extract Image features from image_feature_categories
    image_features = list(
        image_df.columns[
            image_df.columns.str.startswith(tuple(image_feature_categories))
        ]
    )

    image_features_df = image_df[image_features]

    image_features_df.columns = pd.Index([
        f"Image_{x}"
        if not x.startswith("Image_") and not x.startswith("Count_")
        else f"Metadata_{x}"
        if x.startswith("Count_")
        else x
        for x in image_features_df.columns
    ])

    # Add image_cols and strata to the dataframe
    image_features_df = pd.concat(
        [image_df[list(np.union1d(image_cols, strata))], image_features_df], axis=1
    )

    return image_features_df


def get_pairwise_correlation(
    population_df: pd.DataFrame, method: str = "pearson"
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Given a population dataframe, calculate all pairwise correlations.

    Parameters
    ----------
    population_df : pd.DataFrame
        Includes metadata and observation features.
    method : str, default "pearson"
        Which correlation matrix to use to test cutoff.
    Returns
    -------
    tuple of (pd.DataFrame, pd.DataFrame)
        A tuple of two DataFrames. The first is a symmetrical correlation matrix.
        The second is a long format DataFrame of pairwise correlations.
    """

    # Check that the input method is supported
    corrected_method: Literal["pearson", "kendall", "spearman"] = (
        check_correlation_method(method)
    )

    # Get a symmetrical correlation matrix. Use numpy for non NaN/Inf matrices.
    has_nan = np.any(np.isnan(population_df.values))
    has_inf = np.any(np.isinf(population_df.values))
    if corrected_method == "pearson" and not (has_nan or has_inf):
        pop_names = population_df.columns
        data_cor_df = pd.DataFrame(
            np.corrcoef(population_df.transpose()), index=pop_names, columns=pop_names
        )
    else:
        data_cor_df = population_df.corr(method=corrected_method)

    # Create a copy of the dataframe to generate upper triangle of zeros
    data_cor_natri_df = data_cor_df.copy()

    # Replace upper triangle in correlation matrix with NaN
    data_cor_natri_df = data_cor_natri_df.where(
        np.tril(np.ones(data_cor_natri_df.shape), k=-1).astype(bool)
    )

    # Acquire pairwise correlations in a long format
    # Note that we are using the NaN upper triangle DataFrame
    pairwise_df = data_cor_natri_df.stack().reset_index()
    pairwise_df.columns = pd.Index(["pair_a", "pair_b", "correlation"])

    return data_cor_df, pairwise_df
