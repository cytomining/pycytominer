"""
Utility function to manipulate cell profiler features
"""

import os
import pathlib
from typing import Optional, Union

import pandas as pd
import yaml

blocklists_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "blocklists.yaml"
)

default_blocklist_name = "default"


class Blocklist:
    """Container for named and user-provided blocklist features."""

    def __init__(
        self,
        blocklist_name: Optional[Union[str, list[str]]] = None,
        features_to_block: Optional[list[str]] = None,
        blocklists_file: Union[str, pathlib.Path] = blocklists_file,
    ):
        """Create a blocklist from a named registry entry and/or features to block.

        Parameters
        ----------
        blocklist_name : str or list of str, optional
            Name(s) of blocklists stored in the packaged blocklist registry.
            If None, the blocklist starts empty. When multiple names are
            provided, entries are appended in the provided order. Use
            `"default"`` to explicitly load the
            packaged default blocklist.
        features_to_block : list of str, optional
            Feature names to append to the named blocklist. If
            ``blocklist_name`` is None, these are the only blocklisted features.
        blocklists_file : path-like object, default packaged blocklists.yaml
            YAML file mapping blocklist names to feature lists.
        """
        self.blocklist_name = blocklist_name
        self.features: list[str] = []

        # Start with features from any packaged blocklist names the user chose.
        for name in _normalize_blocklist_names(blocklist_name):
            self.features.extend(_load_named_blocklist(name, blocklists_file))

        # Then append explicit feature names so users can combine packaged
        # blocklists with project-specific exclusions.
        if features_to_block is not None:
            self.add(features_to_block)

    def add(self, features: list[str]) -> None:
        """Add one or more feature names to the blocklist."""
        if not isinstance(features, list):
            raise TypeError("Blocklist.add() requires a list of feature names.")

        self.features = self.features + [str(feature) for feature in features]

    def to_list(self) -> list[str]:
        """Return blocklist features as a list."""
        return self.features.copy()

    def __iter__(self):
        return iter(self.features)

    def __len__(self):
        return len(self.features)


def _load_named_blocklist(
    blocklist_name: str,
    blocklists_file: Union[str, pathlib.Path] = blocklists_file,
) -> list[str]:
    """Load a blocklist entry by name from the YAML registry.

    Parameters
    ----------
    blocklist_name : str
        Name of the blocklist to load. This is the top-level YAML key in
        ``blocklists_file``; for example, ``"default"`` loads the list under
        the ``default:`` key in ``blocklists.yaml``.
    blocklists_file : str or pathlib.Path, default packaged blocklists.yaml
        YAML file mapping blocklist names to feature lists.

    Returns
    -------
    list of str
        Feature names from the named blocklist. Values loaded from YAML are
        converted to strings.

    Raises
    ------
    ValueError
        If the registry is not a mapping, ``blocklist_name`` is not present, or
        the selected registry entry is not a list of feature names.
    """
    with pathlib.Path(blocklists_file).open() as blocklist_stream:
        blocklists = yaml.safe_load(blocklist_stream)

    if not isinstance(blocklists, dict):
        raise ValueError(
            "Blocklist registry must be a mapping of blocklist names to feature lists."
        )

    if blocklist_name not in blocklists:
        blocklist_names = ", ".join(sorted(blocklists))
        raise ValueError(
            f"Unknown blocklist name '{blocklist_name}'. "
            f"Choose one of: {blocklist_names}"
        )

    blocklist = blocklists[blocklist_name]
    if not isinstance(blocklist, list):
        raise ValueError(
            "Each blocklist registry entry must be a list of feature names. "
            "Feature names may be strings or values that can be converted to "
            "strings."
        )

    return [str(feature) for feature in blocklist]


def _normalize_blocklist_names(
    blocklist_name: Optional[Union[str, list[str]]],
) -> list[str]:
    """Return blocklist names as a list."""
    if blocklist_name is None:
        return []
    if isinstance(blocklist_name, str):
        return [blocklist_name]
    if isinstance(blocklist_name, list):
        return [str(name) for name in blocklist_name]

    raise TypeError("blocklist_name must be a string, a list of strings, or None.")


def get_blocklist_features(
    blocklist: Optional[Union[str, list[str], Blocklist]] = None,
    blocklist_name: Optional[Union[str, list[str]]] = None,
    population_df: Optional[pd.DataFrame] = None,
) -> list[str]:
    """Get a list of blocklist features.

    Parameters
    ----------
    blocklist : str, list of str, or Blocklist, optional
        Feature name(s) to exclude. If None, no user-provided blocklist
        features are used.
    blocklist_name : str or list of str, optional
        Name(s) of packaged blocklists to load when ``blocklist`` is None. Each
        name corresponds to a top-level key in the packaged YAML registry (for
        example, ``default`` in ``blocklists.yaml``). If None and ``blocklist``
        is also None, the packaged default blocklist (``default_blocklist_name``)
        is used. Multiple names are loaded in the order provided.
    population_df : pd.DataFrame, optional
        Profile dataframe used to subset blocklist features.

    Returns
    -------
    blocklist_features : list of str
        Features to exclude from downstream analysis.
    """

    if blocklist is None:
        # No explicit features were provided; use the named registry entry if
        # requested, otherwise fall back to the packaged default blocklist.
        resolved_name = (
            blocklist_name if blocklist_name is not None else default_blocklist_name
        )
        blocklist_features = Blocklist(blocklist_name=resolved_name).to_list()
    elif isinstance(blocklist, Blocklist):
        # A Blocklist object may already combine named and explicit features.
        blocklist_features = blocklist.to_list()
    elif isinstance(blocklist, str):
        # Treat a single string as one feature name, not as an iterable of characters.
        blocklist_features = [blocklist]
    elif isinstance(blocklist, list):
        # Lists are already feature collections; normalize values to strings.
        blocklist_features = [str(feature) for feature in blocklist]
    else:
        raise TypeError(
            "blocklist must be a feature-name string, a list of feature names, "
            "or a Blocklist."
        )

    if isinstance(population_df, pd.DataFrame):
        population_features = population_df.columns.tolist()
        # Keep only blocklisted features that are present in this profile table.
        blocklist_features = [x for x in blocklist_features if x in population_features]

    return blocklist_features


def label_compartment(
    cp_features: list[str], compartment: str, metadata_cols: list[str]
) -> list[str]:
    """Assign compartment label to each features as a prefix.

    Parameters
    ----------
    cp_features : list of str
        All features being used.
    compartment : str
       Measured compartment.
    metadata_cols : list
        Columns that should be considered metadata.

    Returns
    -------
    cp_features: list of str
        Recoded column names with appropriate metadata and compartment labels.
    """

    compartment = compartment.title()
    avail_compartments = ["Cells", "Cytoplasm", "Nuceli", "Image", "Barcode"]

    if compartment not in avail_compartments:
        raise ValueError(f"provide valid compartment. One of: {avail_compartments}")

    cp_features = [
        f"Metadata_{x}" if x in metadata_cols else f"{compartment}_{x}"
        for x in cp_features
    ]

    return cp_features


def infer_cp_features(
    population_df: pd.DataFrame,
    compartments: Union[str, list[str]] = ["Cells", "Nuclei", "Cytoplasm"],
    metadata: bool = False,
    image_features: bool = False,
) -> list[str]:
    """Given CellProfiler output data read as a DataFrame, output feature column names as a list.

    Inferred feature columns will match expected CellProfiler prefixes (for
    example, ``Cells_``, ``Cytoplasm_``, and ``Nuclei_``). When
    ``image_features=True``, the function excludes non-numeric ``Image_*``
    columns from inferred features. This is important for use cases that
    combine profile features with image payload columns under the ``Image_*``
    prefix, such as OME-Arrow. The function also excludes columns with nested
    object values, even if they use a CellProfiler-like prefix.

    Parameters
    ----------
    population_df : pd.DataFrame
        DataFrame from which features are to be inferred.
    compartments : list of str, default ["Cells", "Nuclei", "Cytoplasm"]
        Compartments from which Cell Painting features were extracted.
    metadata : bool, default False
        Whether or not to infer metadata features.
        If metadata is set to True, find column names that begin with the `Metadata_` prefix.
        This convention is expected by CellProfiler defaults.
    image_features : bool, default False
        Whether or not to include ``Image_*`` columns in inferred features.
        When True, Pycytominer includes numeric image features alongside the
        default CellProfiler compartments, while still excluding non-numeric
        ``Image_*`` columns. This avoids treating image payload columns as
        profile features in data layouts that store both under the same
        ``Image_*`` prefix, such as OME-Arrow-backed tables.

    Returns
    -------
    features: list of str
        List of inferred Cell Painting feature column names.
    """

    compartments = convert_compartment_format_to_list(compartments)
    compartments = [x.title() for x in compartments]

    if image_features:
        compartments = list({"Image", *compartments})

    features = []
    for col in population_df.columns.tolist():
        if not any(col.startswith(x.title()) for x in compartments):
            continue

        # Exclude nested object payloads while allowing scalar object values.
        if population_df[col].dtype == "object":
            non_null_values = population_df[col].dropna()
            if any(not pd.api.types.is_scalar(value) for value in non_null_values):
                continue

        if col.startswith("Image_") and not pd.api.types.is_numeric_dtype(
            population_df[col]
        ):
            continue

        features.append(col)

    if metadata:
        features = population_df.columns[
            population_df.columns.str.startswith("Metadata_")
        ].tolist()

    if len(features) == 0:
        raise ValueError(
            "No features or metadata found. Pycytominer expects CellProfiler column names by default. "
            "If you're using non-CellProfiler data, please do not 'infer' features. "
            "Instead, check if the function has a `features` or `meta_features` parameter, and input column names manually."
        )

    return features


def count_na_features(population_df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    """Given a population dataframe and features, count how many nas per feature.

    Parameters
    ----------
    population_df : pd.DataFrame
        DataFrame of profiles.
    features : list of str
        Features present in the population dataframe.

    Returns
    -------
    Dataframe of NA counts per feature
    """

    return pd.DataFrame(population_df.loc[:, features].isna().sum(), columns=["num_na"])


def drop_outlier_features(
    population_df: pd.DataFrame,
    features: Union[str, list[str]] = "infer",
    samples: str = "all",
    outlier_cutoff: Union[int, float] = 500,
) -> list[str]:
    """Exclude a feature if its min or max absolute value is greater than the threshold.

    Parameters
    ----------
    population_df : pd.DataFrame
        DataFrame that includes metadata and observation features.
    features : list of str or str, default "infer"
        Features present in the population dataframe. If "infer",
        then assume CellProfiler feature conventions
        (start with ``Cells_``, ``Nuclei_``, or ``Cytoplasm_``)
    samples : str, default "all"
        List of samples to perform operation on. The function uses a pd.DataFrame.query()
        function, so you should  structure samples in this fashion. An example is
        "Metadata_treatment == 'control'" (include all quotes).
        If "all", use all samples to calculate.
    outlier_cutoff : int or float, default 500
        Threshold to remove features if absolute value is greater.
        See https://github.com/cytomining/pycytominer/issues/237 for details.

    Returns
    -------
    outlier_features: list of str
        Features greater than the threshold.
    """

    # Subset the DataFrame if specific samples are specified
    # If "all", use the entire DataFrame without subsetting
    if samples != "all":
        # Using pandas query to filter rows based on the conditions provided in the
        # samples parameter
        population_df = population_df.query(expr=samples)

    # Infer  CellProfiler features if 'features' is set to 'infer'
    if features == "infer":
        # Infer CellProfiler features
        feature_list: list[str] = infer_cp_features(population_df)

    else:
        # Subset the DataFrame to only include the features of interest
        # this would be more tailored to non-CellProfiler features
        feature_list = [features] if isinstance(features, str) else list(features)

    population_df = population_df.loc[:, feature_list]

    # Get the max and min values for each feature
    max_feature_values = population_df.max().abs()
    min_feature_values = population_df.min().abs()

    # Identify features with max or min values greater than the outlier cutoff
    outlier_features = max_feature_values[
        (max_feature_values > outlier_cutoff) | (min_feature_values > outlier_cutoff)
    ].index.tolist()

    return outlier_features


def convert_compartment_format_to_list(
    compartments: Union[list[str], str],
) -> list[str]:
    """Converts compartment to a list.

    Parameters
    ----------
    compartments : list of str or str
        Cell Painting compartment(s).

    Returns
    -------
    compartments : list of str
        List of Cell Painting compartments.
    """

    return compartments if isinstance(compartments, list) else [compartments]
