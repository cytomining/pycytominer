"""
Blocklist utilities for excluding unwanted features from profile DataFrames.

Although the packaged default blocklist targets known-noisy CellProfiler
features, the ``Blocklist`` class works with any feature names — including
embeddings, custom morphological measurements, or any other column-based
profile type.
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
    """A collection of feature names to exclude from downstream analysis.

    A ``Blocklist`` holds feature names that are known to be noisy,
    uninformative, or otherwise undesirable.  While the packaged default
    targets known-problematic CellProfiler measurements, ``Blocklist`` works
    with any column-based profile — CellProfiler features, embeddings, custom
    morphological measurements, or any other feature type.  It can be built
    from any combination of three sources:

    1. **Packaged named lists** — pycytominer ships a ``blocklists.yaml``
       registry whose top-level keys are named lists of features.  Pass one or
       more names via ``blocklist_name`` to load them.  The key ``"default"``
       loads the curated pycytominer default.
    2. **Explicit feature names** — pass a list of column names directly via
       ``features_to_block``.
    3. **Custom YAML registry** — supply your own YAML file via
       ``blocklists_file`` and reference its named lists with
       ``blocklist_name``.  Feature names can follow any naming convention
       (CellProfiler prefixes, embedding dimensions, custom names, etc.).
       The file must follow the format::

           my_list:
             - embedding_dim_42
             - Cells_MyFeature_A

           another_list:
             - my_custom_feature

       Any top-level key becomes a valid ``blocklist_name``.

    When constructing, named list(s) are loaded first (in order), then
    ``features_to_block`` entries are appended.  Duplicates are preserved;
    call :meth:`to_list` and deduplicate manually if needed.

    Parameters
    ----------
    blocklist_name : str or list of str, optional
        Name(s) of lists to load from the blocklist registry. When multiple
        names are given, entries are appended in the order provided. If
        ``None``, no named list is loaded. Use ``"default"`` to load the
        curated pycytominer default blocklist.
    features_to_block : list of str, optional
        Additional feature names to append after loading any named list(s).
        If ``blocklist_name`` is ``None``, these are the only blocklisted
        features.
    blocklists_file : path-like, default packaged ``blocklists.yaml``
        Path to a YAML registry mapping list names to feature lists.
        Defaults to pycytominer's packaged registry.  Supply a custom path
        to use your own feature lists (see format above).

    Examples
    --------
    Use the packaged default blocklist (recommended starting point):

    >>> bl = Blocklist(blocklist_name="default")
    >>> isinstance(bl.to_list(), list)
    True

    Block an explicit set of project-specific features:

    >>> bl = Blocklist(features_to_block=["Cells_MyFeature", "Nuclei_MyFeature"])
    >>> bl.to_list()
    ['Cells_MyFeature', 'Nuclei_MyFeature']

    Extend the packaged default with project-specific exclusions:

    >>> bl = Blocklist(
    ...     blocklist_name="default",
    ...     features_to_block=["Cells_MyFeature"],
    ... )

    Use a custom YAML registry instead of the packaged one (feature names can
    follow any convention — CellProfiler prefixes, embedding dimensions, etc.):

    >>> import pathlib
    >>> # my_blocklists.yaml contains:
    >>> # qc_fails:
    >>> #   - embedding_dim_42
    >>> #   - Cells_Texture_BadChannel
    >>> bl = Blocklist(
    ...     blocklist_name="qc_fails",
    ...     blocklists_file=pathlib.Path("my_blocklists.yaml"),
    ... )

    Pass the result directly to :func:`~pycytominer.feature_select.feature_select`:

    >>> import pandas as pd
    >>> from pycytominer import feature_select
    >>> bl = Blocklist(
    ...     blocklist_name="default",
    ...     features_to_block=["Cells_MyFeature"],
    ... )
    >>> # df = feature_select(profiles, operation="blocklist", blocklist=bl)

    See Also
    --------
    get_blocklist_features : Resolve a ``Blocklist`` (or shorthand forms) to a
        plain list of feature names present in a given profile DataFrame.
    feature_select : Apply blocklist (and other) feature-selection operations
        to a profile DataFrame.
    """

    def __init__(
        self,
        blocklist_name: Optional[Union[str, list[str]]] = None,
        features_to_block: Optional[list[str]] = None,
        blocklists_file: Union[str, pathlib.Path] = blocklists_file,
    ):  # Parameters are documented in the class docstring.
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
    blocklist: Optional[Union[str, list[str], "Blocklist"]] = None,
    blocklist_name: Optional[Union[str, list[str]]] = None,
    population_df: Optional[pd.DataFrame] = None,
) -> list[str]:
    """Resolve blocklist inputs to a list of feature names present in a DataFrame.

    Accepts the same shorthand forms supported by
    :func:`~pycytominer.feature_select.feature_select` and returns a plain
    list of feature names, optionally filtered to only those that exist in
    ``population_df``.  When both ``blocklist`` and ``blocklist_name`` are
    ``None``, the packaged default blocklist is used.  For full details on
    blocklist construction and customization, see :class:`Blocklist`.

    Parameters
    ----------
    blocklist : str, list of str, or Blocklist, optional
        Feature name(s) to exclude.  A :class:`Blocklist` object may be
        passed directly for full customization (custom YAML, combined
        named + explicit features, etc.).  If ``None``, ``blocklist_name``
        or the packaged default is used instead.
    blocklist_name : str or list of str, optional
        Name(s) of packaged blocklists to load when ``blocklist`` is
        ``None``.  If both are ``None``, falls back to
        ``default_blocklist_name`` (``"default"``).
    population_df : pd.DataFrame, optional
        When provided, the returned list is filtered to only feature names
        that appear as columns in this DataFrame.

    Returns
    -------
    blocklist_features : list of str
        Feature names to exclude from downstream analysis.

    See Also
    --------
    Blocklist : Full reference for blocklist construction and customization.
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
