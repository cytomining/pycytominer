"""
Annotates profiles with metadata information
"""

import warnings
from typing import Literal, Optional, Union

import pandas as pd

from pycytominer.cyto_utils import (
    annotate_cmap,
    cp_clean,
    infer_cp_features,
    load_platemap,
    load_profiles,
    prepare_external_metadata_for_annotate,
)
from pycytominer.cyto_utils.util import write_to_file_if_user_specifies_output_details


@write_to_file_if_user_specifies_output_details
def annotate(
    profiles: Union[str, pd.DataFrame],
    platemap: Union[str, pd.DataFrame],
    join_on: Union[str, list[str]] = ["Metadata_well_position", "Metadata_Well"],
    output_file: Optional[str] = None,
    output_type: Optional[
        Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
    ] = "csv",
    add_metadata_id_to_platemap: bool = True,
    format_broad_cmap: bool = False,
    clean_cellprofiler: bool = True,
    external_metadata: Optional[Union[str, pd.DataFrame]] = None,
    external_join_on: Optional[Union[str, list[str]]] = None,
    compression_options: Optional[Union[str, dict[str, str]]] = None,
    float_format: Optional[str] = None,
    cmap_args: Optional[dict[str, Union[str]]] = None,
    platemap_sep: Optional[str] = None,
    **kwargs,
) -> pd.DataFrame:
    """Add metadata to aggregated profiles.

    Parameters
    ----------
    profiles : pd.DataFrame or file
        DataFrame or file path of profiles.
    platemap : pd.DataFrame or file
        Dataframe or file path of platemap metadata.
    join_on : list or str, default: ["Metadata_well_position", "Metadata_Well"]
        Which variables to merge profiles and plate. The first element indicates variable(s) in platemap and the second element indicates variable(s) in profiles to merge using. Note the setting of `add_metadata_id_to_platemap`
    output_file : str, optional
        If not specified, will return the annotated profiles. We recommend that this output file be suffixed with "_augmented.csv".
    output_type : str, optional
        If provided, will write annotated profiles as a specified file type (either CSV or parquet).
        If not specified and output_file is provided, then the file will be outputed as CSV as default.
    add_metadata_id_to_platemap : bool, default True
        Whether the plate map variables possibly need "Metadata" pre-pended
    format_broad_cmap : bool, default False
        Whether we need to add columns to make compatible with Broad CMAP naming conventions.

        .. warning::
            The ``format_broad_cmap`` parameter is deprecated and will be
            removed in a future Pycytominer release.
    clean_cellprofiler: bool, default True
        Clean specific CellProfiler feature names by dropping
        Image_ prefix.
        Default is true as the most common use case is
        annotating CellProfiler profiles, but this can be
        set to False if you are not using CellProfiler.
    external_metadata : pd.DataFrame or file, optional
        DataFrame or file with additional metadata information.
        Most common use case is a QC.parquet file with QC flags for each profile
        that comes from coSMicQC. File paths are loaded via :func:`load_profiles`;
        on Windows, CSV/TSV files are not supported — pass a Parquet file or a
        pre-loaded DataFrame instead (see the Windows note in :func:`load_profiles`).
    external_join_on : str or list, optional
        Merge column(s) shared by the annotated profiles and external metadata.
        When provided, these keys are used on both sides of the external merge.
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    cmap_args : dict, default None
        Potential keyword arguments for annotate_cmap(). See cyto_utils/annotate_custom.py for more details.

        .. warning::
            The ``cmap_args`` parameter is deprecated and will be
            removed in a future Pycytominer release.
    platemap_sep : str, optional
        Column delimiter for the platemap file (e.g. ``","`` for CSV, ``"\\t"``
        for TSV). Only applies when ``platemap`` is a file path — ignored when
        a DataFrame is passed directly.

        When ``None`` (the default), the delimiter is detected automatically.
        Automatic detection can be unreliable on Windows for tab-separated files;
        pass ``platemap_sep="\\t"`` explicitly in that case.

    Returns
    -------
    pd.DataFrame
        DataFrame of annotated features. If output_file=None, then return the
        DataFrame. If you specify output_file, profiles will be written on disk
        based on provided output_file path.
    """

    # Load Data
    profiles = load_profiles(profiles)
    platemap = load_platemap(platemap, add_metadata_id_to_platemap, sep=platemap_sep)

    annotated = platemap.merge(
        profiles,
        left_on=join_on[0],
        right_on=join_on[1],
        how="inner",
        suffixes=("_platemap", None),
    )
    if join_on[0] != join_on[1]:
        annotated = annotated.drop(join_on[0], axis="columns")

    # Add specific Connectivity Map (CMAP) formatting
    if format_broad_cmap:
        # raise deprecation warning when format_broad_cmap is set to True
        warnings.warn(
            "The `format_broad_cmap` parameter in annotate() is deprecated and will be "
            "removed in a future release.",
            category=DeprecationWarning,
            stacklevel=2,
        )

        annotated = annotate_cmap(
            annotated,
            annotate_join_on=join_on[1],
            cell_id="unknown" if not cmap_args else cmap_args.get("cell_id", "unknown"),
            perturbation_mode="none"
            if not cmap_args
            else cmap_args.get("perturbation_mode", "none"),
        )

    # Check that external metadata and join keys are being provided together
    if (external_metadata is None) != (external_join_on is None):
        raise ValueError(
            "Both `external_metadata` and `external_join_on` must be provided together."
        )

    # Add external metadata if provided (including a QC.parquet file with QC flags)
    if isinstance(external_metadata, str):
        external_metadata = load_profiles(external_metadata)

    if isinstance(external_metadata, pd.DataFrame):
        external_metadata = prepare_external_metadata_for_annotate(external_metadata)

        annotated = (
            annotated
            .merge(
                external_metadata,
                on=external_join_on,
                how="left",
                suffixes=(None, "_external"),
            )
            .reset_index(drop=True)
            .drop_duplicates()
        )

    if clean_cellprofiler:
        annotated = cp_clean(annotated)

    # Reorder annotated metadata columns
    meta_cols = infer_cp_features(annotated, metadata=True)
    other_cols = annotated.drop(meta_cols, axis="columns").columns.tolist()
    annotated = annotated.loc[:, meta_cols + other_cols]
    return annotated
