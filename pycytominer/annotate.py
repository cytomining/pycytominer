"""
Annotates profiles with metadata information
"""

import os
from typing import Literal, Optional, Union

import pandas as pd

from pycytominer.cyto_utils import (
    annotate_cmap,
    cp_clean,
    infer_cp_features,
    load_platemap,
    load_profiles,
    output,
)


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
    external_join_left: Optional[bool] = None,
    external_join_right: Optional[bool] = None,
    compression_options: Optional[Union[str, dict[str, str]]] = None,
    float_format: Optional[str] = None,
    cmap_args: Optional[dict[str, Union[str]]] = None,
    **kwargs,
) -> Union[pd.DataFrame, str]:
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
    clean_cellprofiler: bool, default True
        Clean specific CellProfiler feature names.
    external_metadata : str, optional
        File with additional metadata information
    external_join_left : str, optional
        Merge column in the profile metadata.
    external_join_right: str, optional
        Merge column in the external metadata.
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    cmap_args : dict, default None
        Potential keyword arguments for annotate_cmap(). See cyto_utils/annotate_custom.py for more details.

    Returns
    -------
    str or pd.DataFrame
        pd.DataFrame:
            DataFrame of annotated features. If output_file=None, then return the
            DataFrame. If you specify output_file, then write to file and do not return
            data.
        str:
            If output_file is provided, then the function returns the path to the
    """

    # Load Data
    profiles = load_profiles(profiles)
    platemap = load_platemap(platemap, add_metadata_id_to_platemap)

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
        annotated = annotate_cmap(
            annotated,
            annotate_join_on=join_on[1],
            cell_id="unknown" if not cmap_args else cmap_args.get("cell_id", "unknown"),
            perturbation_mode="none"
            if not cmap_args
            else cmap_args.get("perturbation_mode", "none"),
        )

    if clean_cellprofiler:
        annotated = cp_clean(annotated)

    if isinstance(external_metadata, str):
        if not os.path.exists(external_metadata):
            raise FileNotFoundError(
                f"external metadata at {external_metadata} does not exist"
            )

        external_metadata = pd.read_csv(external_metadata)

    if isinstance(external_metadata, pd.DataFrame):
        # Make a copy of the external metadata to avoid modifying the original dataframe
        external_metadata = external_metadata.copy()

        external_metadata.columns = pd.Index([
            f"Metadata_{x}" if not x.startswith("Metadata_") else x
            for x in external_metadata.columns
        ])

        annotated = (
            annotated
            .merge(
                external_metadata,
                left_on=external_join_left,
                right_on=external_join_right,
                how="left",
                suffixes=(None, "_external"),
            )
            .reset_index(drop=True)
            .drop_duplicates()
        )

    # Reorder annotated metadata columns
    meta_cols = infer_cp_features(annotated, metadata=True)
    other_cols = annotated.drop(meta_cols, axis="columns").columns.tolist()

    annotated = annotated.loc[:, meta_cols + other_cols]

    if output_file is not None:
        return output(
            df=annotated,
            output_filename=output_file,
            output_type=output_type,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return annotated
