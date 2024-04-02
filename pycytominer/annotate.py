"""
Annotates profiles with metadata information
"""

import os

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
    profiles,
    platemap,
    join_on=["Metadata_well_position", "Metadata_Well"],
    output_file=None,
    output_type="csv",
    add_metadata_id_to_platemap=True,
    format_broad_cmap=False,
    clean_cellprofiler=True,
    external_metadata=None,
    external_join_left=None,
    external_join_right=None,
    compression_options=None,
    float_format=None,
    cmap_args={},
    **kwargs,
):
    """Add metadata to aggregated profiles.

    Parameters
    ----------
    profiles : pandas.core.frame.DataFrame or file
        DataFrame or file path of profiles.
    platemap : pandas.core.frame.DataFrame or file
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
    cmap_args : dict, default {}
        Potential keyword arguments for annotate_cmap(). See cyto_utils/annotate_custom.py for more details.

    Returns
    -------
    annotated : pandas.core.frame.DataFrame, optional
        DataFrame of annotated features. If output_file=None, then return the
        DataFrame. If you specify output_file, then write to file and do not return
        data.
    """

    # Load Data
    profiles = load_profiles(profiles)
    platemap = load_platemap(platemap, add_metadata_id_to_platemap)

    annotated = platemap.merge(
        profiles,
        left_on=join_on[0],
        right_on=join_on[1],
        how="inner",
        suffixes=["_platemap", None],
    )
    if join_on[0] != join_on[1]:
        annotated = annotated.drop(join_on[0], axis="columns")

    # Add specific Connectivity Map (CMAP) formatting
    if format_broad_cmap:
        annotated = annotate_cmap(annotated, annotate_join_on=join_on[1], **cmap_args)

    if clean_cellprofiler:
        annotated = cp_clean(annotated)

    if not isinstance(external_metadata, pd.DataFrame):
        if external_metadata is not None:
            if not os.path.exists(external_metadata):
                raise FileNotFoundError(
                    f"external metadata at {external_metadata} does not exist"
                )

            external_metadata = pd.read_csv(external_metadata)
    else:
        # Make a copy of the external metadata to avoid modifying the original column names
        external_metadata = external_metadata.copy()

    if isinstance(external_metadata, pd.DataFrame):
        external_metadata.columns = [
            f"Metadata_{x}" if not x.startswith("Metadata_") else x
            for x in external_metadata.columns
        ]

        annotated = (
            annotated.merge(
                external_metadata,
                left_on=external_join_left,
                right_on=external_join_right,
                how="left",
                suffixes=[None, "_external"],
            )
            .reset_index(drop=True)
            .drop_duplicates()
        )

    # Reorder annotated metadata columns
    meta_cols = infer_cp_features(annotated, metadata=True)
    other_cols = annotated.drop(meta_cols, axis="columns").columns.tolist()

    annotated = annotated.loc[:, meta_cols + other_cols]

    if output_file is not None:
        output(
            df=annotated,
            output_filename=output_file,
            output_type=output_type,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return annotated
