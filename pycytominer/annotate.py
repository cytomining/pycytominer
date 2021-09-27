"""
Annotates profiles with metadata information
"""

import os
import numpy as np
import pandas as pd
from pycytominer.cyto_utils import (
    output,
    infer_cp_features,
    load_platemap,
    load_profiles,
    annotate_cmap,
    cp_clean,
)


def annotate(
    profiles,
    platemap,
    join_on=["Metadata_well_position", "Metadata_Well"],
    output_file="none",
    add_metadata_id_to_platemap=True,
    format_broad_cmap=False,
    clean_cellprofiler=True,
    external_metadata="none",
    external_join_left="none",
    external_join_right="none",
    compression_options=None,
    float_format=None,
    cmap_args={},
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
        DataFrame of annotated features. If output_file="none", then return the
        DataFrame. If you specify output_file, then write to file and do not return
        data.
    """

    # Load Data
    profiles = load_profiles(profiles)
    platemap = load_platemap(platemap, add_metadata_id_to_platemap)

    annotated = platemap.merge(
        profiles, left_on=join_on[0], right_on=join_on[1], how="inner"
    ).drop(join_on[0], axis="columns")

    # Add specific Connectivity Map (CMAP) formatting
    if format_broad_cmap:
        annotated = annotate_cmap(annotated, annotate_join_on=join_on[1], **cmap_args)

    if clean_cellprofiler:
        annotated = cp_clean(annotated)

    if not isinstance(external_metadata, pd.DataFrame):
        if external_metadata != "none":
            assert os.path.exists(
                external_metadata
            ), "external metadata at {} does not exist".format(external_metadata)

            external_metadata = pd.read_csv(external_metadata)

    if isinstance(external_metadata, pd.DataFrame):
        external_metadata.columns = [
            "Metadata_{}".format(x) if not x.startswith("Metadata_") else x
            for x in external_metadata.columns
        ]

        annotated = (
            annotated.merge(
                external_metadata,
                left_on=external_join_left,
                right_on=external_join_right,
                how="left",
            )
            .reset_index(drop=True)
            .drop_duplicates()
        )

    # Reorder annotated metadata columns
    meta_cols = infer_cp_features(annotated, metadata=True)
    other_cols = annotated.drop(meta_cols, axis="columns").columns.tolist()

    annotated = annotated.loc[:, meta_cols + other_cols]

    if output_file != "none":
        output(
            df=annotated,
            output_filename=output_file,
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return annotated
