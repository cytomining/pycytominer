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
    """
    Exclude features that have correlations above a certain threshold

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    platemap - either pandas DataFrame or a file that stores platemap metadata
    join_on - list of length two indicating which variables to merge profiles and plate
              [default: ["Metadata_well_position", "Metadata_Well"]]. The first element
              indicates variable(s) in platemap and the second element indicates
              variable(s) in profiles to merge using.
              Note the setting of `add_metadata_id_to_platemap`
    output_file - [default: "none"] if provided, will write annotated profiles to file
                  if not specified, will return the annotated profiles. We recommend
                  that this output file be suffixed with "_augmented.csv".
    add_metadata_id_to_platemap - [default: True] boolean if the platemap variables possibly need "Metadata" pre-pended
    format_broad_cmap - [default: False] boolean if we need to add columns to make
                        compatible with Broad CMAP naming conventions.
    external_metadata - [default: "none"] a string indicating a file with additional
                        metadata information
    external_join_left - [default: "none"] the merge column in the profile metadata
    external_join_right - [default: "none"] the merge column in the external metadata
    compression_options - the mechanism to compress [default: None] See cyto_utils/output.py for options.
    float_format - decimal precision to use in writing output file [default: None]
                       For example, use "%.3g" for 3 decimal precision.
    cmap_args - [default: {}] - potential keyword arguments for annotate_cmap().
                See cyto_utils/annotate_cmap.py for more details.

    Return:
    Pandas DataFrame of annotated profiles or written to file
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
