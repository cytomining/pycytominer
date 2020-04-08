"""
Annotates profiles with metadata information
"""

import os
import numpy as np
import pandas as pd
from pycytominer.cyto_utils.output import output
from pycytominer.cyto_utils import infer_cp_features


def annotate(
    profiles,
    platemap,
    cell_id="unknown",
    join_on=["Metadata_well_position", "Metadata_Well"],
    output_file="none",
    add_metadata_id_to_platemap=True,
    format_broad_cmap=False,
    perturbation_mode="none",
    external_metadata="none",
    external_join_left="none",
    external_join_right="none",
    compression=None,
    float_format=None,
):
    """
    Exclude features that have correlations above a certain threshold

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    platemap - either pandas DataFrame or a file that stores platemap metadata
    cell_id - [default: "unknown"] provide a string to annotate cell id column
    join_on - list of length two indicating which variables to merge profiles and plate
              [default: ["Metadata_well_position", "Metadata_Well"]]. The first element
              indicates variable(s) in platemap and the second element indicates
              variable(s) in profiles to merge using.
              Note the setting of `add_metadata_id_to_platemap`
    output_file - [default: "none"] if provided, will write annotated profiles to file
                  if not specified, will return the annotated profiles. We recommend
                  that this output file be suffixed with "_augmented.csv".
    add_metadata_id_to_platemap - boolean if the platemap variables should be recoded
    format_broad_cmap - [default: False] boolean if we need to add columns to make
                        compatible with Broad CMAP naming conventions.
    perturbation_mode - [default: "none"] - either "chemical", "genetic" or "none" and only
                        active if format_broad_cmap == True
    external_metadata - [default: "none"] a string indicating a file with additional
                        metadata information
    external_join_left - [default: "none"] the merge column in the profile metadata
    external_join_right - [default: "none"] the merge column in the external metadata
    compression - the mechanism to compress [default: None]
    float_format - decimal precision to use in writing output file [default: None]
                       For example, use "%.3g" for 3 decimal precision.

    Return:
    Pandas DataFrame of annotated profiles or written to file
    """

    # Load Data
    if not isinstance(profiles, pd.DataFrame):
        try:
            profiles = pd.read_csv(profiles)
        except FileNotFoundError:
            raise FileNotFoundError("{} profile file not found".format(profiles))

    if not isinstance(platemap, pd.DataFrame):
        try:
            platemap = pd.read_csv(platemap, sep="\t")
        except FileNotFoundError:
            raise FileNotFoundError("{} platemap file not found".format(platemap))

    if add_metadata_id_to_platemap:
        platemap.columns = [
            "Metadata_{}".format(x) if not x.startswith("Metadata_") else x
            for x in platemap.columns
        ]

    annotated = platemap.merge(
        profiles, left_on=join_on[0], right_on=join_on[1], how="inner"
    ).drop(join_on[0], axis="columns")

    if format_broad_cmap:

        pert_opts = ["none", "chemical", "genetic"]
        assert (
            perturbation_mode in pert_opts
        ), "perturbation mode must be one of {}".format(pert_opts)

        assert (
            "Metadata_broad_sample" in annotated.columns
        ), "Are you sure this is a CMAP file? 'Metadata_broad_sample column not found.'"

        annotated = annotated.assign(
            Metadata_pert_id=annotated.Metadata_broad_sample.str.extract(
                r"(BRD[-N][A-Z0-9]+)"
            ),
            Metadata_pert_mfc_id=annotated.Metadata_broad_sample,
            Metadata_pert_well=annotated.loc[:, join_on[1]],
            Metadata_pert_id_vendor="",
        )

        if "Metadata_pert_iname" in annotated.columns:
            annotated = annotated.assign(
                Metadata_pert_mfc_desc=annotated.Metadata_pert_iname,
                Metadata_pert_name=annotated.Metadata_pert_iname,
            )

        if "Metadata_cell_id" not in annotated.columns:
            annotated = annotated.assign(Metadata_cell_id=cell_id)

        if perturbation_mode == "chemical":
            annotated = annotated.assign(
                Metadata_broad_sample_type=[
                    "control" if x in ["DMSO", np.nan] else "trt"
                    for x in annotated.Metadata_broad_sample
                ]
            )

            # Generate Metadata_broad_sample column
            annotated.loc[
                annotated.Metadata_broad_sample_type == "control",
                "Metadata_broad_sample",
            ] = "DMSO"
            annotated.loc[
                annotated.Metadata_broad_sample == "empty", "Metadata_broad_sample_type"
            ] = "empty"

            if "Metadata_mmoles_per_liter" in annotated.columns:
                annotated.loc[
                    annotated.Metadata_broad_sample_type == "control",
                    "Metadata_mmoles_per_liter",
                ] = 0

            if "Metadata_solvent" in annotated.columns:
                annotated = annotated.assign(
                    Metadata_pert_vehicle=annotated.Metadata_solvent
                )
            if "Metadata_mg_per_ml" in annotated.columns:
                annotated.loc[
                    annotated.Metadata_broad_sample_type == "control",
                    "Metadata_mg_per_ml",
                ] = 0

        if perturbation_mode == "genetic":
            if "Metadata_pert_name" in annotated.columns:
                annotated = annotated.assign(
                    Metadata_broad_sample_type=[
                        "control" if x == "EMPTY" else "trt"
                        for x in annotated.Metadata_pert_name
                    ]
                )

        if "Metadata_broad_sample_type" in annotated.columns:
            annotated = annotated.assign(
                Metadata_pert_type=annotated.Metadata_broad_sample_type
            )
        else:
            annotated = annotated.assign(
                Metadata_pert_type="", Metadata_broad_sample_type=""
            )

    # Add specific Connectivity Map (CMAP) formatting
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
            compression=compression,
            float_format=float_format,
        )
    else:
        return annotated
