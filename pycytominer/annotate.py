"""
Annotates profiles with metadata information
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils.output import output


def annotate(
    profiles,
    platemap,
    join_on=["Metadata_well_position", "Metadata_Well"],
    output_file="none",
    add_metadata_id_to_platemap=True,
    format_broad_cmap=False,
    perturbation_mode="none",
    external_metadata
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
    external_metadata - [default: "none"] a string indicating a file with additional
                        metadata information
    format_broad_cmap - [default: False] boolean if we need to add columns to make
                        compatible with Broad CMAP naming conventions.
    perturbation_mode - [default: "none"] - either "chemical", "genetic" or "none" and only
                        active if format_broad_cmap == True
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

        annotated = annotated.assign(
            Metadata_pert_id=annotated.Metadata_broad_sample.str.extract(
                r"(BRD[-N][A-Z0-9]+)"
            ),
            Metadata_pert_mfc_id=annotated.Metadata_broad_sample,
            Metadata_pert_well=join_on[0],
            Metadata_pert_id_vendor="",
        )

        if "Metadata_cell_id" not in annotated.columns:
            annotated = annotated.assign(Metadata_cell_id=cell_id)

        if perturbation_mode == "chemical":
            annotated = annotated.assign(
                Metadata_broad_sample_type=[
                    "control" if x in ["DMSO", np.nan] else "trt"
                    for x in annotated.Metadata_broad_sample
                ],
                Metadata_pert_vehicle=annotated.Metadata_solvent,
            )

            annotated.loc[
                annotated.Metadata_broad_sample_type == "control",
                "Metadata_broad_sample",
            ] = "DMSO"
            annotated.loc[
                annotated.Metadata_broad_sample == "empty", "Metadata_broad_sample_type"
            ] = "empty"
            annotated.loc[
                annotated.Metadata_mmoles_per_liter == "control",
                "Metadata_broad_sample",
            ] = 0

            if "Metadata_mg_per_ml" in annotated.columns:
                annotated.loc[
                    annotated.Metadata_broad_sample_type == "control",
                    "Metadata_mg_per_ml",
                ] = 0

        if perturbation_mode == "genetic":
            annotated = annotated.assign(
                Metadata_broad_sample_type=[
                    "control" if x == "EMPTY" else "trt"
                    for x in annotated.Metadata_pert_name
                ]
            )

        annotated = annotated.assign(
            Metadata_pert_type=annotated.Metadata_broad_sample_type
        )

    if external_metadata != "none":
        assert os.path.exists(external_metadata)
        external_metadata_df = pd.read_csv(external_metadata)
        external_metadata_df.columns = [
            "Metadata_{}".format(x) if not x.startswith("Metadata_") else x
            for x in external_metadata_df.columns
        ]



    if output_file != "none":
        output(
            df=annotated,
            output_filename=output_file,
            compression=compression,
            float_format=float_format,
        )
    else:
        return annotated
