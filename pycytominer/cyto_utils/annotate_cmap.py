import os
import numpy as np
import pandas as pd


def annotate_cmap(annotated, cell_id="unknown", perturbation_mode="none"):
    """
        cell_id - [default: "unknown"] provide a string to annotate cell id column
    """
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

    return annotated
