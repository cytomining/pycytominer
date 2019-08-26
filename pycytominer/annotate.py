"""
Annotates profiles with metadata information
"""

import numpy as np
import pandas as pd
from pycytominer.cyto_utils.compress import compress


def annotate(
    profiles,
    platemap,
    join_on=["Metadata_well_position", "Metadata_Well"],
    output_file="none",
    add_metadata_id_to_platemap=True,
    **kwargs,
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
    add_metadata_id_to_platemap - boolean if the platemap variables should be recoded

    Return:
    Pandas DataFrame of annotated profiles or written to file
    """
    how = kwargs.pop("how", None)
    float_format = kwargs.pop("float_format", None)

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

    if output_file != "none":
        compress(
            df=annotated,
            output_filename=output_file,
            how=how,
            float_format=float_format,
        )
    else:
        return annotated
