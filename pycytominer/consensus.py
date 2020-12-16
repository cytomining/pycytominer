"""
Acquire consensus signatures for input samples
"""

import numpy as np
import pandas as pd

from pycytominer import aggregate
from pycytominer.cyto_utils import (
    output,
    modz,
    load_profiles,
    check_consensus_operation,
)


def consensus(
    profiles,
    replicate_columns=["Metadata_Plate", "Metadata_Well"],
    operation="median",
    features="infer",
    output_file="none",
    compression=None,
    float_format=None,
    modz_args={"method": "spearman"},
):
    """Form level 5 consensus profile data.

    :param profiles: A file or pandas DataFrame of profile data
    :type profiles: str
    :param replicate_columns: Metadata columns indicating which replicates to collapse, defaults to ["Metadata_Plate", "Metadata_Well"]
    :type replicate_columns: list
    :param operation: The method used to form consensus profiles, defaults to "median"
    :type operation: str
    :param features: The features to collapse, defaults to "infer"
    :type features: str, list
    :param output_file: If specified, the location to write the file, defaults to "none"
    :type output_file: str
    :param modz_args: Additional custom arguments passed as kwargs if operation="modz". See pycytominer.cyto_utils.modz for more details.
    :type modz_args: dict
    :param compression: the method to compress output data, defaults to None. See pycytominer.cyto_utils.output.py for options
    :type compression: str
    :param float_format: decimal precision to use in writing output file, defaults to None. For example, use "%.3g" for 3 decimal precision.

    :Example:

    import pandas as pd
    from pycytominer import consensus

    data_df = pd.concat(
        [
            pd.DataFrame(
                {
                    "Metadata_Plate": "X",
                    "Metadata_Well": "a",
                    "Cells_x": [0.1, 0.3, 0.8],
                    "Nuclei_y": [0.5, 0.3, 0.1],
                }
            ),
            pd.DataFrame(
                {
                    "Metadata_Plate": "X",
                    "Metadata_Well": "b",
                    "Cells_x": [0.4, 0.2, -0.5],
                    "Nuclei_y": [-0.8, 1.2, -0.5],
                }
            ),
        ]
    ).reset_index(drop=True)

    consensus_df = consensus(
        profiles=data_df,
        replicate_columns=["Metadata_Plate", "Metadata_Well"],
        operation="median",
        features="infer",
        output_file="none",
    )
    """
    # Confirm that the operation is supported
    check_consensus_operation(operation)

    # Load Data
    profiles = load_profiles(profiles)

    if operation == "modz":
        consensus_df = modz(
            population_df=profiles,
            replicate_columns=replicate_columns,
            features=features,
            **modz_args
        )
    else:
        consensus_df = aggregate(
            population_df=profiles,
            strata=replicate_columns,
            features=features,
            operation=operation,
            subset_data_df="none",
        )

    if output_file != "none":
        output(
            df=consensus_df,
            output_filename=output_file,
            compression=compression,
            float_format=float_format,
        )
    else:
        return consensus_df


data_df = pd.concat(
    [
        pd.DataFrame(
            {
                "Metadata_Plate": "X",
                "Metadata_Well": "a",
                "Cells_x": [0.1, 0.3, 0.8],
                "Nuclei_y": [0.5, 0.3, 0.1],
            }
        ),
        pd.DataFrame(
            {
                "Metadata_Plate": "X",
                "Metadata_Well": "b",
                "Cells_x": [0.4, 0.2, -0.5],
                "Nuclei_y": [-0.8, 1.2, -0.5],
            }
        ),
    ]
).reset_index(drop=True)
