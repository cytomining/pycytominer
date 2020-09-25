"""
Acquire consensus signatures for input samples
"""

import numpy as np
import pandas as pd

from pycytominer import aggregate
from pycytominer.cyto_utils import output, modz, check_consensus_operation


def consensus(
    profiles,
    replicate_columns=["Metadata_Plate", "Metadata_Well"],
    operation="median",
    features="infer",
    modz_method="spearman",
    output_file="none",
    modz_min_weight=0.01,
    modz_precision=4,
    compression=None,
    float_format=None,
):
    # Confirm that the operation is supported
    check_consensus_operation(operation)

    if operation == "modz":
        consensus_df = modz(
            population_df=profiles,
            replicate_columns=replicate_columns,
            features=features,
            method=modz_method,
            min_weight=modz_min_weight,
            precision=modz_precision,
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
