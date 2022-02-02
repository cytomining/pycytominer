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
    compression_options=None,
    float_format=None,
    modz_args={"method": "spearman"},
):
    """Form level 5 consensus profile data.

    Parameters
    ----------
    profiles : pandas.core.frame.DataFrame or file
        DataFrame or file of profiles.
    replicate_columns : list, defaults to ["Metadata_Plate", "Metadata_Well"]
        Metadata columns indicating which replicates to collapse
    operation : str, defaults to "median"
        The method used to form consensus profiles.
    features : list
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume cell painting features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    output_file : str, optional
        If provided, will write consensus profiles to file. If not specified, will
        return the normalized profiles as output.
    compression_options : str or dict, optional
        Contains compression options as input to
        pd.DataFrame.to_csv(compression=compression_options). pandas version >= 1.2.
    float_format : str, optional
        Decimal precision to use in writing output file as input to
        pd.DataFrame.to_csv(float_format=float_format). For example, use "%.3g" for 3
        decimal precision.
    modz_args : dict, optional
        Additional custom arguments passed as kwargs if operation="modz".
        See pycytominer.cyto_utils.modz for more details.

    Returns
    -------
    consensus_df : pandas.core.frame.DataFrame, optional
        The consensus profile DataFrame. If output_file="none", then return the
        DataFrame. If you specify output_file, then write to file and do not return
        data.

    Examples
    --------
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
            compression_options=compression_options,
            float_format=float_format,
        )
    else:
        return consensus_df
