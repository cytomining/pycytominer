"""
Acquire consensus signatures for input samples
"""

from typing import Any, Literal, Optional, Union, cast

import pandas as pd

from pycytominer.aggregate import aggregate
from pycytominer.cyto_utils import check_consensus_operation, load_profiles, modz
from pycytominer.cyto_utils.util import write_to_file_if_user_specifies_output_details


@write_to_file_if_user_specifies_output_details
def consensus(
    profiles: pd.DataFrame,
    replicate_columns: list[str] = ["Metadata_Plate", "Metadata_Well"],
    operation: str = "median",
    features: Union[str, list[str]] = "infer",
    output_file: Optional[str] = None,
    output_type: Optional[
        Literal["csv", "parquet", "anndata_h5ad", "anndata_zarr"]
    ] = "csv",
    compression_options: Optional[Union[str, dict[str, Any]]] = None,
    float_format: Optional[str] = None,
    modz_args: Optional[dict[str, Union[int, float, str]]] = {"method": "spearman"},
) -> Union[pd.DataFrame, str]:
    """Form level 5 consensus profile data.

    Parameters
    ----------
    profiles : pd.DataFrame or file
        DataFrame or file of profiles.
    replicate_columns : list, defaults to ["Metadata_Plate", "Metadata_Well"]
        Metadata columns indicating which replicates to collapse
    operation : str, defaults to "median"
        The method used to form consensus profiles.
    features : list
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume features are from CellProfiler output and
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    output_file : str, optional
        If provided, will write consensus profiles to file. If not specified, will
        return the normalized profiles as output.
    output_type : str, optional
        If provided, will write consensus profiles as a specified file type (either CSV or parquet).
        If not specified and output_file is provided, then the file will be outputed as CSV as default.
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
    str or pd.DataFrame
        pd.DataFrame:
            The consensus profile DataFrame. If output_file=None, then return the
            DataFrame. If you specify output_file, then write to file and do not return
            data.
        str:
            If output_file is provided, then the function returns the path to the
            output file.

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
        output_file=None,
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
            method="spearman"
            if not modz_args
            else str(modz_args.get("method", "spearman")),
            min_weight=0.01
            if not modz_args
            else float(modz_args.get("min_weight", 0.01)),
            precision=4 if not modz_args else int(modz_args.get("precision", 4)),
        )
    else:
        consensus_df = cast(
            pd.DataFrame,
            aggregate(
                population_df=profiles,
                strata=replicate_columns,
                features=features,
                operation=operation,
                subset_data_df=None,
            ),
        )

    return consensus_df
