import os
import pandas as pd
from pycytominer.cyto_utils.cells import SingleCells
from typing import Any, Sequence


def sqlite_to_df(
    data_path: str,
    metadata_path: str = None,
    image_cols: Sequence = ["TableNumber", "ImageNumber", "Metadata_Site"],
    strata: Sequence = ["Metadata_Plate", "Metadata_Well"],
    compute_subsample: bool = False,
    compression_options: Any = None,
    float_format: Any = None,
    single_cell_normalize: bool = True,
    normalize_args: Any = None,
    metadata_identifier: str = "Metadata_",
    metadata_merge_on: Sequence = ["Metadata_Plate", "Metadata_Well"],
):
    """Function to convert SQLite file to Pandas DataFrame."""

    # Define test SQL file
    sql_file = "sqlite:////" + os.path.abspath(data_path)

    # define dataframe
    ap = SingleCells(
        sql_file=sql_file,
        image_cols=image_cols,
        strata=strata,
    )

    # Merge compartments and meta information into one dataframe
    df_merged_sc = ap.merge_single_cells(
        sc_output_file="none",
        compute_subsample=compute_subsample,
        compression_options=compression_options,
        float_format=float_format,
        single_cell_normalize=single_cell_normalize,
        normalize_args=normalize_args,
    )

    # In case metadata is provided, merge into existing dataframe
    if metadata_path:
        # Load additional information of file
        df_info = pd.read_csv(metadata_path)

        # Select only metadata
        _info_meta = [m for m in df_info.columns if m.startswith(
            metadata_identifier)]

        # Merge single cell dataframe with additional information
        df_merged_sc = df_merged_sc.merge(
            right=df_info[_info_meta], how="left", on=metadata_merge_on
        )

    return df_merged_sc
