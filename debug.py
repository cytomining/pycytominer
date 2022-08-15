#!/usr/bin/python3

# imports
import os
import pandas as pd
from pycytominer.cyto_utils.cells import SingleCells


# define test SQL file
filename = "BR00117010"
sql_file = "sqlite:////" + os.path.abspath(
    f"../perturbmatch/datasets/{filename}.sqlite")

# define dataframe
ap = SingleCells(
    sql_file=sql_file,
    image_cols=["TableNumber", "ImageNumber", "Metadata_Site"],
    strata=["Metadata_Plate", "Metadata_Well"]
)

# merge comparments and metainformation into one dataframe
df_merged_sc = ap.merge_single_cells(
    sc_output_file="none",
    compute_subsample=False,
    compression_options=None,
    float_format=None,
    single_cell_normalize=True,
    normalize_args=None,
)

# load additional information of file
df_info = pd.read_csv(f"../perturbmatch/datasets/{filename}_augmented.csv")

# select only metadata
df_info_meta = [m for m in df_info.columns if m.startswith("Metadata_")]

# merge single cell dataframe with additional information
df_merged_sc = df_merged_sc.merge(
    right=df_info_meta, how="left", on=["Metadata_Plate", "Metadata_Well"])

df_merged_sc.to_parquet(f"../perturbmatch/datasets/{filename}.parquet")
