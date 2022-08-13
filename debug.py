#!/usr/bin/python3

# imports
import os
import pandas as pd
from pycytominer.cyto_utils.cells import SingleCells


# define test SQL file
sql_file = "sqlite:////" + os.path.abspath(
    "../perturbmatch/datasets/BR00117010.sqlite")
add_file = "sqlite:////" + os.path.abspath(
    "../perturbmatch/datasets/BR00117010.sqlite")

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
    test=True,
    test_n=100
)

# load additional information of file
df_info = pd.read_csv("../perturbmatch/datasets/BR00117010_augmented.csv")

# merge single cell dataframe with additional information
df_merged_sc = df_merged_sc.merge(right=df_info, how="left", on="Metadata_Well")
