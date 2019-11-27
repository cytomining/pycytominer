"""
Transform profiles into a gct (Gene Cluster Text) file
A gct is a tab deliminted text file that traditionally stores gene expression data
File Format Description: https://clue.io/connectopedia/gct_format

Modified from cytominer_scripts "write_gcg" written in R
https://github.com/broadinstitute/cytominer_scripts/blob/master/write_gct.R
"""

import csv
import numpy as np
import pandas as pd
from pycytominer.cyto_utils.features import infer_cp_features


def write_gct(profiles, output_file, features="infer", version="#1.3"):
    """
    Convert profiles to a .gct file

    Arguments:
    profiles - either pandas DataFrame or a file that stores profile data
    output_file - the name of the gct file to save processed data to
    features - a list of features present in the population dataframe [default: "infer"]
               if "infer", then assume cell painting features are those that start with
               "Cells_", "Nuclei_", or "Cytoplasm_"
    create_row_annotations - [default: True]


    Return:
    Pandas DataFrame of audits or written to file
    """

    # Note, only version 1.3 is currently supported
    assert version == "#1.3", "Only version #1.3 is currently supported."

    # Step 1: Create first two rows of data
    cp_features = infer_cp_features(profiles)

    metadata_df = profiles.loc[:, profiles.columns.str.contains("Metadata_")]
    feature_df = profiles.loc[:, cp_features].reset_index(drop=True).transpose()

    nrow_feature, ncol_features = feature_df.shape
    _, ncol_metadata = metadata_df.shape
    data_dimensions = [nrow_feature, ncol_features, 1, ncol_metadata]

    # Step 2: Get the sample metadata portion of the output file
    metadata_part = metadata_df.transpose()
    metadata_part.columns = ["SAMPLE_{}".format(x) for x in metadata_part.columns]
    metadata_part = (
        metadata_part.transpose()
        .reset_index()
        .rename({"index": "id"}, axis="columns")
        .transpose()
    )
    metadata_part.index = metadata_part.index.str.lstrip("Metadata_")

    # Step 3: Compile feature metadata (Note that this is not fully implemented)
    feature_metadata = (
        ["cp_feature_name"] + [np.nan] * ncol_metadata + feature_df.index.tolist()
    )

    # Step 4: Combine metadata and feature parts
    full_df = pd.concat([metadata_part, feature_df], axis="rows")
    full_df.insert(0, column="feature_metadata", value=feature_metadata)
    full_df = full_df.reset_index()

    # Step 5: Write output gct file
    with open(output_file, "w", newline="") as gctfile:
        gctwriter = csv.writer(gctfile, delimiter="\t")
        gctwriter.writerow([version])
        gctwriter.writerow(data_dimensions)
        for feature, row in full_df.iterrows():
            gctwriter.writerow(row)
