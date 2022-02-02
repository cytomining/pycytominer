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
from pycytominer.cyto_utils import infer_cp_features


def write_gct(
    profiles,
    output_file,
    features="infer",
    meta_features="infer",
    feature_metadata="none",
    version="#1.3",
):
    """Convert profiles to a .gct file

    Parameters
    ----------
    profiles : pandas.core.frame.DataFrame
        DataFrame of profiles.
    output_file : str
        If provided, will write gct to file.
    features : list
        A list of strings corresponding to feature measurement column names in the
        `profiles` DataFrame. All features listed must be found in `profiles`.
        Defaults to "infer". If "infer", then assume cell painting features are those
        prefixed with "Cells", "Nuclei", or "Cytoplasm".
    meta_features : list
        A list of strings corresponding to metadata column names in the `profiles`
        DataFrame. All features listed must be found in `profiles`. Defaults to "infer".
        If "infer", then assume metadata features are those prefixed with "Metadata"
    feature_metadata : pandas.core.frame.DataFrame, default "none"
    version : str, default "#1.3"
        Important for gct loading into Morpheus

    Returns
    -------
    None
        Writes gct to file
    """

    # Note, only version 1.3 is currently supported
    assert version == "#1.3", "Only version #1.3 is currently supported."

    # Step 1: Create first two rows of data
    if features == "infer":
        features = infer_cp_features(profiles)
    feature_df = profiles.loc[:, features].reset_index(drop=True).transpose()

    # Separate out metadata features
    if meta_features == "infer":
        meta_features = infer_cp_features(profiles, metadata=True)
    metadata_df = profiles.loc[:, meta_features]

    # Step 2: Get the sample metadata portion of the output file
    metadata_part = metadata_df.transpose()
    metadata_part.columns = ["SAMPLE_{}".format(x) for x in metadata_part.columns]
    metadata_part = (
        metadata_part.transpose()
        .reset_index()
        .rename({"index": "id"}, axis="columns")
        .transpose()
    )
    metadata_part.index = [x.replace("Metadata_", "") for x in metadata_part.index]

    nrow_feature, ncol_features = feature_df.shape
    _, ncol_metadata = metadata_df.shape

    # Step 3: Compile feature metadata
    full_df = pd.concat([metadata_part, feature_df], axis="rows")
    if isinstance(feature_metadata, pd.DataFrame):
        nrow_metadata = feature_metadata.shape[1]
        assert (
            "id" in feature_metadata.index.tolist()
        ), "make sure feature metadata has row named 'id' that stores feature metadata names!"
        full_df = feature_metadata.merge(
            full_df, how="right", left_index=True, right_index=True
        )
    else:
        feature_metadata = (
            ["cp_feature_name"] + [np.nan] * ncol_metadata + feature_df.index.tolist()
        )
        nrow_metadata = 1
        full_df.insert(0, column="feature_metadata", value=feature_metadata)
    full_df = full_df.reset_index()

    # Step 4: Compile all data dimensions
    data_dimensions = [nrow_feature, ncol_features, nrow_metadata, ncol_metadata]

    # Step 5: Write output gct file
    with open(output_file, "w", newline="") as gctfile:
        gctwriter = csv.writer(gctfile, delimiter="\t")
        gctwriter.writerow([version])
        gctwriter.writerow(data_dimensions)
        for feature, row in full_df.iterrows():
            gctwriter.writerow(row)
