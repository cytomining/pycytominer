import os
import pathlib
import random
import tempfile

import numpy as np
import pandas as pd
import pytest
from pycytominer import aggregate, annotate, normalize
from pycytominer.cyto_utils import (
    get_default_compartments,
    get_default_linking_cols,
    infer_cp_features,
)
from pycytominer.cyto_utils.cells import SingleCells, _sqlite_strata_conditions
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

random.seed(123)


def build_random_data(
    compartment="cells",
    ImageNumber=sorted(["x", "y"] * 50),
    TableNumber=sorted(["x_hash", "y_hash"] * 50),
):
    a_feature = random.sample(range(1, 1000), 100)
    b_feature = random.sample(range(1, 1000), 100)
    c_feature = random.sample(range(1, 1000), 100)
    d_feature = random.sample(range(1, 1000), 100)
    data_df = pd.DataFrame(
        {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
    ).reset_index(drop=True)

    data_df.columns = [
        "{}_{}".format(compartment.capitalize(), x) for x in data_df.columns
    ]

    data_df = data_df.assign(
        ObjectNumber=list(range(1, 51)) * 2,
        ImageNumber=ImageNumber,
        TableNumber=TableNumber,
    )

    return data_df


# Get temporary directory
TMPDIR = tempfile.gettempdir()

# Launch a sqlite connection
TMP_SQLITE_FILE = f"sqlite:///{TMPDIR}/test.sqlite"

TEST_ENGINE = create_engine(TMP_SQLITE_FILE)

# Setup data
CELLS_DF = build_random_data(compartment="cells")
CYTOPLASM_DF = build_random_data(compartment="cytoplasm").assign(
    Cytoplasm_Parent_Cells=(list(range(1, 51)) * 2)[::-1],
    Cytoplasm_Parent_Nuclei=(list(range(1, 51)) * 2)[::-1],
)
NUCLEI_DF = build_random_data(compartment="nuclei")
IMAGE_DF = pd.DataFrame(
    {
        "TableNumber": ["x_hash", "y_hash"],
        "ImageNumber": ["x", "y"],
        "Metadata_Plate": ["plate", "plate"],
        "Metadata_Well": ["A01", "A02"],
        "Metadata_Site": [1, 1],
    }
)

IMAGE_DF_ADDITIONAL_FEATURES = pd.DataFrame(
    {
        "TableNumber": ["x_hash", "y_hash"],
        "ImageNumber": ["x", "y"],
        "Metadata_Plate": ["plate", "plate"],
        "Metadata_Well": ["A01", "A01"],
        "Metadata_Site": [1, 2],
        "Count_Cells": [50, 50],
        "Granularity_1_Mito": [3.0, 4.0],
        "Texture_Variance_RNA_20_00": [12.0, 14.0],
        "Texture_InfoMeas2_DNA_5_02": [5.0, 1.0],
    }
)

# platemap metadata df for optional annotation of SingleCells
PLATEMAP_DF = pd.DataFrame(
    {
        "well_position": ["A01", "A02"],
        "gene": ["x", "y"],
    }
).reset_index(drop=True)


# Ingest data into temporary sqlite file
IMAGE_DF.to_sql(name="image", con=TEST_ENGINE, index=False, if_exists="replace")
CELLS_DF.to_sql(name="cells", con=TEST_ENGINE, index=False, if_exists="replace")
CYTOPLASM_DF.to_sql(name="cytoplasm", con=TEST_ENGINE, index=False, if_exists="replace")
NUCLEI_DF.to_sql(name="nuclei", con=TEST_ENGINE, index=False, if_exists="replace")

# Create a new table with a fourth compartment
NEW_FILE = f"sqlite:///{TMPDIR}/test_new.sqlite"
NEW_COMPARTMENT_DF = build_random_data(compartment="new")

TEST_NEW_ENGINE = create_engine(NEW_FILE)

IMAGE_DF.to_sql(name="image", con=TEST_NEW_ENGINE, index=False, if_exists="replace")
CELLS_DF.to_sql(name="cells", con=TEST_NEW_ENGINE, index=False, if_exists="replace")
NEW_CYTOPLASM_DF = CYTOPLASM_DF.assign(
    Cytoplasm_Parent_New=(list(range(1, 51)) * 2)[::-1]
)
NEW_CYTOPLASM_DF.to_sql(
    name="cytoplasm", con=TEST_NEW_ENGINE, index=False, if_exists="replace"
)
NUCLEI_DF.to_sql(name="nuclei", con=TEST_NEW_ENGINE, index=False, if_exists="replace")
NEW_COMPARTMENT_DF.to_sql(
    name="new", con=TEST_NEW_ENGINE, index=False, if_exists="replace"
)

NEW_COMPARTMENTS = ["cells", "cytoplasm", "nuclei", "new"]

NEW_LINKING_COLS = get_default_linking_cols()
NEW_LINKING_COLS["cytoplasm"]["new"] = "Cytoplasm_Parent_New"
NEW_LINKING_COLS["new"] = {"cytoplasm": "ObjectNumber"}

# Ingest data with additional image features to temporary sqlite file

IMAGE_FILE = f"sqlite:///{TMPDIR}/test_image.sqlite"

TEST_ENGINE_IMAGE = create_engine(IMAGE_FILE)

IMAGE_DF_ADDITIONAL_FEATURES.to_sql(
    name="image", con=TEST_ENGINE_IMAGE, index=False, if_exists="replace"
)
CELLS_DF.to_sql(name="cells", con=TEST_ENGINE_IMAGE, index=False, if_exists="replace")
CYTOPLASM_DF.to_sql(
    name="cytoplasm", con=TEST_ENGINE_IMAGE, index=False, if_exists="replace"
)
NUCLEI_DF.to_sql(name="nuclei", con=TEST_ENGINE_IMAGE, index=False, if_exists="replace")

# Ingest data with different image table name
IMAGE_DIFF_FILE = f"sqlite:///{TMPDIR}/test_image_diff_table_name.sqlite"

TEST_ENGINE_IMAGE_DIFF = create_engine(IMAGE_DIFF_FILE)

IMAGE_DF.to_sql(
    name="Per_Image", con=TEST_ENGINE_IMAGE_DIFF, index=False, if_exists="replace"
)
CELLS_DF.to_sql(
    name="cells", con=TEST_ENGINE_IMAGE_DIFF, index=False, if_exists="replace"
)
CYTOPLASM_DF.to_sql(
    name="cytoplasm", con=TEST_ENGINE_IMAGE_DIFF, index=False, if_exists="replace"
)
NUCLEI_DF.to_sql(
    name="nuclei", con=TEST_ENGINE_IMAGE_DIFF, index=False, if_exists="replace"
)

# Setup SingleCells Class
AP = SingleCells(sql_file=TMP_SQLITE_FILE)
AP_SUBSAMPLE = SingleCells(
    sql_file=TMP_SQLITE_FILE,
    subsample_n=2,
    subsampling_random_state=123,
)

# Warning expected for compartment "new" because is not in default compartment list.
with pytest.warns(UserWarning, match="Non-canonical compartment detected: new"):
    AP_NEW = SingleCells(
        sql_file=NEW_FILE,
        load_image_data=False,
        compartments=NEW_COMPARTMENTS,
        compartment_linking_cols=NEW_LINKING_COLS,
    )

AP_IMAGE_ALL_FEATURES = SingleCells(
    sql_file=IMAGE_FILE,
    add_image_features=True,
    image_feature_categories=["Count", "Granularity", "Texture"],
)

AP_IMAGE_SUBSET_FEATURES = SingleCells(
    sql_file=IMAGE_FILE,
    add_image_features=True,
    image_feature_categories=["Count", "Texture"],
)

AP_IMAGE_COUNT = SingleCells(
    sql_file=IMAGE_FILE, add_image_features=True, image_feature_categories=["Count"]
)

AP_IMAGE_DIFF_NAME = SingleCells(
    sql_file=IMAGE_DIFF_FILE, load_image_data=False, image_feature_categories=["Count"]
)

SUBSET_FEATURES = [
    "TableNumber",
    "ImageNumber",
    "ObjectNumber",
    "Cells_Parent_Nuclei",
    "Cytoplasm_Parent_Cells",
    "Cytoplasm_Parent_Nuclei",
    "Cells_a",
    "Cytoplasm_a",
    "Nuclei_a",
]
AP_SUBSET = SingleCells(sql_file=TMP_SQLITE_FILE, features=SUBSET_FEATURES)


def test_SingleCells_init():
    """
    Testing initialization of SingleCells
    """
    assert AP.sql_file == TMP_SQLITE_FILE
    assert AP.strata == ["Metadata_Plate", "Metadata_Well"]
    assert AP.merge_cols == ["TableNumber", "ImageNumber"]
    assert AP.image_cols == ["TableNumber", "ImageNumber", "Metadata_Site"]
    pd.testing.assert_frame_equal(
        IMAGE_DF.sort_index(axis=1), AP.image_df.sort_index(axis=1)
    )
    assert AP.features == "infer"
    assert AP.subsample_frac == 1
    assert AP_SUBSAMPLE.subsample_frac == 1
    assert AP.subsample_n == "all"
    assert AP_SUBSAMPLE.subsample_n == 2
    assert AP.subset_data_df == "none"
    assert AP.output_file == "none"
    assert AP.aggregation_operation == "median"
    assert not AP.is_aggregated
    assert AP.subsampling_random_state == "none"
    assert AP_SUBSAMPLE.subsampling_random_state == 123
    assert AP.fields_of_view == "all"
    assert AP.fields_of_view_feature == "Metadata_Site"
    assert AP.object_feature == "Metadata_ObjectNumber"
    assert AP.compartment_linking_cols == get_default_linking_cols()
    assert AP.compartments == get_default_compartments()


def test_SingleCells_reset_variables():
    """
    Testing initialization of SingleCells
    """
    ap_switch = SingleCells(sql_file=TMP_SQLITE_FILE)
    assert ap_switch.subsample_frac == 1
    assert ap_switch.subsample_n == "all"
    assert ap_switch.subsampling_random_state == "none"
    ap_switch.set_subsample_frac(0.8)
    assert ap_switch.subsample_frac == 0.8
    ap_switch.set_subsample_frac(1)
    ap_switch.set_subsample_n(4)
    assert ap_switch.subsample_n == 4
    ap_switch.set_subsample_random_state(42)
    assert ap_switch.subsampling_random_state == 42

    with pytest.raises(AssertionError) as errorinfo:
        ap_switch.set_subsample_frac(0.8)
    assert "Do not set both subsample_frac and subsample_n" in str(
        errorinfo.value.args[0]
    )

    with pytest.raises(ValueError) as errorinfo:
        ap_switch.set_subsample_frac(1)
        ap_switch.set_subsample_n("wont work")

    assert "subsample n must be an integer or coercable" in str(errorinfo.value.args[0])


def test_SingleCells_count():
    count_df = AP.count_cells()
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cell_count": [50, 50],
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_load_compartment():
    loaded_compartment_df = AP.load_compartment(compartment="cells")
    pd.testing.assert_frame_equal(
        loaded_compartment_df,
        CELLS_DF.reindex(columns=loaded_compartment_df.columns),
        check_dtype=False,
    )

    # Test non-canonical compartment loading
    loaded_compartment_df = AP_NEW.load_compartment("new")
    pd.testing.assert_frame_equal(
        NEW_COMPARTMENT_DF.reindex(columns=loaded_compartment_df.columns),
        loaded_compartment_df,
        check_dtype=False,
    )

    # test load_compartment with non-default default_datatype_float
    # create new SingleCells based on AP
    float32_loaded_compartment_df = SingleCells(
        sql_file=TMP_SQLITE_FILE, default_datatype_float=np.float32
    ).load_compartment(compartment="cells")

    # for uniformly handling metadata types for both dataframes
    metadata_types = {"ObjectNumber": "int64"}

    # updated column datatypes for manual comparisons with CELLS_DF
    cells_df_comparison_types = {
        colname: np.float32
        for colname in CELLS_DF.columns
        # check for only columns which are of float type
        if pd.api.types.is_float(CELLS_DF[colname].dtype)
        # check for columns which are of 'int64' type
        # note: pd.api.types.is_integer sometimes is unable to detect int64
        or CELLS_DF[colname].dtype == "int64"
        # avoid recasting the metadata_types
        and colname not in metadata_types.keys()
    }

    # create deep copy of CELLS_DF with manually re-typed float columns as float32
    # and cast any float type columns to float32 for expected comparison
    cells_df_for_compare = CELLS_DF.copy(deep=True).astype(cells_df_comparison_types)[
        # use float32_loaded_compartment_df column order for comparison
        float32_loaded_compartment_df.columns
    ]

    # cast metadata types in the same way for comparisons
    float32_loaded_compartment_df = float32_loaded_compartment_df.astype(metadata_types)
    cells_df_for_compare = cells_df_for_compare.astype(metadata_types)

    # perform comparison of dataframes
    pd.testing.assert_frame_equal(
        float32_loaded_compartment_df,
        cells_df_for_compare,
    )


def test_sc_count_sql_table():
    # Iterate over initialized compartments
    for compartment in AP.compartments:
        result_row_count = AP.count_sql_table_rows(table=compartment)
        assert result_row_count == 100


def test_get_sql_table_col_names():
    # Iterate over initialized compartments
    for compartment in AP.compartments:
        expected_meta_cols = ["ObjectNumber", "ImageNumber", "TableNumber"]
        expected_feat_cols = [
            f"{compartment.capitalize()}_{i}" for i in ["a", "b", "c", "d"]
        ]
        if compartment == "cytoplasm":
            expected_feat_cols += ["Cytoplasm_Parent_Cells", "Cytoplasm_Parent_Nuclei"]
        col_name_result = AP.get_sql_table_col_names(table=compartment)
        assert sorted(col_name_result) == sorted(
            expected_feat_cols + expected_meta_cols
        )
        meta_cols, feat_cols = AP.split_column_categories(col_name_result)
        assert meta_cols == expected_meta_cols
        assert feat_cols == expected_feat_cols


def test_merge_single_cells():
    sc_merged_df = AP.merge_single_cells()

    # Assert that the image data was merged
    assert all(x in sc_merged_df.columns for x in ["Metadata_Plate", "Metadata_Well"])

    # Assert that metadata columns were renamed appropriately
    for x in AP.full_merge_suffix_rename:
        assert AP.full_merge_suffix_rename[x] == f"Metadata_{x}"

    # Perform a manual merge
    manual_merge = CYTOPLASM_DF.merge(
        CELLS_DF,
        left_on=["TableNumber", "ImageNumber", "Cytoplasm_Parent_Cells"],
        right_on=["TableNumber", "ImageNumber", "ObjectNumber"],
        suffixes=["_cytoplasm", "_cells"],
    ).merge(
        NUCLEI_DF,
        left_on=["TableNumber", "ImageNumber", "Cytoplasm_Parent_Nuclei"],
        right_on=["TableNumber", "ImageNumber", "ObjectNumber"],
        suffixes=["_cytoplasm", "_nuclei"],
    )

    manual_merge = IMAGE_DF.merge(manual_merge, on=AP.merge_cols, how="right").rename(
        AP.full_merge_suffix_rename, axis="columns"
    )

    # Confirm that the merge correctly reversed the object number (opposite from Parent)
    assert (
        sc_merged_df.Metadata_ObjectNumber_cytoplasm.tolist()[::-1]
        == sc_merged_df.Metadata_ObjectNumber.tolist()
    )
    assert (
        manual_merge.Metadata_ObjectNumber_cytoplasm.tolist()[::-1]
        == sc_merged_df.Metadata_ObjectNumber.tolist()
    )
    assert (
        manual_merge.Metadata_ObjectNumber_cytoplasm.tolist()[::-1]
        == sc_merged_df.Metadata_ObjectNumber.tolist()
    )
    assert (
        manual_merge.Metadata_ObjectNumber_cells.tolist()
        == sc_merged_df.Metadata_ObjectNumber.tolist()
    )

    # Confirm the merge and adding merge options
    for method in ["standardize", "robustize"]:
        for samples in ["all", "Metadata_ImageNumber == 'x'"]:
            for features in ["infer", ["Cytoplasm_a", "Cells_a"]]:
                norm_method_df = AP.merge_single_cells(
                    single_cell_normalize=True,
                    normalize_args={
                        "method": method,
                        "samples": samples,
                        "features": features,
                    },
                )

                manual_merge_normalize = normalize(
                    manual_merge, method=method, samples=samples, features=features
                )

                pd.testing.assert_frame_equal(
                    norm_method_df.sort_index(axis=1),
                    manual_merge_normalize.sort_index(axis=1),
                    check_dtype=False,
                )


@pytest.mark.skip(
    reason="This test will soon fail because of a logic error in merge_single_cells"
)
def test_merge_single_cells_non_canonical():
    # The test raises this warning:
    # FutureWarning: Passing 'suffixes' which cause duplicate columns
    # {'ObjectNumber_cytoplasm'} in the result is deprecated and will raise a
    # MergeError in a future version.
    # See https://github.com/cytomining/pycytominer/issues/266

    # Test non-canonical compartment merging
    new_sc_merge_df = AP_NEW.merge_single_cells()

    assert sum(new_sc_merge_df.columns.str.startswith("New")) == 4
    assert (
        NEW_COMPARTMENT_DF.ObjectNumber.tolist()[::-1]
        == new_sc_merge_df.Metadata_ObjectNumber_new.tolist()
    )

    norm_new_method_df = AP_NEW.merge_single_cells(
        single_cell_normalize=True,
        normalize_args={
            "method": "standardize",
            "samples": "all",
            "features": "infer",
        },
    )

    norm_new_method_no_feature_infer_df = AP_NEW.merge_single_cells(
        single_cell_normalize=True,
        normalize_args={
            "method": "standardize",
            "samples": "all",
        },
    )

    default_feature_infer_df = AP_NEW.merge_single_cells(single_cell_normalize=True)

    pd.testing.assert_frame_equal(
        norm_new_method_df, default_feature_infer_df, check_dtype=False
    )
    pd.testing.assert_frame_equal(
        norm_new_method_df, norm_new_method_no_feature_infer_df
    )

    new_compartment_cols = infer_cp_features(
        NEW_COMPARTMENT_DF, compartments=AP_NEW.compartments
    )
    traditional_norm_df = normalize(
        AP_NEW.image_df.merge(NEW_COMPARTMENT_DF, on=AP.merge_cols),
        features=new_compartment_cols,
        samples="all",
        method="standardize",
    )

    pd.testing.assert_frame_equal(
        norm_new_method_df.loc[:, new_compartment_cols].abs().describe(),
        traditional_norm_df.loc[:, new_compartment_cols].abs().describe(),
    )


def test_merge_single_cells_subsample():
    for subsample_frac in [0.1, 0.5, 0.9]:
        ap_subsample = SingleCells(
            sql_file=TMP_SQLITE_FILE, subsample_frac=subsample_frac
        )

        sc_merged_df = ap_subsample.merge_single_cells(
            sc_output_file="none",
            compute_subsample=True,
            compression_options=None,
            float_format=None,
            single_cell_normalize=True,
            normalize_args=None,
        )

        # Assert that the image data was merged
        assert all(
            x in sc_merged_df.columns for x in ["Metadata_Plate", "Metadata_Well"]
        )

        # Assert that metadata columns were renamed appropriately
        for x in ap_subsample.full_merge_suffix_rename:
            assert ap_subsample.full_merge_suffix_rename[x] == f"Metadata_{x}"

        # Assert that the subsample fraction worked
        assert sc_merged_df.shape[0] == CELLS_DF.shape[0] * subsample_frac

    for subsample_n in [2, 5, 10]:
        ap_subsample = SingleCells(sql_file=TMP_SQLITE_FILE, subsample_n=subsample_n)

        sc_merged_df = ap_subsample.merge_single_cells(
            sc_output_file="none",
            compute_subsample=True,
            compression_options=None,
            float_format=None,
            single_cell_normalize=True,
            normalize_args=None,
        )

        # Assert that the number of each strata should be even
        assert subsample_n == int(
            sc_merged_df.loc[:, ap_subsample.strata].value_counts().values.mean()
        )


def test_merge_single_cells_annotate():
    """
    Tests SingleCells.merge_single_cells using optional annotate functionality
    """

    expected_sc_merged_df = annotate(
        profiles=AP.merge_single_cells(),
        platemap=PLATEMAP_DF,
        join_on=["Metadata_well_position", "Metadata_Well"],
    )
    sc_merged_df = AP.merge_single_cells(
        platemap=PLATEMAP_DF, join_on=["Metadata_well_position", "Metadata_Well"]
    )

    pd.testing.assert_frame_equal(sc_merged_df, expected_sc_merged_df)


def test_merge_single_cells_cytominer_database_test_file():
    """
    Tests SingleCells.merge_single_cells using cytominer-database test file
    """

    # read test file based on cytominer-database exports
    sql_path = pathlib.Path(
        f"{os.path.dirname(__file__)}/../test_data/cytominer_database_example_data/test_SQ00014613.sqlite",
    )
    csv_path = pathlib.Path(
        f"{os.path.dirname(__file__)}/../test_data/cytominer_database_example_data/test_SQ00014613.csv.gz",
    )
    parquet_path = pathlib.Path(
        f"{os.path.dirname(__file__)}/../test_data/cytominer_database_example_data/test_SQ00014613.parquet",
    )
    sql_url = f"sqlite:///{sql_path}"
    print(sql_url)

    # build SingleCells from database
    sc_p = SingleCells(
        sql_url,
        strata=["Image_Metadata_Plate", "Image_Metadata_Well"],
        image_cols=["TableNumber", "ImageNumber"],
    )

    # gather base merge_single_cells df
    merged_sc = sc_p.merge_single_cells()

    # test csv output from merge_single_cells
    result_file = sc_p.merge_single_cells(
        sc_output_file=pathlib.Path(
            f"{TMPDIR}/test_SQ00014613.csv.gz",
        ),
        compression_options={"method": "gzip"},
    )
    # note: pd.DataFrame datatypes sometimes appear automatically changed on-read, so we cast
    # the result_file dataframe using the base dataframe's types.
    pd.testing.assert_frame_equal(
        pd.read_csv(csv_path).astype(merged_sc.dtypes.to_dict()),
        pd.read_csv(result_file).astype(merged_sc.dtypes.to_dict()),
    )

    # test parquet output from merge_single_cells
    result_file = sc_p.merge_single_cells(
        sc_output_file=pathlib.Path(
            f"{TMPDIR}/test_SQ00014613.parquet",
        ),
        output_type="parquet",
    )
    # note: pd.DataFrame datatypes sometimes appear automatically changed on-read, so we cast
    # the result_file dataframe using the base dataframe's types.
    pd.testing.assert_frame_equal(
        pd.read_parquet(parquet_path).astype(merged_sc.dtypes.to_dict()),
        pd.read_parquet(result_file).astype(merged_sc.dtypes.to_dict()),
    )

    # test parquet output from merge_single_cells with annotation meta
    merged_sc = sc_p.merge_single_cells(
        join_on=["Metadata_well_position", "Image_Metadata_Well"],
        platemap=PLATEMAP_DF,
    )
    result_file = sc_p.merge_single_cells(
        sc_output_file=pathlib.Path(
            f"{TMPDIR}/test_SQ00014613.parquet",
        ),
        output_type="parquet",
        join_on=["Metadata_well_position", "Image_Metadata_Well"],
        platemap=PLATEMAP_DF,
    )
    # note: pd.DataFrame datatypes sometimes appear automatically changed on-read, so we cast
    # the result_file dataframe using the base dataframe's types.
    pd.testing.assert_frame_equal(
        merged_sc,
        pd.read_parquet(result_file).astype(merged_sc.dtypes.to_dict()),
    )


def test_aggregate_comparment():
    df = IMAGE_DF.merge(CELLS_DF, how="inner", on=["TableNumber", "ImageNumber"])
    result = aggregate(df)
    ap_result = AP.aggregate_compartment("cells")

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Cells_a": [368.0, 583.5],
            "Cells_b": [482.0, 478.5],
            "Cells_c": [531.0, 461.5],
            "Cells_d": [585.5, 428.0],
        }
    )

    pd.testing.assert_frame_equal(result, expected_result)
    pd.testing.assert_frame_equal(result, ap_result)
    pd.testing.assert_frame_equal(ap_result, expected_result)


def test_aggregate_profiles():
    result = AP.aggregate_profiles()

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Metadata_Object_Count": [50, 50],
            "Metadata_Site_Count": [1, 1],
            "Cells_a": [368.0, 583.5],
            "Cells_b": [482.0, 478.5],
            "Cells_c": [531.0, 461.5],
            "Cells_d": [585.5, 428.0],
            "Cytoplasm_a": [479.5, 495.5],
            "Cytoplasm_b": [445.5, 459.0],
            "Cytoplasm_c": [407.5, 352.0],
            "Cytoplasm_d": [533.0, 545.0],
            "Nuclei_a": [591.5, 435.5],
            "Nuclei_b": [574.0, 579.0],
            "Nuclei_c": [588.5, 538.5],
            "Nuclei_d": [483.0, 560.0],
        }
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis=1), expected_result.sort_index(axis=1)
    )

    # Confirm aggregation after merging single cells
    sc_df = AP.merge_single_cells()
    sc_aggregated_df = aggregate(sc_df, compute_object_count=True).sort_index(
        axis="columns"
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis="columns").drop("Metadata_Site_Count", axis="columns"),
        sc_aggregated_df,
    )


def test_aggregate_subsampling_count_cells():
    count_df = AP_SUBSAMPLE.count_cells()
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cell_count": [50, 50],
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)

    assert isinstance(
        AP_SUBSAMPLE.aggregate_profiles(compute_subsample=True), pd.DataFrame
    )

    count_df = AP_SUBSAMPLE.count_cells(count_subset=True)
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cell_count": [2, 2],
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_aggregate_subsampling_profile():
    assert isinstance(
        AP_SUBSAMPLE.aggregate_profiles(compute_subsample=True), pd.DataFrame
    )

    expected_subset = pd.DataFrame(
        {
            "ImageNumber": sorted(["x", "y"] * 2),
            "Metadata_Plate": ["plate"] * 4,
            "Metadata_Site": [1] * 4,
            "Metadata_Well": sorted(["A01", "A02"] * 2),
            "TableNumber": sorted(["x_hash", "y_hash"] * 2),
            "Metadata_ObjectNumber": [46, 3] * 2,
        }
    )

    pd.testing.assert_frame_equal(AP_SUBSAMPLE.subset_data_df, expected_subset)


def test_aggregate_subsampling_profile_output():
    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Metadata_Site_Count": [1] * 2,
            "Metadata_Object_Count": [AP_SUBSAMPLE.subsample_n] * 2,
            "Cells_a": [110.0, 680.5],
            "Cells_b": [340.5, 201.5],
            "Cells_c": [285.0, 481.0],
            "Cells_d": [352.0, 549.0],
            "Cytoplasm_a": [407.5, 705.5],
            "Cytoplasm_b": [650.0, 439.5],
            "Cytoplasm_c": [243.5, 78.5],
            "Cytoplasm_d": [762.5, 625.0],
            "Nuclei_a": [683.5, 171.0],
            "Nuclei_b": [50.5, 625.0],
            "Nuclei_c": [431.0, 483.0],
            "Nuclei_d": [519.0, 286.5],
        }
    )

    # test CSV-based output
    output_result = AP_SUBSAMPLE.aggregate_profiles(
        output_file=pathlib.Path(f"{TMPDIR}/test_aggregate_output.csv.gz"),
        compute_subsample=True,
        compression_options={"method": "gzip"},
    )
    result = pd.read_csv(output_result)

    pd.testing.assert_frame_equal(result, expected_result)

    # test parquet-based output
    output_result = AP_SUBSAMPLE.aggregate_profiles(
        output_file=pathlib.Path(f"{TMPDIR}/test_aggregate_output.parquet"),
        output_type="parquet",
        compute_subsample=True,
    )
    result = pd.read_parquet(output_result)

    pd.testing.assert_frame_equal(result, expected_result)


def test_aggregate_subsampling_profile_output_multiple_queries():
    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Metadata_Site_Count": [1] * 2,
            "Metadata_Object_Count": [AP_SUBSAMPLE.subsample_n] * 2,
            "Cells_a": [110.0, 680.5],
            "Cells_b": [340.5, 201.5],
            "Cells_c": [285.0, 481.0],
            "Cells_d": [352.0, 549.0],
            "Cytoplasm_a": [407.5, 705.5],
            "Cytoplasm_b": [650.0, 439.5],
            "Cytoplasm_c": [243.5, 78.5],
            "Cytoplasm_d": [762.5, 625.0],
            "Nuclei_a": [683.5, 171.0],
            "Nuclei_b": [50.5, 625.0],
            "Nuclei_c": [431.0, 483.0],
            "Nuclei_d": [519.0, 286.5],
        }
    )

    # test CSV-based output
    output_result = AP_SUBSAMPLE.aggregate_profiles(
        output_file=pathlib.Path(f"{TMPDIR}/test_aggregate_output.csv.gz"),
        compute_subsample=True,
        compression_options={"method": "gzip"},
        n_aggregation_memory_strata=1,  # this will force multiple queries from each compartment
    )
    result = pd.read_csv(output_result)

    pd.testing.assert_frame_equal(result, expected_result)

    # test parquet-based output
    output_result = AP_SUBSAMPLE.aggregate_profiles(
        output_file=pathlib.Path(f"{TMPDIR}/test_aggregate_output.parquet"),
        output_type="parquet",
        compute_subsample=True,
        n_aggregation_memory_strata=1,  # this will force multiple queries from each compartment
    )
    result = pd.read_parquet(output_result)

    pd.testing.assert_frame_equal(result, expected_result)


def test_n_aggregation_memory_strata():
    df_n1 = AP.aggregate_profiles(n_aggregation_memory_strata=1)
    df_n2 = AP.aggregate_profiles(n_aggregation_memory_strata=2)
    df_n3 = AP.aggregate_profiles(n_aggregation_memory_strata=3)
    df_n_large = AP.aggregate_profiles(n_aggregation_memory_strata=1000)

    pd.testing.assert_frame_equal(df_n1, df_n2)
    pd.testing.assert_frame_equal(df_n1, df_n3)
    pd.testing.assert_frame_equal(df_n1, df_n_large)


def test_invalid_n_aggregation_memory_strata():
    # expect an AssertionError when an invalid parameter value is specified
    with pytest.raises(AssertionError):
        AP.aggregate_profiles(n_aggregation_memory_strata=0)


def test_sqlite_strata_conditions():
    df = pd.DataFrame(
        data={
            "TableNumber": [[1], [2], [3], [4]],
            "ImageNumber": [[1], [1, 2, 3], [1, 2], [1]],
        }
    )

    n1_expected_output = [
        "(TableNumber in (1) and ImageNumber in (1))",
        "(TableNumber in (2) and ImageNumber in (1, 2, 3))",
        "(TableNumber in (3) and ImageNumber in (1, 2))",
        "(TableNumber in (4) and ImageNumber in (1))",
    ]
    out1 = _sqlite_strata_conditions(
        df=df,
        dtypes={"TableNumber": "integer", "ImageNumber": "integer"},
        n=1,
    )
    for s1, s2 in zip(n1_expected_output, out1):
        assert s1 == s2

    n2_expected_output = [
        "(TableNumber in ('1') and ImageNumber in (1)) or (TableNumber in ('2') and ImageNumber in (1, 2, 3))",
        "(TableNumber in ('3') and ImageNumber in (1, 2)) or (TableNumber in ('4') and ImageNumber in (1))",
    ]
    out2 = _sqlite_strata_conditions(
        df=df,
        dtypes={"TableNumber": "text", "ImageNumber": "integer"},
        n=2,
    )
    for s1, s2 in zip(n2_expected_output, out2):
        assert s1 == s2


def test_aggregate_count_cells_multiple_strata():
    # Lauch a sqlite connection
    tmp_sqlite_file = f"sqlite:///{TMPDIR}/test_strata.sqlite"

    test_engine = create_engine(tmp_sqlite_file)

    # Setup data
    base_image_number = sorted(["x", "y"] * 50)
    base_table_number = sorted(["x_hash_a", "x_hash_b", "y_hash_a", "y_hash_b"] * 25)
    cells_df = build_random_data(
        compartment="cells",
        ImageNumber=base_image_number,
        TableNumber=base_table_number,
    )
    cytoplasm_df = build_random_data(
        compartment="cytoplasm",
        ImageNumber=base_image_number,
        TableNumber=base_table_number,
    )
    nuclei_df = build_random_data(
        compartment="nuclei",
        ImageNumber=base_image_number,
        TableNumber=base_table_number,
    )
    image_df = pd.DataFrame(
        {
            "TableNumber": ["x_hash_a", "x_hash_b", "y_hash_a", "y_hash_b"],
            "ImageNumber": ["x", "x", "y", "y"],
            "Metadata_Plate": ["plate"] * 4,
            "Metadata_Well": ["A01", "A02"] * 2,
            "Metadata_Site": [1, 1, 2, 2],
        }
    ).sort_values(by="Metadata_Well")

    # Ingest data into temporary sqlite file
    image_df.to_sql(name="image", con=test_engine, index=False, if_exists="replace")
    cells_df.to_sql(name="cells", con=test_engine, index=False, if_exists="replace")
    cytoplasm_df.to_sql(
        name="cytoplasm", con=test_engine, index=False, if_exists="replace"
    )
    nuclei_df.to_sql(name="nuclei", con=test_engine, index=False, if_exists="replace")

    # Setup SingleCells Class
    ap_strata = SingleCells(
        sql_file=tmp_sqlite_file,
        subsample_n="4",
        strata=["Metadata_Plate", "Metadata_Well", "Metadata_Site"],
    )

    count_df = ap_strata.count_cells()
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate"] * 4,
            "Metadata_Well": sorted(["A01", "A02"] * 2),
            "Metadata_Site": [1, 2] * 2,
            "cell_count": [25] * 4,
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)

    assert isinstance(
        ap_strata.aggregate_profiles(compute_subsample=True), pd.DataFrame
    )

    count_df = ap_strata.count_cells(count_subset=True)
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate"] * 4,
            "Metadata_Well": sorted(["A01", "A02"] * 2),
            "Metadata_Site": [1, 2] * 2,
            "cell_count": [4] * 4,
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_add_image_features():
    result = AP_IMAGE_ALL_FEATURES.aggregate_profiles()
    expected_result_all_features = pd.DataFrame(
        {
            "Metadata_Plate": ["plate"],
            "Metadata_Well": ["A01"],
            "Metadata_Site_Count": [2],
            "Metadata_Object_Count": [100],
            "Metadata_Count_Cells": [100],
            "Image_Granularity_1_Mito": [3.5],
            "Image_Texture_Variance_RNA_20_00": [13.0],
            "Image_Texture_InfoMeas2_DNA_5_02": [3.0],
            "Cells_a": [530.0],
            "Cells_b": [478.5],
            "Cells_c": [489.5],
            "Cells_d": [514.5],
            "Cytoplasm_a": [480.5],
            "Cytoplasm_b": [446.5],
            "Cytoplasm_c": [383.0],
            "Cytoplasm_d": [539.5],
            "Nuclei_a": [481.0],
            "Nuclei_b": [574.0],
            "Nuclei_c": [571.0],
            "Nuclei_d": [493.0],
        }
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis=1), expected_result_all_features.sort_index(axis=1)
    )

    result = AP_IMAGE_SUBSET_FEATURES.aggregate_profiles()
    expected_result_subset_features = pd.DataFrame(
        {
            "Metadata_Plate": ["plate"],
            "Metadata_Well": ["A01"],
            "Metadata_Site_Count": [2],
            "Metadata_Object_Count": [100],
            "Metadata_Count_Cells": [100],
            "Image_Texture_Variance_RNA_20_00": [13.0],
            "Image_Texture_InfoMeas2_DNA_5_02": [3.0],
            "Cells_a": [530.0],
            "Cells_b": [478.5],
            "Cells_c": [489.5],
            "Cells_d": [514.5],
            "Cytoplasm_a": [480.5],
            "Cytoplasm_b": [446.5],
            "Cytoplasm_c": [383.0],
            "Cytoplasm_d": [539.5],
            "Nuclei_a": [481.0],
            "Nuclei_b": [574.0],
            "Nuclei_c": [571.0],
            "Nuclei_d": [493.0],
        }
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis=1), expected_result_subset_features.sort_index(axis=1)
    )

    result = AP_IMAGE_COUNT.aggregate_profiles()
    expected_result_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate"],
            "Metadata_Well": ["A01"],
            "Metadata_Site_Count": [2],
            "Metadata_Object_Count": [100],
            "Metadata_Count_Cells": [100],
            "Cells_a": [530.0],
            "Cells_b": [478.5],
            "Cells_c": [489.5],
            "Cells_d": [514.5],
            "Cytoplasm_a": [480.5],
            "Cytoplasm_b": [446.5],
            "Cytoplasm_c": [383.0],
            "Cytoplasm_d": [539.5],
            "Nuclei_a": [481.0],
            "Nuclei_b": [574.0],
            "Nuclei_c": [571.0],
            "Nuclei_d": [493.0],
        }
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis=1), expected_result_count.sort_index(axis=1)
    )


def test_load_non_canonical_image_table():
    """
    Loading an image table with non-canonical image table name
    """
    # test for exception loading image table with default table name "image"
    with pytest.raises(OperationalError):
        AP_IMAGE_DIFF_NAME.load_image()

    AP_IMAGE_DIFF_NAME.load_image(image_table_name="Per_Image")
    pd.testing.assert_frame_equal(
        AP_IMAGE_DIFF_NAME.image_df.sort_index(axis="columns"),
        IMAGE_DF.sort_index(axis="columns"),
    )

    result = AP_IMAGE_DIFF_NAME.aggregate_profiles()

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Metadata_Object_Count": [50, 50],
            "Metadata_Site_Count": [1, 1],
            "Cells_a": [368.0, 583.5],
            "Cells_b": [482.0, 478.5],
            "Cells_c": [531.0, 461.5],
            "Cells_d": [585.5, 428.0],
            "Cytoplasm_a": [479.5, 495.5],
            "Cytoplasm_b": [445.5, 459.0],
            "Cytoplasm_c": [407.5, 352.0],
            "Cytoplasm_d": [533.0, 545.0],
            "Nuclei_a": [591.5, 435.5],
            "Nuclei_b": [574.0, 579.0],
            "Nuclei_c": [588.5, 538.5],
            "Nuclei_d": [483.0, 560.0],
        }
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis=1), expected_result.sort_index(axis=1)
    )

    # Confirm aggregation after merging single cells
    sc_df = AP_IMAGE_DIFF_NAME.merge_single_cells()
    sc_aggregated_df = aggregate(sc_df, compute_object_count=True).sort_index(
        axis="columns"
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis="columns").drop("Metadata_Site_Count", axis="columns"),
        sc_aggregated_df,
    )
