import os
import random
import pytest
import tempfile
import pandas as pd
from sqlalchemy import create_engine

from pycytominer import aggregate, normalize
from pycytominer.cyto_utils.cells import SingleCells, _sqlite_strata_conditions
from pycytominer.cyto_utils import (
    get_default_linking_cols,
    get_default_compartments,
    infer_cp_features,
)

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
tmpdir = tempfile.gettempdir()

# Launch a sqlite connection
file = "sqlite:///{}/test.sqlite".format(tmpdir)

test_engine = create_engine(file)
test_conn = test_engine.connect()

# Setup data
cells_df = build_random_data(compartment="cells")
cytoplasm_df = build_random_data(compartment="cytoplasm").assign(
    Cytoplasm_Parent_Cells=(list(range(1, 51)) * 2)[::-1],
    Cytoplasm_Parent_Nuclei=(list(range(1, 51)) * 2)[::-1],
)
nuclei_df = build_random_data(compartment="nuclei")
image_df = pd.DataFrame(
    {
        "TableNumber": ["x_hash", "y_hash"],
        "ImageNumber": ["x", "y"],
        "Metadata_Plate": ["plate", "plate"],
        "Metadata_Well": ["A01", "A02"],
        "Metadata_Site": [1, 1],
    }
)

image_df_additional_features = pd.DataFrame(
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

# Ingest data into temporary sqlite file
image_df.to_sql("image", con=test_engine, index=False, if_exists="replace")
cells_df.to_sql("cells", con=test_engine, index=False, if_exists="replace")
cytoplasm_df.to_sql("cytoplasm", con=test_engine, index=False, if_exists="replace")
nuclei_df.to_sql("nuclei", con=test_engine, index=False, if_exists="replace")

# Create a new table with a fourth compartment
new_file = "sqlite:///{}/test_new.sqlite".format(tmpdir)
new_compartment_df = build_random_data(compartment="new")

test_new_engine = create_engine(new_file)
test_new_conn = test_new_engine.connect()

image_df.to_sql("image", con=test_new_engine, index=False, if_exists="replace")
cells_df.to_sql("cells", con=test_new_engine, index=False, if_exists="replace")
new_cytoplasm_df = cytoplasm_df.assign(
    Cytoplasm_Parent_New=(list(range(1, 51)) * 2)[::-1]
)
new_cytoplasm_df.to_sql(
    "cytoplasm", con=test_new_engine, index=False, if_exists="replace"
)
nuclei_df.to_sql("nuclei", con=test_new_engine, index=False, if_exists="replace")
new_compartment_df.to_sql("new", con=test_new_engine, index=False, if_exists="replace")

new_compartments = ["cells", "cytoplasm", "nuclei", "new"]

new_linking_cols = get_default_linking_cols()
new_linking_cols["cytoplasm"]["new"] = "Cytoplasm_Parent_New"
new_linking_cols["new"] = {"cytoplasm": "ObjectNumber"}

# Ingest data with additional image features to temporary sqlite file

image_file = "sqlite:///{}/test_image.sqlite".format(tmpdir)

test_engine_image = create_engine(image_file)
test_conn_image = test_engine_image.connect()

image_df_additional_features.to_sql(
    "image", con=test_engine_image, index=False, if_exists="replace"
)
cells_df.to_sql("cells", con=test_engine_image, index=False, if_exists="replace")
cytoplasm_df.to_sql(
    "cytoplasm", con=test_engine_image, index=False, if_exists="replace"
)
nuclei_df.to_sql("nuclei", con=test_engine_image, index=False, if_exists="replace")

# Setup SingleCells Class
ap = SingleCells(file_or_conn=file)
ap_subsample = SingleCells(
    file_or_conn=file,
    subsample_n=2,
    subsampling_random_state=123,
)
ap_new = SingleCells(
    file_or_conn=new_file,
    load_image_data=False,
    compartments=new_compartments,
    compartment_linking_cols=new_linking_cols,
)

ap_image_all_features = SingleCells(
    file_or_conn=image_file,
    add_image_features=True,
    image_feature_categories=["Count", "Granularity", "Texture"],
)

ap_image_subset_features = SingleCells(
    file_or_conn=image_file,
    add_image_features=True,
    image_feature_categories=["Count", "Texture"],
)

ap_image_count = SingleCells(
    file_or_conn=image_file, add_image_features=True, image_feature_categories=["Count"]
)


def test_SingleCells_init():
    """
    Testing initialization of SingleCells
    """
    assert ap.file_or_conn == file
    assert ap.strata == ["Metadata_Plate", "Metadata_Well"]
    assert ap.merge_cols == ["TableNumber", "ImageNumber"]
    assert ap.image_cols == ["TableNumber", "ImageNumber", "Metadata_Site"]
    pd.testing.assert_frame_equal(
        image_df.sort_index(axis=1), ap.image_df.sort_index(axis=1)
    )
    assert ap.features == "infer"
    assert ap.subsample_frac == 1
    assert ap_subsample.subsample_frac == 1
    assert ap.subsample_n == "all"
    assert ap_subsample.subsample_n == 2
    assert ap.subset_data_df == "none"
    assert ap.output_file == "none"
    assert ap.aggregation_operation == "median"
    assert not ap.is_aggregated
    assert ap.subsampling_random_state == "none"
    assert ap_subsample.subsampling_random_state == 123
    assert ap.fields_of_view == "all"
    assert ap.fields_of_view_feature == "Metadata_Site"
    assert ap.object_feature == "Metadata_ObjectNumber"
    assert ap.compartment_linking_cols == get_default_linking_cols()
    assert ap.compartments == get_default_compartments()


def test_SingleCells_reset_variables():
    """
    Testing initialization of SingleCells
    """
    ap_switch = SingleCells(file_or_conn=file)
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
    count_df = ap.count_cells()
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cell_count": [50, 50],
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_load_compartment():
    loaded_compartment_df = ap.load_compartment(compartment="cells")
    pd.testing.assert_frame_equal(loaded_compartment_df, cells_df)

    # Test non-canonical compartment loading
    pd.testing.assert_frame_equal(new_compartment_df, ap_new.load_compartment("new"))


def test_merge_single_cells():
    sc_merged_df = ap.merge_single_cells()

    # Assert that the image data was merged
    assert all(x in sc_merged_df.columns for x in ["Metadata_Plate", "Metadata_Well"])

    # Assert that metadata columns were renamed appropriately
    for x in ap.full_merge_suffix_rename:
        assert ap.full_merge_suffix_rename[x] == "Metadata_{x}".format(x=x)

    # Perform a manual merge
    manual_merge = cytoplasm_df.merge(
        cells_df,
        left_on=["TableNumber", "ImageNumber", "Cytoplasm_Parent_Cells"],
        right_on=["TableNumber", "ImageNumber", "ObjectNumber"],
        suffixes=["_cytoplasm", "_cells"],
    ).merge(
        nuclei_df,
        left_on=["TableNumber", "ImageNumber", "Cytoplasm_Parent_Nuclei"],
        right_on=["TableNumber", "ImageNumber", "ObjectNumber"],
        suffixes=["_cytoplasm", "_nuclei"],
    )

    manual_merge = image_df.merge(manual_merge, on=ap.merge_cols, how="right").rename(
        ap.full_merge_suffix_rename, axis="columns"
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

                norm_method_df = ap.merge_single_cells(
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
                )

    # Test non-canonical compartment merging
    new_sc_merge_df = ap_new.merge_single_cells()

    assert sum(new_sc_merge_df.columns.str.startswith("New")) == 4
    assert (
        new_compartment_df.ObjectNumber.tolist()[::-1]
        == new_sc_merge_df.Metadata_ObjectNumber_new.tolist()
    )

    norm_new_method_df = ap_new.merge_single_cells(
        single_cell_normalize=True,
        normalize_args={
            "method": "standardize",
            "samples": "all",
            "features": "infer",
        },
    )

    norm_new_method_no_feature_infer_df = ap_new.merge_single_cells(
        single_cell_normalize=True,
        normalize_args={
            "method": "standardize",
            "samples": "all",
        },
    )

    default_feature_infer_df = ap_new.merge_single_cells(single_cell_normalize=True)

    pd.testing.assert_frame_equal(norm_new_method_df, default_feature_infer_df)
    pd.testing.assert_frame_equal(
        norm_new_method_df, norm_new_method_no_feature_infer_df
    )

    new_compartment_cols = infer_cp_features(
        new_compartment_df, compartments=ap_new.compartments
    )
    traditional_norm_df = normalize(
        ap_new.image_df.merge(new_compartment_df, on=ap.merge_cols),
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
        ap_subsample = SingleCells(file_or_conn=file, subsample_frac=subsample_frac)

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
            assert ap_subsample.full_merge_suffix_rename[x] == "Metadata_{x}".format(
                x=x
            )

        # Assert that the subsample fraction worked
        assert sc_merged_df.shape[0] == cells_df.shape[0] * subsample_frac

    for subsample_n in [2, 5, 10]:
        ap_subsample = SingleCells(file_or_conn=file, subsample_n=subsample_n)

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


def test_aggregate_comparment():
    df = image_df.merge(cells_df, how="inner", on=["TableNumber", "ImageNumber"])
    result = aggregate(df)
    ap_result = ap.aggregate_compartment("cells")

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
    result = ap.aggregate_profiles()

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
    sc_df = ap.merge_single_cells()
    sc_aggregated_df = aggregate(sc_df, compute_object_count=True).sort_index(
        axis="columns"
    )

    pd.testing.assert_frame_equal(
        result.sort_index(axis="columns").drop("Metadata_Site_Count", axis="columns"),
        sc_aggregated_df,
    )


def test_aggregate_subsampling_count_cells():
    count_df = ap_subsample.count_cells()
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cell_count": [50, 50],
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)

    profiles = ap_subsample.aggregate_profiles(compute_subsample=True)

    count_df = ap_subsample.count_cells(count_subset=True)
    expected_count = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cell_count": [2, 2],
        }
    )
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_aggregate_subsampling_profile():
    result = ap_subsample.aggregate_profiles(compute_subsample=True)

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

    pd.testing.assert_frame_equal(ap_subsample.subset_data_df, expected_subset)


def test_aggregate_subsampling_profile_compress():
    compress_file = os.path.join(tmpdir, "test_aggregate_compress.csv.gz")

    _ = ap_subsample.aggregate_profiles(
        output_file=compress_file,
        compute_subsample=True,
        compression_options={"method": "gzip"},
    )
    result = pd.read_csv(compress_file)

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Metadata_Site_Count": [1] * 2,
            "Metadata_Object_Count": [ap_subsample.subsample_n] * 2,
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

    pd.testing.assert_frame_equal(result, expected_result)


def test_aggregate_subsampling_profile_compress_multiple_queries():
    compress_file = os.path.join(tmpdir, "test_aggregate_compress.csv.gz")

    _ = ap_subsample.aggregate_profiles(
        output_file=compress_file,
        compute_subsample=True,
        compression_options={"method": "gzip"},
        n_aggregation_memory_strata=1,  # this will force multiple queries from each compartment
    )
    result = pd.read_csv(compress_file)

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "Metadata_Site_Count": [1] * 2,
            "Metadata_Object_Count": [ap_subsample.subsample_n] * 2,
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

    pd.testing.assert_frame_equal(result, expected_result)


def test_n_aggregation_memory_strata():

    df_n1 = ap.aggregate_profiles(n_aggregation_memory_strata=1)
    df_n2 = ap.aggregate_profiles(n_aggregation_memory_strata=2)
    df_n3 = ap.aggregate_profiles(n_aggregation_memory_strata=3)
    df_n_large = ap.aggregate_profiles(n_aggregation_memory_strata=1000)

    pd.testing.assert_frame_equal(df_n1, df_n2)
    pd.testing.assert_frame_equal(df_n1, df_n3)
    pd.testing.assert_frame_equal(df_n1, df_n_large)


def test_invalid_n_aggregation_memory_strata():
    # expect an AssertionError when an invalid parameter value is specified
    with pytest.raises(AssertionError):
        ap.aggregate_profiles(n_aggregation_memory_strata=0)


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
    file = "sqlite:///{}/test_strata.sqlite".format(tmpdir)

    test_engine = create_engine(file)
    test_conn = test_engine.connect()

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
    image_df.to_sql("image", con=test_engine, index=False, if_exists="replace")
    cells_df.to_sql("cells", con=test_engine, index=False, if_exists="replace")
    cytoplasm_df.to_sql("cytoplasm", con=test_engine, index=False, if_exists="replace")
    nuclei_df.to_sql("nuclei", con=test_engine, index=False, if_exists="replace")

    # Setup SingleCells Class
    ap_strata = SingleCells(
        file_or_conn=file,
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

    profiles = ap_strata.aggregate_profiles(compute_subsample=True)

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
    result = ap_image_all_features.aggregate_profiles()
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

    result = ap_image_subset_features.aggregate_profiles()
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

    result = ap_image_count.aggregate_profiles()
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
