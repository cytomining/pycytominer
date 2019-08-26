import os
import random
import tempfile
import pandas as pd
from sqlalchemy import create_engine
from pycytominer.aggregate import AggregateProfiles
from pycytominer.aggregate import aggregate

random.seed(123)


def build_random_data(compartment="cells"):
    a_feature = random.sample(range(1, 1000), 100)
    b_feature = random.sample(range(1, 1000), 100)
    c_feature = random.sample(range(1, 1000), 100)
    d_feature = random.sample(range(1, 1000), 100)
    data_df = pd.DataFrame(
        {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
    ).reset_index(drop=True)

    data_df.columns = ["{}_{}".format(compartment, x) for x in data_df.columns]

    data_df = data_df.assign(
        ObjectNumber=list(range(1, 51)) * 2,
        ImageNumber=sorted(["x", "y"] * 50),
        TableNumber=sorted(["x_hash", "y_hash"] * 50),
    )

    return data_df


# Get temporary directory
tmpdir = tempfile.gettempdir()

# Lauch a sqlite connection
file = "sqlite:///{}/test.sqlite".format(tmpdir)

test_engine = create_engine(file)
test_conn = test_engine.connect()

# Setup data
cells_df = build_random_data(compartment="cells")
cytoplasm_df = build_random_data(compartment="cytoplasm")
nuclei_df = build_random_data(compartment="nuclei")
image_df = pd.DataFrame(
    {
        "TableNumber": ["x_hash", "y_hash"],
        "ImageNumber": ["x", "y"],
        "Metadata_Plate": ["plate", "plate"],
        "Metadata_Well": ["A01", "A02"],
    }
)

# Ingest data into temporary sqlite file
image_df.to_sql("image", con=test_engine, index=False, if_exists="replace")
cells_df.to_sql("cells", con=test_engine, index=False, if_exists="replace")
cytoplasm_df.to_sql("cytoplasm", con=test_engine, index=False, if_exists="replace")
nuclei_df.to_sql("nuclei", con=test_engine, index=False, if_exists="replace")

# Setup AggregateProfiles Class
ap = AggregateProfiles(sql_file=file)
ap_subsample = AggregateProfiles(
    sql_file=file, subsample_n=2, subsampling_random_state=123
)


def test_AggregateProfiles_init():
    """
    Testing initialization of AggregateProfiles
    """
    assert ap.sql_file == file
    assert ap.strata == ["Metadata_Plate", "Metadata_Well"]
    assert ap.merge_cols == ["TableNumber", "ImageNumber"]
    assert ap.features == "all"
    pd.testing.assert_frame_equal(image_df, ap.image_df)
    assert ap.subsample_frac == 1
    assert ap_subsample.subsample_frac == 1
    assert ap.subsample_n == "all"
    assert ap_subsample.subsample_n == 2
    assert ap.subset_data == "none"
    assert ap.output_file == "none"
    assert ap.operation == "median"
    assert not ap.is_aggregated
    assert ap.subsampling_random_state == "none"
    assert ap_subsample.subsampling_random_state == 123


def test_AggregateProfiles_count():
    count_df = ap.count_cells()
    expected_count = pd.DataFrame({"Metadata_Well": ["A01", "A02"], "plate": [50, 50]})
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_aggregate_comparment():
    df = image_df.merge(cells_df, how="inner", on=["TableNumber", "ImageNumber"])
    result = aggregate(df)
    ap_result = ap.aggregate_compartment("cells")

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cells_a": [368.0, 583.5],
            "cells_b": [482.0, 478.5],
            "cells_c": [531.0, 461.5],
            "cells_d": [585.5, 428.0],
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
            "cells_a": [368.0, 583.5],
            "cells_b": [482.0, 478.5],
            "cells_c": [531.0, 461.5],
            "cells_d": [585.5, 428.0],
            "cytoplasm_a": [479.5, 495.5],
            "cytoplasm_b": [445.5, 459.0],
            "cytoplasm_c": [407.5, 352.0],
            "cytoplasm_d": [533.0, 545.0],
            "nuclei_a": [591.5, 435.5],
            "nuclei_b": [574.0, 579.0],
            "nuclei_c": [588.5, 538.5],
            "nuclei_d": [483.0, 560.0],
        }
    )

    pd.testing.assert_frame_equal(result, expected_result)


def test_aggregate_subsampling_count_cells():
    count_df = ap_subsample.count_cells()
    expected_count = pd.DataFrame({"Metadata_Well": ["A01", "A02"], "plate": [50, 50]})
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)

    profiles = ap_subsample.aggregate_profiles()

    count_df = ap_subsample.count_cells(count_subset=True)
    expected_count = pd.DataFrame({"Metadata_Well": ["A01", "A02"], "plate": [2, 2]})
    pd.testing.assert_frame_equal(count_df, expected_count, check_names=False)


def test_aggregate_subsampling_profile():
    result = ap_subsample.aggregate_profiles()

    expected_subset = pd.DataFrame(
        {
            "TableNumber": sorted(["x_hash", "y_hash"] * 2),
            "ImageNumber": sorted(["x", "y"] * 2),
            "Metadata_Plate": ["plate"] * 4,
            "Metadata_Well": sorted(["A01", "A02"] * 2),
            "ObjectNumber": [46, 3] * 2,
        }
    )

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cells_a": [110.0, 680.5],
            "cells_b": [340.5, 201.5],
            "cells_c": [285.0, 481.0],
            "cells_d": [352.0, 549.0],
            "cytoplasm_a": [407.5, 705.5],
            "cytoplasm_b": [650.0, 439.5],
            "cytoplasm_c": [243.5, 78.5],
            "cytoplasm_d": [762.5, 625.0],
            "nuclei_a": [683.5, 171.0],
            "nuclei_b": [50.5, 625.0],
            "nuclei_c": [431.0, 483.0],
            "nuclei_d": [519.0, 286.5],
        }
    )

    pd.testing.assert_frame_equal(ap_subsample.subset_data, expected_subset)


def test_aggregate_subsampling_profile_compress():
    compress_file = os.path.join(tmpdir, "test_aggregate_compress.csv.gz")

    _ = ap_subsample.aggregate_profiles(output_file=compress_file, how="gzip")
    result = pd.read_csv(compress_file)

    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate"],
            "Metadata_Well": ["A01", "A02"],
            "cells_a": [110.0, 680.5],
            "cells_b": [340.5, 201.5],
            "cells_c": [285.0, 481.0],
            "cells_d": [352.0, 549.0],
            "cytoplasm_a": [407.5, 705.5],
            "cytoplasm_b": [650.0, 439.5],
            "cytoplasm_c": [243.5, 78.5],
            "cytoplasm_d": [762.5, 625.0],
            "nuclei_a": [683.5, 171.0],
            "nuclei_b": [50.5, 625.0],
            "nuclei_c": [431.0, 483.0],
            "nuclei_d": [519.0, 286.5],
        }
    )

    pd.testing.assert_frame_equal(result, expected_result)
