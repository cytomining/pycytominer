import os
import pytest
import pandas as pd
from pycytominer.cyto_utils import (
    aggregate_fields_count,
    aggregate_image_features,
)

image_df_site = pd.DataFrame(
    {
        "Metadata_Plate": ["plate", "plate", "plate", "plate", "plate", "plate"],
        "Metadata_Well": ["A01", "A01", "A01", "A02", "A02", "A03"],
        "Metadata_Site": [1, 2, 3, 1, 2, 1],
    }
)

image_site_all = pd.DataFrame(
    {
        "TableNumber": [
            "x1_hash",
            "y1_hash",
            "z1_hash",
            "x2_hash",
            "y2_hash",
            "x3_hash",
        ],
        "ImageNumber": ["x1", "y1", "z1", "x2", "y2", "x3"],
        "Metadata_Plate": ["plate", "plate", "plate", "plate", "plate", "plate"],
        "Metadata_Well": ["A01", "A01", "A01", "A02", "A02", "A03"],
        "Metadata_Count_Cells": [40, 30, 25, 70, 100, 20],
        "Metadata_Count_Cytoplasm": [100, 40, 25, 75, 35, 20],
        "Image_Granularity_1": [3.0, 4.0, 1.0, 8.0, 2.0, 10.0],
        "Image_Texture_1": [31.0, 14.0, 12.0, 4.0, 6.0, 14.0],
    }
)

df = pd.DataFrame(
    {
        "TableNumber": [
            "x1_hash",
            "y1_hash",
            "z1_hash",
            "x2_hash",
            "y2_hash",
            "x3_hash",
        ],
        "ImageNumber": ["x1", "y1", "z1", "x2", "y2", "x3"],
        "Metadata_Plate": ["plate", "plate", "plate", "plate", "plate", "plate"],
        "Metadata_Well": ["A01", "A01", "A01", "A02", "A02", "A03"],
    }
)


def test_aggregate_fields_count():
    expected_result = pd.DataFrame(
        {
            "Metadata_Plate": ["plate", "plate", "plate"],
            "Metadata_Well": ["A01", "A02", "A03"],
            "Metadata_Site_Count": [3, 2, 1],
        }
    )
    strata = ["Metadata_Plate", "Metadata_Well"]
    result = aggregate_fields_count(image_df_site, strata, "Metadata_Site")

    pd.testing.assert_frame_equal(
        expected_result.sort_index(axis=1), result.sort_index(axis=1)
    )


def test_aggregate_image_features():
    expected_result = pd.DataFrame(
        {
            "TableNumber": [
                "x1_hash",
                "y1_hash",
                "z1_hash",
                "x2_hash",
                "y2_hash",
                "x3_hash",
            ],
            "ImageNumber": ["x1", "y1", "z1", "x2", "y2", "x3"],
            "Metadata_Plate": ["plate", "plate", "plate", "plate", "plate", "plate"],
            "Metadata_Well": ["A01", "A01", "A01", "A02", "A02", "A03"],
            "Metadata_Count_Cells": [95, 95, 95, 170, 170, 20],
            "Metadata_Count_Cytoplasm": [165, 165, 165, 110, 110, 20],
        }
    )

    image_feature_categories = ["Count"]
    image_cols = ["TableNumber", "ImageNumber"]
    strata = ["Metadata_Plate", "Metadata_Well"]
    result = aggregate_image_features(
        df, image_site_all, image_feature_categories, image_cols, strata, "median"
    )

    pd.testing.assert_frame_equal(
        expected_result.sort_index(axis=1), result.sort_index(axis=1)
    )

    expected_result = pd.DataFrame(
        {
            "TableNumber": [
                "x1_hash",
                "y1_hash",
                "z1_hash",
                "x2_hash",
                "y2_hash",
                "x3_hash",
            ],
            "ImageNumber": ["x1", "y1", "z1", "x2", "y2", "x3"],
            "Metadata_Plate": ["plate", "plate", "plate", "plate", "plate", "plate"],
            "Metadata_Well": ["A01", "A01", "A01", "A02", "A02", "A03"],
            "Metadata_Count_Cells": [95, 95, 95, 170, 170, 20],
            "Metadata_Count_Cytoplasm": [165, 165, 165, 110, 110, 20],
            "Image_Granularity_1": [3.0, 3.0, 3.0, 5.0, 5.0, 10.0],
            "Image_Texture_1": [14.0, 14.0, 14.0, 5.0, 5.0, 14.0],
        }
    )

    image_feature_categories = ["Count", "Granularity", "Texture"]
    result = aggregate_image_features(
        df, image_site_all, image_feature_categories, image_cols, strata, "median"
    )

    pd.testing.assert_frame_equal(
        expected_result.sort_index(axis=1), result.sort_index(axis=1)
    )

    expected_result = pd.DataFrame(
        {
            "TableNumber": [
                "x1_hash",
                "y1_hash",
                "z1_hash",
                "x2_hash",
                "y2_hash",
                "x3_hash",
            ],
            "ImageNumber": ["x1", "y1", "z1", "x2", "y2", "x3"],
            "Metadata_Plate": ["plate", "plate", "plate", "plate", "plate", "plate"],
            "Metadata_Well": ["A01", "A01", "A01", "A02", "A02", "A03"],
            "Image_Granularity_1": [3.0, 3.0, 3.0, 5.0, 5.0, 10.0],
            "Image_Texture_1": [14.0, 14.0, 14.0, 5.0, 5.0, 14.0],
        }
    )

    image_feature_categories = ["Granularity", "Texture"]
    result = aggregate_image_features(
        df=df,
        image_features_df=image_site_all,
        image_feature_categories=image_feature_categories,
        image_cols=image_cols,
        strata=strata,
        aggregation_operation="median",
    )

    pd.testing.assert_frame_equal(
        expected_result.sort_index(axis=1), result.sort_index(axis=1)
    )
