"""This tests parse_cp_features"""

from pycytominer.cyto_utils.parse_cp_features import parse_cp_features
import pathlib
import pandas as pd


def test_parse_feature():
    feature_strings = [
        "Cells_Texture_SumVariance_RNA_5",
        "Nuclei_Intensity_MaxIntensityEdge_DNA",
        "Cytoplasm_Correlation_Correlation_DNA_RNA",
        "Image_AreaShape_Compactness",
    ]

    for feature in feature_strings:
        result = parse_cp_features(feature)
        assert isinstance(result, dict)
        assert result["feature"] == feature
        assert result["compartment"] is not None
        assert result["feature_group"] is not None
        assert result["feature_type"] is not None
        assert result["channel"] is not None


def test_parse_feature_with_file():
    cp_features_file = f"{pathlib.Path(__file__).parent.parent}/test_data/parse_cp_features_example_data/cp_features.txt"
    with open(cp_features_file, "r") as file:
        features = file.read().splitlines()
        for feature in features:
            result = parse_cp_features(feature)
            assert isinstance(result, dict)
            assert result["feature"] == feature
            assert result["compartment"] is not None
            assert result["feature_group"] is not None
            assert result["feature_type"] is not None
            assert result["channel"] is not None

        parsed_features_df = pd.DataFrame(
            [parse_cp_features(feature.strip()) for feature in features]
        )

        pd.testing.assert_frame_equal(
            parsed_features_df,
            pd.read_csv(
                f"{pathlib.Path(__file__).parent.parent}/test_data/parse_cp_features_example_data/parsed_features.csv"
            ),
        )
