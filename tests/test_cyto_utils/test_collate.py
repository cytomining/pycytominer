import os
import pathlib
import pytest

import pandas as pd
from pycytominer.cyto_utils.collate import collate

batch = "2021_04_20_Target2"

plate = "BR00121431"

TEST_DIR = pathlib.Path(__file__).parents[1].absolute()
ROOT_DIR = TEST_DIR.parent.absolute()

test_config_location = ROOT_DIR.joinpath(
    "pycytominer", "cyto_utils", "database_config", "ingest_config.ini"
)

test_data_location = TEST_DIR / "test_data" / "collate"

test_backend_location = (
    TEST_DIR / "test_data" / "collate" / "backend" / batch / plate / f"{plate}.sqlite"
)

test_csv_location = (
    TEST_DIR / "test_data" / "collate" / "backend" / batch / plate / f"{plate}.csv"
)

main_csv_location = (
    TEST_DIR / "test_data" / "collate" / "backend" / batch / plate / f"{plate}_main.csv"
)


def cleanup():
    if os.path.exists(test_backend_location):
        os.remove(test_backend_location)

    if os.path.exists(test_csv_location):
        os.remove(test_csv_location)


def validate(test_file, main_file, should_be_equal=True):
    test = pd.read_csv(test_file)
    main = pd.read_csv(main_file)
    if should_be_equal:
        pd.testing.assert_frame_equal(test, main)
    else:
        with pytest.raises(AssertionError):
            pd.testing.assert_frame_equal(test, main)
    return test, main


def test_base_case():
    cleanup()

    collate(
        "2021_04_20_Target2",
        test_config_location,
        "BR00121431",
        base_directory=test_data_location,
        tmp_dir=test_data_location,
        add_image_features=False,
        printtoscreen=False,
    )
    assert os.path.exists(test_backend_location)

    validate(test_csv_location, main_csv_location)

    cleanup()


def test_base_case_with_image_features():
    cleanup()

    collate(
        "2021_04_20_Target2",
        test_config_location,
        "BR00121431",
        base_directory=test_data_location,
        tmp_dir=test_data_location,
        add_image_features=True,
        image_feature_categories=["Granularity"],
        printtoscreen=False,
    )
    assert os.path.exists(test_backend_location)

    test, main = validate(test_csv_location, main_csv_location, should_be_equal=False)

    test = test.drop(columns=[x for x in test.columns if "Image_Granularity" in x])

    pd.testing.assert_frame_equal(test, main)

    cleanup()


def test_overwrite():
    cleanup()

    collate(
        "2021_04_20_Target2",
        test_config_location,
        "BR00121431",
        base_directory=test_data_location,
        tmp_dir=test_data_location,
        add_image_features=False,
        printtoscreen=False,
    )
    assert os.path.exists(test_backend_location)

    validate(test_csv_location, main_csv_location)

    collate(
        "2021_04_20_Target2",
        test_config_location,
        "BR00121431",
        base_directory=test_data_location,
        tmp_dir=test_data_location,
        overwrite=True,
        add_image_features=False,
        printtoscreen=False,
    )

    with pytest.raises(SystemExit) as exitcode:
        collate(
            "2021_04_20_Target2",
            test_config_location,
            "BR00121431",
            base_directory=test_data_location,
            tmp_dir=test_data_location,
            add_image_features=False,
            printtoscreen=False,
        )
    assert "overwrite is set to False" in exitcode.value.code


cleanup()


def test_aggregate_only():
    cleanup()

    with pytest.raises(SystemExit) as exitcode:
        collate(
            "2021_04_20_Target2",
            test_config_location,
            "BR00121431",
            base_directory=test_data_location,
            tmp_dir=test_data_location,
            aggregate_only=True,
            add_image_features=False,
            printtoscreen=False,
        )
    assert "does not exist" in exitcode.value.code

    collate(
        "2021_04_20_Target2",
        test_config_location,
        "BR00121431",
        base_directory=test_data_location,
        tmp_dir=test_data_location,
        add_image_features=False,
        printtoscreen=False,
    )

    assert os.path.exists(test_csv_location)

    os.remove(test_csv_location)

    collate(
        "2021_04_20_Target2",
        test_config_location,
        "BR00121431",
        base_directory=test_data_location,
        tmp_dir=test_data_location,
        aggregate_only=True,
        add_image_features=False,
        printtoscreen=False,
    )

    validate(test_csv_location, main_csv_location)

    cleanup()
