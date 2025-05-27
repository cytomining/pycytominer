import os
import pathlib

import pandas as pd
import pytest

from pycytominer.cyto_utils.collate import collate

# Set constants
BATCH = "2021_04_20_Target2"
PLATE = "BR00121431"
TEST_DIR = pathlib.Path(__file__).parents[1].absolute()
ROOT_DIR = TEST_DIR.parent.absolute()
TEST_CONFIG_LOCATION = ROOT_DIR.joinpath(
    "pycytominer", "cyto_utils", "database_config", "ingest_config.ini"
)
TEST_DATA_LOCATION = TEST_DIR / "test_data" / "collate"
TEST_BACKEND_LOCATION = (
    TEST_DATA_LOCATION / "backend" / BATCH / PLATE / f"{PLATE}.sqlite"
)
TEST_CSV_LOCATION = TEST_DATA_LOCATION / "backend" / BATCH / PLATE / f"{PLATE}.csv"
MAIN_CSV_LOCATION = TEST_DATA_LOCATION / "backend" / BATCH / PLATE / f"{PLATE}_main.csv"


def cleanup():
    if os.path.exists(TEST_BACKEND_LOCATION):
        os.remove(TEST_BACKEND_LOCATION)

    if os.path.exists(TEST_CSV_LOCATION):
        os.remove(TEST_CSV_LOCATION)


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
        TEST_CONFIG_LOCATION,
        "BR00121431",
        base_directory=TEST_DATA_LOCATION,
        tmp_dir=TEST_DATA_LOCATION,
        add_image_features=False,
        printtoscreen=False,
    )
    assert os.path.exists(TEST_BACKEND_LOCATION)

    validate(TEST_CSV_LOCATION, MAIN_CSV_LOCATION)

    cleanup()


def test_base_case_with_image_features():
    cleanup()

    collate(
        "2021_04_20_Target2",
        TEST_CONFIG_LOCATION,
        "BR00121431",
        base_directory=TEST_DATA_LOCATION,
        tmp_dir=TEST_DATA_LOCATION,
        add_image_features=True,
        image_feature_categories=["Granularity"],
        printtoscreen=False,
    )
    assert os.path.exists(TEST_BACKEND_LOCATION)

    test, main = validate(TEST_CSV_LOCATION, MAIN_CSV_LOCATION, should_be_equal=False)

    test = test.drop(columns=[x for x in test.columns if "Image_Granularity" in x])

    pd.testing.assert_frame_equal(test, main)

    cleanup()


def test_overwrite():
    cleanup()

    collate(
        "2021_04_20_Target2",
        TEST_CONFIG_LOCATION,
        "BR00121431",
        base_directory=TEST_DATA_LOCATION,
        tmp_dir=TEST_DATA_LOCATION,
        add_image_features=False,
        printtoscreen=False,
    )
    assert os.path.exists(TEST_BACKEND_LOCATION)

    validate(TEST_CSV_LOCATION, MAIN_CSV_LOCATION)

    collate(
        "2021_04_20_Target2",
        TEST_CONFIG_LOCATION,
        "BR00121431",
        base_directory=TEST_DATA_LOCATION,
        tmp_dir=TEST_DATA_LOCATION,
        overwrite=True,
        add_image_features=False,
        printtoscreen=False,
    )

    with pytest.raises(SystemExit) as exitcode:
        collate(
            "2021_04_20_Target2",
            TEST_CONFIG_LOCATION,
            "BR00121431",
            base_directory=TEST_DATA_LOCATION,
            tmp_dir=TEST_DATA_LOCATION,
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
            TEST_CONFIG_LOCATION,
            "BR00121431",
            base_directory=TEST_DATA_LOCATION,
            tmp_dir=TEST_DATA_LOCATION,
            aggregate_only=True,
            add_image_features=False,
            printtoscreen=False,
        )
    assert "does not exist" in exitcode.value.code

    collate(
        "2021_04_20_Target2",
        TEST_CONFIG_LOCATION,
        "BR00121431",
        base_directory=TEST_DATA_LOCATION,
        tmp_dir=TEST_DATA_LOCATION,
        add_image_features=False,
        printtoscreen=False,
    )

    assert os.path.exists(TEST_CSV_LOCATION)

    os.remove(TEST_CSV_LOCATION)

    collate(
        "2021_04_20_Target2",
        TEST_CONFIG_LOCATION,
        "BR00121431",
        base_directory=TEST_DATA_LOCATION,
        tmp_dir=TEST_DATA_LOCATION,
        aggregate_only=True,
        add_image_features=False,
        printtoscreen=False,
    )

    validate(TEST_CSV_LOCATION, MAIN_CSV_LOCATION)

    cleanup()
