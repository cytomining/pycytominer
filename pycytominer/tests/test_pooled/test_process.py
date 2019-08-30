import os
import random
import pytest
import warnings
import tempfile
import numpy as np
import pandas as pd
from pycytominer.pooled.process import PooledCellPainting
from pycytominer.cyto_utils.util import load_known_metadata_dictionary

random.seed(123)


def output_random_data(site_dir, compartment="Cells"):
    a_feature = random.sample(range(1, 1000), 100)
    b_feature = random.sample(range(1, 1000), 100)
    c_feature = random.sample(range(1, 1000), 100)
    d_feature = random.sample(range(1, 1000), 100)
    data_df = pd.DataFrame(
        {"a": a_feature, "b": b_feature, "c": c_feature, "d": d_feature}
    ).reset_index(drop=True)

    data_df = data_df.assign(
        ObjectNumber=list(range(1, 51)) * 2,
        ImageNumber=sorted(["x", "y"] * 50),
        TableNumber=sorted(["x_hash", "y_hash"] * 50),
    )

    data_df.to_csv(os.path.join(site_dir, "{}.csv".format(compartment)), index=False)


tmpdir = tempfile.gettempdir()

batch = "temp_batch"
batch_dir = os.path.join(tmpdir, batch)
sites = ["site_a", "site_b"]
site_a_dir = os.path.join(tmpdir, batch, "site_a")
site_b_dir = os.path.join(tmpdir, batch, "site_b")
meta_cols = ["ObjectNumber", "ImageNumber", "TableNumber"]

meta_df = pd.DataFrame(
    {
        "compartment": ["cells"] * 3 + ["nuclei"] * 3 + ["cytoplasm"] * 3,
        "feature": meta_cols * 3,
    }
)

metadata_file = os.path.join(tmpdir, "metadata_temp.txt")
meta_df.to_csv(metadata_file, sep="\t", index=False)
metadata_dict = load_known_metadata_dictionary(metadata_file)

site_dirs = {}
for site in sites:
    site_dir = os.path.join(tmpdir, batch, site)
    site_dirs[site] = site_dir
    os.makedirs(site_dir, exist_ok=True)

for site_dir in site_dirs:
    for comp in ["Cells", "Nuclei", "Cytoplasm", "BarcodeFoci"]:
        output_random_data(site_dirs[site_dir], compartment=comp)

cytoplasm_merge_cols = [
    "Metadata_Cytoplasm_ImageNumber",
    "Metadata_Cytoplasm_ObjectNumber",
]

pcp = PooledCellPainting(
    directory=batch_dir,
    cytoplasm_to_cell_columns=cytoplasm_merge_cols,
    cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
)


def test_PCP_init():
    assert pcp.directory == batch_dir
    assert pcp.compartments == ["Cells", "Cytoplasm", "Nuclei"]
    assert pcp.cells_merge_columns == [
        "Metadata_Cells_ImageNumber",
        "Metadata_Cells_ObjectNumber",
    ]
    assert pcp.nuclei_merge_columns == [
        "Metadata_Nuclei_ImageNumber",
        "Metadata_Nuclei_ObjectNumber",
    ]
    assert pcp.cytoplasm_to_cell_columns == cytoplasm_merge_cols
    assert pcp.cytoplasm_to_nuclei_columns == cytoplasm_merge_cols
    assert not pcp.output_sites
    assert pcp.normalize_output
    assert pcp.normalize_sample_subset == "all"
    assert pcp.normalize_method == "standardize"
    assert pcp.compression == "gzip"
    assert pcp.float_format is None
    assert pcp.whiten_center
    assert pcp.prebuild_file_list


def test_pcp_build_file_list():
    pcp.build_file_list()
    expected_site_a_dictionary = {
        "batch": batch,
        "site": "site_a",
        "site_directory": site_a_dir,
        "paths": [
            os.path.join(site_a_dir, "Cytoplasm.csv"),
            os.path.join(site_a_dir, "Nuclei.csv"),
            os.path.join(site_a_dir, "Cells.csv"),
        ],
        "barcode_foci": os.path.join(site_a_dir, "BarcodeFoci.csv"),
    }
    expected_site_b_dictionary = {
        "batch": batch,
        "site": "site_b",
        "site_directory": site_b_dir,
        "paths": [
            os.path.join(site_b_dir, "Cytoplasm.csv"),
            os.path.join(site_b_dir, "Nuclei.csv"),
            os.path.join(site_b_dir, "Cells.csv"),
        ],
        "barcode_foci": os.path.join(site_b_dir, "BarcodeFoci.csv"),
    }

    # dictionary are not ordered
    assert len(pcp.file_structure) == 2
    if pcp.file_structure[0]["site"] == "site_a":
        assert pcp.file_structure[0]["batch"] == batch
        assert pcp.file_structure[0]["site"] == "site_a"
        assert pcp.file_structure[0]["site_directory"] == site_a_dir
        expected_paths = sorted(
            [
                os.path.join(site_a_dir, "Cytoplasm.csv"),
                os.path.join(site_a_dir, "Nuclei.csv"),
                os.path.join(site_a_dir, "Cells.csv"),
            ]
        )
        assert sorted(pcp.file_structure[0]["paths"]) == expected_paths
        assert pcp.file_structure[0]["barcode_foci"] == os.path.join(
            site_a_dir, "BarcodeFoci.csv"
        )
    else:
        assert pcp.file_structure[0]["batch"] == batch
        assert pcp.file_structure[0]["site"] == "site_b"
        assert pcp.file_structure[0]["site_directory"] == site_b_dir
        expected_paths = sorted(
            [
                os.path.join(site_a_dir, "Cytoplasm.csv"),
                os.path.join(site_a_dir, "Nuclei.csv"),
                os.path.join(site_a_dir, "Cells.csv"),
            ]
        )
        assert sorted(pcp.file_structure[0]["paths"]) == expected_paths
        assert pcp.file_structure[0]["barcode_foci"] == os.path.join(
            site_b_dir, "BarcodeFoci.csv"
        )


def test_pcp_label_features():
    df = pd.read_csv(os.path.join(site_b_dir, "Cells.csv"))
    label_df = pcp.label_features(
        df=df,
        compartment="Cells",
        metadata_columns=meta_cols,
        map_barcode_as_metadata=False,
    )

    expected_columns = [
        "Metadata_Cells_ObjectNumber",
        "Metadata_Cells_ImageNumber",
        "Metadata_Cells_TableNumber",
        "Cells_a",
        "Cells_b",
        "Cells_c",
        "Cells_d",
    ]

    assert label_df.columns.tolist() == expected_columns

    df = pd.read_csv(os.path.join(site_b_dir, "Cells.csv")).assign(
        Barcode_Column_Test=1
    )
    label_df = pcp.label_features(
        df=df,
        compartment="Cells",
        metadata_columns=meta_cols,
        map_barcode_as_metadata=False,
    )
    assert label_df.columns.tolist() == expected_columns + ["Cells_Barcode_Column_Test"]

    label_df = pcp.label_features(
        df=df,
        compartment="Cells",
        metadata_columns=meta_cols,
        map_barcode_as_metadata=True,
    )
    assert sorted(label_df.columns.tolist()) == sorted(
        expected_columns + ["Metadata_Cells_Barcode_Column_Test"]
    )


def test_pcp_get_barcode_cols():
    df = pd.read_csv(os.path.join(site_b_dir, "Cells.csv")).assign(
        Barcode_Column_Test=1
    )

    result = pcp.get_barcode_cols(df)
    expected_result = ["Barcode_Column_Test"]

    assert result == expected_result


def test_get_compartment_dictionary():
    compartment_dict = pcp.get_compartment_dictionary(
        compartment_paths=pcp.file_structure[0]["paths"],
        metadata_dict=metadata_dict,
        map_barcode_as_metadata=True,
    )

    cell_cols = sorted(compartment_dict["Cells"].columns.tolist())
    expected_cell_cols = [
        "Cells_a",
        "Cells_b",
        "Cells_c",
        "Cells_d",
        "Metadata_Cells_ImageNumber",
        "Metadata_Cells_ObjectNumber",
        "Metadata_Cells_TableNumber",
    ]
    nuc_cols = sorted(compartment_dict["Nuclei"].columns.tolist())
    expected_nuc_cols = [
        "Metadata_Nuclei_ImageNumber",
        "Metadata_Nuclei_ObjectNumber",
        "Metadata_Nuclei_TableNumber",
        "Nuclei_a",
        "Nuclei_b",
        "Nuclei_c",
        "Nuclei_d",
    ]
    cyto_cols = sorted(compartment_dict["Cytoplasm"].columns.tolist())
    expected_cyto_cols = [
        "Cytoplasm_a",
        "Cytoplasm_b",
        "Cytoplasm_c",
        "Cytoplasm_d",
        "Metadata_Cytoplasm_ImageNumber",
        "Metadata_Cytoplasm_ObjectNumber",
        "Metadata_Cytoplasm_TableNumber",
    ]
    assert cell_cols == expected_cell_cols
    assert nuc_cols == expected_nuc_cols
    assert cyto_cols == expected_cyto_cols


def test_merge_compartments():
    compartment_dict = pcp.get_compartment_dictionary(
        compartment_paths=pcp.file_structure[0]["paths"],
        metadata_dict=metadata_dict,
        map_barcode_as_metadata=True,
    )

    merged_df = pcp.merge_compartments(compartment_dict=compartment_dict)

    expected_columns = [
        "Metadata_Cells_ObjectNumber",
        "Metadata_Cells_ImageNumber",
        "Metadata_Cells_TableNumber",
        "Cells_a",
        "Cells_b",
        "Cells_c",
        "Cells_d",
        "Metadata_Cytoplasm_ObjectNumber",
        "Metadata_Cytoplasm_ImageNumber",
        "Metadata_Cytoplasm_TableNumber",
        "Cytoplasm_a",
        "Cytoplasm_b",
        "Cytoplasm_c",
        "Cytoplasm_d",
        "Metadata_Nuclei_ObjectNumber",
        "Metadata_Nuclei_ImageNumber",
        "Metadata_Nuclei_TableNumber",
        "Nuclei_a",
        "Nuclei_b",
        "Nuclei_c",
        "Nuclei_d",
    ]

    assert merged_df.columns.tolist() == expected_columns


def test_process_sites():
    pcp = PooledCellPainting(
        directory=batch_dir,
        cytoplasm_to_cell_columns=cytoplasm_merge_cols,
        cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
        normalize_output=False,
        output_sites=False,
    )

    processed_df = pcp.process_site(
        file_info=pcp.file_structure[0], metadata_file=metadata_file
    )
    expected_last_columns = ["Metadata_Site", "Metadata_Batch"]
    assert processed_df.columns.tolist()[-2:] == expected_last_columns

    pcp = PooledCellPainting(
        directory=batch_dir,
        cytoplasm_to_cell_columns=cytoplasm_merge_cols,
        cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
        normalize_output=True,
        output_sites=False,
    )

    processed_df = pcp.process_site(
        file_info=pcp.file_structure[0], metadata_file=metadata_file
    )
    expected_columns = [
        "Metadata_Cells_ObjectNumber",
        "Metadata_Cells_ImageNumber",
        "Metadata_Cells_TableNumber",
        "Metadata_Cytoplasm_ObjectNumber",
        "Metadata_Cytoplasm_ImageNumber",
        "Metadata_Cytoplasm_TableNumber",
        "Metadata_Nuclei_ObjectNumber",
        "Metadata_Nuclei_ImageNumber",
        "Metadata_Nuclei_TableNumber",
        "Metadata_Site",
        "Metadata_Batch",
        "Cells_a",
        "Cells_b",
        "Cells_c",
        "Cells_d",
        "Cytoplasm_a",
        "Cytoplasm_b",
        "Cytoplasm_c",
        "Cytoplasm_d",
        "Nuclei_a",
        "Nuclei_b",
        "Nuclei_c",
        "Nuclei_d",
    ]
    assert processed_df.columns.tolist() == expected_columns
    for cols in processed_df.columns.tolist():
        if not cols.startswith("Metadata_"):
            assert np.abs(processed_df.Cells_a.mean().round()) == 0

    pcp = PooledCellPainting(
        directory=batch_dir,
        cytoplasm_to_cell_columns=cytoplasm_merge_cols,
        cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
        normalize_output=True,
        output_sites=True,
    )

    pcp.process_site(file_info=pcp.file_structure[0], metadata_file=metadata_file)

    merged_norm_site_dir = pcp.file_structure[0]["site_directory"]
    merged_norm_site = pcp.file_structure[0]["site"]
    process_file = os.path.join(
        merged_norm_site_dir, "{}_merged_normalized.csv.gz".format(merged_norm_site)
    )
    processed_df = pd.read_csv(process_file)

    assert processed_df.columns.tolist() == expected_columns
    for cols in processed_df.columns.tolist():
        if not cols.startswith("Metadata_"):
            assert np.abs(processed_df.Cells_a.mean().round()) == 0


def test_pcp_process_batch():
    pcp = PooledCellPainting(
        directory=batch_dir,
        cytoplasm_to_cell_columns=cytoplasm_merge_cols,
        cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
        normalize_output=True,
        output_sites=True,
    )

    pcp.process_batch(metadata_file=metadata_file, map_barcode_as_metadata=False)

    for file_info in pcp.file_structure:
        site_directory = file_info["site_directory"]
        site = file_info["site"]
        process_file = os.path.join(
            site_directory, "{}_merged_normalized.csv.gz".format(site)
        )
        assert os.path.isfile(process_file)

        processed_df = pd.read_csv(process_file)

        for cols in processed_df.columns.tolist():
            if not cols.startswith("Metadata_"):
                assert np.abs(processed_df.Cells_a.mean().round()) == 0

    with pytest.warns(UserWarning) as w:
        warnings.simplefilter("always")

        pcp = PooledCellPainting(
            directory=batch_dir,
            cytoplasm_to_cell_columns=cytoplasm_merge_cols,
            cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
            normalize_output=True,
            output_sites=False,
        )

        pcp.process_batch(metadata_file=metadata_file, map_barcode_as_metadata=False)

        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)


def test_pcp_concatenate_sites():
    pcp = PooledCellPainting(
        directory=batch_dir,
        cytoplasm_to_cell_columns=cytoplasm_merge_cols,
        cytoplasm_to_nuclei_columns=cytoplasm_merge_cols,
        normalize_output=True,
        output_sites=True,
    )

    pcp.process_batch(metadata_file=metadata_file, map_barcode_as_metadata=False)
    concat_df = pcp.concatenate_sites()

    expected_columns = [
        "Metadata_Batch",
        "Metadata_Site",
        "Metadata_Cells_ImageNumber",
        "Metadata_Cells_ObjectNumber",
        "Metadata_Cells_TableNumber",
        "Metadata_Cytoplasm_ImageNumber",
        "Metadata_Cytoplasm_ObjectNumber",
        "Metadata_Cytoplasm_TableNumber",
        "Metadata_Nuclei_ImageNumber",
        "Metadata_Nuclei_ObjectNumber",
        "Metadata_Nuclei_TableNumber",
        "Cells_a",
        "Cells_b",
        "Cells_c",
        "Cells_d",
        "Cytoplasm_a",
        "Cytoplasm_b",
        "Cytoplasm_c",
        "Cytoplasm_d",
        "Nuclei_a",
        "Nuclei_b",
        "Nuclei_c",
        "Nuclei_d",
    ]

    assert concat_df.columns.tolist() == expected_columns
    assert sorted(list(concat_df.Metadata_Site.unique())) == ["site_a", "site_b"]
    assert list(list(concat_df.Metadata_Batch.unique())) == ["temp_batch"]
    assert concat_df.shape == (200, 23)

    concat_file = os.path.join(tmpdir, "full_concat_test.csv.gz")
    pcp.concatenate_sites(output_file=concat_file)
    concat_from_file_df = pd.read_csv(concat_file)

    assert concat_df.columns.tolist() == expected_columns
    assert sorted(list(concat_df.Metadata_Site.unique())) == ["site_a", "site_b"]
    assert list(list(concat_df.Metadata_Batch.unique())) == ["temp_batch"]
    assert concat_df.shape == (200, 23)
