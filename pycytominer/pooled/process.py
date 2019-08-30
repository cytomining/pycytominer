import os
import numpy as np
import pandas as pd
import warnings
import multiprocessing
from joblib import Parallel, delayed

from pycytominer.normalize import normalize
from pycytominer.cyto_utils.output import output, infer_compression_suffix
from pycytominer.cyto_utils.util import (
    check_compartments,
    load_known_metadata_dictionary,
)

default_metadata_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "metadata_feature_dictionary.txt"
)


class PooledCellPainting:
    """
    Class to process and normalize pooled cell painting (pcp) experiments. Two steps are
    unique to a pcp experiment: (1) Merge compartments per site and (2) normalize
    sites given pcp barcodes.
    """

    def __init__(
        self,
        directory,
        compartments=["Cells", "Cytoplasm", "Nuclei"],
        cells_merge_columns=[
            "Metadata_Cells_ImageNumber",
            "Metadata_Cells_ObjectNumber",
        ],
        nuclei_merge_columns=[
            "Metadata_Nuclei_ImageNumber",
            "Metadata_Nuclei_ObjectNumber",
        ],
        cytoplasm_to_cell_columns=[
            "Metadata_Cytoplasm_ImageNumber",
            "Metadata_Cytoplasm_Parent_Cells",
        ],
        cytoplasm_to_nuclei_columns=[
            "Metadata_Cytoplasm_ImageNumber",
            "Metadata_Cytoplasm_Parent_Nuclei",
        ],
        output_sites=False,
        normalize_output=True,
        normalize_sample_subset="all",
        normalize_method="standardize",
        prebuild_file_list=True,
        **kwargs
    ):
        """
        Arguments:
        directory - str of the experiment directory, typically path ending with batch id
        barcode_map_df - pandas dataframe mapping barcode assignments to perturbations
        compartments - str of single or list of all compartments to use
                       [default: '["Cells", "Cytoplasm", "Nuclei"]']
        cells_merge_columns - list of columns mapping cells compartment to cytoplasm
        nuclei_merge_columns - list of columns mapping nuclei compartment to cytoplasm
        cytoplasm_to_cell_columns - list of columns mapping cytoplasm to cells
        cytoplasm_to_nuclei_columns - list of columns mapping cytoplasm to nuclei
        normalize_output - boolean if profiles should be normalized [default: True]
        normalize_sample_subset - str indicating which metadata column and values to
                                  use to subset. The control samples are often used
                                  here [default: 'all']
                                  Note: the format of this variable will be used in a
                                  pd.query() function. An example is
                                  "Metadata_treatment == 'control'" (include all quotes)
        normalize_method - string indicating how the dataframe will be normalized
                           [default: 'standardize']
        output_sites - boolean if true will output individual files for each site
                       [default: False]
        """
        # Set self variables
        self.directory = directory
        self.compartments = compartments
        self.cells_merge_columns = cells_merge_columns
        self.nuclei_merge_columns = nuclei_merge_columns
        self.cytoplasm_to_cell_columns = cytoplasm_to_cell_columns
        self.cytoplasm_to_nuclei_columns = cytoplasm_to_nuclei_columns
        self.normalize_output = normalize_output
        self.normalize_sample_subset = normalize_sample_subset
        self.normalize_method = normalize_method
        self.output_sites = output_sites
        self.prebuild_file_list = prebuild_file_list

        self.compression = kwargs.pop("compression", "gzip")
        self.float_format = kwargs.pop("float_format", None)
        self.whiten_center = kwargs.pop("whiten_center", True)

        # Check compartments specified
        check_compartments(self.compartments)

        # Compartments must be title case, ensure that they are
        self._capitalize_compartments()

        # If more than one compartment is provided, Cytoplasm must be present
        if isinstance(self.compartments, list):
            assert (
                "Cytoplasm" in self.compartments
            ), "Cytoplasm must be present because it maps together Nuclei and Cells"

        if self.prebuild_file_list:
            self.build_file_list()

    def _capitalize_compartments(self):
        if isinstance(self.compartments, list):
            self.compartments = [x.title() for x in self.compartments]
        elif isinstance(compartments, str):
            self.compartments = self.compartments.title()

    def set_directory(self, directory):
        self.directory = directory

    def set_compartments(self, compartments):
        check_compartments(compartments)
        self.compartments = compartments
        self._capitalize_compartments()

    def build_file_list(self, batch_id="infer", directory=None):

        if directory is not None:
            self.set_directory(directory)

        if batch_id == "infer":
            self.batch_id = os.path.basename(self.directory)
        else:
            self.batch_id = batch_id

        self.file_structure = []
        for site in os.listdir(self.directory):
            site_files = os.listdir(os.path.join(self.directory, site))
            paths = [
                os.path.join(self.directory, site, x)
                for x in site_files
                if os.path.splitext(x)[0] in self.compartments
            ]
            file_info = {
                "batch": self.batch_id,
                "site": site,
                "site_directory": os.path.join(self.directory, site),
                "paths": paths,
                "barcode_foci": os.path.join(
                    self.directory,
                    site,
                    [x for x in site_files if "BarcodeFoci" in x][0],
                ),
            }
            self.file_structure.append(file_info)

    def process_site(
        self,
        file_info,
        barcode_id_columns=None,
        metadata_file=default_metadata_file,
        map_barcode_as_metadata=True,
    ):
        """
        Append compartment and metadata prefixes to features

        Arguments:
        file_info - dictionary with three keys: ("batch", "site", "paths")
        barcode_id_columns - list of length two with first element indicating the
                             column(s) of the barcode_map_df and the second element
                             indicating the column(s) of profile_df to merge using
        metadata_file - file pointing to known metadata features per compartment
        map_barcode_as_metadata - boolean if columns containing the string "Barcode"
                                  are considered metadata

        Output:
        dictionary of dataframes with compartment as keys and pandas dataframes with
        columns prefixed with metadata or compartment labels.
        """
        batch = file_info["batch"]
        site = file_info["site"]
        site_dir = file_info["site_directory"]
        paths = file_info["paths"]
        barcode_foci_file = file_info["barcode_foci"]

        metadata_dict = load_known_metadata_dictionary(metadata_file)

        compartment_dict = self.get_compartment_dictionary(
            compartment_paths=paths,
            metadata_dict=metadata_dict,
            map_barcode_as_metadata=map_barcode_as_metadata,
        )
        merged_df = self.merge_compartments(compartment_dict=compartment_dict)
        merged_df = merged_df.assign(Metadata_Site=site, Metadata_Batch=batch)

        # Not currently supported
        # See https://github.com/broadinstitute/pooled-cell-painting-analysis/issues/34
        # merged_df = self.merge_barcode_mapping_df(
        #     profile_df=merged_df, barcode_id_columns=barcode_id_columns
        # )

        output_file_string = "{}_merged".format(site)
        if self.normalize_output:
            merged_df = normalize(
                profiles=merged_df,
                samples=self.normalize_sample_subset,
                method=self.normalize_method,
                output_file="none",
            )
            output_file_string = "{}_normalized".format(output_file_string)

        if self.output_sites:
            file_suffix = infer_compression_suffix(self.compression)
            output_file_string = "{}.csv{}".format(output_file_string, file_suffix)
            output(
                df=merged_df,
                output_filename=os.path.join(site_dir, output_file_string),
                compression=self.compression,
                float_format=self.float_format,
            )
        else:
            return merged_df

    def process_batch(
        self,
        barcode_id_columns=None,
        parallel=False,
        num_cores="infer",
        metadata_file=default_metadata_file,
        map_barcode_as_metadata=True,
    ):
        if not hasattr(self, "file_structure"):
            self.build_file_list()

        if not self.output_sites:
            self.output_sites = True
            warnings.warn(
                "output_sites must be True to run `process_batch`. Setting output_sites=True"
            )
        if num_cores == "infer":
            num_cores = multiprocessing.cpu_count() - 1

        if parallel:
            Parallel(n_jobs=num_cores)(
                delayed(self.process_site)(
                    plate=file_info,
                    barcode_id_columns=barcode_id_columns,
                    metadata_file=metadata_file,
                    map_barcode_as_metadata=True,
                )
                for file_info in self.file_structure
            )
        else:
            for file_info in self.file_structure:
                self.process_site(
                    file_info=file_info,
                    barcode_id_columns=barcode_id_columns,
                    metadata_file=metadata_file,
                    map_barcode_as_metadata=True,
                )

    def get_compartment_dictionary(
        self, compartment_paths, metadata_dict, map_barcode_as_metadata
    ):
        if isinstance(self.compartments, list):
            df_dict = {x: [] for x in self.compartments}
        else:
            df_dict = {self.compartments: []}

        for file in compartment_paths:
            compartment = os.path.basename(os.path.splitext(file)[0])
            df = pd.read_csv(file, dtype=object)

            metadata_columns = metadata_dict[compartment.lower()]
            df_dict[compartment] = self.label_features(
                df=df,
                compartment=compartment,
                metadata_columns=metadata_columns,
                map_barcode_as_metadata=map_barcode_as_metadata,
            )
        return df_dict

    def merge_compartments(self, compartment_dict):
        """
        Given a dictionary of profiles with labeled columns, merge into a single
        dataframe for downstream processing
        """
        if len(compartment_dict) == 1:
            return compartment_dict[self.compartments]

        cytoplasm_df = compartment_dict["Cytoplasm"]
        if "Cells" in self.compartments:
            cytoplasm_df = compartment_dict["Cells"].merge(
                cytoplasm_df,
                left_on=self.cells_merge_columns,
                right_on=self.cytoplasm_to_cell_columns,
                how="inner",
            )
        if "Nuclei" in self.compartments:
            cytoplasm_df = cytoplasm_df.merge(
                compartment_dict["Nuclei"],
                left_on=self.cytoplasm_to_nuclei_columns,
                right_on=self.nuclei_merge_columns,
                how="inner",
            )

        return cytoplasm_df

    def concatenate_sites(self, output_file=None):
        """
        Get all of the processed profiles and concatenate
        """
        # Grab all of the processed files
        pcp_merged_files = []
        for file_info in self.file_structure:
            site_dir = file_info["site_directory"]
            site = file_info["site"]
            file_start = "{}_merged".format(site)

            merge_f = [x for x in os.listdir(site_dir) if x.startswith(file_start)][0]
            merge_f = os.path.join(site_dir, merge_f)
            pcp_merged_files.append(merge_f)

        # Concatenate them together, ensuring columns are sorted
        df = pd.concat(
            [pd.read_csv(x) for x in pcp_merged_files], sort=True
        ).reset_index(drop=True)

        # Reorder columns
        batch_metadata_cols = ["Metadata_Batch", "Metadata_Site"]
        column_order = batch_metadata_cols + [
            x
            for x in df.columns
            if x.startswith("Metadata_") and x not in batch_metadata_cols
        ]
        for comp in self.compartments:
            column_order += [
                x for x in df.columns if x.startswith("{}_".format(comp.title()))
            ]

        df = df.reindex(columns=column_order)

        # Output
        if output_file is not None:
            output(
                df=df,
                output_filename=output_file,
                compression=self.compression,
                float_format=self.float_format,
            )
        else:
            return df

    def label_features(
        self, df, compartment, metadata_columns, map_barcode_as_metadata=True
    ):
        """
        Assign each column in the dataframe as a compartment feature or metadata

        Arguments:
        df - pandas dataframe storing cell painting profiles
        compartment - str indicating the compartment to subset
        metadata_columns - list of column names to append "Metadata_" prefix
        map_barcode_as_metadata - boolean if columns containing the string "Barcode"
                                  are considered metadata
        """
        check_compartments(compartment)

        # Include all columns that contain the term "barcode"
        if map_barcode_as_metadata:
            metadata_columns += self.get_barcode_cols(df)

        metadata_prefix = "Metadata_{}_".format(compartment.title())
        feature_prefix = "{}_".format(compartment.title())

        id_df = df.loc[:, metadata_columns].add_prefix(metadata_prefix)
        feature_df = (
            df.drop(metadata_columns, axis="columns").add_prefix(feature_prefix)
        ).astype(float)

        return pd.concat([id_df, feature_df], axis="columns")

    def get_barcode_cols(self, df):
        """
        Given a profile data frame and id_columns, append barcode columns

        Arguments:
        df - dataframe storing metadata and features for cell painting experiment

        Output:
        list of metadata columns
        """
        return df.columns[df.columns.str.contains("Barcode")].tolist()
