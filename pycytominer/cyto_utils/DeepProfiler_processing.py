import os
import pathlib
import numpy as np
import pandas as pd

from pycytominer import aggregate
from pycytominer.cyto_utils import infer_cp_features


def load_npz(npz_file):
    npz = np.load(npz_file)
    files = npz.files
    assert len(files) == 1

    df = pd.DataFrame(npz[files[0]])

    return df


class AggregateDeepProfiler:
    def __init__(
        self,
        index_file,
        profile_dir,
        aggregate_operation="median",
        aggregate_on="well",
        file_delimiter="_",
        file_extension=".npz",
    ):
        self.index_file = index_file
        self.profile_dir = profile_dir
        self.aggregate_operation = aggregate_operation
        self.aggregate_on = aggregate_on
        self.file_delimiter = file_delimiter
        self.file_extension = file_extension
        if not self.file_extension.startswith("."):
            self.file_extension = f".{self.file_extension}"
        self.index_df = pd.read_csv(index_file)

    def build_filenames(self):
        """
        Single cell profile file names indicated by plate, well, and site information
        """
        self.filenames = self.index_df.apply(
            self.build_filename_from_index, axis="columns"
        )
        self.filenames = [
            pathlib.PurePath(f"{self.profile_dir}/{x}") for x in self.filenames
        ]

    def build_filename_from_index(self, row):
        plate = row["Metadata_Plate"]
        well = row["Metadata_Well"]
        site = row["Metadata_Site"]

        filename = f"{plate}_{well}_{site}{self.file_extension}"
        return filename

    def extract_filename_metadata(self, npz_file, delimiter="_"):
        """
        Format: plate_well_site.npz
        """
        base_file = os.path.basename(npz_file).strip(".npz").split(delimiter)
        site = base_file[-1]
        well = base_file[-2]
        plate = delimiter.join(base_file[:-2])

        return {"site": site, "well": well, "plate": plate}

    def setup_aggregate(self):
        if not hasattr(self, "filenames"):
            self.build_filenames()

        self.file_aggregate = {}
        for filename in self.filenames:
            file_info = self.extract_filename_metadata(filename, self.file_delimiter)
            file_key = file_info[self.aggregate_on]

            if self.aggregate_on == "site":
                file_key = (
                    f"{file_info['plate']}_{file_info['well']}_{file_info['site']}"
                )

            if self.aggregate_on == "well":
                file_key = f"{file_info['plate']}_{file_info['well']}"

            if file_key in self.file_aggregate:
                self.file_aggregate[file_key]["files"].append(filename)
            else:
                self.file_aggregate[file_key] = {}
                self.file_aggregate[file_key]["files"] = [filename]

            self.file_aggregate[file_key]["metadata"] = file_info

    def aggregate_deep(self):
        if not hasattr(self, "file_aggregate"):
            self.setup_aggregate()

        self.aggregated_profiles = []
        self.aggregate_merge_col = f"Metadata_{self.aggregate_on.capitalize()}_Position"

        for metadata_level in self.file_aggregate:
            df = pd.concat(
                [load_npz(x) for x in self.file_aggregate[metadata_level]["files"]]
            )
            meta_df = pd.DataFrame(
                self.file_aggregate[metadata_level]["metadata"], index=[0]
            )
            meta_df.columns = [f"Metadata_{x.capitalize()}" for x in meta_df.columns]

            if self.aggregate_on == "well":
                meta_df = meta_df.drop("Metadata_Site", axis="columns")

            features = df.columns.tolist()
            df = df.assign(Metadata_Aggregate_On=self.aggregate_on)
            df = aggregate(
                population_df=df,
                strata="Metadata_Aggregate_On",
                features=features,
                operation=self.aggregate_operation,
            )
            df.loc[:, self.aggregate_merge_col] = metadata_level
            df = meta_df.merge(df, left_index=True, right_index=True)
            self.aggregated_profiles.append(df)

        self.aggregated_profiles = pd.concat([x for x in self.aggregated_profiles])
        self.aggregated_profiles.columns = [
            str(x) for x in self.aggregated_profiles.columns
        ]
        meta_features = infer_cp_features(self.aggregated_profiles, metadata=True)
        reindex_features = [str(x) for x in features]
        self.aggregated_profiles = self.aggregated_profiles.reindex(
            meta_features + reindex_features, axis="columns"
        )

    def annotate_deep(
        self, annotate_cols, merge_cols=["Metadata_Plate", "Metadata_Well"]
    ):
        if not hasattr(self, "aggregated_profiles"):
            self.aggregate_deep()

        meta_df = self.index_df.loc[:, annotate_cols].drop_duplicates()

        meta_df.columns = [
            "Metadata_{}".format(x) if not x.startswith("Metadata_") else x
            for x in meta_df.columns
        ]

        return meta_df.merge(self.aggregated_profiles, on=merge_cols, how="inner")
