"""
Utility function to augment a metadata file with X,Y locations of cells in each image
"""

import pathlib
import pandas as pd
import sqlite3
import boto3
import botocore
import tempfile
import collections


class CellLocation:
    """This class holds all the functions augment a metadata file with X,Y
    locations of cells in each image.

    In the metadata file, which is either a CSV or a Parquet file,
    - Each row is single multi-channel image
    - Each image is indexed by multiple columns, e.g., `Metadata_Plate`, `Metadata_Well`,`Metadata_Site`

    The single_cell SQLite file contains at least two tables
    - `Nuclei`, which has the single-cell-level readouts, including location information
    - `Image`, which has the image-level readouts, as well metadata to link to the metadata file

    In the `Nuclei` table,
    - Each row is a cell
    - Each cell has at least 3 columns: `Nuclei_Location_Center_X`, `Nuclei_Location_Center_Y`, `ImageNumber`

    In the `Image` table,
    - Each row is an image
    - Each image has at least the same columns as the images in the metadata file are indexed by, e.g., `Metadata_Plate`,`Metadata_Well`,`Metadata_Site`

    The methods in this class do the following
    - Read the metadata file
    - Read the single_cell file
    - For each image in the metadata file, find the corresponding image in the single_cell file
    - For each cell in the corresponding image, find the X,Y location
    - Add the X,Y locations of all cells to the metadata file in the corresponding row, packed into a single column


    Attributes
    ----------
    metadata_input : str or Pandas DataFrame
        Path to the input metadata file or a Pandas DataFrame

    single_cell_input : str or sqlite3.Connection
        Path to the single_cell file or a sqlite3.Connection object

    augmented_metadata_output : str
        Path to the output file. If None, the metadata file is not saved to disk

    image_column : default = 'ImageNumber'
        Name of the column in the metadata file that links to the single_cell file

    object_column : default = 'ObjectNumber'
        Name of the column in the single_cell file that identifies each cell

    cell_x_loc : default = 'Nuclei_Location_Center_X'
        Name of the column in the single_cell file that contains the X location of each cell

    cell_y_loc : default = 'Nuclei_Location_Center_Y'
        Name of the column in the single_cell file that contains the Y location of each cell

    Methods
    -------
    add_cell_location()
        Augment the metadata file and optionally save it to a file

    """

    def __init__(
        self,
        metadata_input: str or pd.DataFrame,
        single_cell_input: str or sqlite3.Connection,
        augmented_metadata_output: str = None,
        overwrite: bool = False,
        image_column: str = "ImageNumber",
        object_column: str = "ObjectNumber",
        image_index: list = ["Metadata_Plate", "Metadata_Well", "Metadata_Site"],
        cell_x_loc: str = "Nuclei_Location_Center_X",
        cell_y_loc: str = "Nuclei_Location_Center_Y",
    ):
        self.metadata_input = self._expanduser(metadata_input)
        self.augmented_metadata_output = self._expanduser(augmented_metadata_output)
        self.single_cell_input = self._expanduser(single_cell_input)
        self.overwrite = overwrite
        self.image_column = image_column
        self.object_column = object_column
        self.image_index = image_index
        self.cell_x_loc = cell_x_loc
        self.cell_y_loc = cell_y_loc

    def _expanduser(self, obj):
        """Expand the user home directory in a path"""
        if obj is not None and isinstance(obj, str) and not obj.startswith("s3://"):
            return pathlib.Path(obj).expanduser().as_posix()

        else:
            return obj

    def _parse_s3_path(self, s3_path):
        """Parse an S3 path into a bucket and key

        Parameters
        ----------
        s3_path : str
            The S3 path

        Returns
        -------
        str
            The bucket
        str
            The key
        """

        s3_path = s3_path.replace("s3://", "")

        bucket = s3_path.split("/")[0]

        key = "/".join(s3_path.split("/")[1:])

        return bucket, key

    def _s3_file_exists(self, s3_path):
        """Check if a file exists on S3

        Parameters
        ----------
        s3_path : str
            The path to the file on S3

        Returns
        -------
        bool
            True if the file exists on S3, False otherwise
        """

        s3 = boto3.resource("s3")

        bucket, key = self._parse_s3_path(s3_path)

        try:
            s3.Object(bucket, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise
        else:
            return True

    def _download_s3(self, uri):
        """
        Download a file from S3, save it to a temporary directory, and return the path to the file
        """
        s3 = boto3.resource("s3")

        bucket, key = self._parse_s3_path(uri)

        tmp_dir = tempfile.mkdtemp()
        tmp_file = pathlib.Path(tmp_dir) / pathlib.Path(key).name

        s3.Bucket(bucket).download_file(key, tmp_file)

        return tmp_file

    def _load_metadata(self):
        """Load the metadata into a Pandas DataFrame

        Returns
        -------
        Pandas DataFrame
            The metadata loaded into a Pandas DataFrame
        """

        if not isinstance(self.metadata_input, pd.DataFrame):
            # verify that the metadata file is a CSV or a Parquet file

            if not (
                self.metadata_input.endswith(".csv")
                or self.metadata_input.endswith(".parquet")
            ):
                raise ValueError("Metadata file must be a CSV or a Parquet file")

            # load the metadata file into a Pandas DataFrame
            if self.metadata_input.endswith(".csv"):
                df = pd.read_csv(self.metadata_input, dtype=str)
            else:
                df = pd.read_parquet(self.metadata_input)
                # cast all columns to string
                df = df.astype(str)
        else:
            df = self.metadata_input

        # verify that the image index columns are present in the metadata object

        if not all(elem in df.columns for elem in self.image_index):
            raise ValueError(
                f"Image index columns {self.image_index} are not present in the metadata file"
            )

        return df

    def _convert_to_per_row_dict(self, df):
        # define a dictionary to store the output
        output_df_list = collections.defaultdict(list)

        # iterate over each group of cells in the merged DataFrame
        group_cols = self.image_index + [self.image_column]

        for group_values, cell_df in df.groupby(group_cols):
            # add the image-level information to the output dictionary
            for key, value in zip(group_cols, group_values):
                output_df_list[key].append(value)

            # convert the cell DataFrame to a dictionary
            cell_dict = cell_df.to_dict(orient="list")

            # iterate over each cell in the cell DataFrame
            row_cell_dicts = []
            for object_column, cell_x_loc, cell_y_loc in zip(
                cell_dict[self.object_column],
                cell_dict[self.cell_x_loc],
                cell_dict[self.cell_y_loc],
            ):
                # add the cell information to a dictionary
                row_cell_dicts.append(
                    {
                        self.object_column: object_column,
                        self.cell_x_loc: cell_x_loc,
                        self.cell_y_loc: cell_y_loc,
                    }
                )

            # add the cell-level information to the output dictionary
            output_df_list["CellCenters"].append(row_cell_dicts)

        # convert the output dictionary to a Pandas DataFrame
        return pd.DataFrame(output_df_list)

    def _load_single_cell(self):
        """Load the required columns from the `Image` and `Nuclei` tables in the single_cell file or sqlite3.Connection object into a Pandas DataFrame

        Returns
        -------
        Pandas DataFrame
            The required columns from the `Image` and `Nuclei` tables loaded into a Pandas DataFrame
        """

        if isinstance(self.single_cell_input, str):
            # check if the single_cell file is a SQLite file

            if not self.single_cell_input.endswith(".sqlite"):
                raise ValueError("single_cell file must be a SQLite file")

            # if the single_cell file is an S3 path, download it to a temporary file
            if self.single_cell_input.startswith("s3://"):
                temp_single_cell_input = self._download_s3(self.single_cell_input)

                # connect to the single_cell file
                conn = sqlite3.connect(temp_single_cell_input)

            else:
                # connect to the single_cell file
                conn = sqlite3.connect(self.single_cell_input)
        else:
            conn = self.single_cell_input

        # Verify that the Image and Nuclei tables are present in single_cell

        c = conn.cursor()

        c.execute("SELECT name FROM sqlite_master WHERE type='table';")

        tables = c.fetchall()

        tables = [x[0] for x in tables]

        if not ("Image" in tables and "Nuclei" in tables):
            raise ValueError(
                "Image and Nuclei tables are not present in the single_cell file"
            )

        # Verify that the required columns are present in the single_cell file

        c.execute("PRAGMA table_info(Nuclei);")

        nuclei_columns = c.fetchall()

        nuclei_columns = [x[1] for x in nuclei_columns]

        if not (
            self.image_column in nuclei_columns
            and self.object_column in nuclei_columns
            and self.cell_x_loc in nuclei_columns
            and self.cell_y_loc in nuclei_columns
        ):
            raise ValueError(
                "Required columns are not present in the Nuclei table in the SQLite file"
            )

        c.execute("PRAGMA table_info(Image);")

        image_columns = c.fetchall()

        image_columns = [x[1] for x in image_columns]

        if not (
            self.image_column in image_columns
            and all(elem in image_columns for elem in self.image_index)
        ):
            raise ValueError(
                f"Required columns are not present in the Image table in the SQLite file"
            )

        image_index_str = ", ".join(self.image_index)

        # merge the Image and Nuclei tables in SQL

        merge_query = f"""
        SELECT Nuclei.{self.image_column},Nuclei.{self.object_column},Nuclei.{self.cell_x_loc},Nuclei.{self.cell_y_loc},Image.{image_index_str}
        FROM Nuclei
        INNER JOIN Image
        ON Nuclei.{self.image_column} = Image.{self.image_column};
        """

        merged_df = pd.read_sql_query(merge_query, conn)

        conn.close()

        # if the single_cell file was downloaded from S3, delete the temporary file
        if "temp_single_cell_input" in locals():
            pathlib.Path(temp_single_cell_input).unlink()

        # Cast the cell location columns to float
        merged_df[self.cell_x_loc] = merged_df[self.cell_x_loc].astype(float)
        merged_df[self.cell_y_loc] = merged_df[self.cell_y_loc].astype(float)

        # Cast the object column to int
        merged_df[self.object_column] = merged_df[self.object_column].astype(int)

        # Cast the image index columns to str
        for col in self.image_index:
            merged_df[col] = merged_df[col].astype(str)

        merged_df = self._convert_to_per_row_dict(merged_df)

        return merged_df

    def add_cell_location(self):
        """Add the X,Y locations of all cells to the metadata file in the corresponding row, packed into a single column.
        Optionally, save the augmented metadata file as a Parquet file.

        Returns
        -------
        Pandas DataFrame
            Either a data frame or the path to a Parquet file with the X,Y locations of all cells packed into a single column
        """

        # If self.augmented_metadata_output is not None and it is a str and the file already exists, there is nothing to do
        if (
            self.augmented_metadata_output is not None
            and isinstance(self.augmented_metadata_output, str)
            and self.overwrite is False
            and (
                (
                    self.augmented_metadata_output.startswith("s3://")
                    and self._s3_file_exists(self.augmented_metadata_output)
                )
                or (
                    not self.augmented_metadata_output.startswith("s3://")
                    and pathlib.Path(self.augmented_metadata_output).exists()
                )
            )
        ):
            return self.augmented_metadata_output

        # Load the data
        metadata_df = self._load_metadata()
        single_cell_df = self._load_single_cell()

        # Merge the data and single_cell tables
        augmented_metadata_df = pd.merge(
            metadata_df,
            single_cell_df,
            on=self.image_index,
            how="left",
        )

        # If self.augmented_metadata_output is not None, save the data
        if self.augmented_metadata_output is not None:
            # switch to https://github.com/cytomining/pycytominer/blob/master/pycytominer/cyto_utils/output.py
            # if we want to support more file types
            augmented_metadata_df.to_parquet(
                self.augmented_metadata_output, index=False
            )
            return self.augmented_metadata_output
        else:
            return augmented_metadata_df
