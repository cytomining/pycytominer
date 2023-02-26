"""
Utility function to augment a metadata file with X,Y locations of cells in each image
"""

import pandas as pd
import sqlite3


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

    single_cell_file : str or sqlite3.Connection
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
    load_metadata()
        Load the metadata file into a Pandas DataFrame

    load_single_cell()
        Load the required columns from the `Image` and `Nuclei` tables in the single_cell file into a Pandas DataFrame

    add_cell_location()
        Augment the metadata file and optionally save it to a file

    """

    def __init__(
        self,
        metadata_input: str or pd.DataFrame,
        single_cell_input: str or sqlite3.Connection,
        augmented_metadata_output: str = None,
        image_column: str = "ImageNumber",
        object_column: str = "ObjectNumber",
        image_index=["Metadata_Plate", "Metadata_Well", "Metadata_Site"],
        cell_x_loc: str = "Nuclei_Location_Center_X",
        cell_y_loc: str = "Nuclei_Location_Center_Y",
    ):
        self.metadata_input = metadata_input
        self.augmented_metadata_output = augmented_metadata_output
        self.single_cell_input = single_cell_input
        self.image_column = image_column
        self.object_column = object_column
        self.image_index = image_index
        self.cell_x_loc = cell_x_loc
        self.cell_y_loc = cell_y_loc

    def load_metadata(self):
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
                df = pd.read_csv(self.metadata_input)
            else:
                df = pd.read_parquet(self.metadata_input)
        else:
            df = self.metadata_input

        # verify that the image index columns are present in the metadata object

        if not all(elem in df.columns for elem in self.image_index):
            raise ValueError(
                f"Image index columns {self.image_index} are not present in the metadata file"
            )

        return df

    def load_single_cell(self):
        """Load the required columns from the `Image` and `Nuclei` tables in the single_cell file or sqlite3.Connection object into a Pandas DataFrame

        Returns
        -------
        Pandas DataFrame
            The required columns from the `Image` and `Nuclei` tables loaded into a Pandas DataFrame
        """

        if isinstance(self.single_cell_input, str):
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
                f"Required columns are not present in the Nuclei table in the SQLite file"
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

        # Load the required columns from the single_cell file

        nuclei_query = f"SELECT {self.image_column},{self.object_column},{self.cell_x_loc},{self.cell_y_loc} FROM Nuclei;"

        image_index_str = ", ".join(self.image_index)

        image_query = f"SELECT  {self.image_column},{image_index_str} FROM Image;"

        nuclei_df = pd.read_sql_query(nuclei_query, conn)

        image_df = pd.read_sql_query(image_query, conn)

        conn.close()

        # Merge the Image and Nuclei tables
        merged_df = pd.merge(image_df, nuclei_df, on=self.image_column, how="inner")

        # Cast the cell location columns to float
        merged_df[self.cell_x_loc] = merged_df[self.cell_x_loc].astype(float)
        merged_df[self.cell_y_loc] = merged_df[self.cell_y_loc].astype(float)

        # Cast the object column to int
        merged_df[self.object_column] = merged_df[self.object_column].astype(int)

        # Group and nest the X,Y locations of all cells in each image
        merged_df = (
            merged_df.groupby(self.image_index)
            .agg(
                {self.object_column: list, self.cell_x_loc: list, self.cell_y_loc: list}
            )
            .reset_index()
        )

        return merged_df

    def add_cell_location(self):
        """Add the X,Y locations of all cells to the metadata file in the corresponding row, packed into a single column.
        Optionally, save the augmented metadata file as a Parquet file.

        Returns
        -------
        Pandas DataFrame
            The Parquet file with the X,Y locations of all cells packed into a single column
        """
        # Load the data
        metadata_df = self.load_metadata()
        single_cell_df = self.load_single_cell()

        # Merge the data and single_cell tables
        augmented_metadata_df = pd.merge(
            metadata_df,
            single_cell_df,
            on=self.image_index,
            how="left",
        )

        # If self.augmented_metadata_output) is not None, save the data
        if self.augmented_metadata_output is not None:
            augmented_metadata_df.to_parquet(
                self.augmented_metadata_output, index=False
            )
        else:
            return augmented_metadata_df
