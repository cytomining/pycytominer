"""
Utility function to augment a Parquet file with X,Y locations of cells in each image
"""

import pandas as pd
import sqlite3


class CellLocation:
    """This class holds all the functions augment a Parquet files with X,Y locations
    of cells in each image.

    In the Parquet file,
    - Each row is single multi-channel image
    - Each such image is indexed by 3 columns: `Metadata_Plate`,`Metadata_Well`,`Metadata_Site`

    The SQLite file contains at least two tables
    - `Nuclei`, which has the single-cell-level readouts, including location information
    - `Image`, which has the image-level readouts, as well metadata to link to the Parquet file

    In the `Nuclei` table,
    - Each row is a cell
    - Each cell has at least 3 columns: `Nuclei_Location_Center_X`, `Nuclei_Location_Center_Y`, `ImageNumber`

    In the `Image` table,
    - Each row is an image
    - Each image has at least 3 columns: `Metadata_Plate`,`Metadata_Well`,`Metadata_Site`


    The methods in this class do the following
    - Read the Parquet file
    - Read the SQLite file
    - For each image in the Parquet file, find the corresponding image in the SQLite file
    - For each cell in the corresponding image, find the X,Y location
    - Add the X,Y locations of all cells to the Parquet file in the corresponding row, packed into a single column


    Attributes
    ----------
    parquet_file_input : str
        Path to the input Parquet file

    parquet_file_output : str
        Path to the output Parquet file

    sqlite_file : str
        Path to the SQLite file

    image_column : default = 'ImageNumber'
        Name of the column in the Parquet file that links to the SQLite file

    object_column : default = 'ObjectNumber'
        Name of the column in the SQLite file that identifies each cell

    cell_x_loc : default = 'Nuclei_Location_Center_X'
        Name of the column in the SQLite file that contains the X location of each cell

    cell_y_loc : default = 'Nuclei_Location_Center_Y'
        Name of the column in the SQLite file that contains the Y location of each cell

    Methods
    -------
    load_data()
        Load the Parquet file into a Pandas DataFrame

    load_sqlite()
        Load the required columns from the `Image` and `Nuclei` tables in the SQLite file into a Pandas DataFrame

    run()
        Augment the Parquet file and save it

    """

    def __init__(
        self,
        parquet_file_input: str,
        sqlite_file: str = str,
        parquet_file_output: str = None,
        image_column: str = "ImageNumber",
        object_column: str = "ObjectNumber",
        image_index=["Metadata_Plate", "Metadata_Well", "Metadata_Site"],
        cell_x_loc: str = "Nuclei_Location_Center_X",
        cell_y_loc: str = "Nuclei_Location_Center_Y",
    ):
        self.parquet_file_input = parquet_file_input
        self.parquet_file_output = parquet_file_output
        self.sqlite_file = sqlite_file
        self.image_column = image_column
        self.object_column = object_column
        self.image_index = image_index
        self.cell_x_loc = cell_x_loc
        self.cell_y_loc = cell_y_loc

    def load_data(self):
        """Load the Parquet file into a Pandas DataFrame

        Returns
        -------
        Pandas DataFrame
            The Parquet file loaded into a Pandas DataFrame
        """
        df = pd.read_parquet(self.parquet_file_input)

        # verify that the image index columns are present in the Parquet file

        if not all(elem in df.columns for elem in self.image_index):
            raise ValueError(
                f"Image index columns {self.image_index} are not present in the Parquet file"
            )

        return df

    def load_sqlite(self):
        """Load the required columns from the `Image` and `Nuclei` tables in the SQLite file into a Pandas DataFrame

        Returns
        -------
        Pandas DataFrame
            The required columns from the `Image` and `Nuclei` tables in the SQLite file loaded into a Pandas DataFrame
        """
        # Load the required columns from the SQLite file

        nuclei_query = f"SELECT {self.image_column},{self.object_column},{self.cell_x_loc},{self.cell_y_loc} FROM Nuclei;"

        image_index_str = ", ".join(self.image_index)

        image_query = f"SELECT  {self.image_column},{image_index_str} FROM Image;"

        conn = sqlite3.connect(self.sqlite_file)

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
        """Add the X,Y locations of all cells to the Parquet file in the corresponding row, packed into a single column

        Returns
        -------
        Pandas DataFrame
            The Parquet file with the X,Y locations of all cells packed into a single column
        """
        # Load the data
        data_df = self.load_data()
        sqlite_df = self.load_sqlite()

        # Merge the data and SQLite tables
        merged_df = pd.merge(
            data_df,
            sqlite_df,
            on=self.image_index,
            how="left",
        )

        return merged_df

    def run(self):
        """Augment the Parquet file and save it"""
        # Add the cell location
        merged_df = self.add_cell_location()

        # Save the data
        merged_df.to_parquet(self.parquet_file_output)
