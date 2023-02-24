# This is a command line interface for pycytominer/cyto_utils/cell_locations.py

import argparse
from pycytominer.cyto_utils.cell_locations import CellLocation


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Utility function to augment a Parquet file with X,Y locations of cells in each image"
    )
    parser.add_argument(
        "--input_parquet_file",
        help="Path to the input Parquet file",
        required=True,
    )
    parser.add_argument(
        "--sqlite_file",
        help="Path to the SQLite file",
        required=True,
    )
    parser.add_argument(
        "--output_parquet_file",
        help="Path to the output Parquet file",
        required=True,
    )
    parser.add_argument(
        "--image_column",
        help="Name of the column in the Parquet file that links to the SQLite file",
        default="ImageNumber",
    )
    parser.add_argument(
        "--object_column",
        help="Name of the column in the SQLite file that identifies each cell",
        default="ObjectNumber",
    )
    parser.add_argument(
        "--cell_x_loc",
        help="Name of the column in the SQLite file that contains the X location of each cell",
        default="Nuclei_Location_Center_X",
    )
    parser.add_argument(
        "--cell_y_loc",
        help="Name of the column in the SQLite file that contains the Y location of each cell",
        default="Nuclei_Location_Center_Y",
    )
    args = parser.parse_args()

    cell_loc_obj = CellLocation(
        parquet_file=args.input_parquet_file,
        sqlite_file=args.sqlite_file,
        image_column=args.image_column,
        object_column=args.object_column,
        cell_x_loc=args.cell_x_loc,
        cell_y_loc=args.cell_y_loc,
    )

    cell_loc = cell_loc_obj.add_cell_location()

    cell_loc.to_parquet(args.output_parquet_file, index=False)
