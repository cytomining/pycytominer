#!/bin/bash

# Create SQLite and LoadData CSV files for testing cell locations
#
# Steps:
# 1. Download SQLite file from S3
# 2. Download LoadData CSV file from S3
# 3. Query SQLite to select specific columns of all rows of the `Image` and `Nuclei` table in the SQLite file where `ImageNumber` is 1 or 2.
# 4. Create the SQLite file fixture using the output of the SQL queries
# 5. Create a new LoadData CSV fixture file with only the rows corresponding to the rows in SQLite file fixture

# Download SQLite file
aws s3 cp s3://cellpainting-gallery/cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/BR00126114.sqlite .

# Download LoadData CSV file
aws s3 cp s3://cellpainting-gallery/cpg0016-jump/source_4/workspace/load_data_csv/2021_08_23_Batch12/BR00126114/load_data_with_illum.parquet .

# Write a SQL query to select rows of the `Image` table in the SQLite file where `ImageNumber` is 1 or 2.
# Only select the columns: `Metadata_Plate`, `Metadata_Well`, `Metadata_Site`, `ImageNumber`

sqlite3 -header -csv BR00126114.sqlite "SELECT Metadata_Plate, Metadata_Well, Metadata_Site, ImageNumber FROM Image WHERE ImageNumber = 1 OR ImageNumber = 2;" > image_query.csv


# Write a SQL query to select rows of the `Nuclei` table in the SQLite file where `ImageNumber` is 1 or 2.
# Only select the columns: `ImageNumber`, `ObjectNumber`, `Nuclei_Location_Center_X`, `Nuclei_Location_Center_Y`

sqlite3 -header -csv BR00126114.sqlite "SELECT ImageNumber, ObjectNumber, Nuclei_Location_Center_X, Nuclei_Location_Center_Y FROM Nuclei WHERE ImageNumber = 1 LIMIT 10;" > nuclei_query_1.csv
sqlite3 -header -csv BR00126114.sqlite "SELECT ImageNumber, ObjectNumber, Nuclei_Location_Center_X, Nuclei_Location_Center_Y FROM Nuclei WHERE ImageNumber = 2 LIMIT 10;" > nuclei_query_2.csv

csvstack nuclei_query_1.csv nuclei_query_2.csv > nuclei_query.csv

# Create a text file with the following SQL commands:

cat << EOF > create_tables.sql
.mode csv
.import image_query.csv Image
.import nuclei_query.csv Nuclei
EOF

cat create_tables.sql

# run the SQL commands in the text file to create the SQLite file

sqlite3 test_BR00126114.sqlite < create_tables.sql

# Print the list of tables in the SQLite file

sqlite3 test_BR00126114.sqlite ".tables"

# Print the contents of the `Image` table in the SQLite file

sqlite3 test_BR00126114.sqlite "SELECT * FROM Image;"

# Print the contents of the `Nuclei` table in the SQLite file

sqlite3 test_BR00126114.sqlite "SELECT * FROM Nuclei;"

cat << EOF > create_parquet.py
import pandas as pd
load_data = pd.read_parquet("load_data_with_illum.parquet")
load_data = load_data.astype({"Metadata_Plate": str, "Metadata_Well": str, "Metadata_Site": str})
image_query = pd.read_csv("image_query.csv")
image_query = image_query[["Metadata_Plate", "Metadata_Well", "Metadata_Site"]]
image_query = image_query.astype({"Metadata_Plate": str, "Metadata_Well": str, "Metadata_Site": str})
merged_df = image_query.merge(load_data, on=["Metadata_Plate", "Metadata_Well", "Metadata_Site"])
merged_df.to_parquet("load_data_with_illum_subset.parquet")
EOF

python create_parquet.py

