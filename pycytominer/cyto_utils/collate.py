import os
import subprocess
import sys

# parameters needed - batch ID, config file, plate ID
# options needed - download or not, use a column as a plate name, numge or not, pipeline name, remote base directory, temp directory, overwrite or not

## Steps

# check if the file exists (note this used to happen after download but not sure, why, that seems like a bad idea)

# optionally download files

# run cytominer_database

# add a plate column if you need to

# index

# at this point, collate.R did an aggregation, but since the recipe does that already I don't think that NEEDS to live here (but it could if we want)

# copy back to S3

# delete CSV files if downloaded

