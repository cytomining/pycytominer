"""
Module used for pycytominer configuration details.
"""

import pandas as pd

# configure pandas copy_on_write for 3.0.0 requirements
# see: https://pandas.pydata.org/pandas-docs/version/2.2.0/whatsnew/v2.2.0.html#copy-on-write
pd.options.mode.copy_on_write = True
