"""
Module used for pycytominer configuration details.
"""

import pandas as pd

# configure pandas copy_on_write for 3.0.0 requirements
# see: https://pandas.pydata.org/pandas-docs/version/2.2.0/whatsnew/v2.2.0.html#copy-on-write
# note: we use a conditional here to avoid exceptions
# with versions of pandas which don't include this option.
if "copy_on_write" in dir(pd.options.mode):
    pd.options.mode.copy_on_write = True
