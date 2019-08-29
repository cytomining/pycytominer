"""
Miscellaneous utility function
"""

import os
import pandas as pd

default_metadata_file = os.path.join(
    os.path.dirname(__file__), "..", "data", "metadata_feature_dictionary.txt"
)


def check_compartments(compartments):
    valid_compartments = ["cells", "cytoplasm", "nuclei"]
    error_str = "compartment not supported, use one of {}".format(
        valid_compartments
    )
    if isinstance(compartments, list):
        compartments = [x.lower() for x in compartments]
        assert all([x in valid_compartments for x in compartments]), error_str
    elif isinstance(compartments, str):
        compartments = compartments.lower()
        assert compartments in valid_compartments, error_str


def load_known_metadata_dictionary(metadata_file=default_metadata_file):
    """
    From a tab separated text file (two columns: ["compartment", "feature"]) load
    previously known metadata columns per compartment

    Arguments:
    metadata_file - file location of the metadata text file

    Output:
    a dictionary mapping compartments (keys) to previously known metadata (values)
    """
    metadata_dict = {}
    with open(metadata_file) as meta_fh:
        next(meta_fh)
        for line in meta_fh:
            compartment, feature = line.strip().split("\t")
            compartment = compartment.lower()
            if compartment in metadata_dict:
                metadata_dict[compartment].append(feature)
            else:
                metadata_dict[compartment] = []

    return metadata_dict
