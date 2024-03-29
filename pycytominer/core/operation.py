"""This module contains the Operation class which is a base class for other file/dataframe operations in pycytominer."""

from typing import TypeVar

import pandas as pd
from pandera.typing import DataFrame
from pydantic import BaseModel, Field, FilePath, NewPath

from .profiles import Profiles

Operation = TypeVar("Operation", bound="Operation")
DataframeOperation = TypeVar("DataframeOperation", bound="DataframeOperation")
FileOperation = TypeVar("FileOperation", bound="FileOperation")


class Operation(BaseModel):
    """Base class for other file/dataframe operations in pycytominer."""

    input_profiles: Profiles = Field(
        ..., title="Profiles", description="Profiles data model."
    )


class DataframeOperation(Operation):
    """Base class for dataframe operations in pycytominer."""

    input_df: DataFrame = Field(
        ...,
        title="Dataframe",
        description="A dataframe containing profiles data including metadata and features",
        examples=[
            pd.DataFrame(
                {
                    "Metadata_Plate": ["plate1", "plate1", "plate2"],
                    "Metadata_Well": ["A01", "A02", "B01"],
                    "Metadata_Treatment": ["control", "control", "control"],
                    "Feature_A": [1, 2, 3],
                    "Feature_B": [4, 5, 6],
                    "Feature_C": [7, 8, 9],
                }
            )
        ],
    )


class FileOperation(DataframeOperation):
    """Base class for file operations in pycytominer."""

    input_path: FilePath = Field(
        ..., title="Input path", description="Path to the input file"
    )
    output_path: NewPath = Field(
        ..., title="Output path", description="Path to the output file"
    )
