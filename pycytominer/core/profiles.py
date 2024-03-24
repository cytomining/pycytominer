from typing import Optional

from pandera.typing import DataFrame, Index
from pandera import DataFrameSchema
from pydantic import BaseModel, Field


class Profiles(BaseModel):
    """Profiles data model.

    This model is used to store profiles data including metadata and features.

    Attributes:
    - df: DataFrame: Profiles dataframe
    - metadata_columns: Optional[Index]: Columns that contain metadata information
    - features_columns: Optional[Index]: Columns that contain feature information
    - strata_columns: Optional[Index]: Columns that contain strata information
    """

    df: DataFrame = Field(
        ...,
        title="Profiles dataframe",
        description="A dataframe containing profiles data including metadata and features",
        examples=[],
    )
    columns: Optional[Index] = Field(
        title="Columns",
        description="Full list of columns in the profiles dataframe",
        examples=[
            [
                "Metadata_Plate",
                "Metadata_Well",
                "Metadata_Treatment",
                "Feature_A",
                "Feature_B",
                "Feature_C",
            ]
        ],
    )
    strata_columns: Optional[Index] = Field(
        title="Strata columns",
        description="Columns that contain strata information (must be a subset of metadata columns)",
        examples=[
            ["Metadata_Plate", "Metadata_Well"],
            Index(["PlateBarcode", "WellPosition"]),
        ],
    )
    metadata_columns: Optional[Index] = Field(
        title="Metadata columns",
        description="Columns that contain metadata information",
        examples=[["Metadata_Plate", "Metadata_Well", "Metadata_Treatment"]],
    )
    features_columns: Optional[Index] = Field(
        title="Feature columns",
        description="Columns that contain feature information",
    )
    schema: Optional[DataFrameSchema] = Field(
        title="Profiles schema",
        description="A Pandera schema for the profiles dataframe",
    )


class SingleCellProfiles(Profiles):
    """Single cell profiles data model."""


class AggregatedProfiles(Profiles):
    """Aggregate profiles data model."""


class AnnotatedProfiles(Profiles):
    """Annotated profiles data model."""


class NormalizedProfiles(Profiles):
    """Normalized profiles data model."""


class ConsensusProfiles(Profiles):
    """Consensus profiles data model."""
