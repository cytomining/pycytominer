import numpy as np
import pandas as pd
from sklearn import metrics, model_selection
from sklearn.ensemble import RandomForestClassifier
from typing import List, Optional, Tuple, Union
import shap

def get_shap_df(shaps: shap.Explainer) -> pd.DataFrame:
    """
    Convert SHAP values to a DataFrame.

    Parameters:
    - shaps (shap.Explainer): SHAP values.

    Returns:
    - pd.DataFrame: DataFrame of SHAP values.
    """
    return (
        pd.DataFrame(shaps.values, columns=shaps.feature_names)
        .rename_axis("Sample")
        .reset_index()
        .melt(id_vars="Sample", var_name="Feature", value_name="Shap Value")
    )

# ... [rest of the functions with added docstrings and type hinting]

def train_test_split(
    df: pd.DataFrame,
    variable: str,
    frac: float = 0.8,
    augment: Optional[callable] = None,
    groupby: Optional[str] = None,
    seed: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    Split the data into training and test sets.

    Parameters:
    ... [describe each parameter]

    Returns:
    - Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]: Training and test data and labels.
    """
    if df.empty:
        return None

    X = df
    y = df.index.to_frame()[[variable]].astype(str)

    if groupby:
        return groupby_train_split(df, variable, groupby, frac=frac, seed=seed)

    if augment:
        X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, stratify=y, random_state=seed)
        X_train, y_train = augment(X_train, y_train)
        return X_train, X_test, y_train, y_test

    return model_selection.train_test_split(X, y, stratify=y, random_state=seed)
