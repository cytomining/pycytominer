import os
from typing import List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import shap
from scipy import stats
from sklearn import metrics, model_selection
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.decomposition import PCA
from sklearn.ensemble import BaggingClassifier, IsolationForest, RandomForestClassifier
from sklearn.feature_selection import RFE, RFECV, SelectFromModel
from sklearn.metrics import classification_report
from sklearn.preprocessing import LabelEncoder, PowerTransformer, StandardScaler, normalize, power_transform, robust_scale, scale

from .. import models


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


def get_shap_values(
    df: pd.DataFrame,
    model: BaseEstimator,
    variable: str = "Cell",
    groupby: Optional[str] = None,
    augment: Optional[str] = None,
    shap_samples: int = 100,
    samples: Optional[int] = None,
    *args,
    **kwargs,
) -> shap.Explainer:
    """
    Calculate SHAP values for a given model and data.

    Parameters:
    ... [describe each parameter]

    Returns:
    - shap.Explainer: SHAP values.
    """
    X, y = df, list(df.index.get_level_values(variable))
    X100 = shap.utils.sample(np.array(X), 100)

    X_train, X_test, y_train, y_test = (df.apply(pd.to_numeric).pipe(models.train_test_split,
        variable, groupby=groupby, augment=augment
    ))

    y_train = LabelEncoder().fit(y).transform(y_train)
    y_test = LabelEncoder().fit(y).transform(y_test)

    model.fit(X_train.values, y_train)
    model.score(X_test.values, y_test)

    explainer = shap.Explainer(model.predict, X100, *args, **kwargs)
    shap_values = explainer(X)

    return shap_values


def leaf_model(
    df: pd.DataFrame,
    model_class: BaseEstimator = RandomForestClassifier,
    variable: str = "Cell",
    groupby: Optional[str] = None,
    augment: Optional[str] = None,
    kfolds: int = 1,
) -> pd.DataFrame:
    """
    Train a model and return feature importances.

    Parameters:
    ... [describe each parameter]

    Returns:
    - pd.DataFrame: DataFrame of feature importances.
    """
    importance_list = []

    for fold in range(1, kfolds + 1):
        model = model_class()
        X_train, X_test, y_train, y_test = models.train_test_split(df, variable, groupby=groupby, augment=augment, seed=fold)

        model.fit(X_train.values, y_train)

        print(classification_report(y_test, model.predict(X_test)))
        print(metrics.cohen_kappa_score(y_test, model.predict(X_test)))

        importance = (
            pd.DataFrame(
                model.feature_importances_,
                index=pd.Series(X_train.columns, name="Feature"),
                columns=["Importance"],
            )
            .assign(Fold=fold)
            .sort_values(ascending=False, by="Importance")
        )

        importance["Cumulative Importance"] = importance.cumsum()["Importance"]
        importance_list.append(importance)

    return pd.concat(importance_list)
