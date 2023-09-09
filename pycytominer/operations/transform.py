"""Transform observation variables by specified groups.

References
----------
.. [1] Kessy et al. 2016 "Optimal Whitening and Decorrelation" arXiv: https://arxiv.org/abs/1512.00809
"""

import os
import numpy as np
import pandas as pd
from scipy.linalg import eigh
from scipy.stats import median_abs_deviation
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import StandardScaler


class Spherize(BaseEstimator, TransformerMixin):
    """Class to apply a sphering transform (aka whitening) data in the base sklearn
    transform API. Note, this implementation is modified/inspired from the following
    sources:
    1) A custom function written by Juan C. Caicedo
    2) A custom ZCA function at https://github.com/mwv/zca
    3) Notes from Niranj Chandrasekaran (https://github.com/cytomining/pycytominer/issues/90)
    4) The R package "whitening" written by Strimmer et al (http://strimmerlab.org/software/whitening/)
    5) Kessy et al. 2016 "Optimal Whitening and Decorrelation" [1]_

    Attributes
    ----------
    epsilon : float
        fudge factor parameter
    center : bool
        option to center the input X matrix
    method : str
        a string indicating which class of sphering to perform
    """

    def __init__(self, epsilon=1e-6, center=True, method="ZCA"):
        """
        Parameters
        ----------
        epsilon : float, default 1e-6
            fudge factor parameter
        center : bool, default True
            option to center the input X matrix
        method : str, default "ZCA"
            a string indicating which class of sphering to perform
        """
        avail_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]

        self.epsilon = epsilon
        self.center = center

        assert (
            method in avail_methods
        ), f"Error {method} not supported. Select one of {avail_methods}"
        self.method = method

        assert (
            self.method not in ["PCA-cor", "ZCA-cor"] or self.center
        ), "PCA-cor and ZCA-cor require center=True"

    def fit(self, X, y=None):
        """Identify the sphering transform given self.X

        Parameters
        ----------
        X : pandas.core.frame.DataFrame
            dataframe to fit sphering transform

        Returns
        -------
        self
            With computed weights attribute
        """
        X = X.values

        if self.method in ["PCA-cor", "ZCA-cor"]:
            self.standard_scaler = StandardScaler().fit(X)
            X = self.standard_scaler.transform(X)
        else:
            if self.center:
                self.mean_centerer = StandardScaler(with_mean=True, with_std=False).fit(
                    X
                )
                X = self.mean_centerer.transform(X)

        # Get the number of observations
        N = X.shape[0]

        # Get the eigenvalues and eigenvectors of the covariance matrix using SVD
        _, Sigma, Vt = np.linalg.svd(X, full_matrices=False)

        Sigma = Sigma + self.epsilon

        self.W = (Vt / Sigma[:, np.newaxis]).transpose() * np.sqrt(N - 1)

        # If ZCA, perform additional rotation
        if self.method in ["ZCA", "ZCA-cor"]:
            self.W = self.W @ Vt

        return self

    def transform(self, X, y=None):
        """Perform the sphering transform

        Parameters
        ----------
        X : pd.core.frame.DataFrame
            Profile dataframe to be transformed using the precompiled weights
        y : None
            Has no effect; only used for consistency in sklearn transform API

        Returns
        -------
        pandas.core.frame.DataFrame
            Spherized dataframe
        """

        columns = X.columns

        X = X.values

        if self.method in ["PCA-cor", "ZCA-cor"]:
            X = self.standard_scaler.transform(X)
        else:
            if self.center:
                X = self.mean_centerer.transform(X)

        if self.method in ["PCA", "ZCA"]:
            columns = ["PC" + str(i) for i in range(1, X.shape[1] + 1)]

        return pd.DataFrame(X @ self.W, columns=columns)


class RobustMAD(BaseEstimator, TransformerMixin):
    """Class to perform a "Robust" normalization with respect to median and mad

        scaled = (x - median) / mad

    Attributes
    ----------
    epsilon : float
        fudge factor parameter
    """

    def __init__(self, epsilon=1e-18):
        self.epsilon = epsilon

    def fit(self, X, y=None):
        """Compute the median and mad to be used for later scaling.

        Parameters
        ----------
        X : pandas.core.frame.DataFrame
            dataframe to fit RobustMAD transform

        Returns
        -------
        self
            With computed median and mad attributes
        """
        # Get the mean of the features (columns) and center if specified
        self.median = X.median()
        # The scale param is required to preserve previous behavior. More info at:
        # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.median_absolute_deviation.html#scipy.stats.median_absolute_deviation
        self.mad = pd.Series(
            median_abs_deviation(X, nan_policy="omit", scale=1 / 1.4826),
            index=self.median.index,
        )
        return self

    def transform(self, X, copy=None):
        """Apply the RobustMAD calculation

        Parameters
        ----------
        X : pandas.core.frame.DataFrame
            dataframe to fit RobustMAD transform

        Returns
        -------
        pandas.core.frame.DataFrame
            RobustMAD transformed dataframe
        """
        return (X - self.median) / (self.mad + self.epsilon)
