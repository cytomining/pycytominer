"""
Transform observation variables by specified groups
"""

import os
import numpy as np
import pandas as pd
from scipy.linalg import eigh
from scipy.stats import median_absolute_deviation
from sklearn.base import BaseEstimator, TransformerMixin


class Whiten(BaseEstimator, TransformerMixin):
    """
    Class to whiten data in the base sklearn transform API
    Note, this implementation is modified/inspired from the following sources:
    1) A custom function written by Juan C. Caicedo
    2) A custom ZCA function at https://github.com/mwv/zca
    3) Notes from Niranj Chandrasekaran (https://github.com/cytomining/pycytominer/issues/90)
    4) The R package "whitening" written by Strimmer et al (http://strimmerlab.org/software/whitening/)
    5) Kessy et al. 2016 "Optimal Whitening and Decorrelation"
    """

    def __init__(self, epsilon=1e-6, center=True, method="ZCA"):
        """
        Arguments:
        epsilon - fudge factor parameter
        center - option to center input X matrix
        method - a string indicating which class of whitening to perform
        """
        avail_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]

        self.epsilon = epsilon
        self.center = center

        assert (
            method in avail_methods
        ), f"Error {method} not supported. Select one of {avail_methods}"
        self.method = method

    def fit(self, X, y=None):
        """
        Identify the whitening transform given self.X

        Argument:
        X - dataframe to fit whitening transform
        """
        # Get the mean of the features (columns) and center if specified
        self.mu = X.mean()
        if self.center:
            X = X - self.mu

        # Get the covariance matrix
        C = (1 / X.shape[0]) * np.dot(X.transpose(), X)

        if self.method in ["PCA", "ZCA"]:
            # Get the eigenvalues and eigenvectors of the covariance matrix
            s, U = eigh(C)

            # Fix sign ambiguity of eigenvectors
            U = pd.DataFrame(U * np.sign(np.diag(U)))

            # Process the eigenvalues into a diagonal matrix and fix rounding errors
            D = np.diag(1.0 / np.sqrt(s.clip(self.epsilon)))

            # Calculate the whitening matrix
            self.W = np.dot(D, U.transpose())

            # If ZCA, perform additional rotation
            if self.method == "ZCA":
                self.W = np.dot(U, self.W)

        if self.method in ["PCA-cor", "ZCA-cor"]:
            # Get the correlation matrix
            R = np.corrcoef(X.transpose())

            # Get the eigenvalues and eigenvectors of the correlation matrix
            try:
                t, G = eigh(R)
            except ValueError:
                raise ValueError(
                    "Divide by zero error, make sure low variance columns are removed"
                )

            # Fix sign ambiguity of eigenvectors
            G = pd.DataFrame(G * np.sign(np.diag(G)))

            # Process the eigenvalues into a diagonal matrix and fix rounding errors
            D = np.diag(1.0 / np.sqrt(t.clip(self.epsilon)))

            # process the covariance diagonal matrix and fix rounding errors
            v = np.diag(1.0 / np.sqrt(np.diag(C).clip(self.epsilon)))

            # Calculate the whitening matrix
            self.W = np.dot(np.dot(D, G.transpose()), v)

            # If ZCA-cor, perform additional rotation
            if self.method == "ZCA-cor":
                self.W = np.dot(G, self.W)

        return self

    def transform(self, X, y=None):
        """
        Perform the whitening transform
        """
        return np.dot(X - self.mu, self.W.transpose())


class RobustMAD(BaseEstimator, TransformerMixin):
    """
    Class to perform a "Robust" normalization with respect to median and mad

        scaled = (x - median) / mad
    """

    def __init__(self, epsilon=1e-18):
        self.epsilon = epsilon

    def fit(self, X, y=None):
        """
        Compute the median and mad to be used for later scaling.

        Argument:
        X - pandas dataframe to fit RobustMAD transform
        """
        # Get the mean of the features (columns) and center if specified
        self.median = X.median()
        self.mad = pd.Series(
            median_absolute_deviation(X, nan_policy="omit"), index=self.median.index
        )
        return self

    def transform(self, X, copy=None):
        """
        Apply the RobustMAD calculation

        Argument:
        X - pandas dataframe to apply RobustMAD transform
        """
        return (X - self.median) / (self.mad + self.epsilon)
