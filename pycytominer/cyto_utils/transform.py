"""
Transform observation variables by specified groups
"""

import os
import numpy as np
import pandas as pd
from scipy.linalg import eigh
from sklearn.base import BaseEstimator, TransformerMixin


class Whiten(BaseEstimator, TransformerMixin):
    """
    Class to whiten data in the base sklearn transform API
    Note, this implementation is modified from a function written by Juan C. Caicedo
    """

    def __init__(self, epsilon=1e-18, center=True):
        """
        Arguments:
        epsilon - fudge factor parameter
        center - option to center input X matrix
        """
        self.epsilon = epsilon
        self.center = center

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
        C = np.dot(X.transpose(), X) / X.shape[0]

        # Get the eigenvalues and eigenvectors of the covariance matrix
        s, V = eigh(C)
        D = np.diag(1.0 / np.sqrt(s + self.epsilon))

        # Calculate the whitening matrix
        self.W = np.dot(np.dot(V, D), V.transpose())
        return self

    def transform(self, X, y=None):
        """
        Whiten an input matrix a given population dataframe
        """
        return np.dot(X - self.mu, self.W)
