"""Transform observation variables by specified groups.

References
----------
.. [1] Kessy et al. 2016 "Optimal Whitening and Decorrelation" arXiv: https://arxiv.org/abs/1512.00809
"""

import numpy as np
import pandas as pd
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

    def __init__(self, epsilon=1e-6, center=True, method="ZCA", return_numpy=False):
        """
        Parameters
        ----------
        epsilon : float, default 1e-6
            fudge factor parameter
        center : bool, default True
            option to center the input X matrix
        method : str, default "ZCA"
            a string indicating which class of sphering to perform
        return_numpy: bool, default False
            option to return ndarray, instead of dataframe
        """
        avail_methods = ["PCA", "ZCA", "PCA-cor", "ZCA-cor"]

        self.epsilon = epsilon
        self.center = center
        self.return_numpy = return_numpy

        if method not in avail_methods:
            raise ValueError(
                f"Error {method} not supported. Select one of {avail_methods}"
            )
        self.method = method

        # PCA-cor and ZCA-cor require center=True because we assumed we are
        # only ever interested in computing centered Pearson correlation
        # https://stackoverflow.com/questions/23891391/uncentered-pearson-correlation

        if self.method in ["PCA-cor", "ZCA-cor"] and not self.center:
            raise ValueError("PCA-cor and ZCA-cor require center=True")

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
        # Get Numpy representation of the DataFrame
        X = X.values

        if self.method in ["PCA-cor", "ZCA-cor"]:
            # The projection matrix for PCA-cor and ZCA-cor is the same as the
            # projection matrix for PCA and ZCA, respectively, on the standardized
            # data. So, we first standardize the data, then compute the projection

            self.standard_scaler = StandardScaler().fit(X)
            variances = self.standard_scaler.var_
            if np.any(variances == 0):
                raise ValueError(
                    "Divide by zero error, make sure low variance columns are removed"
                )

            X = self.standard_scaler.transform(X)
        else:
            if self.center:
                self.mean_centerer = StandardScaler(with_mean=True, with_std=False).fit(
                    X
                )
                X = self.mean_centerer.transform(X)

        # Get the number of observations and variables
        n, d = X.shape

        # Checking the rank of matrix X considering the number of samples (n) and features (d).
        r = np.linalg.matrix_rank(X)

        # Case 1: More features than samples (n < d).
        # If centered (mean of each feature subtracted), one dimension becomes dependent, reducing rank to n - 1.
        # If not centered, the max rank is limited by n, as there can't be more independent vectors than samples.
        # Case 2: More samples than features or equal (n >= d).
        # Here, the max rank is limited by d (number of features), assuming each provides unique information.

        if not ((r == d) | (self.center & (r == n - 1)) | (not self.center & (r == n))):
            raise ValueError(
                "The data matrix X is not full rank: n = {n}, d = {d}, r = {r}."
                "Perfect linear dependencies are unusual in data matrices so something seems amiss."
                "Check for linear dependencies in the data and remove them."
            )

        # Get the eigenvalues and eigenvectors of the covariance matrix
        # by computing the SVD on the data matrix
        # https://stats.stackexchange.com/q/134282/8494
        _, Sigma, Vt = np.linalg.svd(X, full_matrices=True)

        # if n <= d then Sigma has shape (n,) so it will need to be expanded to
        # d filled with the value r'th element of Sigma
        if n <= d:
            # Do some error checking
            if Sigma.shape[0] != n:
                error_detail = f"When n <= d, Sigma should have shape (n,) i.e. ({n}, ) but it is {Sigma.shape}"
                context = (
                    "the call to `np.linalg.svd` in `pycytominer.transform.Spherize`"
                )
                raise ValueError(f"{error_detail}. This is likely a bug in {context}.")

            if (r != n - 1) & (r != n):
                error_detail = f"When n <= d, the rank should be n - 1 or n i.e. {n - 1} or {n} but it is {r}"
                context = (
                    "the call to `np.linalg.svd` in `pycytominer.transform.Spherize`"
                )
                raise ValueError(f"{error_detail}. This is likely a bug in {context}.")

            Sigma = np.concatenate((Sigma[0:r], np.repeat(Sigma[r - 1], d - r)))

        Sigma = Sigma + self.epsilon

        # From https://arxiv.org/abs/1512.00809, the PCA whitening matrix is
        #
        # W = Lambda^(-1/2) E^T
        #
        # where
        # - E is the eigenvector matrix
        # - Lambda is the (diagonal) eigenvalue matrix
        # of cov(X), where X is the data matrix
        # (i.e., cov(X) = E Lambda E^T)
        #
        # However, W can also be computed using the Singular Value Decomposition
        # (SVD) of X. Assuming X is mean-centered (zero mean for each feature),
        # the SVD of X is given by:
        #
        # X = U S V^T
        #
        # The covariance matrix of X can be expressed using its SVD:
        #
        # cov(X) = (X^T X) / (n - 1)
        #        = (V S^2 V^T) / (n - 1)
        #
        # By comparing cov(X) = E * Lambda * E^T with the SVD form, we identify:
        #
        #   E = V (eigenvectors)
        #   Lambda = S^2 / (n - 1) (eigenvalues)
        #
        # Thus, Lambda^(-1/2) can be expressed as:
        #
        #   Lambda^(-1/2) = inv(S) * sqrt(n - 1)
        #
        # Therefore, the PCA Whitening matrix W becomes:
        #
        # W = Lambda^(-1/2) * E^T
        #   = (inv(S) * sqrt(n - 1)) * V^T
        #   = (inv(S) * V^T) * sqrt(n - 1)
        #
        # In NumPy, this is implemented as:
        #
        #   W = (np.linalg.inv(S) @ Vt) * np.sqrt(n - 1)
        #
        # But computing `np.linalg.inv(S)` is memory-intensive.
        #
        # A more memory-efficient alternative is:
        #
        #   W = (Vt / Sigma[:, np.newaxis]) * np.sqrt(n - 1)
        #
        # where Sigma contains the diagonal elements of S (singular values).

        self.W = (Vt / Sigma[:, np.newaxis]).transpose() * np.sqrt(n - 1)

        # If ZCA, perform additional rotation
        if self.method in ["ZCA", "ZCA-cor"]:
            # Note: There was previously a requirement r==d otherwise the
            # ZCA transform would not be well defined. However, it later appeared
            # that this requirement was not necessary.

            self.W = self.W @ Vt

        # number of columns of self.W should be equal to that of X
        if not (self.W.shape[1] == X.shape[1]):
            raise ValueError(
                f"Error: W has {self.W.shape[1]} columns, X has {X.shape[1]} columns"
            )

        if self.W.shape[1] != X.shape[1]:
            error_detail = (
                f"The number of columns of W should be equal to that of X."
                f"However, W has {self.W.shape[1]} columns, X has {X.shape[1]} columns"
            )
            context = "the call to `np.linalg.svd` in `pycytominer.transform.Spherize`"
            raise ValueError(f"{error_detail}. This is likely a bug in {context}.")

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

        # Get Numpy representation of the DataFrame
        X = X.values

        if self.method in ["PCA-cor", "ZCA-cor"]:
            X = self.standard_scaler.transform(X)
        else:
            if self.center:
                X = self.mean_centerer.transform(X)

        if self.method in ["PCA", "PCA-cor"]:
            columns = ["PC" + str(i) for i in range(1, X.shape[1] + 1)]

        self.columns = columns

        XW = X @ self.W

        if self.return_numpy:
            return XW
        else:
            return pd.DataFrame(XW, columns=columns)


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
