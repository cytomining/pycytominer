"""
Functionality in pycytominer is modified from https://github.com/cytomining/cytominer

The original authors of the cytominer package are:
Tim Becker, Allen Goodman, Claire McQuin, Mohammad Rohban, and Shantanu Singh
"""

from setuptools import setup
from setuptools import find_packages

setup(
    name="pycytominer",
    description="package for processing and mining morphological profiles",
    long_description="package to normalize, manipulate, and process morphological profiling data. The python implementation of cytominer.",
    maintainer="Gregory Way",
    maintainer_email="gregory.way@gmail.com",
    url="https://github.com/cytomining/pycytominer",
    packages=find_packages(),
    license="BSD 3-Clause License",
    install_requires=["numpy", "pandas", "scikit-learn"],
    python_requires=">=3.4",
)
