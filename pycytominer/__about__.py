from importlib.metadata import PackageNotFoundError, version

__project__ = "pycytominer"

try:
    __version__ = version(__project__)
except PackageNotFoundError:
    __version__ = "0.0.0"

__version_tuple__ = tuple(__version__.replace("+", ".").replace("-", ".").split("."))
__license__ = "BSD 3-Clause License"
__author__ = "Pycytominer Contributors"
