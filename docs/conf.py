# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from datetime import date

file_loc = os.path.split(__file__)[0]
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(file_loc), ".")))

import pycytominer
import groundwork_sphinx_theme

# -- Project information -----------------------------------------------------
project = pycytominer.__about__.__project__
author = pycytominer.__about__.__author__
copyright = "Copyright 2019 - {date} {author}".format(
    date=date.today().year, author=author
)

# The short X.Y version
version = pycytominer.__about__.__version__
# The full version, including alpha/beta/rc tags
release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "autoapi.extension",
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.coverage",
]
autoapi_dirs = ["../pycytominer"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "groundwork"

html_theme_options = {
    "sidebar_width": "240px",
    "stickysidebar": True,
    "stickysidebarscrollable": True,
    "contribute": True,
    "github_fork": "useblocks/groundwork",
    "github_user": "useblocks",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
