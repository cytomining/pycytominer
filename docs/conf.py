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
import pathlib
from datetime import date

sys.path.insert(0, os.path.abspath(".."))

import pycytominer

# -- Project information -----------------------------------------------------

project = pycytominer.__about__.__project__
author = pycytominer.__about__.__author__
copyright = "Copyright 2019 - {date} {author}".format(
    date=date.today().year, author=author
)

# The full version, including alpha/beta/rc tags
version = pycytominer.__about__.__version__
release = version

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon", "sphinx_copybutton", "m2r2"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["**tests**"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
pycytominer_hex = "#88239A"
# Furo theme options specified here: https://pradyunsg.me/furo/
html_theme = "furo"
# Furo theme option colors specified here:
# https://github.com/pradyunsg/furo/blob/main/src/furo/assets/styles/variables/_colors.scss
html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": pycytominer_hex,
        "color-brand-content": pycytominer_hex,
        "color-api-pre-name": pycytominer_hex,
        "color-api-name": pycytominer_hex,
    },
    "dark_css_variables": {
        "color-brand-primary": pycytominer_hex,
        "color-brand-content": pycytominer_hex,
        "color-api-pre-name": pycytominer_hex,
        "color-api-name": pycytominer_hex,
    },
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]
html_logo = "../logo/just-icon.svg"
