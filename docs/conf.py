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

import dunamai

sys.path.insert(0, os.path.abspath(".."))

# Ignore rules regarding import order which is required for sphinx build process
import pycytominer  # noqa: E402, RUF100

# -- Project information -----------------------------------------------------

project = "Pycytominer"
author = pycytominer.__about__.__author__
project_copyright = f"Copyright 2019 - {date.today().year} {author}"

# Get the version from Git tags via dunamai
auto_version = dunamai.Version.from_git()
version = auto_version.serialize()
release = version

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "nbsphinx",
    "myst_parser",
    "sphinxcontrib.mermaid",
]

# Render notebooks without executing them during docs builds — outputs are
# pre-computed locally and committed, so no kernel is needed at build time.
nbsphinx_execute = "never"

# Disable require.js: nbsphinx loads it for interactive widgets, but our
# tutorials have none.  Leaving it enabled causes a conflict where mermaid.js
# detects AMD, registers as a module, and never sets window.mermaid, so
# mermaid.initialize() throws a ReferenceError and diagrams fail to render.
nbsphinx_requirejs_path = ""

# Use a mermaid version with solid Unicode + flowchart syntax support.
# v10.2.0 (the sphinxcontrib-mermaid default) misparses non-ASCII characters
# such as middle-dot (U+00B7) and right-arrow (U+2192) in node labels.
mermaid_version = "10.6.1"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "build", "**tests**"]


# -- Options for HTML output -------------------------------------------------
# The theme to use for HTML and HTML Help pages.
# Furo theme options specified here: https://pradyunsg.me/furo/
html_theme = "furo"

# colors used for styling the HTML output in light or dark mode
pycytominer_hex_light = "#88239A"
pycytominer_hex_dark = "#CF72DF"

# Furo theme option colors specified here:
# https://github.com/pradyunsg/furo/blob/main/src/furo/assets/styles/variables/_colors.scss
html_theme_options = {
    "light_css_variables": {
        "color-brand-primary": pycytominer_hex_light,
        "color-brand-content": pycytominer_hex_light,
        "color-api-pre-name": pycytominer_hex_light,
        "color-api-name": pycytominer_hex_light,
    },
    "dark_css_variables": {
        "color-brand-primary": pycytominer_hex_dark,
        "color-brand-content": pycytominer_hex_dark,
        "color-api-pre-name": pycytominer_hex_dark,
        "color-api-name": pycytominer_hex_dark,
    },
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_logo = "../logo/just-icon.svg"
