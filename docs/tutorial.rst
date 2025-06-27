Tutorials
=========

`This <https://github.com/cytomining/pipeline-examples#readme>`_ tutorial shows how to run a image-based profiling pipeline using Pycytominer. Using IPython notebooks, it walks through the following steps:

#. Downloading a dataset of single cell `CellProfiler <https://cellprofiler.org/>`_ profiles.
#. Processing the profiles using PyCytominer. This includes the following steps:
    #. Data initialization
    #. Single cell aggregation to create well-level profiles
    #. Addition of experiment metadata to the well-level profiles
    #. Profile normalization
    #. Feature selection
    #. Forming consensus signatures
#. Evaluating the profile quality using `cytominer-eval <https://github.com/cytomining/cytominer-eval>`_.