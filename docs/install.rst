Install
=======

To install Pycytominer, use pip:

.. code-block:: bash

   pip install pycytominer

You can also install Pycytominer with conda:

.. code-block:: bash

   conda install -c conda-forge pycytominer

Docker Hub container images of Pycytominer are made available through Docker Hub.
These images follow a tagging scheme that extends our release sematic versioning which may be found within our [CONTRIBUTING.md Docker Hub Image Releases](https://github.com/cytomining/pycytominer/blob/main/CONTRIBUTING.md#docker-hub-image-releases) documentation.

.. code-block:: bash
   # pull the latest pycytominer image and run a module
   docker run --platform=linux/amd64 cytomining/pycytominer:latest python -m pycytominer.<modules go here>
