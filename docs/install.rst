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

Command-line usage
==================

Pycytominer provides a simple CLI for file-based workflows. These commands read
profiles from disk and write outputs to disk.

.. code-block:: bash

   # Aggregate profiles (parquet output)
   python -m pycytominer aggregate \
     --profiles path/to/profiles.csv.gz \
     --output_file path/to/profiles_aggregated.parquet \
     --output_type parquet \
     --strata Metadata_Plate,Metadata_Well \
     --features Cells_AreaShape_Area,Cytoplasm_AreaShape_Area

   # Annotate profiles with a platemap (compressed CSV output)
   python -m pycytominer annotate \
     --profiles path/to/profiles_aggregated.parquet \
     --platemap path/to/platemap.csv \
     --output_file path/to/profiles_augmented.csv.gz \
     --join_on Metadata_well_position,Metadata_Well

   # Normalize profiles (parquet output)
   python -m pycytominer normalize \
     --profiles path/to/profiles_augmented.csv.gz \
     --output_file path/to/profiles_normalized.parquet \
     --output_type parquet \
     --features Cells_AreaShape_Area,Cytoplasm_AreaShape_Area \
     --meta_features Metadata_Plate,Metadata_Well \
     --samples "Metadata_treatment == 'control'" \
     --method standardize

   # Feature selection (compressed CSV output)
   python -m pycytominer feature_select \
     --profiles path/to/profiles_normalized.parquet \
     --output_file path/to/profiles_feature_selected.csv.gz \
     --features Cells_AreaShape_Area,Cytoplasm_AreaShape_Area \
     --operation variance_threshold,correlation_threshold

   # Consensus profiling (parquet output)
   python -m pycytominer consensus \
     --profiles path/to/profiles_feature_selected.csv.gz \
     --output_file path/to/profiles_consensus.parquet \
     --output_type parquet \
     --replicate_columns Metadata_Plate,Metadata_Well \
     --features Cells_AreaShape_Area,Cytoplasm_AreaShape_Area \
     --operation median

   # pull a commit-based version of pycytominer (b1bb292) and run an interactive bash session within the container
   docker run -it --platform=linux/amd64 cytomining/pycytominer:pycytominer-1.1.0.post16.dev0_b1bb292 bash

   # pull a scheduled update of pycytominer, map the present working directory to /opt within the container, and run a python script.
   docker run -v $PWD:/opt --platform=linux/amd64 cytomining/pycytominer:pycytominer-1.1.0.post16.dev0_b1bb292_240417 python /opt/script.py
