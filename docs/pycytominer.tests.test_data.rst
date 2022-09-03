pycytominer.tests.test_data
===========================

Documenting test data found within this project.

cytominer-database Example Data
-------------------------------

The following `cytominer-database <https://github.com/cytomining/cytominer-database>`_ generated file is provided for use in example or test scenarios.
It has been shrunk from the original size for demonstrational or efficiency purposes.

* `test_SQ00014613.sqlite <https://github.com/cytomining/pycytominer/tree/master/pycytominer/data/cytominer-database_example_data/test_SQ00014613.sqlite>`_

  * Original: `SQ00014613.sqlite <https://nih.figshare.com/articles/dataset/Cell_Health_-_Cell_Painting_Single_Cell_Profiles/9995672?file=18506036>`_
  
  * Code for Generating test_SQ00014613.sqlite: `Source <https://github.com/cytomining/pycytominer/tree/master/pycytominer/data/cytominer-database_example_data/shrink_SQ00014613.sqlite.py>`_
  
  * Table details:

    * Image: 2 rows with distinct (unique) TableNumber and ImageNumber
    * Cells: 2 rows with ObjectNumber 1 for each distinct (unique) TableNumber and ImageNumber.
    * Cytoplasm: 2 rows with ObjectNumber 1 for each distinct (unique) TableNumber and ImageNumber.
    * Nuclei: 2 rows with ObjectNumber 1 for each distinct (unique) TableNumber and ImageNumber.

* `test_SQ00014613.csv.gz <https://github.com/cytomining/pycytominer/tree/master/pycytominer/data/cytominer-database_example_data/test_SQ00014613.csv.gz>`_

  * Original: Based on SingleCells merge_single_cells output compressed with gzip from `test_SQ00014613.sqlite <https://github.com/cytomining/pycytominer/tree/master/pycytominer/data/cytominer-database_example_data/test_SQ00014613.sqlite>`_

* `test_SQ00014613.parquet <https://github.com/cytomining/pycytominer/tree/master/pycytominer/data/cytominer-database_example_data/test_SQ00014613.parquet>`_

  * Original: Based on SingleCells merge_single_cells output with snappy compression from `test_SQ00014613.sqlite <https://github.com/cytomining/pycytominer/tree/master/pycytominer/data/cytominer-database_example_data/test_SQ00014613.sqlite>`_
