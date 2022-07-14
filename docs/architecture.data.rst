Data Architecture
=================

Pycytominer data architecture documentation.

Distinct Upstream Data Sources
------------------------------

Pycytominer has distinct data flow contingent on upstream data source. Various projects are used to generate 
different kinds of data which are handled differently within Pycytominer.

* `CellProfiler <https://github.com/CellProfiler/CellProfiler>`_ Generates `CSV <https://en.wikipedia.org/wiki/Comma-separated_values>`_
  data used by Pycytominer.
* `Cytominer-Database <https://github.com/cytomining/cytominer-database>`_ Generates `SQLite <https://www.sqlite.org/>`_
  databases (which includes table data based on CellProfiler CSV's mentioned above) used by Pycytominer.
* `DeepProfiler <https://github.com/cytomining/DeepProfiler>`_ Generates 
  `NPZ <https://numpy.org/doc/stable/reference/routines.io.html?highlight=npz%20format#numpy-binary-files-npy-npz>`_
  data used by Pycytominer.

SQLite Data
-----------

Pycytominer in some areas consumes SQLite data sources. This data source is currently considered
somewhat deprecated for Pycytominer work.

**SQLite Data Structure**

.. mermaid::

   erDiagram
      Image ||--o{ Cytoplasm : contains
      Image ||--o{ Cells : contains
      Image ||--o{ Nuclei : contains
      
Related SQLite databases have a structure loosely based around the above diagram. There
are generally four tables: Image, Cytoplasm, Cells, and Nuclei. Each Image may contain
zero to many Cells, Nuclei, or Cytoplasm data rows.

**SQLite Compartments**

The tables Cytoplasm, Cells, and Nuclei are generally referenced as "compartments". While 
these are often included within related SQLite datasets, other compartments may be involved
as well.

**SQLite Common Fields**

Each of the above tables include ``TableNumber`` and ``ImageNumber`` fields which are 
cross-related to data in other tables. ``ObjectNumber`` is sometimes also but not guaranteed
to be related to data across tables.


**SQLite Data Production**

.. mermaid::

   flowchart LR
      subgraph Data
         direction LR
         cellprofiler_data[(CSV Files)] -.-> cytominerdatabase_data[(SQLite File)]
         cytominerdatabase_data[(SQLite File)]
      end
      subgraph Projects
         direction LR
         CellProfiler
         Cytominer-Database 
         Pycytominer
      end
      CellProfiler --> cellprofiler_data
      cellprofiler_data --> Cytominer-Database
      Cytominer-Database --> cytominerdatabase_data
      cytominerdatabase_data --> Pycytominer

Related SQLite data is originally created from `CellProfiler <https://github.com/CellProfiler/CellProfiler>`_
CSV data exports. This CSV data is then converted to SQLite by 
`Cytominer-Database <https://github.com/cytomining/cytominer-database>`_.

**Cytominer-Database Data Transformations**

* Cytominer-Database adds a field to all CSV tables from CellProfiler labeled ``TableNumber``. This field
  is added to address dataset uniqueness as CellProfiler sometimes resets ``ImageNumber``.

Parquet Data
------------

Pycytominer provides capabilities to convert into and utilize `Apache Parquet <https://parquet.apache.org/>`_
data.

**Parquet from Cytominer-Database SQLite Data Sources**

.. mermaid::

   flowchart LR
      subgraph Data
         direction LR
         cellprofiler_data[(CSV Files)] -.-> cytominerdatabase_data[(SQLite File)]
         cytominerdatabase_data[(SQLite File)] -.-> pycytominer_data[(Parquet File)]
      end
      subgraph Projects
         direction LR
         CellProfiler
         Cytominer-Database 
         subgraph Pycytominer
               direction LR
               Pycytominer_conversion[Parquet Conversion]
               Pycytominer_work[Parquet-based Work]
         end
      end
      CellProfiler --> cellprofiler_data
      cellprofiler_data --> Cytominer-Database
      Cytominer-Database --> cytominerdatabase_data
      cytominerdatabase_data --> Pycytominer_conversion
      Pycytominer_conversion --> pycytominer_data
      pycytominer_data --> Pycytominer_work

Pycytominer includes the capability to convert related `Cytominer-Database <https://github.com/cytomining/cytominer-database>`_
SQLite-based data into parquet. The resulting format includes SQLite table data in a single file, using joinable keys 
``TableNumber`` and ``ImageNumber`` and none-type values to demonstrate data relationships (or lack thereof). 

Conversion work may be performed using the following module: :ref:`sqliteconvert`

An Example of the resulting parquet data format for Pycytominer may be found below:


+--------------+--------------+-------------------------+---------------------+-----------------------+------------------------+----------------------------+------------------------+--------------------------+
| TableNumber  | ImageNumber  | Cytoplasm_ObjectNumber  | Cells_ObjectNumber  | Nuclei_ObjectNumber  | Image_Fields...(many)  | Cytoplasm_Fields...(many)  | Cells_Fields...(many)  | Nuclei_Fields...(many)  |
+--------------+--------------+-------------------------+---------------------+-----------------------+------------------------+----------------------------+------------------------+--------------------------+
| 123abc       | 1            | Null                    | Null                | Null                  | Image Data...          | Null                       | Null                   | Null                     |
+--------------+--------------+-------------------------+---------------------+-----------------------+------------------------+----------------------------+------------------------+--------------------------+
| 123abc       | 1            | 1                       | Null                | Null                  | Null                   | Cytoplasm Data...          | Null                   | Null                     |
+--------------+--------------+-------------------------+---------------------+-----------------------+------------------------+----------------------------+------------------------+--------------------------+
| 123abc       | 1            | Null                    | 1                   | Null                  | Null                   | Null                       | Cells Data...          | Null                     |
+--------------+--------------+-------------------------+---------------------+-----------------------+------------------------+----------------------------+------------------------+--------------------------+
| 123abc       | 1            | Null                    | Null                | 1                     | Null                   | Null                       | Null                   | Nuclei Data...          |
+--------------+--------------+-------------------------+---------------------+-----------------------+------------------------+----------------------------+------------------------+--------------------------+

