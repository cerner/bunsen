.. Bunsen documentation master file

Bunsen: FHIR Data with Apache Spark
===================================
Bunsen lets users load, transform, and analyze FHIR data with Apache Spark. It offers Java
and Python APIs to convert FHIR resources into Apache Spark Datasets, which then can be explored
with the full power of that platform, including with Spark SQL.

.. toctree::
   :maxdepth: 1

   introduction
   java_usage
   how_bunsen_works
   pythonapis
   snomed_and_loinc

Compatibility Matrix
--------------------
The table below shows which versions of Spark and FHIR are
supported with Bunsen.

+--------------+-------------+----+
|Bunsen release|Spark version|FHIR|
+--------------+-------------+----+
|         0.1.0|          2.1|STU3|
+--------------+-------------+----+

References
==========

* :ref:`genindex`
* :ref:`modindex`
