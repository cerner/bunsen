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
   fhir_versions
   how_bunsen_works
   stu3_pythonapis
   r4_pythonapis
   snomed_and_loinc
   docker
   building

Compatibility Matrix
--------------------
The table below shows which versions of Spark and FHIR are
supported with Bunsen.

+--------------+-------------+--------+
|Bunsen release|Spark version|FHIR    |
+--------------+-------------+--------+
|         0.4.*|          2.3|STU3, R4|
+--------------+-------------+--------+
|         0.3.*|          2.2|STU3, R4|
+--------------+-------------+--------+
|         0.2.*|          2.2|    STU3|
+--------------+-------------+--------+
|         0.1.*|          2.1|    STU3|
+--------------+-------------+--------+

References
==========

* :ref:`genindex`
* :ref:`modindex`
