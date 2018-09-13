Working with SNOMED, LOINC, and Other Ontologies
================================================

Bunsen supports querying data with arbitrary code systems, but includes specialized support for
importing SNOMED and LOINC since they are prevalent in a number of FHIR-related use cases.

.. toctree::
   :maxdepth: 2

Users must load the reference data in order to  use queries aware of LOINC, SNOMED, or other terminologies.
Users are responsible for ensuring they have the appropriate licenses to use this content and downloading
it from the given sources.

Obtaining LOINC
---------------
The LOINC Multiaxial Hierarchy is published at https://loinc.org/download/loinc-multiaxial-hierarchy/.
It can be downloaded and extracted into the mappings root with the following structure. Bunsen can ingest
the multiaxial hierarchy from the CSV file available in that download, such as:

/path/to/mappings/loinc_hierarchy/2.56/LOINC_2.56_MULTI-AXIAL_HIERARCHY.CSV

Obtaining SNOMED
----------------
SNOMED RF2 Files are published at https://www.nlm.nih.gov/healthit/snomedct/international.html.
It can be downloaded and extracted into the mappings root with the following structure. Bunsen can
ingest the relationship snapshot, such as:

/path/to/mappings/snomedct_rf2/20160901/Snapshot/Terminology/sct2_Relationship_Snapshot_US1000124_20160901.txt

Loading the Ontologies
----------------------
Once the content is downloaded, users can import it with the following commands. See the
:py:func:`~bunsen.codes.loinc.with_loinc_hierarchy` and :py:func:`~bunsen.codes.snomed.with_relationships`
functions for details.



>>> from bunsen.stu3.codes import create_hierarchies
>>> from bunsen.codes.loinc import with_loinc_hierarchy
>>> from bunsen.codes.snomed import with_relationships
>>>
>>> # Add SNOMED to the value sets
>>> snomed_hierarchy= with_relationships(
>>>       spark,
>>>       create_hierarchies(spark),
>>>       '/path/to/mappings/snomedct_rf2/20160901/Snapshot/Terminology/sct2_Relationship_Snapshot_US1000124_20160901.txt',
>>>       '20160901')
>>>
>>> # Add LOINC to the value sets
>>> loinc_hierarchy= with_loinc_hierarchy(
>>>        spark,
>>>        snomed_hierarchy,
>>>        '/path/to/mappings/loinc_hierarchy/2.56/LOINC_2.56_MULTI-AXIAL_HIERARCHY.CSV',
>>>        '2.56')
>>>
>>> # Make sure that a database called `ontologies` is created before writing to the database.
>>> spark.sql("CREATE DATABASE IF NOT EXISTS ontologies")
>>>
>>> # Write the SNOMED and LOINC data to the ontologies database, where it is visible
>>> # in Bunsen's valueset functions.
>>> loinc_hierarchy.write_to_database('ontologies')
>>> snomed_hierarchy.write_to_database('ontologies')


SNOMED and LOINC Import APIs
----------------------------

.. automodule:: bunsen.codes.loinc
    :members:
    :undoc-members:

.. automodule:: bunsen.codes.snomed
    :members:
    :undoc-members:
