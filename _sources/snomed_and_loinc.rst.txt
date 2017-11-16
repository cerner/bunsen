Importing SNOMED and LOINC
==========================

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
:py:func:`~bunsen.mapping.loinc.with_loinc_hierarchy` and :py:func:`~bunsen.snomed.loinc.with_relationships`
functions for details.

>>> from bunsen.mapping import get_empty
>>> from bunsen.mapping.loinc import with_loinc_hierarchy
>>> from bunsen.mapping.snomed import with_relationships
>>>
>>> # Add SNOMED ot the concept maps
>>> concept_maps = with_relationships(
>>>       spark,
>>>       get_empty(spark),
>>>       '/path/to/mappings/snomedct_rf2/20160901/Snapshot/Terminology/sct2_Relationship_Snapshot_US1000124_20160901.txt',
>>>       '20160901')
>>>
>>> # Add LOINC to the concept maps.
>>> concept_maps = with_loinc_hierarchy(
>>>        spark,
>>>        concept_maps,
>>>        '/path/to/mappings/loinc_hierarchy/2.56/LOINC_2.56_MULTI-AXIAL_HIERARCHY.CSV',
>>>       '2.56')
>>>
>>> # Write the SNOMED and LOINC data to the ontologies database, where it is visible
>>> # in Bunsen's valueset functions.
>>> concept_maps.write_to_database('ontologies')


FHIR ConceptMap APIs
--------------------

.. automodule:: bunsen.mapping
    :members:
    :undoc-members:

.. automodule:: bunsen.mapping.loinc
    :members:
    :undoc-members:

.. automodule:: bunsen.mapping.snomed
    :members:
    :undoc-members: