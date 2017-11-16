"""
Support for importing the LOINC Hierarchy into Bunsen.
"""

from bunsen.mapping import ConceptMaps

def with_loinc_hierarchy(sparkSession, concept_maps, loinc_hierarchy_path, loinc_version):
    """
    Returns a concept maps instance that includes the LOINC hierarchy read
    from the given location.
    """
    loinc = sparkSession._jvm.com.cerner.bunsen.mappings.systems.Loinc

    jconcept_maps = loinc.withLoincHierarchy(sparkSession._jsparkSession,
                                             concept_maps._jconcept_maps,
                                             loinc_hierarchy_path,
                                             loinc_version)

    return ConceptMaps(sparkSession, jconcept_maps)