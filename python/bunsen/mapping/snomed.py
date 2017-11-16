"""
Support for importing SNOMED relationship files into Bunsen.
"""

from bunsen.mapping import ConceptMaps

def with_relationships(sparkSession, concept_maps, snomed_relationship_path, snomed_version):
    """
    Returns a concept maps instance that includes the SNOMED relationships read
    from the given location.
    """
    snomed = sparkSession._jvm.com.cerner.bunsen.mappings.systems.Snomed

    jconcept_maps = snomed.withRelationships(sparkSession._jsparkSession,
                                             concept_maps._jconcept_maps,
                                             snomed_relationship_path,
                                             snomed_version)

    return ConceptMaps(sparkSession, jconcept_maps)
