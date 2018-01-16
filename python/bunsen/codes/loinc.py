"""
Support for importing the LOINC Hierarchy into Bunsen.
"""

from bunsen.codes import Hierarchies

def with_loinc_hierarchy(sparkSession, hierarchies, loinc_hierarchy_path, loinc_version):
    """
    Returns a hierarchies instance that includes the LOINC hierarchy read
    from the given location.
    """
    loinc = sparkSession._jvm.com.cerner.bunsen.codes.systems.Loinc

    jhierarchies = loinc.withLoincHierarchy(sparkSession._jsparkSession,
                                           hierarchies._jhierarchies,
                                           loinc_hierarchy_path,
                                           loinc_version)

    return Hierarchies(sparkSession, jhierarchies)
