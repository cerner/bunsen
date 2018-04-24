"""
Bunsen Python API for working with Code Systems.
"""

from bunsen.codes import ConceptMaps, ValueSets, Hierarchies

def get_concept_maps(spark_session, database='ontologies'):
    """
    Returns a :class:`ConceptMaps` instance for the given database.
    """
    jconcept_maps = spark_session._jvm.com.cerner.bunsen.stu3.codes \
      .ConceptMaps.getFromDatabase(spark_session._jsparkSession, database)

    jfunctions = spark_session._jvm.com.cerner.bunsen.stu3.python.Functions

    return ConceptMaps(spark_session, jconcept_maps, jfunctions)

def create_concept_maps(spark_session):
    """
    Creates a new, empty :py:class:`ConceptMaps` instance.
    """
    jconcept_maps = spark_session._jvm.com.cerner.bunsen.stu3.codes \
      .ConceptMaps.getEmpty(spark_session._jsparkSession)

    jfunctions = spark_session._jvm.com.cerner.bunsen.stu3.python.Functions

    return ConceptMaps(spark_session, jconcept_maps, jfunctions)

def get_value_sets(spark_session, database='ontologies'):
    """
    Returns a :class:`ValueSets` instance for the given database.
    """
    jvalue_sets = spark_session._jvm.com.cerner.bunsen.stu3.codes \
      .ValueSets.getFromDatabase(spark_session._jsparkSession, database)

    jfunctions = spark_session._jvm.com.cerner.bunsen.stu3.python.Functions

    return ValueSets(spark_session, jvalue_sets, jfunctions)

def create_value_sets(spark_session):
    """
    Creates a new, empty :class:`ValueSets` instance.
    """
    jvalue_sets = spark_session._jvm.com.cerner.bunsen.stu3.codes \
      .ValueSets.getEmpty(spark_session._jsparkSession)

    jfunctions = spark_session._jvm.com.cerner.bunsen.stu3.python.Functions

    return ValueSets(spark_session, jvalue_sets, jfunctions)


def get_hierarchies(spark_session, database='ontologies'):
    """
    Returns a :class:`Hierarchies` instance for the given database.
    """
    jhierarchies = spark_session._jvm.com.cerner.bunsen.codes \
        .Hierarchies.getFromDatabase(spark_session._jsparkSession, database)

    return Hierarchies(spark_session, jhierarchies)

def create_hierarchies(spark_session):
    """
    Creates a new, empty :class:`Hierarchies` instance.
    """
    jhierarchies = spark_session._jvm.com.cerner.bunsen.codes \
        .Hierarchies.getEmpty(spark_session._jsparkSession)

    return Hierarchies(spark_session, jhierarchies)
