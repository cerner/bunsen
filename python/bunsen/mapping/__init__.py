"""
Core library for working with `Concept Maps <https://www.hl7.org/fhir/conceptmap.html>`_
in Bunsen. This class See the :py:class:`~bunsen.mapping.ConceptMaps` class for details.
"""

from pyspark.sql import functions, DataFrame
import collections

def get_default(spark_session):
    jconcept_maps = spark_session._jvm.com.cerner.bunsen.mappings \
      .ConceptMaps.getDefault(spark_session._jsparkSession)

    return ConceptMaps(spark_session, jconcept_maps)

def get_empty(spark_session):
    jconcept_maps = spark_session._jvm.com.cerner.bunsen.mappings \
      .ConceptMaps.getEmpty(spark_session._jsparkSession)

    return ConceptMaps(spark_session, jconcept_maps)

def _add_mappings_to_map(jvm, concept_map, mappings):
    """
    Helper function to add a collection of mappings in the form of a list of
    [(source_system, source_value, target_system, target_value, equivalence)] tuples
    to the given concept map.
    """
    groups = collections.defaultdict(list)

    for (ss, sv, ts, tv, eq) in mappings:
        groups[(ss,ts)].append((sv,tv,eq))

    for (source_system, target_system), values in groups.items():
        group = concept_map.addGroup()

        group.setSource(source_system)
        group.setTarget(target_system)

        for (source_value, target_value, equivalence) in values:
            element = group.addElement()
            element.setCode(source_value)
            target = element.addTarget()
            target.setCode(target_value)

            if equivalence is not None:

                enumerations = jvm.org.hl7.fhir.dstu3.model.Enumerations

                equivEnum = enumerations.ConceptMapEquivalence.fromCode(equivalence)

                target.setEquivalence(equivEnum)


class ConceptMaps(object):
    """
    An immutable collection of FHIR Concept Maps to be used to
    map value sets and for ontologically-based queries.
    """

    def __init__(self, spark_session, jconcept_maps):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jconcept_maps = jconcept_maps

    def latest_version(self, uri):
        """
        Returns the latest version of a map, or None if there is none."
        """
        df = get_maps().where(df.uri == functions.lit(uri))
        results = df.agg({"version": "max"}).collect()
        return results[0].min if resuls.count() > 0 else None

    def get_maps(self):
        """
        Returns a dataset of FHIR ConceptMaps without the nested mapping content,
        allowing users to explore mapping metadata.

        The mappings themselves are excluded because they can become quite large,
        so users should use the get_mappings method above to explore a table of
        them.
        """
        return DataFrame(self._jconcept_maps.getMaps(), self._spark_session)

    def get_mappings(self, uri=None, version=None):
        """
        Returns a dataset of all mappings.
        """
        df = DataFrame(self._jconcept_maps.getMappings(), self._spark_session)

        if uri is not None:
            df = df.where(df.conceptMapUri == functions.lit(uri))

        if version is not None:
            df = df.where(df.conceptMapVersion == functions.lit(version))

        return df

    def get_ancestors(self, uri=None, version=None):
        """
        Returns a dataset of all ancestors.
        """
        df = DataFrame(self._jconcept_maps.getAncestors(), self._spark_session)

        if uri is not None:
            df = df.where(df.conceptMapUri == functions.lit(uri))

        if version is not None:
            df = df.where(df.conceptMapVersion == functions.lit(version))

        return df

    def get_map_as_xml(self, url, version):
        """
        Returns an XML string containing the specified concept map.
        """
        concept_map = self._jconcept_maps.getConceptMap(url, version)
        return self._jvm.com.cerner.bunsen.python.Functions.resourceToXml(concept_map)

    def with_new_map(self,
                     url,
                     version,
                     source,
                     target,
                     experimental=True,
                     mappings=[]):
        """
        Returns a new ConceptMaps instance with the given map added. Callers
        may include a list of mappings tuples in the form of
        [(source_system, source_value, target_system, target_value, equivalence)].
        """
        concept_map = self._jvm.org.hl7.fhir.dstu3.model.ConceptMap()
        concept_map.setUrl(url)
        concept_map.setVersion(version)
        concept_map.setSource(self._jvm.org.hl7.fhir.dstu3.model.UriType(source))
        concept_map.setTarget(self._jvm.org.hl7.fhir.dstu3.model.UriType(target))

        if (experimental):
            concept_map.setExperimental(True)

        _add_mappings_to_map(self._jvm, concept_map, mappings)

        map_as_list = self._jvm.java.util.Collections.singletonList(concept_map)

        return ConceptMaps(self._spark_session,
                           self._jconcept_maps.withConceptMaps(map_as_list))

    def add_mappings(self,
                     url,
                     version,
                     mappings):
        """
        Returns a new ConceptMaps instance with the given mappings added to an existing map.
        The mappings parameter must be a list of tuples of the form
        [(source_system, source_value, target_system, target_value, equivalence)].
        """
        concept_map = self._jconcept_maps.getConceptMap(url, version)

        _add_mappings_to_map(self._jvm, concept_map, mappings)

        map_as_list = self._jvm.java.util.Collections.singletonList(concept_map)

        return ConceptMaps(self._spark_session,
                           self._jconcept_maps.withConceptMaps(map_as_list))

    def write_to_database(self, database):
        """
        Writes the mapping content to the given database, creating a mappings
        and conceptmaps table if they don't exist.
        """
        self._jconcept_maps.writeToDatabase(database)
