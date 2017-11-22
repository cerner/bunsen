import os

from tempfile import mkdtemp
from pytest import fixture

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from bunsen.mapping.loinc import with_loinc_hierarchy
from bunsen.mapping.snomed import with_relationships
from bunsen.mapping import get_empty, get_default
from bunsen.bundles import load_from_directory, extract_entry, save_as_database, to_bundle

import xml.etree.ElementTree as ET

EXPECTED_COLUMNS = {'sourceValueSet', 'targetValueSet', 'sourceSystem',
                    'sourceValue', 'targetSystem', 'targetValue', 'equivalence',
                    'conceptMapUri', 'conceptMapVersion'}

@fixture(scope="session")
def spark_session(request):
  """
  Fixture for creating a Spark Session available for all tests in this
  testing session.
  """

  # Get the shaded JAR for testing purposes.
  shaded_jar =  os.environ['SHADED_JAR_PATH']

  spark = SparkSession.builder \
    .appName('Foresight-test') \
    .master('local[2]') \
    .config('spark.jars', shaded_jar) \
    .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
    .config('spark.sql.warehouse.dir', mkdtemp()) \
    .config('javax.jdo.option.ConnectionURL',
            'jdbc:derby:memory:metastore_db;create=true') \
    .enableHiveSupport() \
    .getOrCreate()

  request.addfinalizer(lambda: spark.stop())

  return spark


# Concept Maps Tests


def test_add_map(spark_session):

  concept_maps = get_empty(spark_session)

  snomed_to_loinc = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent'), # Blood pressure
                     ('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
                     ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  updated = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=snomed_to_loinc)

  assert updated.get_maps().count() == 1
  assert updated.get_mappings().where(col('conceptMapUri') == 'urn:cerner:test:snomed-to-loinc').count() == 3

def test_add_to_existing(spark_session):

  concept_maps = get_empty(spark_session)

  # Create an existing map
  with_existing = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                            version='0.1',
                                            source='urn:cerner:test:valueset',
                                            target='http://hl7.org/fhir/ValueSet/observation-code',
                                            mappings=[('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent')])


  updates = [('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
             ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  updated = with_existing.add_mappings(url='urn:cerner:test:snomed-to-loinc',
                                       version='0.1',
                                       mappings=updates)

  # Original concept map should be unchanged.
  assert with_existing.get_maps().count() == 1
  assert with_existing.get_mappings().where(col('conceptMapUri') == 'urn:cerner:test:snomed-to-loinc').count() == 1

  # Updated concept map should have the new mappings.
  assert updated.get_maps().count() == 1
  assert updated.get_mappings().where(col('conceptMapUri') == 'urn:cerner:test:snomed-to-loinc').count() == 3
  assert updated.get_ancestors().count() == 0

def test_get_map_as_xml(spark_session):

  concept_maps = get_empty(spark_session)

  snomed_to_loinc = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent'), # Blood pressure
                     ('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
                     ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  updated = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=snomed_to_loinc)

  xml_str = updated.get_map_as_xml('urn:cerner:test:snomed-to-loinc', '0.1')

  root = ET.fromstring(xml_str)
  assert root.tag == '{http://hl7.org/fhir}ConceptMap'

def test_write_maps(spark_session):

  concept_maps = get_empty(spark_session)

  snomed_to_loinc = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent'), # Blood pressure
                     ('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
                     ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  updated = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=snomed_to_loinc)

  spark_session.sql('create database if not exists ontologies')
  spark_session.sql('drop table if exists ontologies.mappings')
  spark_session.sql('drop table if exists ontologies.ancestors')
  spark_session.sql('drop table if exists ontologies.conceptmaps')

  updated.write_to_database('ontologies')

  # Check that the maps were written by reloading and inspecting them.
  reloaded = get_default(spark_session)

  assert reloaded.get_maps().count() == 1
  assert reloaded.get_mappings().where(col('conceptMapUri') == 'urn:cerner:test:snomed-to-loinc').count() == 3

# LOINC Tests
def test_read_hierarchy_file(spark_session):
  mappings = with_loinc_hierarchy(
      spark_session,
      get_empty(spark_session),
      'tests/resources/LOINC_HIERARCHY_SAMPLE.CSV',
      '2.56').get_mappings()

  assert set(mappings.columns) == EXPECTED_COLUMNS

# SNOMED Tests

def test_read_relationship_file(spark_session):
  mappings = with_relationships(
      spark_session,
      get_empty(spark_session),
      'tests/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT',
      '20160901').get_mappings()

  assert set(mappings.columns) == EXPECTED_COLUMNS

# Bundles Tests
@fixture(scope="session")
def bundles(spark_session):
  return load_from_directory(spark_session, 'tests/resources/bundles', 1)


def test_load_from_directory(bundles):
  assert len(bundles.collect()) == 3


def test_extract_entry(spark_session, bundles):
  assert extract_entry(spark_session, bundles, 'Condition').count() == 5

def test_save_as_database(spark_session):
  spark_session.sql("CREATE DATABASE IF NOT EXISTS test_db")

  save_as_database(
      spark_session,
      'tests/resources/bundles',
      'test_db', 'Condition', 'Patient', 'Observation',
      minPartitions=1)

  assert spark_session.sql("SELECT * FROM test_db.condition").count() == 5
  assert spark_session.sql("SELECT * FROM test_db.patient").count() == 3
  assert spark_session.sql("SELECT * FROM test_db.observation").count() == 72

def test_to_bundle(spark_session, bundles):
  conditions = extract_entry(spark_session, bundles, 'Condition')

  assert to_bundle(spark_session, conditions) != None
