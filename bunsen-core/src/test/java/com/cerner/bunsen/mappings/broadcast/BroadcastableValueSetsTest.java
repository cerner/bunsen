package com.cerner.bunsen.mappings.broadcast;

import com.cerner.bunsen.mappings.Hierarchies;
import com.cerner.bunsen.mappings.ValueSets;
import com.cerner.bunsen.mappings.systems.Loinc;
import com.cerner.bunsen.mappings.systems.Snomed;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for {@link BroadcastableValueSets}.
 */
public class BroadcastableValueSetsTest {

  private static SparkSession spark;

  /**
   * Sets up Spark and loads test value sets.
   */
  @BeforeClass
  public static void setUp() throws IOException {

    // Create a local spark session using an in-memory metastore.
    // We must also use Hive and set the partition mode to non-strict to
    // support dynamic partitions.
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("UdfsTest")
        .enableHiveSupport()
        .config("javax.jdo.option.ConnectionURL",
            "jdbc:derby:memory:metastore_db;create=true")
        .config("hive.exec.dynamic.partition.mode",
            "nonstrict")
        .config("spark.sql.warehouse.dir",
            Files.createTempDirectory("spark_warehouse").toString())
        .getOrCreate();

    spark.sql("CREATE DATABASE ontologies");

    Hierarchies withLoinc = Loinc.withLoincHierarchy(spark,
        Hierarchies.getEmpty(spark),
        "src/test/resources/LOINC_HIERARCHY_SAMPLE.CSV",
        "2.56");

    Hierarchies withLoincAndSnomed = Snomed.withRelationships(spark,
        withLoinc,
        "src/test/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT",
        "20160901");

    withLoincAndSnomed.writeToDatabase(Hierarchies.HIERARCHIES_DATABASE);

    ValueSets.getEmpty(spark)
        .withValueSetsFromDirectory("src/test/resources/valuesets")
        .writeToDatabase(ValueSets.VALUE_SETS_DATABASE);
  }

  /**
   * Tears down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testCustom() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addCode("testparent", "urn:cerner:system", "123")
        .addCode("testparent", "urn:cerner:system", "456")
        .addCode("testother", "urn:cerner:system", "789")
        .build(spark, ValueSets.getEmpty(spark), Hierarchies.getEmpty(spark));

    Assert.assertTrue(valueSets.hasCode("testparent",
        "urn:cerner:system",
        "123"));

    Assert.assertTrue(valueSets.hasCode("testparent",
        "urn:cerner:system",
        "456"));

    // This value should be in the other valueset, so check for false.
    Assert.assertFalse(valueSets.hasCode("testparent",
        "urn:cerner:system",
        "789"));

    Assert.assertTrue(valueSets.hasCode("testother",
        "urn:cerner:system",
        "789"));
  }

  @Test
  public void testLoadLoinc() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addCode("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4")
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_URI,
            "2.56")
        .build(spark, ValueSets.getEmpty(spark), Hierarchies.getDefault(spark));

    Assert.assertTrue(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "5821-4")); // "is a" LP14419-3

    Assert.assertTrue(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "LP14419-3")); // value set includes parent code

    Assert.assertFalse(valueSets.hasCode("bp",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "1234-5")); // not "is a" LP14419-3
  }

  @Test
  public void testLoadLatestLoinc() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addCode("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4")
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_URI)
        .build(spark, ValueSets.getEmpty(spark), Hierarchies.getDefault(spark));

    Assert.assertTrue(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "5821-4")); // "is a" LP14419-3

    Assert.assertTrue(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "LP14419-3")); // value set includes parent code

    Assert.assertFalse(valueSets.hasCode("bp",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "1234-5")); // not "is a" LP14419-3
  }

  @Test
  public void testLoadReference() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addReference("married",
            "urn:cerner:bunsen:valueset:married_maritalstatus",
            "0.0.1")
        .build(spark, ValueSets.getDefault(spark), Hierarchies.getEmpty(spark));

    Assert.assertTrue(valueSets.hasCode("married",
        "http://hl7.org/fhir/v3/MaritalStatus",
        "M"));

    Assert.assertFalse(valueSets.hasCode("married",
        "http://hl7.org/fhir/v3/MaritalStatus",
        "U"));
  }

  @Test
  public void testLoadLatestReference() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addReference("married",
            "urn:cerner:bunsen:valueset:married_maritalstatus")
        .build(spark, ValueSets.getDefault(spark), Hierarchies.getEmpty(spark));

    Assert.assertTrue(valueSets.hasCode("married",
        "http://hl7.org/fhir/v3/MaritalStatus",
        "M"));

    Assert.assertFalse(valueSets.hasCode("married",
        "http://hl7.org/fhir/v3/MaritalStatus",
        "U"));
  }

  @Test
  public void testGetValueSet() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addCode("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4")
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_URI)
        .addReference("married",
            "urn:cerner:bunsen:valueset:married_maritalstatus")
        .build(spark, ValueSets.getDefault(spark), Hierarchies.getDefault(spark));

    Assert.assertTrue(ImmutableSet.of("bp", "leukocytes", "married")
        .containsAll(valueSets.getReferenceNames()));

    Map<String,Set<String>> leukocyteValues = valueSets.getValues("leukocytes");
    Map<String,Set<String>> genderValues = valueSets.getValues("married");

    Assert.assertTrue(leukocyteValues.containsKey("http://loinc.org"));
    Assert.assertTrue(ImmutableSet.of("LP14419-3", "5821-4")
        .containsAll(leukocyteValues.get("http://loinc.org")));

    Assert.assertTrue(genderValues.containsKey("http://hl7.org/fhir/v3/MaritalStatus"));
    Assert.assertTrue(ImmutableSet.of("M")
        .containsAll(genderValues.get("http://hl7.org/fhir/v3/MaritalStatus")));
  }
}
