package com.cerner.bunsen.mappings.broadcast;

import com.cerner.bunsen.mappings.ConceptMaps;
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
   * Sets up Spark and loads test mappings.
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

    spark.sql("create database " + ConceptMaps.MAPPING_DATABASE);

    ConceptMaps empty = ConceptMaps.getEmpty(spark);

    ConceptMaps withLoinc = Loinc.withLoincHierarchy(spark,
        empty,
        "src/test/resources/LOINC_HIERARCHY_SAMPLE.CSV",
        "2.56");

    ConceptMaps withLoincAndSnomed = Snomed.withRelationships(spark,
        withLoinc,
        "src/test/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT",
        "20160901");

    withLoincAndSnomed.writeToDatabase(ConceptMaps.MAPPING_DATABASE);
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
        .addCode("testparent", Loinc.LOINC_CODE_SYSTEM_URI, "123")
        .addCode("testparent", Loinc.LOINC_CODE_SYSTEM_URI, "456")
        .addCode("testother", Loinc.LOINC_CODE_SYSTEM_URI, "789")
        .build(spark, ConceptMaps.getDefault(spark));

    Assert.assertTrue(valueSets.hasCode("testparent",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "123"));

    Assert.assertTrue(valueSets.hasCode("testparent",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "456"));

    // This value should be in the other valueset, so check for false.
    Assert.assertFalse(valueSets.hasCode("testparent",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "789"));

    Assert.assertTrue(valueSets.hasCode("testother",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "789"));
  }

  @Test
  public void testLoadLoinc() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addDescendantsOf("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4",
            Loinc.LOINC_HIERARCHY_MAPPING_URI,
            "2.56")
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_MAPPING_URI,
            "2.56")
        .build(spark, ConceptMaps.getDefault(spark));

    Assert.assertTrue(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "5821-4")); // "is a" LP14419-3

    Assert.assertFalse(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "1234-5")); // not "is a" LP14419-3
  }

  @Test
  public void testLoadLatestLoinc() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addDescendantsOf("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4",
            Loinc.LOINC_HIERARCHY_MAPPING_URI)
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_MAPPING_URI)
        .build(spark, ConceptMaps.getDefault(spark));

    Assert.assertTrue(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "5821-4")); // "is a" LP14419-3

    Assert.assertFalse(valueSets.hasCode("leukocytes",
        Loinc.LOINC_CODE_SYSTEM_URI,
        "1234-5")); // not "is a" LP14419-3
  }

  @Test
  public void testGetValueset() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addDescendantsOf("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4",
            Loinc.LOINC_HIERARCHY_MAPPING_URI)
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_MAPPING_URI)
        .build(spark, ConceptMaps.getDefault(spark));

    Assert.assertTrue(ImmutableSet.of("bp", "leukocytes")
        .containsAll(valueSets.getReferenceNames()));

    Map<String,Set<String>> leukocyteValues = valueSets.getValues("leukocytes");

    Assert.assertTrue(leukocyteValues.containsKey("http://loinc.org"));

    Assert.assertTrue(ImmutableSet.of("LP14419-3", "5821-4")
        .containsAll(leukocyteValues.get("http://loinc.org")));
  }
}
