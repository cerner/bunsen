package com.cerner.bunsen.mappings.systems;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import com.cerner.bunsen.mappings.Mapping;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test to import SNOMED mappings.
 */
public class SnomedTest {

  private static SparkSession spark;

  private static Dataset<Mapping> snomedMappings;

  /**
   * Sets up Spark and loads the SNOMED mappings for testing.
   */
  @BeforeClass
  public static void setUp() {

    spark = SparkSession.builder()
        .master("local[2]")
        .appName("SnomedTest")
        .getOrCreate();

    snomedMappings = Snomed.readRelationshipFile(spark,
        "src/test/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT",
        "20160901");

    snomedMappings.cache();
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
  public void testHasParent() {

    List<Mapping> mappings = snomedMappings
        .where(col("sourceValue")
            .equalTo(lit("44054006")))
        .collectAsList();

    Assert.assertEquals(1, mappings.size());
    Assert.assertEquals("73211009",
        mappings.get(0).getTargetValue());
  }

  @Test
  public void checkConceptMapUri() {

    // All imported rows should have the expected concept map URI.
    Assert.assertEquals(snomedMappings.count(),
        snomedMappings
            .where(col("conceptMapUri")
                .equalTo(lit(Snomed.SNOMED_HIERARCHY_MAPPING_URI)))
            .count());
  }

  @Test
  public void checkVersion() {

    // All imported rows should have the expected concept map version.
    Assert.assertEquals(snomedMappings.count(),
        snomedMappings
            .where(col("conceptMapVersion")
                .equalTo(lit("20160901")))
            .count());
  }

  @Test
  public void checkSystems() {

    Assert.assertEquals(snomedMappings.count(),
        snomedMappings
            .where(col("sourceSystem")
                .equalTo(lit(Snomed.SNOMED_CODE_SYSTEM_URI)))
            .where(col("targetSystem")
                .equalTo(lit(Snomed.SNOMED_CODE_SYSTEM_URI)))
            .count());
  }
}
