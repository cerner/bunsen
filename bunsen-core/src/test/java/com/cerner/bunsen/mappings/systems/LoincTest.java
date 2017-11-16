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
 * Unit test to import LOINC mappings.
 */
public class LoincTest {

  private static SparkSession spark;

  private static Dataset<Mapping> loincMappings;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() {

    spark = SparkSession.builder()
        .master("local[2]")
        .appName("LoincTest")
        .getOrCreate();

    loincMappings = Loinc.readMultiaxialHierarchyFile(spark,
        "src/test/resources/LOINC_HIERARCHY_SAMPLE.CSV",
        "2.56");

    loincMappings.cache();
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

    List<Mapping> mappings = loincMappings
        .where(col("sourceValue")
            .equalTo(lit("LP14559-6")))
        .collectAsList();

    Assert.assertEquals(1, mappings.size());
    Assert.assertEquals("LP31755-9",
        mappings.get(0).getTargetValue());
  }

  @Test
  public void checkConceptMapUri() {

    // All imported rows should have the expected concept map URI.
    Assert.assertEquals(loincMappings.count(),
        loincMappings
            .where(col("conceptMapUri")
                .equalTo(lit(Loinc.LOINC_HIERARCHY_MAPPING_URI)))
            .count());
  }

  @Test
  public void checkVersion() {

    // All imported rows should have the expected concept map version.
    Assert.assertEquals(loincMappings.count(),
        loincMappings
            .where(col("conceptMapVersion")
                .equalTo(lit("2.56")))
            .count());
  }

  @Test
  public void checkSystems() {

    Assert.assertEquals(loincMappings.count(),
        loincMappings
            .where(col("sourceSystem")
                .equalTo(lit(Loinc.LOINC_CODE_SYSTEM_URI)))
            .where(col("targetSystem")
                .equalTo(lit(Loinc.LOINC_CODE_SYSTEM_URI)))
            .count());
  }
}
