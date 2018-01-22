package com.cerner.bunsen;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Patient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests work working with bundles.
 */
public class BundlesTest {

  private static final FhirEncoders encoders = FhirEncoders.forStu3().getOrCreate();
  private static SparkSession spark;
  private static JavaRDD<Bundle> bundles;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() throws IOException {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("BundlesTest")
        .config("spark.sql.warehouse.dir",
            Files.createTempDirectory("spark_warehouse").toString())
        .getOrCreate();

    bundles = Bundles.loadFromDirectory(spark, "src/test/resources/xml/bundles", 1).cache();
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
  public void testLoadBundleXml() {

    Assert.assertEquals(3,
        bundles.collect().size());
  }

  @Test
  public void testLoadBundleJson() {

    JavaRDD<Bundle> bundles = Bundles.loadFromDirectory(spark,
        "src/test/resources/json/bundles", 1);

    Assert.assertEquals(3,
        bundles.collect().size());
  }

  @Test
  public void getGetConditions() {

    Dataset<Condition> conditions = Bundles.extractEntry(spark,
        bundles,
        Condition.class);

    Assert.assertEquals(5, conditions.count());
  }

  private void checkPatients(Dataset<Patient> patients) {
    List<String> patientIds = patients
        .collectAsList()
        .stream()
        .map(Patient::getId)
        .collect(Collectors.toList());

    Assert.assertEquals(3, patientIds.size());

    List<String> expectedIds = ImmutableList.of(
        "Patient/6666001",
        "Patient/1032702",
        "Patient/9995679");

    Assert.assertTrue(patientIds.containsAll(expectedIds));
  }

  private void checkConditions(Dataset<Condition> conditions) {
    List<String> conditionIds = conditions
        .collectAsList()
        .stream()
        .map(Condition::getId)
        .collect(Collectors.toList());

    Assert.assertEquals(5, conditionIds.size());

    List<String> expectedIds = ImmutableList.of(
        "Condition/119",
        "Condition/120",
        "Condition/121",
        "Condition/122",
        "Condition/123");

    Assert.assertTrue(conditionIds.containsAll(expectedIds));
  }

  @Test
  public void testGetResourcesByClass() {

    Dataset<Patient> patients = Bundles.extractEntry(spark, bundles, Patient.class);
    Dataset<Condition> conditions = Bundles.extractEntry(spark, bundles, Condition.class);

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testGetResourcesByName() {

    Dataset<Patient> patients = Bundles.extractEntry(spark, bundles, "Patient");
    Dataset<Condition> conditions = Bundles.extractEntry(spark, bundles, "Condition");

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testGetCaseInsensitive() {

    Dataset<Patient> patients = Bundles.extractEntry(spark, bundles, "patIENt");
    Dataset<Condition> conditions = Bundles.extractEntry(spark, bundles, "conDiTion");

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testSaveAsDatabase() {

    Bundles.saveAsDatabase(spark,
        bundles,
        "bundlesdb",
        "patient", "condition", "observation");

    Dataset<Patient> patients = spark
        .sql("select * from bundlesdb.patient")
        .as(encoders.of(Patient.class));

    checkPatients(patients);

    Dataset<Condition> conditions = spark
        .sql("select * from bundlesdb.condition")
        .as(encoders.of(Condition.class));

    checkConditions(conditions);

    // Ensure the included observations are present as well.
    Assert.assertEquals(72,
        spark.sql("select * from bundlesdb.observation")
            .count());
  }
}
