package com.cerner.bunsen.stu3;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.Bundles;
import com.cerner.bunsen.spark.Bundles.BundleContainer;
import com.cerner.bunsen.spark.SparkRowConverter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Provenance;
import org.hl7.fhir.dstu3.model.ResourceType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests work working with bundles.
 */
public class BundlesTest {

  // private static final FhirEncoders encoders = FhirEncoders.forStu3().getOrCreate();
  private static SparkSession spark;
  private static Bundles bundles;
  private static JavaRDD<BundleContainer> bundlesRdd;
  private static JavaRDD<BundleContainer> bundlesWithContainedRdd;

  private static final FhirContext fhirContext =  FhirContexts.forStu3();

  private static SparkRowConverter patientConverter =
      SparkRowConverter.forResource(fhirContext,
          "http://hl7.org/fhir/StructureDefinition/Patient");

  private static final SparkRowConverter conditionConverter =
      SparkRowConverter.forResource(fhirContext,
          "http://hl7.org/fhir/StructureDefinition/Condition");

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

    bundles = Bundles.forStu3();

    bundlesRdd = bundles.loadFromDirectory(spark,
        "src/test/resources/xml/bundles", 1).cache();

    bundlesWithContainedRdd = bundles.loadFromDirectory(spark,
        "src/test/resources/json/bundles-with-contained", 1).cache();
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
        bundlesRdd.count());
  }

  @Test
  public void testLoadBundleJson() {

    JavaRDD<BundleContainer> bundles = BundlesTest.bundles.loadFromDirectory(spark,
        "src/test/resources/json/bundles", 1);

    Assert.assertEquals(3,
        bundles.count());
  }

  @Test
  public void testRetrieveBundle() {
    BundleContainer container = bundlesRdd.first();

    Bundle bundle = (Bundle) container.getBundle();

    Assert.assertNotNull(bundle);
    Assert.assertTrue(bundle.getEntry().size() > 0);
  }

  @Test
  public void getGetConditions() {

    Dataset<Row> conditions = bundles.extractEntry(spark,
        bundlesRdd,
        Condition.class);

    Assert.assertEquals(5, conditions.count());
  }

  private void checkPatients(Dataset<Row> patients) {

    checkPatients(patients, patientConverter);
  }

  private void checkPatients(Dataset<Row> patients, SparkRowConverter patientConverter) {

    List<String> patientIds = patients
        .collectAsList()
        .stream()
        .map(row ->
            patientConverter.rowToResource(row)
                .getIdElement()
                .getValue())
        .collect(Collectors.toList());

    Assert.assertEquals(3, patientIds.size());

    List<String> expectedIds = ImmutableList.of(
        "Patient/6666001",
        "Patient/1032702",
        "Patient/9995679");

    Assert.assertTrue(patientIds.containsAll(expectedIds));
  }

  private void checkConditions(Dataset<Row> conditions) {

    checkConditions(conditions, conditionConverter);
  }

  private void checkConditions(Dataset<Row> conditions, SparkRowConverter conditionConverter) {
    List<String> conditionIds = conditions
        .collectAsList()
        .stream()
        .map(row ->
            conditionConverter.rowToResource(row)
                .getIdElement()
                .getValue())
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

    Dataset<Row> patients = bundles.extractEntry(spark, bundlesRdd, Patient.class);
    Dataset<Row> conditions = bundles.extractEntry(spark, bundlesRdd, Condition.class);

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testGetResourcesByName() {

    Dataset<Row> patients = bundles.extractEntry(spark, bundlesRdd, "Patient");
    Dataset<Row> conditions = bundles.extractEntry(spark, bundlesRdd, "Condition");

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testGetResourceByUrl() {

    String patientUrl = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

    String conditionUrl = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-condition";

    Dataset<Row> patients = bundles.extractEntry(spark, bundlesRdd,
        patientUrl);

    Dataset<Row> conditions = bundles.extractEntry(spark, bundlesRdd,
        conditionUrl);

    checkPatients(patients, SparkRowConverter.forResource(fhirContext, patientUrl));
    checkConditions(conditions, SparkRowConverter.forResource(fhirContext, conditionUrl));
  }

  @Test
  public void testXmlBundleStrings() {

    JavaRDD<String> xmlBundlesRdd = spark.sparkContext()
        .wholeTextFiles("src/test/resources/xml/bundles", 1)
        .toJavaRDD()
        .map(tuple -> tuple._2());

    Dataset<String> xmlBundles = spark.createDataset(xmlBundlesRdd.rdd(),
        Encoders.STRING());

    xmlBundles.write().saveAsTable("xml_bundle_table");

    JavaRDD<BundleContainer> bundles = BundlesTest.bundles.fromXml(
        spark.sql("select value from xml_bundle_table"), "value");

    Dataset<Row> patients = BundlesTest.bundles.extractEntry(spark,
        bundles,
        Patient.class);

    checkPatients(patients);
  }

  @Test
  public void testJsonBundleStrings() {

    JavaRDD<String> jsonBundlesRdd = spark.sparkContext()
        .wholeTextFiles("src/test/resources/json/bundles", 1)
        .toJavaRDD()
        .map(tuple -> tuple._2());

    Dataset<String> jsonBundles = spark.createDataset(jsonBundlesRdd.rdd(),
        Encoders.STRING());

    jsonBundles.write().saveAsTable("json_bundle_table");

    JavaRDD<BundleContainer> bundlesRdd = bundles.fromJson(
        spark.sql("select value from json_bundle_table"), "value");

    Dataset<Row> patients = BundlesTest.bundles.extractEntry(spark,
        bundlesRdd,
        Patient.class);

    checkPatients(patients);
  }

  @Test
  public void testGetResourcesAndContainedResourcesByClass() {

    Class[] containedClasses = new Class[]{Provenance.class};

    Dataset<Row> observations = bundles.extractEntry(spark,
        bundlesWithContainedRdd,
        Observation.class,
        containedClasses);

    SparkRowConverter rowConverter = SparkRowConverter
        .forResource(fhirContext, Observation.class.getSimpleName(),
            Arrays.stream(containedClasses).map(c -> c.getSimpleName())
                .collect(Collectors.toList()));

    Observation observation = (Observation) rowConverter
        .rowToResource(observations.head());

    Assert.assertEquals(1, observations.count());
    Assert.assertEquals(ResourceType.Provenance,
        observation.getContained().get(0).getResourceType());

    // internal references prefixed with #
    String expectedId = "#" + "11000100-4";
    Assert.assertEquals(expectedId, observation.getContained().get(0).getId());
  }

  @Test
  public void testSaveAsDatabase() {

    bundles.saveAsDatabase(spark,
        bundlesRdd,
        "bundlesdb",
        "Patient", "Condition", "Observation");

    Dataset<Row> patients = spark
        .sql("select * from bundlesdb.patient");

    checkPatients(patients);

    Dataset<Row> conditions = spark
        .sql("select * from bundlesdb.condition");

    checkConditions(conditions);

    // Ensure the included observations are present as well.
    Assert.assertEquals(72,
        spark.sql("select * from bundlesdb.observation")
            .count());
  }
}
