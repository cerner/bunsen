package com.cerner.bunsen;

import com.cerner.bunsen.mappings.ConceptMaps;
import com.cerner.bunsen.mappings.broadcast.BroadcastableValueSets;
import com.cerner.bunsen.mappings.systems.Loinc;
import com.cerner.bunsen.mappings.systems.Snomed;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Observation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for {@link ValueSetUdfs}.
 */
public class ValueSetUdfsTest {

  private static final FhirEncoders encoders = FhirEncoders.forStu3().getOrCreate();
  private static SparkSession spark;

  private static CodeableConcept codeable(String system, String value) {

    CodeableConcept concept = new CodeableConcept();

    concept.addCoding()
        .setSystem(system)
        .setCode(value);

    return concept;
  }

  private static Observation observation(String id, String code) {

    Observation observation = new Observation();

    observation.setId(id);
    observation.setCode(codeable(Loinc.LOINC_CODE_SYSTEM_URI, code));

    return observation;
  }

  private static Condition condition(String id, String code) {
    Condition condition = new Condition();

    // Condition based on example from FHIR:
    // https://www.hl7.org/fhir/condition-example.json.html
    condition.setId(id);

    condition.setCode(codeable(Snomed.SNOMED_CODE_SYSTEM_URI, code));

    return condition;
  }

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
        .addDescendantsOf("albumin",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "14959-1",
            Loinc.LOINC_HIERARCHY_MAPPING_URI,
            "2.56")
        .addDescendantsOf("diabetes",
            Snomed.SNOMED_CODE_SYSTEM_URI,
            "73211009",
            Snomed.SNOMED_HIERARCHY_MAPPING_URI,
            "20160901")
        .addDescendantsOf("blood_disorder",
            Snomed.SNOMED_CODE_SYSTEM_URI,
            "266992002",
            Snomed.SNOMED_HIERARCHY_MAPPING_URI,
            "20160901")
        .addDescendantsOf("disorder_history",
            Snomed.SNOMED_CODE_SYSTEM_URI,
            "312850006",
            Snomed.SNOMED_HIERARCHY_MAPPING_URI,
            "20160901")
        .build(spark, withLoincAndSnomed);

    ValueSetUdfs.pushUdf(spark, valueSets);

    Dataset<Observation> loincObservations = spark.createDataset(
        ImmutableList.of(
            observation("leukocytes", "5821-4"), // "is a" LP14419-3
            observation("bp", "8462-4")), // Blood pressure
        encoders.of(Observation.class));

    loincObservations.createOrReplaceTempView("test_loinc_obs");

    // Conditions include history of anemia, which includes a cycling ancestor
    // in our test data. This ensures that can be loaded correctly.
    Dataset<Condition> conditions = spark.createDataset(
        ImmutableList.of(
            // "is a" 73211009 (diabetes)
            condition("diabetes", "44054006"),
            // "is a" 266992002 (history of blood disorder) and
            // 312850006 (history of disorder)
            condition("history_of_anemia", "275538002")),
        encoders.of(Condition.class));

    conditions.createOrReplaceTempView("test_snomed_cond");
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
  public void testLoincHasAncestor() {

    Dataset<Row> results = spark.sql("select id from test_loinc_obs "
        + "where in_valueset(code, 'leukocytes')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("leukocytes", results.head().get(0));
  }

  @Test
  public void testSelfValue() {

    Dataset<Row> results = spark.sql("select id from test_loinc_obs "
        + "where in_valueset(code, 'bp')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("bp", results.head().get(0));
  }

  @Test
  public void testNomatches() {

    Dataset<Row> results = spark.sql("select id from test_loinc_obs "
        + "where in_valueset(code, 'albumin')");

    Assert.assertEquals(0, results.count());
  }

  @Test
  public void testSnomedHasAncestor() {

    Dataset<Row> results = spark.sql("select id from test_snomed_cond "
        + "where in_valueset(code, 'diabetes')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("diabetes", results.head().get(0));
  }

  @Test
  public void testHasCyclicAncestor() {
    Dataset<Row> results = spark.sql("select id from test_snomed_cond "
        + "where in_valueset(code, 'blood_disorder')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("history_of_anemia", results.head().get(0));

    Dataset<Row> ancestorResults = spark.sql("select id from test_snomed_cond "
        + "where in_valueset(code, 'disorder_history')");

    Assert.assertEquals(1, ancestorResults.count());
    Assert.assertEquals("history_of_anemia", ancestorResults.head().get(0));
  }
}
