package com.cerner.bunsen.spark.codes.broadcast;

import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.spark.MockValueSets;
import com.cerner.bunsen.spark.SparkRowConverter;
import com.cerner.bunsen.spark.codes.Hierarchies;
import com.cerner.bunsen.spark.codes.systems.Loinc;
import com.cerner.bunsen.spark.codes.systems.Snomed;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  static MockValueSets emptyValueSets;

  static MockValueSets mockValueSets;

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

    emptyValueSets = new MockValueSets(spark, SparkRowConverter.forResource(
        FhirContexts.forStu3(), "ValueSet"));

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

    mockValueSets = MockValueSets.createWithTestValue(spark, SparkRowConverter.forResource(
        FhirContexts.forStu3(),
        "ValueSet"));
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
        .build(spark, emptyValueSets, Hierarchies.getEmpty(spark));

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
        .build(spark, emptyValueSets, Hierarchies.getDefault(spark));

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
        .build(spark, emptyValueSets, Hierarchies.getDefault(spark));

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
        .addReference("priorities",
            "http://hl7.org/fhir/ValueSet/v3-ActPriority",
            "2017-04-19")
        .build(spark, mockValueSets, Hierarchies.getEmpty(spark));

    Assert.assertTrue(valueSets.hasCode("priorities",
        "http://hl7.org/fhir/v3/ActPriority",
        "EM"));

    Assert.assertFalse(valueSets.hasCode("priorities",
        "http://hl7.org/fhir/v3/ActPriority",
        "R"));
  }

  @Test
  public void testLoadLatestReference() {

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addReference("priorities",
            "http://hl7.org/fhir/ValueSet/v3-ActPriority")
        .build(spark, mockValueSets, Hierarchies.getEmpty(spark));

    Assert.assertTrue(valueSets.hasCode("priorities",
        "http://hl7.org/fhir/v3/ActPriority",
        "EM"));

    Assert.assertFalse(valueSets.hasCode("priorities",
        "http://hl7.org/fhir/v3/ActPriority",
        "R"));
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
        .addReference("priorities",
            "http://hl7.org/fhir/ValueSet/v3-ActPriority")
        .build(spark, mockValueSets, Hierarchies.getDefault(spark));

    Assert.assertTrue(ImmutableSet.of("bp", "leukocytes", "priorities", "types")
        .containsAll(valueSets.getReferenceNames()));

    Map<String,Set<String>> leukocyteValues = valueSets.getValues("leukocytes");
    Map<String,Set<String>> priorityValues = valueSets.getValues("priorities");

    Assert.assertTrue(leukocyteValues.containsKey("http://loinc.org"));
    Assert.assertTrue(ImmutableSet.of("LP14419-3", "5821-4")
        .containsAll(leukocyteValues.get("http://loinc.org")));

    Assert.assertTrue(priorityValues.containsKey("http://hl7.org/fhir/v3/ActPriority"));
    Assert.assertTrue(ImmutableSet.of("EM")
        .containsAll(priorityValues.get("http://hl7.org/fhir/v3/ActPriority")));
  }

  /**
   * Added to address https://github.com/cerner/bunsen/issues/85
   */
  @Test
  public void testThreadSafety() {
    List<Future> futures = new ArrayList<>();
    final ExecutorService pool = Executors.newFixedThreadPool(5);

    for (int i = 0; i < 5; i++) {
      Future future = pool.submit(new BroadcastValueSetsThread(spark, i));
      futures.add(future);
    }

    pool.shutdown();

    boolean finished = false;

    try {
      finished = pool.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException exception) {
      throw new RuntimeException(exception);
    }

    if (!finished) {
      throw new RuntimeException("Timeout occurred while awaiting completion.");
    }

    futures.stream()
        .forEach(future -> {
          try {
            future.get();
          } catch (Exception exception) {
            throw new RuntimeException(exception);
          }
        });
  }

  protected class BroadcastValueSetsThread extends Thread {
    private final SparkSession session;
    private final int threadNum;

    protected BroadcastValueSetsThread(final SparkSession session, final int threadNum) {
      this.session = session.cloneSession();
      this.threadNum = threadNum;
    }

    @Override
    public void run() {
      String bp = "bp" + threadNum;
      String leukocytes = "leukocytes" + threadNum;
      String priorities = "priorities" + threadNum;

      BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
          .addCode(bp,
              Loinc.LOINC_CODE_SYSTEM_URI,
              "8462-4")
          .addDescendantsOf(leukocytes,
              Loinc.LOINC_CODE_SYSTEM_URI,
              "LP14419-3",
              Loinc.LOINC_HIERARCHY_URI)
          .addReference(priorities,
              "http://hl7.org/fhir/ValueSet/v3-ActPriority")
          .build(session, mockValueSets, Hierarchies.getDefault(session));

      Assert.assertNotNull(valueSets);

      Assert.assertTrue(ImmutableSet.of(bp, leukocytes, priorities, "types")
          .containsAll(valueSets.getReferenceNames()));

      Map<String,Set<String>> leukocyteValues = valueSets.getValues(leukocytes);
      Map<String,Set<String>> priorityValues = valueSets.getValues(priorities);

      Assert.assertTrue(leukocyteValues.containsKey("http://loinc.org"));
      Assert.assertTrue(ImmutableSet.of("LP14419-3", "5821-4")
          .containsAll(leukocyteValues.get("http://loinc.org")));

      Assert.assertTrue(priorityValues.containsKey("http://hl7.org/fhir/v3/ActPriority"));
      Assert.assertTrue(ImmutableSet.of("EM")
          .containsAll(priorityValues.get("http://hl7.org/fhir/v3/ActPriority")));
    }
  }
}
