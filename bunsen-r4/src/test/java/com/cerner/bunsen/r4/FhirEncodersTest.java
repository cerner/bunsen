package com.cerner.bunsen.r4;

import com.cerner.bunsen.FhirEncoders;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Annotation;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for FHIR encoders.
 */
public class FhirEncodersTest {

  private static final FhirEncoders encoders =
      FhirEncoders.forR4().getOrCreate();
  private static SparkSession spark;
  private static Patient patient = TestData.newPatient();
  private static Dataset<Patient> patientDataset;
  private static Patient decodedPatient;

  private static Condition condition = TestData.newCondition();
  private static Dataset<Condition> conditionsDataset;
  private static Condition decodedCondition;

  private static Observation observation = TestData.newObservation();
  private static Dataset<Observation> observationsDataset;
  private static Observation decodedObservation;

  private static MedicationRequest medRequest = TestData.newMedRequest();
  private static Dataset<MedicationRequest> medDataset;
  private static MedicationRequest decodedMedRequest;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();

    patientDataset = spark.createDataset(ImmutableList.of(patient),
        encoders.of(Patient.class));
    decodedPatient = patientDataset.head();

    conditionsDataset = spark.createDataset(ImmutableList.of(condition),
        encoders.of(Condition.class));
    decodedCondition = conditionsDataset.head();

    observationsDataset = spark.createDataset(ImmutableList.of(observation),
        encoders.of(Observation.class));
    decodedObservation = observationsDataset.head();

    medDataset = spark.createDataset(ImmutableList.of(medRequest),
        encoders.of(MedicationRequest.class));

    decodedMedRequest = medDataset.head();
  }

  /**
   * Tear down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testResourceId() {

    Assert.assertEquals(condition.getId(),
        conditionsDataset.select("id").head().get(0));
    Assert.assertEquals(condition.getId(),
        decodedCondition.getId());
  }

  @Test
  public void testResourceLanguage() {

    Assert.assertEquals(condition.getLanguage(),
        conditionsDataset.select("language").head().get(0));
    Assert.assertEquals(condition.getLanguage(),
        decodedCondition.getLanguage());
  }


  @Test
  public void boundCode() {

    Assert.assertEquals(condition.getVerificationStatus().toCode(),
        conditionsDataset.select("verificationStatus").head().get(0));
    Assert.assertEquals(condition.getVerificationStatus(),
        decodedCondition.getVerificationStatus());
  }

  @Test
  public void choiceValue() {

    // Our test condition uses the DateTime choice, so use that type and column.
    Assert.assertEquals(((DateTimeType) condition.getOnset()).getValueAsString(),
        conditionsDataset.select("onsetDateTime").head().get(0));

    Assert.assertEquals(condition.getOnset().toString(),
        decodedCondition.getOnset().toString());
  }

  @Test
  public void narrative() {

    Assert.assertEquals(condition.getText().getStatus().toCode(),
        conditionsDataset.select("text.status").head().get(0));
    Assert.assertEquals(condition.getText().getStatus(),
        decodedCondition.getText().getStatus());

    Assert.assertEquals(condition.getText().getDivAsString(),
        conditionsDataset.select("text.div").head().get(0));
    Assert.assertEquals(condition.getText().getDivAsString(),
        decodedCondition.getText().getDivAsString());
  }

  @Test
  public void coding() {

    Coding expectedCoding = condition.getSeverity().getCodingFirstRep();
    Coding actualCoding = decodedCondition.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields so we can easily select and compare individual fields.
    Dataset<Row> severityCodings = conditionsDataset
        .select(functions.explode(conditionsDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    Assert.assertEquals(expectedCoding.getCode(),
        severityCodings.select("code").head().get(0));
    Assert.assertEquals(expectedCoding.getCode(),
        actualCoding.getCode());

    Assert.assertEquals(expectedCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    Assert.assertEquals(expectedCoding.getSystem(),
        actualCoding.getSystem());

    Assert.assertEquals(expectedCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    Assert.assertEquals(expectedCoding.getUserSelected(),
        actualCoding.getUserSelected());

    Assert.assertEquals(expectedCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    Assert.assertEquals(expectedCoding.getDisplay(),
        actualCoding.getDisplay());
  }

  @Test
  public void reference() {

    Assert.assertEquals(condition.getSubject().getReference(),
        conditionsDataset.select("subject.reference").head().get(0));
    Assert.assertEquals(condition.getSubject().getReference(),
        decodedCondition.getSubject().getReference());
  }

  @Test
  public void integer() {

    Assert.assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        patientDataset.select("multipleBirthInteger").head().get(0));
    Assert.assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        ((IntegerType) decodedPatient.getMultipleBirth()).getValue());
  }

  @Test
  public void bigDecimal() {

    BigDecimal originalDecimal = ((Quantity) observation.getValue()).getValue();

    // Use compareTo since equals checks scale as well.
    Assert.assertTrue(originalDecimal.compareTo(
        (BigDecimal) observationsDataset.select("valueQuantity.value")
            .head()
            .get(0)) == 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) decodedObservation
            .getValue())
            .getValue()), 0);
  }

  @Test
  public void annotation() throws FHIRException {

    Annotation original = medRequest.getNoteFirstRep();
    Annotation decoded = decodedMedRequest.getNoteFirstRep();

    Assert.assertEquals(original.getText(),
        medDataset.select(functions.expr("note[0].text")).head().get(0));

    Assert.assertEquals(original.getText(), decoded.getText());
    Assert.assertEquals(original.getAuthorReference().getReference(),
        decoded.getAuthorReference().getReference());

  }

  /**
   * Sanity test with a deep copy to check we didn't break internal state used by copies.
   */
  @Test
  public void testCopyDecoded() {
    Assert.assertEquals(condition.getId(), decodedCondition.copy().getId());
    Assert.assertEquals(medRequest.getId(), decodedMedRequest.copy().getId());
    Assert.assertEquals(observation.getId(), decodedObservation.copy().getId());
    Assert.assertEquals(patient.getId(), decodedPatient.copy().getId());
  }

  @Test
  public void testEmptyAttributes() {
    Map<String, String> attributes = decodedMedRequest.getText().getDiv().getAttributes();

    Assert.assertNotNull(attributes);
    Assert.assertEquals(0, attributes.size());
  }

  @Test
  public void testFromRdd() {

    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

    JavaRDD<Condition> conditionRdd = context.parallelize(ImmutableList.of(condition));

    Dataset<Condition> ds = spark.createDataset(conditionRdd.rdd(),
        encoders.of(Condition.class));

    Condition convertedCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        convertedCondition.getId());
  }

  @Test
  public void testFromParquet() throws IOException {

    Path dirPath = Files.createTempDirectory("encoder_test");

    String path = dirPath.resolve("out.parquet").toString();

    conditionsDataset.write().save(path);

    Dataset<Condition> ds = spark.read()
        .parquet(path)
        .as(encoders.of(Condition.class));

    Condition readCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        readCondition.getId());
  }

  @Test
  public void testEncoderCached() throws IOException {

    Assert.assertSame(encoders.of(Condition.class),
        encoders.of(Condition.class));

    Assert.assertSame(encoders.of(Patient.class),
        encoders.of(Patient.class));
  }
}
