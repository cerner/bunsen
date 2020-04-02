package com.cerner.bunsen.spark;

import ca.uhn.fhir.context.FhirContext;
import com.cerner.bunsen.FhirContexts;
import com.cerner.bunsen.stu3.TestData;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.generic.GenericData.Record;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Condition;
import org.hl7.fhir.dstu3.model.Extension;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Medication;
import org.hl7.fhir.dstu3.model.MedicationRequest;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.dstu3.model.Provenance;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.exceptions.FHIRException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkRowConverterTest {

  private static SparkSession spark;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    spark.stop();
  }

  static FhirContext fhirContext;

  private static final String BASE_VALUESET = "ValueSet";

  private static final Patient testPatient = TestData.newPatient();

  private static Dataset<Row> testPatientDataset;

  private static Patient testPatientDecoded;

  private static final Observation testObservation = TestData.newObservation();

  private static Dataset<Row> testObservationDataset;

  private static Observation testObservationDecoded;

  private static final Observation testObservationNullStatus = TestData.newObservation()
      .setStatus(Observation.ObservationStatus.NULL);

  private static Dataset<Row> testObservationNullStatusDataset;

  private static Observation testObservationDecodedNullStatus;

  private static final Condition testCondition = TestData.newCondition();

  private static Dataset<Row> testConditionDataset;

  private static Condition testConditionDecoded;

  private static final Medication testMedicationOne = TestData.newMedication("test-medication-1");

  private static final Medication testMedicationTwo = TestData.newMedication("test-medication-2");

  private static final Provenance testProvenance = TestData.newProvenance();

  private static final MedicationRequest testMedicationRequest =
      (MedicationRequest) TestData.newMedicationRequest()
          .addContained(testMedicationOne)
          .addContained(testProvenance)
          .addContained(testMedicationTwo);

  private static Dataset<Row> testMedicationRequestDataset;

  private static MedicationRequest testMedicationRequestDecoded;

  private static final Patient testBunsenTestProfilePatient = TestData
      .newBunsenTestProfilePatient();

  private static Dataset<Row> testBunsenTestProfilePatientDataset;

  private static Patient testBunsenTestProfilePatientDecoded;


  /**
   * Loads resource definitions used for testing.
   */
  @BeforeClass
  public static void loadDefinition() throws IOException {

    fhirContext = FhirContexts.forStu3();

    SparkRowConverter patientConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_PATIENT);

    Row testPatientRow = patientConverter.resourceToRow(testPatient);

    testPatientDataset = spark.createDataFrame(Collections.singletonList(testPatientRow),
        patientConverter.getSchema());

    testPatientDecoded = (Patient) patientConverter.rowToResource(testPatientDataset.head());

    SparkRowConverter observationConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_OBSERVATION);

    Row testObservationRow = observationConverter.resourceToRow(testObservation);

    testObservationDataset = spark.createDataFrame(Collections.singletonList(testObservationRow),
        observationConverter.getSchema());

    testObservationDecoded =
        (Observation) observationConverter.rowToResource(testObservationDataset.head());

    Row testObservationNullStatusRow = observationConverter
        .resourceToRow(testObservationNullStatus);

    testObservationNullStatusDataset = spark.createDataFrame(
        Collections.singletonList(testObservationNullStatusRow), observationConverter.getSchema());

    testObservationDecodedNullStatus = (Observation) observationConverter
        .rowToResource(testObservationNullStatusDataset.head());

    SparkRowConverter conditionConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_CONDITION);

    Row testConditionRow = conditionConverter.resourceToRow(testCondition);

    testConditionDataset = spark.createDataFrame(Collections.singletonList(testConditionRow),
        conditionConverter.getSchema());

    testConditionDecoded =
        (Condition) conditionConverter.rowToResource(testConditionDataset.head());

    SparkRowConverter medicationRequestConverter = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_MEDICATION_REQUEST,
        Arrays.asList(TestData.US_CORE_MEDICATION, TestData.PROVENANCE));

    Row testMedicationRequestRow = medicationRequestConverter.resourceToRow(testMedicationRequest);

    testMedicationRequestDataset = spark.createDataFrame(
        Collections.singletonList(testMedicationRequestRow),
        medicationRequestConverter.getSchema());

    testMedicationRequestDecoded = (MedicationRequest) medicationRequestConverter
        .rowToResource(testMedicationRequestDataset.head());

    SparkRowConverter converterBunsenTestProfilePatient = SparkRowConverter
        .forResource(FhirContexts.forStu3(), TestData.BUNSEN_TEST_PATIENT);

    Row testBunsenTestProfilePatientRow = converterBunsenTestProfilePatient
        .resourceToRow(testBunsenTestProfilePatient);

    testBunsenTestProfilePatientDataset = spark
        .createDataFrame(Collections.singletonList(testBunsenTestProfilePatientRow),
            converterBunsenTestProfilePatient.getSchema());

    testBunsenTestProfilePatientDecoded = (Patient) converterBunsenTestProfilePatient
        .rowToResource(testBunsenTestProfilePatientRow);
  }

  @Test
  public void testInteger() {

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
        testPatientDataset.select("multipleBirth.integer").head().get(0));

    Assert.assertEquals(((IntegerType) testPatient.getMultipleBirth()).getValue(),
        ((IntegerType) testPatientDecoded.getMultipleBirth()).getValue());
  }

  @Test
  public void testDecimal() {

    BigDecimal originalDecimal = ((Quantity) testObservation.getValue()).getValue();

    // Use compareTo since equals checks scale as well.
    Assert.assertTrue(originalDecimal.compareTo(
        (BigDecimal) testObservationDataset.select(
            "value.quantity.value").head().get(0)) == 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) testObservationDecoded
            .getValue())
            .getValue()), 0);
  }

  @Test
  public void testPrimitiveMultiplicity() {

    Assert.assertTrue(testPatient.getName().get(0).getFamily()
        .equalsIgnoreCase(testPatientDecoded.getName().get(0).getFamily()));
    Assert.assertTrue(testPatient.getName().get(0).getGiven().get(0).getValueAsString()
        .equals(testPatientDecoded.getName().get(0).getGiven().get(0).getValueAsString()));
    Assert.assertTrue(testPatient.getName().get(0).getGiven().get(1).getValueAsString()
        .equals(testPatientDecoded.getName().get(0).getGiven().get(1).getValueAsString()));
  }

  @Test
  public void testChoice() throws FHIRException {

    // Ensure that a decoded choice type matches the original
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(testPatientDecoded.getMultipleBirth()));
  }

  @Test
  public void testBoundCode() {

    Assert.assertEquals(testObservation.getStatus().toCode(),
        testObservationDataset.select("status").head().get(0));

    Assert.assertEquals(testObservation.getStatus(),
        testObservationDecoded.getStatus());
  }

  @Test
  public void testBoundCodeNull() {

    Assert.assertNull(testObservationNullStatusDataset.select("status").head().get(0));

    Assert.assertNull(testObservationDecodedNullStatus.getStatusElement().getValue());
  }

  @Test
  public void testCoding() {

    Coding testCoding = testCondition.getSeverity().getCodingFirstRep();
    Coding decodedCoding = testConditionDecoded.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields so we can easily select and compare individual fields.
    Dataset<Row> severityCodings = testConditionDataset
        .select(functions.explode(testConditionDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    Assert.assertEquals(testCoding.getCode(),
        severityCodings.select("code").head().get(0));
    Assert.assertEquals(testCoding.getCode(),
        decodedCoding.getCode());

    Assert.assertEquals(testCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    Assert.assertEquals(testCoding.getSystem(),
        decodedCoding.getSystem());

    Assert.assertEquals(testCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    Assert.assertEquals(testCoding.getUserSelected(),
        decodedCoding.getUserSelected());

    Assert.assertEquals(testCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    Assert.assertEquals(testCoding.getDisplay(),
        decodedCoding.getDisplay());
  }

  @Test
  public void testSingleReference() {

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDataset.select("subject.reference").head().get(0));

    Assert.assertEquals("12345", testConditionDataset
        .select("subject.patientId").head().get(0));

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testMultiReferenceTypes() {

    // Row containing the general practitioner from our dataset.
    Row practitioner = testPatientDataset
        .select(functions.explode(functions.col("generalpractitioner")))
        .select("col.organizationId", "col.practitionerId")
        .head();

    String organizationId = practitioner.getString(0);
    String practitionerId = practitioner.getString(1);

    // The reference is not of this type, so the field should be null.
    Assert.assertNull(organizationId);

    // The field with the expected prefix should match the original data.
    Assert.assertEquals(testPatient.getGeneralPractitionerFirstRep().getReference(),
        "Practitioner/" + practitionerId);

    Assert.assertEquals(testCondition.getSubject().getReference(),
        testConditionDecoded.getSubject().getReference());
  }

  @Test
  public void testNestedReference() {

    Identifier practitionerIdentifier =
        testPatient.getGeneralPractitionerFirstRep().getIdentifier();

    Row practitionerIdentifierRow = testPatientDataset
        .select(functions.explode(functions.col("generalpractitioner")))
        .select("col.organizationId", "col.practitionerId", "col.identifier.id",
            "col.identifier.assigner.reference")
        .head();

    Assert.assertEquals(practitionerIdentifier.getId(), practitionerIdentifierRow.get(2));
    Assert.assertEquals(practitionerIdentifier.getAssigner().getReference(),
        practitionerIdentifierRow.get(3));
  }

  @Test
  public void testSimpleExtension() {

    String testBirthSex = testPatient
        .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    String decodedBirthSex = testPatientDecoded
        .getExtensionsByUrl(TestData.US_CORE_BIRTHSEX)
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertEquals(testBirthSex, decodedBirthSex);

    Assert.assertEquals(testBirthSex,
        testPatientDataset.select("birthSex").head().get(0));
  }

  @Test
  public void testNestedExtension() {

    Extension testEthnicity = testPatient
        .getExtensionsByUrl(TestData.US_CORE_ETHNICITY)
        .get(0);

    Coding testOmbCategory = (Coding) testEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    String testText = testEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Extension decodedEthnicity = testPatientDecoded
        .getExtensionsByUrl(TestData.US_CORE_ETHNICITY)
        .get(0);

    Coding decodedOmbCategory = (Coding) decodedEthnicity
        .getExtensionsByUrl("ombCategory")
        .get(0)
        .getValue();

    String decodedText = decodedEthnicity
        .getExtensionsByUrl("text")
        .get(0)
        .getValueAsPrimitive()
        .getValueAsString();

    Assert.assertTrue(testOmbCategory.equalsDeep(decodedOmbCategory));
    Assert.assertEquals(testText, decodedText);

    Row ombCategoryRow = testPatientDataset.select(
        "ethnicity.ombcategory.system",
        "ethnicity.ombcategory.code",
        "ethnicity.ombcategory.display")
        .head();

    Assert.assertEquals(testOmbCategory.getSystem(), ombCategoryRow.get(0));
    Assert.assertEquals(testOmbCategory.getCode(), ombCategoryRow.get(1));
    Assert.assertEquals(testOmbCategory.getDisplay(), ombCategoryRow.get(2));

    Row textRow = testPatientDataset.select("ethnicity.text").head();

    Assert.assertEquals(testText, textRow.get(0));
  }

  @Test
  public void testContainedResources() throws FHIRException {

    Medication testMedicationOne = (Medication) testMedicationRequest.getContained().get(0);
    String testMedicationOneId = testMedicationOne.getId();
    CodeableConcept testMedicationIngredientItem = testMedicationOne.getIngredientFirstRep()
        .getItemCodeableConcept();

    Medication decodedMedicationOne = (Medication) testMedicationRequestDecoded.getContained()
        .get(0);
    String decodedMedicationOneId = decodedMedicationOne.getId();
    CodeableConcept decodedMedicationOneIngredientItem = decodedMedicationOne
        .getIngredientFirstRep()
        .getItemCodeableConcept();

    Assert.assertEquals(testMedicationOneId, decodedMedicationOneId);
    Assert.assertTrue(decodedMedicationOneIngredientItem.equalsDeep(testMedicationIngredientItem));

    Provenance testProvenance = (Provenance) testMedicationRequest.getContained().get(1);
    String testProvenanceId = testProvenance.getId();

    Provenance decodedProvenance = (Provenance) testMedicationRequestDecoded.getContained().get(1);
    String decodedProvenanceId = decodedProvenance.getId();

    Assert.assertEquals(testProvenanceId, decodedProvenanceId);

    Medication testMedicationTwo = (Medication) testMedicationRequest.getContained().get(2);
    String testMedicationTwoId = testMedicationTwo.getId();

    Medication decodedMedicationTwo = (Medication) testMedicationRequestDecoded.getContained()
        .get(2);
    String decodedMedicationTwoId = decodedMedicationTwo.getId();

    Assert.assertEquals(testMedicationTwoId, decodedMedicationTwoId);
  }

  /**
   * Recursively walks the schema to ensure there are no struct fields that are empty.
   */
  private void checkNoEmptyStructs(StructType schema, String fieldName) {

    Assert.assertNotEquals("Struct field " + fieldName + " is empty",
        0,
        schema.fields().length);

    for (StructField field : schema.fields()) {

      if (field.dataType() instanceof StructType) {

        checkNoEmptyStructs((StructType) field.dataType(), field.name());

      } else if (field.dataType() instanceof ArrayType) {

        ArrayType arrayType = (ArrayType) field.dataType();

        if (arrayType.elementType() instanceof StructType) {

          if (!field.name().equals("contained")) {

            checkNoEmptyStructs((StructType) arrayType.elementType(), field.name());
          }
        }
      }
    }
  }

  @Test
  public void testContentReferenceField() {

    // Fields may be a reference to a type defined elsewhere. Make sure they are populated.
    StructType schema = SparkRowConverter.forResource(fhirContext,
        TestData.US_CORE_OBSERVATION).getSchema();

    checkNoEmptyStructs(schema, null);
  }

  @Test
  public void testRecursiveStructure() {

    // ValueSet has a recursive structure, ensure it terminates without an empty struct.
    StructType schema = SparkRowConverter.forResource(fhirContext, "ValueSet").getSchema();

    checkNoEmptyStructs(schema, null);
  }

  @Test
  public void testDecodeWithDifferentProfile() {

    // Decoding with the base profile should still produce the expected fields.
    SparkRowConverter patientConverter = SparkRowConverter.forResource(fhirContext,
        "Patient");

    Patient basePatientDecoded = (Patient) patientConverter
        .rowToResource(testPatientDataset.head());

    Assert.assertEquals(testPatient.getId(), basePatientDecoded.getId());
    Assert.assertEquals(testPatient.getGender(), basePatientDecoded.getGender());
    Assert.assertTrue(testPatient.getMultipleBirth()
        .equalsDeep(basePatientDecoded.getMultipleBirth()));
  }

  @Test
  public void testSimpleExtensionWithBooleanField() {

    Boolean expected = (Boolean) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Boolean actual = testBunsenTestProfilePatientDataset.select("booleanfield").head()
        .getBoolean(0);
    Assert.assertEquals(expected, actual);

    Boolean decodedBooleanField = (Boolean) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_BOOLEAN_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedBooleanField);
  }

  @Test
  public void testSimpleExtensionWithIntegerField() {

    Integer expected = (Integer) testBunsenTestProfilePatient
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Integer actual = testBunsenTestProfilePatientDataset.select("integerfield").head().getInt(0);
    Assert.assertEquals(expected, actual);

    Integer decodedIntegerField = (Integer) testBunsenTestProfilePatientDecoded
        .getExtensionsByUrl(TestData.BUNSEN_TEST_INTEGER_FIELD)
        .get(0)
        .getValueAsPrimitive().getValue();

    Assert.assertEquals(expected, decodedIntegerField);
  }
}
